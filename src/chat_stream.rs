use super::types::StreamInfo;
use super::DB;

use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;

use futures::{SinkExt, StreamExt};

use warp::ws::{WebSocket, Ws};
use warp::Reply;

use chrono::{DateTime, Duration, FixedOffset, Local, TimeZone, Utc};

use tokio;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};

use async_compression::tokio::bufread::ZstdDecoder;

use once_cell::sync::Lazy;

use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    session_token: Option<Uuid>,
    start: i64, // milliseconds UTC timestamp
    end: i64,   // milliseconds UTC timestamp
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Item {
    ts: usize,
    content: String, // lazy response so we don't have to parse the json blob
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Response {
    session_token: Uuid,
    res: Vec<Item>,
}

struct FileReader {
    orphan: Option<(DateTime<FixedOffset>, String)>,
    prev_datetime: DateTime<FixedOffset>,
    stream: StreamInfo,
    lines: Lines<BufReader<ZstdDecoder<BufReader<File>>>>,
}

impl FileReader {
    async fn create_lines(stream: &StreamInfo) -> Lines<BufReader<ZstdDecoder<BufReader<File>>>> {
        let f = File::open(stream.chat_file_path()).await.unwrap();
        let reader = BufReader::new(ZstdDecoder::new(BufReader::new(f)));
        reader.lines()
    }

    fn parse_line(line: &str) -> Result<(DateTime<FixedOffset>, &str), String> {
        let splitted: Vec<_> = line.splitn(2, ' ').collect();

        let date = match DateTime::parse_from_rfc3339(splitted[0]) {
            Err(e) => return Err(e.to_string()),
            Ok(d) => d,
        };

        Ok((date, splitted[1]))
    }

    async fn new(stream: StreamInfo) -> Self {
        let local_ts = Local.timestamp(0, 0);
        let tz_offset = local_ts.offset();

        let lines = Self::create_lines(&stream).await;

        Self {
            orphan: None,
            prev_datetime: DateTime::from_utc(stream.datetime() - Duration::hours(1), *tz_offset),
            stream,
            lines,
        }
    }

    async fn get_between(&mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<Item> {
        // we are going back to the past, so we have to repon the file to seek to the file start.
        if self.prev_datetime > start {
            eprintln!(
                "!!! seeking back prev_datetime={} start={}",
                self.prev_datetime, start
            );

            self.lines = Self::create_lines(&self.stream).await;
        }

        let mut res = Vec::new();

        if let Some((datetime, json)) = self.orphan.take() {
            if start <= datetime {
                self.prev_datetime = datetime;
                res.push(Item {
                    ts: datetime.timestamp_millis() as usize,
                    content: json,
                });
            }
        }

        while let Some(line) = self.lines.next_line().await.unwrap() {
            let (datetime, json) = Self::parse_line(&line).unwrap();

            if datetime > end {
                self.orphan = Some((datetime, json.to_string()));
                break;
            }

            self.prev_datetime = datetime;

            // loop until we reached the starting point
            if datetime < start {
                continue;
            }

            res.push(Item {
                ts: datetime.timestamp_millis() as usize,
                content: json.to_string(),
            });
        }

        res
    }
}

struct CacheItem {
    date: DateTime<Utc>,
    file_reader: FileReader,
}

static CACHE: Lazy<Arc<Mutex<HashMap<Uuid, CacheItem>>>> = Lazy::new(|| {
    let map = HashMap::new();
    Arc::new(Mutex::new(map))
});

pub async fn handle_chat_request(
    stream_id: u64,
    request: Request,
) -> Result<String, warp::Rejection> {
    let (session_token, start, end) = {
        let start = Utc.timestamp_millis(request.start);
        let end = Utc.timestamp_millis(request.end);

        let session_token = request.session_token.unwrap_or_else(|| Uuid::new_v4());

        (session_token, start, end)
    };

    // TODO: we're doing some kind of immutable acces here, which means we should be able to
    // parallise the locking here and do something high perf and cool.
    let messages: Vec<Item> = {
        // get lock on the map, and take the item from it
        let mut map = CACHE.lock().await;
        let entry = map.remove(&session_token);

        // if the item is present and not yet expired, return the item.  Otherwise return None.
        let item = entry.and_then(|mut entry| {
            let now = Utc::now();

            if entry.date < (now - Duration::minutes(10)) {
                None
            } else {
                entry.date = now;
                Some(entry)
            }
        });

        // generate a new reader, and item if needed.  Otherwise, return get old reader.
        let file_reader = if let Some(item) = item {
            println!("cache hit for: {} ({})", stream_id, session_token);
            &mut map
                .entry(session_token.clone())
                .or_insert(CacheItem {
                    date: Utc::now(),
                    file_reader: item.file_reader,
                })
                .file_reader
        } else {
            println!("cache miss for: {} ({})", stream_id, session_token);

            let stream = {
                let db = DB.get().unwrap();
                let db = db.lock().unwrap();
                db.get_streams()
                    .into_iter()
                    .find(|s| s.id == stream_id as i64)
                    .unwrap()
            };
            if !stream.has_chat {
                return Err(warp::reject::not_found());
            }

            let reader = FileReader::new(stream).await;
            &mut map
                .entry(session_token.clone())
                .or_insert(CacheItem {
                    date: Utc::now(),
                    file_reader: reader,
                })
                .file_reader
        };

        file_reader.get_between(start, end).await
    };

    let s = serde_json::to_string(&Response {
        session_token,
        res: messages,
    })
    .unwrap();
    Ok(s)
}
