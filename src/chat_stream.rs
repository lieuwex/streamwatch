use super::types::StreamInfo;
use super::DB;

use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};

use tokio;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

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
    content: Box<RawValue>, // lazy response so we don't have to parse the json blob
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Response {
    session_token: Uuid,
    res: Vec<Item>,
}

struct FileReader {
    orphan: Option<(DateTime<Utc>, Box<RawValue>)>,
    prev_datetime: DateTime<Utc>,
    stream: StreamInfo,
    lines: Lines<BufReader<ZstdDecoder<BufReader<File>>>>,
}

impl FileReader {
    async fn create_lines(stream: &StreamInfo) -> Lines<BufReader<ZstdDecoder<BufReader<File>>>> {
        let f = File::open(stream.chat_file_path()).await.unwrap();
        let reader = BufReader::new(ZstdDecoder::new(BufReader::new(f)));
        reader.lines()
    }

    fn parse_line(line: &str) -> Result<(DateTime<Utc>, &str), String> {
        let splitted: Vec<_> = line.splitn(2, ' ').collect();

        let date = match DateTime::parse_from_rfc3339(splitted[0]) {
            Err(e) => return Err(e.to_string()),
            Ok(d) => d.with_timezone(&Utc),
        };

        Ok((date, splitted[1]))
    }

    async fn new(stream: StreamInfo) -> Self {
        let lines = Self::create_lines(&stream).await;

        Self {
            orphan: None,
            prev_datetime: stream.datetime(),
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
                let json = RawValue::from_string(json.to_string()).unwrap();
                self.orphan = Some((datetime, json));
                break;
            }

            self.prev_datetime = datetime;

            // loop until we reached the starting point
            if datetime < start {
                continue;
            }

            let json = RawValue::from_string(json.to_string()).unwrap();
            res.push(Item {
                ts: datetime.timestamp_millis() as usize,
                content: json,
            });
        }

        res
    }
}

struct CacheItem {
    stream_id: i64,
    last_access: DateTime<Utc>,
    /// `FileReader`, if there is one. If this is `None` it means that the stream does not have a
    /// chat file.
    file_reader: Option<FileReader>,
}

static CACHE: Lazy<Arc<Mutex<HashMap<Uuid, CacheItem>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

pub async fn cache_pruner() {
    let dur = std::time::Duration::from_secs(60 * 10);

    loop {
        let removed_items: Vec<_> = CACHE
            .lock()
            .await
            .drain_filter(|_, v| v.last_access < (Utc::now() - Duration::minutes(10)))
            .collect();

        let n_removed = removed_items.len();
        if n_removed > 0 {
            let s: String = removed_items
                .into_iter()
                .map(|(k, v)| format!("{} ({})", k, v.stream_id))
                .intersperse(", ".to_string())
                .collect();

            println!("pruned {} key(s): {}", n_removed, s);
        }

        tokio::time::sleep(dur).await;
    }
}

pub async fn handle_chat_request(
    stream_id: i64,
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
        let mut map = CACHE.lock().await;
        let mut entry = map.entry(session_token);

        let file_reader = match entry {
            Entry::Occupied(ref mut entry) => {
                println!("cache hit for {} ({})", session_token, stream_id);

                let entry = entry.get_mut();
                entry.last_access = Utc::now();
                &mut entry.file_reader
            }
            Entry::Vacant(entry) => {
                println!("cache miss for {} ({})", session_token, stream_id);

                let stream = match {
                    let db = DB.get().unwrap();
                    let db = db.lock().unwrap();
                    db.get_streams().into_iter().find(|s| s.id == stream_id)
                } {
                    None => return Err(warp::reject::not_found()),
                    Some(s) => s,
                };

                let file_reader = if stream.has_chat {
                    Some(FileReader::new(stream).await)
                } else {
                    None
                };

                &mut entry
                    .insert(CacheItem {
                        stream_id,
                        last_access: Utc::now(),
                        file_reader,
                    })
                    .file_reader
            }
        };

        match file_reader {
            Some(reader) => reader.get_between(start, end).await,
            None => vec![],
        }
    };

    let s = serde_json::to_string(&Response {
        session_token,
        res: messages,
    })
    .unwrap();
    Ok(s)
}
