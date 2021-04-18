use super::types::StreamInfo;
use super::DB;

use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;

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
    /// `FileReader`, if there is one. If this is `None` it means that the stream does not have a
    /// chat file.
    file_reader: Option<FileReader>,
}

static CACHE: Lazy<Arc<Mutex<HashMap<Uuid, CacheItem>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

pub async fn cache_pruner() {
    let dur = std::time::Duration::from_secs(60 * 10);

    loop {
        let now = Utc::now();

        {
            let mut map = CACHE.lock().await;

            let stale_keys: Vec<_> = map
                .iter()
                .filter(|(_, v)| v.date < (now - Duration::minutes(10)))
                .map(|(k, _)| k.to_owned())
                .collect();

            for key in &stale_keys {
                map.remove(key);
            }

            let n_removed = stale_keys.len();
            if n_removed > 0 {
                println!("pruned {} key(s)", n_removed);
            }
        }

        tokio::time::sleep(dur.clone()).await;
    }
}

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
        let mut map = CACHE.lock().await;
        let mut entry = map.entry(session_token);

        let file_reader = match entry {
            Entry::Occupied(ref mut entry) => {
                println!("cache hit for {} ({})", stream_id, session_token);

                let entry = entry.get_mut();
                (*entry).date = Utc::now();
                &mut entry.file_reader
            }
            Entry::Vacant(entry) => {
                println!("cache miss for {} ({})", stream_id, session_token);

                let stream = match {
                    let db = DB.get().unwrap();
                    let db = db.lock().unwrap();
                    db.get_streams()
                        .into_iter()
                        .find(|s| s.id == stream_id as i64)
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
                        date: Utc::now(),
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
