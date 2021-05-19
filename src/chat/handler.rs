use super::file_reader::FileReader;
use super::types::Item;
use crate::DB;

use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};

use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};

use once_cell::sync::Lazy;

use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    session_token: Option<Uuid>,
    start: i64, // milliseconds UTC timestamp
    end: i64,   // milliseconds UTC timestamp
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Response {
    session_token: Uuid,
    res: Vec<Item>,
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
) -> Result<warp::reply::Json, warp::Rejection> {
    let start = Utc.timestamp_millis(request.start);
    let end = Utc.timestamp_millis(request.end);
    let session_token = request.session_token.unwrap_or_else(Uuid::new_v4);

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
                    let mut db = db.lock().await;
                    db.get_streams()
                        .await
                        .into_iter()
                        .find(|s| s.id == stream_id)
                } {
                    None => return Err(warp::reject::not_found()),
                    Some(s) => s,
                };

                let file_reader = if stream.has_chat {
                    Some(FileReader::new(stream).await.unwrap())
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
            Some(reader) => reader.get_between(start, end).await.unwrap(),
            None => vec![],
        }
    };

    Ok(warp::reply::json(&Response {
        session_token,
        res: messages,
    }))
}