use super::db;
use super::file_reader::FileReader;
use super::types::Item;
use crate::{
    check,
    util::{merge, AnyhowError},
    DB, STREAMS_DIR,
};

use std::collections::hash_map::{Entry, HashMap};

use chrono::{serde::ts_milliseconds, DateTime, Duration, Utc};

use tokio::sync::Mutex;

use serde::{Deserialize, Serialize};

use once_cell::sync::Lazy;

use uuid::Uuid;

#[derive(Clone, Debug, Deserialize)]
pub struct Request {
    session_token: Option<Uuid>,
    #[serde(with = "ts_milliseconds")]
    start: DateTime<Utc>,
    #[serde(with = "ts_milliseconds")]
    end: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
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

static CACHE: Lazy<Mutex<HashMap<Uuid, CacheItem>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub async fn cache_pruner() {
    const SLEEP_DURATION: std::time::Duration = std::time::Duration::from_secs(60 * 10);
    let expiration_duration = Duration::minutes(10);

    loop {
        let removed_items: Vec<_> = CACHE
            .lock()
            .await
            .drain_filter(|_, v| v.last_access < (Utc::now() - expiration_duration))
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

        tokio::time::sleep(SLEEP_DURATION).await;
    }
}

pub async fn handle_chat_request(
    stream_id: i64,
    request: Request,
) -> Result<warp::reply::Json, warp::Rejection> {
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
                    check!(db.get_stream_by_id(stream_id).await)
                } {
                    None => return Err(warp::reject::not_found()),
                    Some(s) => s,
                };

                let file_reader = if check!(stream.info.file_name.has_chat(STREAMS_DIR).await) {
                    Some(check!(FileReader::new(stream.info).await))
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

        let file_messages = match file_reader {
            Some(reader) => check!(reader.get_between(request.start, request.end).await),
            None => vec![],
        };
        //let db_messages = check!(db::get_messages(stream_id, request.start, request.end).await);
        //merge(file_messages, db_messages, |x| x.ts).unwrap()
        file_messages
    };

    Ok(warp::reply::json(&Response {
        session_token,
        res: messages,
    }))
}
