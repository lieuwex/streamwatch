use super::DB;

use futures::{SinkExt, StreamExt};

use warp::ws::{WebSocket, Ws};
use warp::Reply;

use chrono::{DateTime, Duration, FixedOffset, Local, TimeZone, Utc};

use tokio;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

use serde::{Deserialize, Serialize};

use async_compression::tokio::bufread::ZstdDecoder;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WebSocketRequest {
    id: usize,
    start: i64, // milliseconds UTC timestamp
    end: i64,   // milliseconds UTC timestamp
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Item {
    ts: usize,
    content: String, // lazy response so we don't have to parse the json blob
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WebSocketResponse {
    id: usize,
    res: Vec<Item>,
}

fn parse_line(line: &str) -> Result<(DateTime<FixedOffset>, &str), String> {
    let splitted: Vec<_> = line.splitn(2, ' ').collect();

    let date = match DateTime::parse_from_rfc3339(splitted[0]) {
        Err(e) => return Err(e.to_string()),
        Ok(d) => d,
    };

    Ok((date, splitted[1]))
}

async fn websocket_handler(stream_id: u64, mut ws: WebSocket) {
    let stream = {
        let db = DB.get().unwrap();
        let db = db.lock().unwrap();
        db.get_streams()
            .into_iter()
            .find(|s| s.id == stream_id as i64)
            .unwrap()
    };
    if !stream.has_chat {
        return;
    }

    let chat_path = stream.chat_file_path();

    // get the stream start date
    let local_ts = Local.timestamp(0, 0);
    let tz_offset = local_ts.offset();
    let mut prev_datetime: DateTime<FixedOffset> =
        DateTime::from_utc(stream.datetime() - Duration::hours(1), *tz_offset);

    // file state
    let f = File::open(chat_path.clone()).await.unwrap();
    let mut reader = BufReader::new(ZstdDecoder::new(BufReader::new(f)));
    let mut lines = reader.lines();

    let mut orphan: Option<(DateTime<_>, String)> = None;
    while let Some(message) = ws.next().await {
        let (id, start, end) = {
            let message = match message {
                Err(_) => return,
                Ok(m) => m,
            };

            let message = match message.to_str() {
                Err(_) => return,
                Ok(m) => m,
            };
            let request: WebSocketRequest = serde_json::from_str(message).unwrap();

            let start = Utc.timestamp_millis(request.start);
            let end = Utc.timestamp_millis(request.end);

            (request.id, start, end)
        };

        // we are going back to the past, so we have to repon the file to seek to the file start.
        if prev_datetime > start {
            eprintln!(
                "!!! seeking back prev_datetime={} start={}",
                prev_datetime, start
            );

            let f = File::open(chat_path.clone()).await.unwrap();
            reader = BufReader::new(ZstdDecoder::new(BufReader::new(f)));

            lines = reader.lines();
        }

        let mut res = Vec::new();

        if let Some((datetime, json)) = orphan.take() {
            if start <= datetime {
                prev_datetime = datetime;
                res.push(Item {
                    ts: datetime.timestamp_millis() as usize,
                    content: json,
                });
            }
        }

        while let Some(line) = lines.next_line().await.unwrap() {
            let (datetime, json) = parse_line(&line).unwrap();

            if datetime > end {
                orphan = Some((datetime, json.to_string()));
                break;
            }

            prev_datetime = datetime;

            // loop until we reached the starting point
            if datetime < start {
                continue;
            }

            res.push(Item {
                ts: datetime.timestamp_millis() as usize,
                content: json.to_string(),
            });
        }

        let s = serde_json::to_string(&WebSocketResponse { id, res }).unwrap();
        if ws.send(warp::ws::Message::text(s)).await.is_err() {
            return;
        }
    }
}

pub fn handle_ws(stream_id: u64, ws: Ws) -> impl Reply {
    ws.on_upgrade(move |ws| websocket_handler(stream_id, ws))
}
