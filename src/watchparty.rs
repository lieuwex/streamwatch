use std::collections::HashMap;
use std::sync::Mutex;

use log::debug;
use once_cell::sync::Lazy;

use futures::{SinkExt, StreamExt};

use tokio::sync::broadcast::{self, error::RecvError};

use futures::future;

use warp::{
    ws::{self, Ws},
    Reply,
};

use serde::{Deserialize, Serialize};

struct PartyInfo {
    id: String,
    tx: broadcast::Sender<ws::Message>,
}

impl PartyInfo {
    fn get_pair(
        &self,
    ) -> (
        broadcast::Sender<ws::Message>,
        broadcast::Receiver<ws::Message>,
    ) {
        (self.tx.clone(), self.tx.subscribe())
    }

    fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

static PARTIES: Lazy<Mutex<HashMap<String, PartyInfo>>> = Lazy::new(|| {
    let map = HashMap::new();
    Mutex::new(map)
});

/// Prune the parties list and remove parties that have no receivers in them.
fn prune_parties() -> usize {
    let mut parties = PARTIES.lock().unwrap();
    parties.drain_filter(|_, v| v.receiver_count() == 0).count()
}

#[derive(Clone, Debug, Deserialize)]
pub struct Request {
    party_id: String,
}

pub fn watch_party_ws(
    request: Request,
    ws: Ws,
) -> future::Ready<Result<warp::reply::Response, warp::Rejection>> {
    let (tx, mut rx) = {
        let id = request.party_id.clone();

        let mut parties = PARTIES.lock().unwrap();
        let party = parties.entry(id.clone()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(16);
            PartyInfo { id, tx }
        });

        party.get_pair()
    };

    let res = ws.on_upgrade(|ws| async move {
        let (mut ws_sink, mut ws_stream) = ws.split();

        let ws_sink_task = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Err(RecvError::Closed) => {
                        // no senders left in party, this should actually not happen
                        eprintln!("no senders left in watchparty, this should not happen?");
                        break;
                    }
                    Err(RecvError::Lagged(n_skipped)) => {
                        eprintln!(
                            "[{}] ignoring {} skipped messages",
                            request.party_id, n_skipped
                        )
                    }
                    Ok(msg) => {
                        if let Err(e) = ws_sink.send(msg).await {
                            // websocket is closed
                            debug!("websocket is closed: {}", e);
                            break;
                        }
                    }
                };
            }
        });

        while let Some(msg) = ws_stream.next().await {
            let msg = msg.unwrap();

            // An error here means that no receivers received our message, since no were available.
            // This is fine, so just ignore the result.
            let _ = tx.send(msg);
        }
        drop(tx);

        // some debugging cruft
        debug!("ws_stream closed closed, waiting for the ws_sink task to end...");
        ws_sink_task.await.unwrap();
        debug!("ws_sink task ended");

        prune_parties();
    });
    future::ok(res.into_response())
}

pub async fn get_watch_parties() -> Result<warp::reply::Json, warp::Rejection> {
    let parties = PARTIES.lock().unwrap();
    let m: HashMap<String, usize> = parties
        .iter()
        .map(|(k, v)| (k.to_owned(), v.receiver_count()))
        .collect();
    Ok(warp::reply::json(&m))
}

/*
#[derive(Serialize, Deserialize)]
struct Message {
    id: u64,
    command: String,
    parameters: Vec<String>,
}

pub fn watch_party(party_id: Option<String>, ws: Ws) {
    ws.on_upgrade(|ws| async {
        let mut id = 0;

        let send = |command: String, parameters: Vec<String>| {
            id += 1;

            let msg = Message {
                id: id - 1,
                command,
                parameters,
            };
            let s = serde_json::to_string(&msg).unwrap();

            ws.send(ws::Message::text(s))
        };

        let reply = |id: u64, command: String, parameters: Vec<String>| {
            let msg = Message {
                id,
                command,
                parameters,
            };
            let s = serde_json::to_string(&msg).unwrap();

            ws.send(ws::Message::text(s))
        };

        tokio::spawn(async {
            while let Some(item) = ws.next().await {
                let item = match item {
                    Err(e) => {
                        eprintln!("error: {}", e);
                        continue;
                    }
                    Ok(i) => i,
                };

                let s = item.to_str().unwrap();
                let msg: Message = match serde_json::from_str(s) {
                    Err(e) => {
                        eprintln!("error: {}", e);
                        continue;
                    }
                    Ok(m) => m,
                };

                match &msg.command {
                    _ => {
                        eprintln!("unknown command received: {}", msg.command);
                    }
                }
            }
        });
    });
}
*/
