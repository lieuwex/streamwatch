use crate::create_preview::get_video_duration_in_secs;
use crate::job_handler::{Job, SENDER};
use crate::types::{GameInfo, GameItem, StreamFileName, StreamInfo};
use crate::{DB, STREAMS_DIR};

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::Path;

use tokio::fs::{read_dir, remove_dir_all, remove_file};
use tokio_stream::wrappers::ReadDirStream;

use futures::stream::iter;
use futures::StreamExt;

use once_cell::sync::Lazy;

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone};

use sqlx::Connection;

use regex::Regex;

static FILE_STEM_REGEX_DATETIME: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap());
static FILE_STEM_REGEX_DATE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap());

macro_rules! log_err {
    ($item:expr) => {
        match $item {
            Ok(_) => {}
            Err(e) => eprintln!("error: {}", e),
        }
    };
}

pub async fn remove_thumbnails_and_preview(stream_id: i64) {
    {
        let db = DB.get().unwrap();
        let mut db = db.lock().await;

        let mut tx = db.conn.begin().await.unwrap();

        // remove preview
        sqlx::query!(
            "DELETE FROM stream_previews WHERE stream_id = ?1",
            stream_id,
        )
        .execute(&mut tx)
        .await
        .unwrap();
        // remove thumbnails
        sqlx::query!(
            "DELETE FROM stream_thumbnails WHERE stream_id = ?1",
            stream_id,
        )
        .execute(&mut tx)
        .await
        .unwrap();

        tx.commit().await.unwrap();
    }

    log_err!(remove_file(StreamInfo::preview_path(stream_id)).await);
    log_err!(remove_dir_all(StreamInfo::thumbnails_path(stream_id)).await);
}

fn parse_filename(path: &Path) -> Option<DateTime<Local>> {
    let stem = path.file_stem().unwrap().to_str().unwrap();

    let naive_datetime = FILE_STEM_REGEX_DATETIME
        .find(stem)
        .and_then(|m| NaiveDateTime::parse_from_str(m.as_str(), "%Y-%m-%d %H:%M:%S").ok())
        .or_else(|| {
            FILE_STEM_REGEX_DATE
                .find(stem)
                .and_then(|m| NaiveDate::parse_from_str(m.as_str(), "%Y-%m-%d").ok())
                .map(|d| d.and_hms(0, 0, 0))
        })?;

    Some(Local.from_local_datetime(&naive_datetime).unwrap())
}

async fn handle_new_stream(
    path: &Path,
    file_name: String,
    file_size: i64,
    possible_games: &mut Vec<GameInfo>,
) {
    let db = DB.get().unwrap();
    let sender = SENDER.get().unwrap();

    let timestamp = match parse_filename(path) {
        Some(date) => date.timestamp(),
        None => {
            eprintln!("error parsing timestamp for {:?}", path);
            0
        }
    };

    let duration = match get_video_duration_in_secs(path).await {
        Ok(d) => d,
        Err(_) => {
            eprintln!("error getting duration for: {:?}", path);
            return;
        }
    };

    let stream_id: i64 = {
        let mut db = db.lock().await;

        let duration = f64::from(duration);
        sqlx::query!(
            "INSERT INTO streams(filename, filesize, ts, duration) values(?1, ?2, ?3, ?4)",
            file_name,
            file_size,
            timestamp,
            duration
        )
        .execute(&mut db.conn)
        .await
        .unwrap()
        .last_insert_rowid()
    };

    struct FoldState<'a> {
        games: Vec<GameInfo>,
        possible_games: &'a mut Vec<GameInfo>,
    }

    let file_name: StreamFileName = file_name.into();
    let games: Vec<GameItem> = iter(file_name.get_extra_info().map(|(datapoints, _)| datapoints))
        .map(iter)
        .flatten()
        .filter(|datapoint| std::future::ready(!datapoint.game.is_empty()))
        .fold(
            FoldState {
                games: vec![],
                possible_games,
            },
            |mut state: FoldState<'_>, datapoint| async {
                let last_item_same_game = state
                    .games
                    .last()
                    .map(|x| x.twitch_name.as_ref().unwrap() == &datapoint.game)
                    .unwrap_or(false);
                if last_item_same_game {
                    return state;
                }

                let game = state
                    .possible_games
                    .iter()
                    .find(|g| {
                        if let Some(twitch_name) = &g.twitch_name {
                            twitch_name == &datapoint.game
                        } else {
                            false
                        }
                    })
                    .cloned();

                let game = match game {
                    Some(g) => g,
                    None => {
                        let game = db
                            .lock()
                            .await
                            .insert_possible_game(GameInfo {
                                id: 0,
                                name: datapoint.game.clone(),
                                twitch_name: Some(datapoint.game),
                                platform: None,
                                start_time: (datapoint.timestamp - timestamp).max(0) as f64,
                            })
                            .await;
                        state.possible_games.push(game.clone());
                        game
                    }
                };

                state.games.push(game);
                state
            },
        )
        .await
        .games
        .into_iter()
        .map(|g| GameItem {
            id: g.id,
            start_time: g.start_time,
        })
        .collect();
    db.lock().await.replace_games(stream_id, games).await;

    sender
        .send(Job::Thumbnails {
            stream_id,
            path: path.to_owned(),
        })
        .await
        .unwrap();
    sender
        .send(Job::Preview {
            stream_id,
            path: path.to_owned(),
        })
        .await
        .unwrap();
}

async fn handle_modified_stream(path: &Path, file_name: String, file_size: i64) {
    let db = DB.get().unwrap();
    let sender = SENDER.get().unwrap();

    let stream_id = {
        let mut db = db.lock().await;

        let file_size = file_size as i64;
        let stream_id = db.get_stream_id_by_filename(&file_name).await.unwrap();

        // update filesize
        sqlx::query!(
            "UPDATE streams SET filesize = ?1 WHERE id = ?2",
            file_size,
            stream_id
        )
        .execute(&mut db.conn)
        .await
        .unwrap();

        stream_id
    };

    remove_thumbnails_and_preview(stream_id).await;

    sender
        .send(Job::Thumbnails {
            stream_id,
            path: path.to_owned(),
        })
        .await
        .unwrap();
    sender
        .send(Job::Preview {
            stream_id,
            path: path.to_owned(),
        })
        .await
        .unwrap();
}

#[derive(PartialEq, Eq)]
enum ItemState {
    Unchanged,
    New,
    Modified,
    Removed,
}
pub async fn scan_streams() -> Result<(), Infallible> {
    let db = DB.get().unwrap();

    let file_name_states = {
        let db_map: HashMap<String, u64> = {
            let mut db = db.lock().await;
            db.get_streams()
                .await
                .into_iter()
                .map(|stream| (stream.file_name.into(), stream.file_size))
                .collect()
        };
        let dir_map: HashMap<String, u64> = {
            let dir = read_dir(STREAMS_DIR).await.unwrap();
            ReadDirStream::new(dir)
                .then(|item| async {
                    let item = item.unwrap();
                    let file_name = item.file_name().to_str().unwrap().to_owned();
                    let size = item.metadata().await.unwrap().len();

                    (file_name, size)
                })
                .collect()
                .await
        };

        let mut m: HashMap<String, (u64, ItemState)> = HashMap::new();

        for dir_file in &dir_map {
            // HACK
            if !(dir_file.0.ends_with(".mp4")
                || dir_file.0.ends_with(".mkv")
                || dir_file.0.ends_with(".webm"))
            {
                continue;
            }

            let state = match db_map.get(dir_file.0) {
                Some(db_file_size) => {
                    if db_file_size == dir_file.1 {
                        ItemState::Unchanged
                    } else {
                        ItemState::Modified
                    }
                }
                None => ItemState::New,
            };

            m.insert(dir_file.0.to_string(), (*dir_file.1, state));
        }

        for db_file in db_map {
            if dir_map.contains_key(&db_file.0) {
                continue;
            }

            m.insert(db_file.0, (db_file.1, ItemState::Removed));
        }

        m
    };

    let mut possible_games = {
        let mut db = db.lock().await;
        db.get_possible_games().await
    };

    let mut all_unchanged = true;
    for (file_name, (file_size, state)) in file_name_states {
        let path = Path::new(STREAMS_DIR).join(file_name.clone());

        match state {
            ItemState::Unchanged => {}
            ItemState::New => {
                println!("got new item: {}", file_name);
                all_unchanged = false;
                handle_new_stream(&path, file_name, file_size as i64, &mut possible_games).await;
            }
            ItemState::Modified => {
                println!("got updated item: {}", file_name);
                all_unchanged = false;

                handle_modified_stream(&path, file_name, file_size as i64).await;
            }
            ItemState::Removed => {
                println!("got removed item: {}", file_name);
                all_unchanged = false;

                let stream_id = db
                    .lock()
                    .await
                    .get_stream_id_by_filename(&file_name)
                    .await
                    .unwrap();
                remove_thumbnails_and_preview(stream_id).await;
                db.lock().await.remove_stream(stream_id).await;
            }
        }
    }

    if all_unchanged {
        println!("no new/modified items found");
    }

    Ok(())
}
