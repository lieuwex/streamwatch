use crate::job_handler::{Job, SENDER};
use crate::{arc_mutex_unwrap, DB, STREAMS_DIR};

use streamwatch_shared::functions::{
    duration_to_seconds_float, get_video_duration, parse_filename,
};
use streamwatch_shared::types::{
    GameFeature, GameInfo, GameItem, StreamDatapoint, StreamFileName, StreamInfo,
};

use std::collections::HashMap;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;

use tokio::fs::{read_dir, remove_dir_all, remove_file};
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReadDirStream;

use futures::stream::iter;
use futures::StreamExt;

use chrono::{Duration, TimeZone, Utc};

use anyhow::{bail, Result};

macro_rules! log_err {
    ($item:expr) => {
        match $item {
            Ok(_) => {}
            Err(e) => eprintln!("error: {}", e),
        }
    };
}

pub async fn remove_thumbnails_and_preview<'c, E>(executor: E, stream_id: i64) -> Result<()>
where
    E: sqlx::Executor<'c, Database = sqlx::sqlite::Sqlite>,
{
    sqlx::query!(
        "UPDATE streams SET thumbnail_count=0, preview_count=0 WHERE id = ?1",
        stream_id,
    )
    .execute(executor)
    .await?;

    log_err!(remove_file(StreamInfo::preview_path(stream_id)).await);
    log_err!(remove_dir_all(StreamInfo::thumbnails_path(stream_id)).await);

    Ok(())
}

async fn handle_new_stream(
    path: &Path,
    file_name: String,
    file_size: i64,
    possible_games: &mut Vec<GameInfo>,
) -> Result<()> {
    let db = DB.get().unwrap();
    let sender = SENDER.get().unwrap();

    let file_name = StreamFileName::from(file_name);

    let timestamp = match parse_filename(path) {
        Some((date, _)) => date.with_timezone(&Utc),
        None => {
            eprintln!("error parsing timestamp for {:?}", path);
            Utc.timestamp(0, 0)
        }
    };

    let duration = match get_video_duration(path).await {
        Ok(d) => d,
        Err(_) => {
            bail!("error getting duration for: {:?}", path);
        }
    };

    let (datapoints, jumpcuts) = file_name
        .get_extra_info_from_file(STREAMS_DIR)
        .await?
        .unwrap_or((vec![], vec![]));

    let tx = Arc::new(Mutex::new(db.pool.begin().await?));

    let stream_id: i64 = {
        let duration = duration_to_seconds_float(&duration);
        let has_chat = file_name.has_chat(STREAMS_DIR).await?;
        let file_name = file_name.as_str();
        let timestamp = timestamp.timestamp();

        let datapoints_json = serde_json::to_string(&{
            // Clone the datapoints vector, but with the game values set to an empty String.
            // This will make sure that the game values are not serialized, since it is
            // superfluous in the database (the games are already inserted in the game_features
            // table).
            let datapoints: Vec<_> = datapoints
                .iter()
                .map(|dp| StreamDatapoint {
                    title: dp.title.clone(),
                    viewcount: dp.viewcount,
                    game: String::new(),
                    timestamp: dp.timestamp,
                })
                .collect();
            datapoints
        })?;
        let jumpcuts_json = serde_json::to_string(&jumpcuts)?;

        let inserted_at = Utc::now().timestamp();

        sqlx::query!(
            "INSERT INTO streams(filename, filesize, ts, duration, preview_count, thumbnail_count, has_chat, datapoints_json, jumpcuts_json, inserted_at) values(?1, ?2, ?3, ?4, 0, 0, ?5, ?6, ?7, ?8)",
            file_name,
            file_size,
            timestamp,
            duration,
            has_chat,
            datapoints_json,
            jumpcuts_json,
            inserted_at,
        )
        .execute(tx.lock().await.deref_mut())
        .await?
        .last_insert_rowid()
    };

    struct FoldState<'a> {
        games: Vec<GameFeature>,
        possible_games: &'a mut Vec<GameInfo>,
        tx: Arc<Mutex<sqlx::Transaction<'a, sqlx::Sqlite>>>,
    }

    let games = iter(datapoints)
        .filter(|datapoint| std::future::ready(!datapoint.game.is_empty()))
        .fold(
            FoldState {
                games: vec![],
                possible_games,
                tx: tx.clone(),
            },
            |mut state: FoldState<'_>, datapoint| async move {
                let last_item_same_game = state
                    .games
                    .last()
                    .map(|g| g.info.twitch_name.as_ref().unwrap() == &datapoint.game)
                    .unwrap_or(false);
                if last_item_same_game {
                    return state;
                }

                let game = state.possible_games.iter().find(|g| {
                    if let Some(twitch_name) = &g.twitch_name {
                        twitch_name == &datapoint.game
                    } else {
                        false
                    }
                });
                let game = match game {
                    Some(g) => g.clone(),
                    None => {
                        let game = db
                            .insert_possible_game(
                                state.tx.clone().lock_owned().await.deref_mut(),
                                datapoint.game.clone(),
                                Some(datapoint.game),
                                None,
                            )
                            .await
                            .unwrap();
                        state.possible_games.push(game.clone());
                        game
                    }
                };

                let start_time = (datapoint.timestamp - timestamp).max(Duration::zero());
                let game = GameFeature::from_game_info(game, start_time);
                state.games.push(game);

                state
            },
        )
        .await
        .games
        .into_iter()
        .map(|g| GameItem {
            id: g.info.id,
            start_time: g.start_time,
        });
    db.replace_games(tx.clone(), stream_id, games).await?;

    arc_mutex_unwrap!(tx)?.commit().await?;

    sender.send(Job::Thumbnails {
        stream_id,
        path: path.to_owned(),
    })?;
    sender.send(Job::Preview {
        stream_id,
        path: path.to_owned(),
    })?;
    sender.send(Job::Loudness { stream_id })?;
    sender.send(Job::Chatspeed { stream_id })?;

    Ok(())
}

async fn handle_modified_stream(path: &Path, file_name: String, file_size: i64) -> Result<()> {
    let db = DB.get().unwrap();
    let sender = SENDER.get().unwrap();

    let timestamp = match parse_filename(path) {
        Some((date, _)) => date.timestamp(),
        None => {
            eprintln!("error parsing timestamp for {:?}", path);
            0
        }
    };

    let duration = match get_video_duration(path).await {
        Ok(d) => d,
        Err(_) => {
            bail!("error getting duration for: {:?}", path);
        }
    };

    let stream_id = db.get_stream_id_by_filename(&file_name).await.unwrap();
    let duration = duration_to_seconds_float(&duration);

    {
        let mut tx = db.pool.begin().await?;

        // update filesize
        sqlx::query!(
            "UPDATE streams SET filesize = ?1, ts = ?2, duration = ?3 WHERE id = ?4",
            file_size,
            timestamp,
            duration,
            stream_id,
        )
        .execute(&mut tx)
        .await?;

        remove_thumbnails_and_preview(&mut tx, stream_id).await?;

        tx.commit().await?;
    }

    sender.send(Job::Thumbnails {
        stream_id,
        path: path.to_owned(),
    })?;
    sender.send(Job::Preview {
        stream_id,
        path: path.to_owned(),
    })?;
    sender.send(Job::Loudness { stream_id })?;
    sender.send(Job::Chatspeed { stream_id })?;

    Ok(())
}

#[derive(PartialEq, Eq)]
enum ItemState {
    Unchanged,
    New,
    Modified,
    Removed,
}
pub async fn scan_streams() -> Result<()> {
    let db = DB.get().unwrap();

    let file_name_states = {
        let db_map: HashMap<String, u64> = {
            db.get_streams()
                .await?
                .into_iter()
                .map(|stream| (stream.info.file_name.into(), stream.info.file_size))
                .collect()
        };
        let dir_map: HashMap<String, u64> = {
            let dir = read_dir(STREAMS_DIR).await?;
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

    let mut possible_games = db.get_possible_games().await?;

    let mut all_unchanged = true;
    for (file_name, (file_size, state)) in file_name_states {
        let path = Path::new(STREAMS_DIR).join(file_name.clone());

        match state {
            ItemState::Unchanged => {}
            ItemState::New => {
                println!("got new item: {}", file_name);
                all_unchanged = false;
                handle_new_stream(&path, file_name, file_size as i64, &mut possible_games).await?;
            }
            ItemState::Modified => {
                println!("got updated item: {}", file_name);
                all_unchanged = false;

                handle_modified_stream(&path, file_name, file_size as i64).await?;
            }
            ItemState::Removed => {
                println!("got removed item: {}", file_name);
                all_unchanged = false;

                let stream_id = db.get_stream_id_by_filename(&file_name).await.unwrap();
                remove_thumbnails_and_preview(&db.pool, stream_id).await?;
                db.remove_stream(stream_id).await?;
            }
        }
    }

    if all_unchanged {
        println!("no new/modified items found");
    }

    Ok(())
}
