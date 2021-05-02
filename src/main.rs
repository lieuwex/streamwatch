#![feature(iter_intersperse)]
#![feature(hash_drain_filter)]
#![feature(destructuring_assignment)]

mod chat_stream;
mod create_preview;
mod db;
mod types;

use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use crate::chat_stream::{cache_pruner, handle_chat_request};
use crate::create_preview::*;
use crate::types::*;

use tokio;
use tokio::fs::{read_dir, remove_dir_all, remove_file};
use tokio::sync;
use tokio_stream::wrappers::ReadDirStream;

use regex::Regex;

use futures::{stream::iter, StreamExt};

use warp;
use warp::Filter;
use warp::Reply;

use once_cell::sync::{Lazy, OnceCell};

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone};

use sqlx::Connection;

pub static FILE_STEM_REGEX_DATETIME: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap());
pub static FILE_STEM_REGEX_DATE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap());

pub static DB: OnceCell<Arc<tokio::sync::Mutex<db::Database>>> = OnceCell::new();
static SENDER: OnceCell<tokio::sync::mpsc::Sender<Job>> = OnceCell::new();
pub static STREAMS_DIR: &'static str = "/streams/lekkerspelen";

static PREVIEW_WORKERS: usize = 4;

fn get_preview_path(stream_id: i64) -> PathBuf {
    Path::new("./previews")
        .join(stream_id.to_string())
        .join("preview.webm")
}
fn get_thumbnails_path(stream_id: i64) -> PathBuf {
    Path::new("./thumbnails").join(stream_id.to_string())
}

macro_rules! log_err {
    ($item:expr) => {
        match $item {
            Ok(_) => {}
            Err(e) => eprintln!("error: {}", e),
        }
    };
}

#[derive(Clone, Debug)]
enum Job {
    Preview { stream_id: i64, path: PathBuf },
    Thumbnails { stream_id: i64, path: PathBuf },
}

async fn streams() -> Result<warp::reply::Json, Infallible> {
    let streams = {
        let db = DB.get().unwrap();
        let mut db = db.lock().await;
        db.get_streams().await
    };

    Ok(warp::reply::json(&streams))
}

async fn replace_games(
    stream_id: i64,
    items: Vec<GameItem>,
) -> Result<warp::reply::Response, Infallible> {
    {
        let db = DB.get().unwrap();
        let mut db = db.lock().await;
        db.replace_games(stream_id, items).await;
    }

    Ok(warp::reply().into_response())
}

async fn replace_persons(
    stream_id: i64,
    person_ids: Vec<i64>,
) -> Result<warp::reply::Response, Infallible> {
    {
        let db = DB.get().unwrap();
        let mut db = db.lock().await;
        db.replace_persons(stream_id, person_ids).await;
    }

    Ok(warp::reply().into_response())
}

async fn get_streams_progress(username: String) -> Result<warp::reply::Response, Infallible> {
    let map = {
        let db = DB.get().unwrap();
        let mut db = db.lock().await;

        let user_id = match db.get_userid_by_username(&username).await {
            None => {
                return Ok(warp::reply::with_status(
                    warp::reply(),
                    warp::http::StatusCode::UNAUTHORIZED,
                )
                .into_response())
            }
            Some(id) => id,
        };

        db.get_streams_progress(user_id).await
    };

    Ok(
        warp::reply::with_status(warp::reply::json(&map), warp::http::StatusCode::FOUND)
            .into_response(),
    )
}
async fn set_streams_progress(
    username: String,
    progress: HashMap<i64, f64>,
) -> Result<warp::reply::Response, Infallible> {
    {
        let db = DB.get().unwrap();
        let mut db = db.lock().await;

        let user_id = match db.get_userid_by_username(&username).await {
            None => {
                return Ok(warp::reply::with_status(
                    warp::reply(),
                    warp::http::StatusCode::UNAUTHORIZED,
                )
                .into_response())
            }
            Some(id) => id,
        };

        db.update_streams_progress(user_id, progress).await;
    }

    Ok(warp::reply().into_response())
}

async fn get_possible_games() -> Result<warp::reply::Json, Infallible> {
    let possible_games = {
        let db = DB.get().unwrap();
        let mut db = db.lock().await;

        db.get_possible_games().await
    };

    Ok(warp::reply::json(&possible_games))
}

async fn get_possible_persons() -> Result<warp::reply::Json, Infallible> {
    let possible_persons = {
        let db = DB.get().unwrap();
        let mut db = db.lock().await;

        db.get_possible_persons().await
    };

    Ok(warp::reply::json(&possible_persons))
}

#[derive(PartialEq, Eq)]
enum ItemState {
    Unchanged,
    New,
    Modified,
    Removed,
}

async fn make_preview(stream_id: i64, path: PathBuf) {
    let sections = get_sections_from_file(&path).await.unwrap();
    println!("[{}] sections are: {:?}", stream_id, sections);

    let start = Instant::now();

    let preview_path = get_preview_path(stream_id);
    create_preview(&path, &preview_path, &sections)
        .await
        .unwrap();

    let db = DB.get().unwrap();
    let mut db = db.lock().await;
    sqlx::query!(
        "INSERT INTO stream_previews(stream_id) values(?1)",
        stream_id
    )
    .execute(&mut db.conn)
    .await
    .unwrap();

    println!("[{}] made preview in {:?}", stream_id, start.elapsed());
}

async fn make_thumbnails(stream_id: i64, path: PathBuf) {
    let sections = get_sections_from_file(&path).await.unwrap();
    println!("[{}] sections are: {:?}", stream_id, sections);

    let start = Instant::now();

    let thumbnail_path = get_thumbnails_path(stream_id);
    let ts: Vec<_> = sections.iter().map(|(a, _)| *a).collect();
    let items = create_thumbnails(&path, &thumbnail_path, &ts)
        .await
        .unwrap();

    let db = DB.get().unwrap();
    let mut db = db.lock().await;
    let mut tx = db.conn.begin().await.unwrap();

    for (i, _) in items.iter().enumerate() {
        let i = i as i64;
        sqlx::query!(
            "INSERT INTO stream_thumbnails(stream_id, thumb_index) values(?1, ?2)",
            stream_id,
            i
        )
        .execute(&mut tx)
        .await
        .unwrap();
    }

    tx.commit().await.unwrap();

    println!(
        "[{}] made {} thumbnails in {:?}",
        stream_id,
        sections.len(),
        start.elapsed()
    );
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

    log_err!(remove_file(get_preview_path(stream_id)).await);
    log_err!(remove_dir_all(get_thumbnails_path(stream_id)).await);
}

fn parse_filename(path_buf: &PathBuf) -> Option<DateTime<Local>> {
    let stem = path_buf.file_stem().unwrap().to_str().unwrap();

    let naive_datetime = FILE_STEM_REGEX_DATETIME
        .find(&stem)
        .and_then(|m| NaiveDateTime::parse_from_str(m.as_str(), "%Y-%m-%d %H:%M:%S").ok())
        .or_else(|| {
            FILE_STEM_REGEX_DATE
                .find(&stem)
                .and_then(|m| NaiveDate::parse_from_str(m.as_str(), "%Y-%m-%d").ok())
                .map(|d| d.and_hms(0, 0, 0))
        })?;

    Some(Local.from_local_datetime(&naive_datetime).unwrap())
}

async fn rescan_streams() -> Result<impl warp::Reply, warp::Rejection> {
    let db = DB.get().unwrap();
    let sender = SENDER.get().unwrap();

    let file_name_states = {
        let db_map: HashMap<String, u64> = {
            let mut db = db.lock().await;
            db.get_streams()
                .await
                .into_iter()
                .map(|stream| (stream.file_name.into_string(), stream.file_size))
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

                let timestamp = match parse_filename(&path) {
                    Some(date) => date.timestamp(),
                    None => {
                        eprintln!("error parsing timestamp for {:?}", path);
                        0
                    }
                };

                let duration = match get_video_duration_in_secs(&path).await {
                    Ok(d) => d,
                    Err(_) => {
                        eprintln!("error getting duration for: {:?}", path);
                        continue;
                    }
                };

                let stream_id: i64 = {
                    let mut db = db.lock().await;

                    let file_size = file_size as i64;
                    let duration = duration as f64;
                    sqlx::query!("INSERT INTO streams(filename, filesize, ts, duration) values(?1, ?2, ?3, ?4)", file_name, file_size, timestamp, duration)
                        .execute(&mut db.conn)
                        .await
                        .unwrap().last_insert_rowid()
                };

                struct FoldState<'a> {
                    games: Vec<GameInfo>,
                    possible_games: &'a mut Vec<GameInfo>,
                }

                let file_name = StreamFileName::from_string(file_name);
                let games: Vec<GameItem> =
                    iter(file_name.get_extra_info().map(|(datapoints, _)| datapoints))
                        .map(iter)
                        .flatten()
                        .filter(|datapoint| std::future::ready(!datapoint.game.is_empty()))
                        .fold(
                            FoldState {
                                games: vec![],
                                possible_games: &mut possible_games,
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
                                                start_time: (datapoint.timestamp - timestamp).max(0)
                                                    as f64,
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
                        path: path.clone(),
                    })
                    .await
                    .unwrap();
                sender.send(Job::Preview { stream_id, path }).await.unwrap();
            }
            ItemState::Modified => {
                println!("got updated item: {}", file_name);
                all_unchanged = false;

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
                        path: path.clone(),
                    })
                    .await
                    .unwrap();
                sender.send(Job::Preview { stream_id, path }).await.unwrap();
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

    Ok(warp::reply())
}

async fn job_watcher(receiver: Arc<sync::Mutex<sync::mpsc::Receiver<Job>>>) {
    loop {
        let job = match {
            let mut receiver = receiver.lock().await;
            receiver.recv().await
        } {
            None => break,
            Some(j) => j,
        };

        match job {
            Job::Preview { stream_id, path } => make_preview(stream_id, path).await,
            Job::Thumbnails { stream_id, path } => make_thumbnails(stream_id, path).await,
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    macro_rules! okky {
        ($cell:expr, $item:expr) => {
            match $cell.set($item) {
                Ok(_) => {}
                Err(_) => panic!("oncecell already full"),
            }
        };
    }

    okky!(DB, db::Database::new().await);

    let (sender, receiver) = sync::mpsc::channel(1);
    let receiver = Arc::new(sync::Mutex::new(receiver));
    okky!(SENDER, sender);

    for _ in 0..PREVIEW_WORKERS {
        let receiver_cloned = receiver.clone();
        tokio::spawn(async move {
            job_watcher(receiver_cloned).await;
        });
    }

    tokio::spawn(async {
        cache_pruner().await;
    });

    warp::serve({
        let cors = warp::cors().allow_any_origin();

        let compressed = (warp::get().and(warp::path!("streams")).and_then(streams))
            .or(warp::patch()
                .and(warp::path!("streams"))
                .and_then(rescan_streams))
            .or(warp::get()
                .and(warp::path!("persons"))
                .and_then(get_possible_persons))
            .or(warp::get()
                .and(warp::path!("games"))
                .and_then(get_possible_games))
            .or(warp::put()
                .and(warp::path!("stream" / i64 / "games"))
                .and(warp::body::json())
                .and_then(replace_games))
            .or(warp::put()
                .and(warp::path!("stream" / i64 / "persons"))
                .and(warp::body::json())
                .and_then(replace_persons))
            .or(warp::get()
                .and(warp::path!("stream" / i64 / "chat"))
                .and(warp::query())
                .and_then(handle_chat_request))
            .or(warp::put()
                .and(warp::path!("user" / String / "progress"))
                .and(warp::body::json())
                .and_then(set_streams_progress))
            .or(warp::get()
                .and(warp::path!("user" / String / "progress"))
                .and_then(get_streams_progress))
            .or(warp::path("video").and(warp::fs::file("./build/index.html")))
            .or(warp::path("login").and(warp::fs::file("./build/index.html")))
            .or(warp::path("static").and(warp::fs::dir("./build/static")))
            .or(warp::path::end().and(warp::fs::file("./build/index.html")))
            .or(warp::path::end().and(warp::fs::dir("./build")))
            .with(warp::compression::gzip());

        let uncompressed = warp::path("stream")
            .and(warp::fs::dir(STREAMS_DIR))
            .or(warp::path("preview").and(warp::fs::dir("./previews")))
            .or(warp::path("thumbnail").and(warp::fs::dir("./thumbnails")));

        compressed.or(uncompressed).with(cors)
    })
    .run(([0, 0, 0, 0], 6070))
    .await;

    Ok(())
}
