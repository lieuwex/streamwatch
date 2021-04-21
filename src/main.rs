#![feature(iter_intersperse)]
#![feature(hash_drain_filter)]

mod chat_stream;
mod create_preview;
mod db;
mod types;

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::chat_stream::{cache_pruner, handle_chat_request};
use crate::create_preview::*;
use crate::types::*;

use tokio;
use tokio::fs::{read_dir, remove_dir_all, remove_file};
use tokio::sync;
use tokio::time;
use tokio_stream::wrappers::ReadDirStream;

use regex::Regex;

use futures::StreamExt;

use warp;
use warp::Filter;
use warp::Reply;

use rusqlite::params;

use once_cell::sync::{Lazy, OnceCell};

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone};

pub static FILE_STEM_REGEX_DATETIME: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap());
pub static FILE_STEM_REGEX_DATE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap());

pub static DB: OnceCell<Arc<Mutex<db::Database>>> = OnceCell::new();
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

fn streams() -> impl warp::Reply {
    let streams = {
        let db = DB.get().unwrap();
        let db = db.lock().unwrap();
        db.get_streams()
    };

    warp::reply::json(&streams)
}

fn replace_games(stream_id: i64, items: Vec<GameItem>) -> impl warp::Reply {
    {
        let db = DB.get().unwrap();
        let db = db.lock().unwrap();
        db.replace_games(stream_id as u64, items);
    }

    warp::reply()
}

fn replace_persons(stream_id: i64, person_ids: Vec<i64>) -> impl warp::Reply {
    {
        let db = DB.get().unwrap();
        let db = db.lock().unwrap();
        db.replace_persons(stream_id as u64, person_ids);
    }

    warp::reply()
}

fn get_streams_progress(username: String) -> warp::reply::Response {
    let map = {
        let db = DB.get().unwrap();
        let db = db.lock().unwrap();

        let user_id = match db.get_userid_by_username(&username) {
            None => {
                return warp::reply::with_status(
                    warp::reply(),
                    warp::http::StatusCode::UNAUTHORIZED,
                )
                .into_response()
            }
            Some(id) => id,
        };

        db.get_streams_progress(user_id)
    };

    warp::reply::with_status(warp::reply::json(&map), warp::http::StatusCode::FOUND).into_response()
}
fn set_streams_progress(username: String, progress: HashMap<i64, f64>) -> impl warp::Reply {
    {
        let db = DB.get().unwrap();
        let db = db.lock().unwrap();

        let user_id = match db.get_userid_by_username(&username) {
            None => {
                return warp::reply::with_status(
                    warp::reply(),
                    warp::http::StatusCode::UNAUTHORIZED,
                )
                .into_response()
            }
            Some(id) => id,
        };

        db.update_streams_progress(user_id, progress)
    }

    warp::reply().into_response()
}

fn get_possible_games() -> impl warp::Reply {
    let possible_games = {
        let db = DB.get().unwrap();
        let db = db.lock().unwrap();

        db.get_possible_games()
    };

    warp::reply::json(&possible_games)
}

fn get_possible_persons() -> impl warp::Reply {
    let possible_persons = {
        let db = DB.get().unwrap();
        let db = db.lock().unwrap();

        db.get_possible_persons()
    };

    warp::reply::json(&possible_persons)
}

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
    let db = db.lock().unwrap();
    db.conn
        .execute(
            "INSERT INTO stream_previews(stream_id) values(?1)",
            params![stream_id],
        )
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
    let db = db.lock().unwrap();

    for (i, _) in items.iter().enumerate() {
        db.conn
            .execute(
                "INSERT INTO stream_thumbnails(stream_id, thumb_index) values(?1, ?2)",
                params![stream_id, i as i32],
            )
            .unwrap();
    }

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
        let db = db.lock().unwrap();

        // remove preview
        db.conn
            .execute(
                "DELETE FROM stream_previews WHERE stream_id = ?1",
                params![stream_id],
            )
            .unwrap();
        // remove thumbnails
        db.conn
            .execute(
                "DELETE FROM stream_thumbnails WHERE stream_id = ?1",
                params![stream_id],
            )
            .unwrap();
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

async fn file_watcher(db: Arc<Mutex<db::Database>>, sender: sync::mpsc::Sender<Job>) {
    loop {
        let file_name_states = {
            let db_map: HashMap<String, u64> = {
                let db = db.lock().unwrap();
                db.get_streams()
                    .into_iter()
                    .map(|stream| (stream.file_name, stream.file_size))
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

        for (file_name, (file_size, state)) in file_name_states {
            let path = Path::new(STREAMS_DIR).join(file_name.clone());

            match state {
                ItemState::Unchanged => {}
                ItemState::New => {
                    println!("got new item: {}", file_name);

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
                        let db = db.lock().unwrap();
                        db.conn.execute(
                            "INSERT INTO streams(filename, filesize, ts, duration) values(?1, ?2, ?3, ?4)",
                            params![file_name, file_size as i64, timestamp, duration as f64],
                        )
                        .unwrap();
                        db.conn.last_insert_rowid()
                    };

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

                    let stream_id = {
                        let db = db.lock().unwrap();

                        let stream_id = db.get_stream_id_by_filename(&file_name).unwrap();

                        // update filesize
                        db.conn
                            .execute(
                                "UPDATE streams SET filesize = ?1 WHERE id = ?2",
                                params![file_size as i64, stream_id],
                            )
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

                    let stream_id = db
                        .lock()
                        .unwrap()
                        .get_stream_id_by_filename(&file_name)
                        .unwrap();
                    remove_thumbnails_and_preview(stream_id).await;
                    db.lock().unwrap().remove_stream(stream_id);
                }
            }
        }

        time::sleep(time::Duration::from_secs(60)).await;
    }
}

/*
async fn file_watcher(db: Arc<Mutex<db::Database>>, sender: sync::mpsc::Sender<Job>) {
    let mut map: HashMap<_, _> = {
        let db = db.lock().unwrap();
        db.get_streams()
            .into_iter()
            .map(|stream| (stream.file_name, stream.file_size))
            .collect()
    };

    loop {
        let mut dir = read_dir(STREAMS_DIR).await.unwrap();

        while let Some(item) = dir.next_entry().await.unwrap() {
            let file_name = item.file_name();
            let size = item.metadata().await.unwrap().len();

            let entry = map.entry(file_name.to_string_lossy().to_string());
            let state = match entry {
                Entry::Vacant(item) => {
                    item.insert(size);
                    ItemState::New
                }
                Entry::Occupied(mut item) => {
                    let old_size = item.get();
                    let modified = *old_size != size;
                    if modified {
                        item.insert(size);
                        ItemState::Modified
                    } else {
                        ItemState::Unchanged
                    }
                }
            };

            let path = Path::new(STREAMS_DIR).join(file_name.clone());

            // HACK
            match path.extension() {
                None => continue,
                Some(e) => {
                    if !(e == "mp4" || e == "mkv" || e == "webm") {
                        continue;
                    }
                }
            }

            // TODO: also handle remove items

            match state {
                ItemState::Unchanged => {}
                ItemState::New => {
                    println!("got new item: {}", file_name.to_string_lossy());

                    let timestamp = {
                        match NaiveDateTime::parse_from_str(
                            path.file_stem().unwrap().to_str().unwrap(),
                            "%Y-%m-%d %H:%M:%S",
                        ) {
                            Ok(date) => date.timestamp(),
                            Err(_) => {
                                eprintln!("error parsing timestamp for: {:?}", path);
                                0
                            }
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
                        let db = db.lock().unwrap();
                        db.conn.execute(
                            "INSERT INTO streams(filename, filesize, ts, duration) values(?1, ?2, ?3, ?4)",
                            params![file_name.to_str().unwrap(), size as i64, timestamp, duration as f64],
                        )
                        .unwrap();
                        db.conn.last_insert_rowid()
                    };

                    sender
                        .send(Job::Thumbnails(stream_id, path.clone()))
                        .await
                        .unwrap();
                    sender.send(Job::Preview(stream_id, path)).await.unwrap();
                }
                ItemState::Modified => {
                    println!("got updated item: {}", file_name.to_string_lossy());

                    let stream_id = {
                        let db = db.lock().unwrap();

                        // get stream_id by filename
                        let stream_id: i64 = db
                            .conn
                            .query_row(
                                "SELECT id from streams where filename = ?1",
                                params![file_name.to_str().unwrap()],
                                |row| row.get(0),
                            )
                            .unwrap();

                        // update filesize
                        db.conn
                            .execute(
                                "UPDATE streams SET filesize = ?1 WHERE id = ?2",
                                params![size as i64, stream_id],
                            )
                            .unwrap();

                        // remove preview
                        db.conn
                            .execute(
                                "DELETE FROM stream_previews WHERE stream_id = ?1",
                                params![stream_id],
                            )
                            .unwrap();
                        // remove thumbnails
                        db.conn
                            .execute(
                                "DELETE FROM stream_thumbnails WHERE stream_id = ?1",
                                params![stream_id],
                            )
                            .unwrap();

                        stream_id
                    };

                    log_err!(remove_file(get_preview_path(stream_id)).await);
                    log_err!(remove_dir_all(get_thumbnails_path(stream_id)).await);

                    sender
                        .send(Job::Thumbnails(stream_id, path.clone()))
                        .await
                        .unwrap();
                    sender.send(Job::Preview(stream_id, path)).await.unwrap();
                }
            }
        }

        time::sleep(time::Duration::from_secs(60)).await;
    }
}
*/

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
    let db = db::Database::new();
    match DB.set(db.clone()) {
        Ok(_) => {}
        Err(_) => panic!("oncecell already full"),
    }

    let (sender, receiver) = sync::mpsc::channel(1);
    let receiver = Arc::new(sync::Mutex::new(receiver));

    tokio::spawn(async move {
        file_watcher(db, sender).await;
    });

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

        let compressed = (warp::path("streams").map(streams))
            .or(warp::get()
                .and(warp::path!("persons"))
                .map(get_possible_persons))
            .or(warp::get()
                .and(warp::path!("games"))
                .map(get_possible_games))
            .or(warp::put()
                .and(warp::path!("stream" / i64 / "games"))
                .and(warp::body::json())
                .map(replace_games))
            .or(warp::put()
                .and(warp::path!("stream" / i64 / "persons"))
                .and(warp::body::json())
                .map(replace_persons))
            .or(warp::get()
                .and(warp::path!("stream" / i64 / "chat"))
                .and(warp::query())
                .and_then(handle_chat_request))
            .or(warp::put()
                .and(warp::path!("user" / String / "progress"))
                .and(warp::body::json())
                .map(set_streams_progress))
            .or(warp::get()
                .and(warp::path!("user" / String / "progress"))
                .map(get_streams_progress))
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
