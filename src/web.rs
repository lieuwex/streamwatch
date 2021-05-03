use crate::chat_stream::handle_chat_request;
use crate::scan::scan_streams;
use crate::types::*;
use crate::{DB, STREAMS_DIR};

use std::collections::HashMap;
use std::convert::Infallible;

use warp::{Filter, Reply};

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

async fn rescan_streams() -> Result<impl warp::Reply, warp::Rejection> {
    scan_streams().await.unwrap();
    Ok(warp::reply())
}

pub async fn run_server() {
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

        let uncompressed = (warp::path("stream").and(warp::fs::dir(STREAMS_DIR)))
            .or(warp::path("preview").and(warp::fs::dir("./previews")))
            .or(warp::path("thumbnail").and(warp::fs::dir("./thumbnails")));

        compressed.or(uncompressed).with(cors)
    })
    .run(([0, 0, 0, 0], 6070))
    .await;
}
