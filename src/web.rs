use crate::types::ConversionProgress;
use crate::util::AnyhowError;
use crate::watchparty::{get_watch_parties, watch_party_ws};
use crate::{chat::handle_chat_request, types::StreamJson};
use crate::{check, DB, STREAMS_DIR};
use crate::{scan::scan_streams, types::GameItem};

use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;

use warp::http::StatusCode;
use warp::{Filter, Reply};

use anyhow::anyhow;

use serde::Deserialize;

macro_rules! reply_status {
    ($reply:expr, $code:expr) => {
        warp::reply::with_status($reply, $code).into_response()
    };

    ($code:expr) => {
        reply_status!(warp::reply(), $code)
    };
}

async fn streams() -> Result<warp::reply::Json, warp::Rejection> {
    let db = DB.get().unwrap();
    let streams: Vec<StreamJson> = check!(db.get_streams().await);
    Ok(warp::reply::json(&streams))
}

async fn processing_streams() -> Result<warp::reply::Json, warp::Rejection> {
    let db = DB.get().unwrap();
    let streams: Vec<ConversionProgress> = check!(db.get_processing_streams().await);
    Ok(warp::reply::json(&streams))
}

async fn replace_games(
    stream_id: i64,
    items: Vec<GameItem>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let db = DB.get().unwrap();
    check!(db.replace_games(stream_id, items).await);

    Ok(warp::reply().into_response())
}

async fn replace_persons(
    stream_id: i64,
    person_ids: Vec<i64>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let db = DB.get().unwrap();
    check!(db.replace_persons(stream_id, person_ids).await);

    Ok(warp::reply().into_response())
}

async fn get_stream_hype(stream_id: i64) -> Result<warp::reply::Json, warp::Rejection> {
    let db = DB.get().unwrap();
    let datapoints = check!(db.get_hype_datapoints(stream_id).await);
    Ok(warp::reply::json(&datapoints))
}

async fn get_stream_ratings(username: String) -> Result<warp::reply::Response, warp::Rejection> {
    let db = DB.get().unwrap();

    let user_id = match db.get_userid_by_username(&username).await {
        None => return Ok(reply_status!(StatusCode::UNAUTHORIZED)),
        Some(id) => id,
    };

    let map = check!(db.get_ratings(user_id).await);

    Ok(reply_status!(warp::reply::json(&map), StatusCode::FOUND))
}

#[derive(Clone, Debug, Deserialize)]
struct RateStreamBody {
    pub username: String,
    pub score: i8,
}
async fn rate_stream(
    stream_id: i64,
    RateStreamBody { username, score }: RateStreamBody,
) -> Result<warp::reply::Response, warp::Rejection> {
    if ![-1, 0, 1].contains(&score) {
        return Ok(reply_status!(StatusCode::BAD_REQUEST));
    }

    let db = DB.get().unwrap();

    let user_id = match db.get_userid_by_username(&username).await {
        None => return Ok(reply_status!(StatusCode::UNAUTHORIZED)),
        Some(id) => id,
    };

    check!(db.set_stream_rating(stream_id, user_id, score).await);

    Ok(warp::reply().into_response())
}

async fn set_custom_title(
    stream_id: i64,
    title: String,
) -> Result<warp::reply::Response, warp::Rejection> {
    let db = DB.get().unwrap();
    check!(db.set_custom_stream_title(stream_id, title).await);
    Ok(warp::reply().into_response())
}

async fn get_streams_progress(username: String) -> Result<warp::reply::Response, warp::Rejection> {
    let db = DB.get().unwrap();

    let user_id = match db.get_userid_by_username(&username).await {
        None => return Ok(reply_status!(StatusCode::UNAUTHORIZED)),
        Some(id) => id,
    };

    let map = check!(db.get_streams_progress(user_id).await);

    Ok(reply_status!(warp::reply::json(&map), StatusCode::FOUND))
}
async fn set_streams_progress(
    username: String,
    progress: HashMap<i64, f64>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let db = DB.get().unwrap();

    let user_id = match db.get_userid_by_username(&username).await {
        None => return Ok(reply_status!(StatusCode::UNAUTHORIZED)),
        Some(id) => id,
    };

    check!(db.update_streams_progress(user_id, progress).await);

    Ok(warp::reply().into_response())
}

async fn get_possible_games() -> Result<warp::reply::Json, warp::Rejection> {
    let db = DB.get().unwrap();
    let possible_games = check!(db.get_possible_games().await);

    Ok(warp::reply::json(&possible_games))
}

async fn add_possible_game(
    info: HashMap<String, String>,
) -> Result<warp::reply::Json, warp::Rejection> {
    let db = DB.get().unwrap();

    let name = check!(info.get("name").ok_or_else(|| anyhow!("name required")));
    let twitch_name = info.get("twitchName");
    let platform = info.get("platform");
    let game_info = check!(
        db.insert_possible_game(name.to_owned(), twitch_name.cloned(), platform.cloned())
            .await
    );

    Ok(warp::reply::json(&game_info))
}

async fn get_possible_persons() -> Result<warp::reply::Json, warp::Rejection> {
    let db = DB.get().unwrap();
    let possible_persons = check!(db.get_possible_persons().await);

    Ok(warp::reply::json(&possible_persons))
}

async fn rescan_streams() -> Result<impl warp::Reply, warp::Rejection> {
    check!(scan_streams().await);
    Ok("scanned streams")
}

async fn get_all_clips() -> Result<warp::reply::Json, warp::Rejection> {
    let db = DB.get().unwrap();
    let clips = check!(db.get_clips(None).await);
    Ok(warp::reply::json(&clips))
}

async fn get_stream_clips(stream_id: i64) -> Result<warp::reply::Json, warp::Rejection> {
    let db = DB.get().unwrap();
    let clips = check!(db.get_clips(Some(stream_id)).await);
    Ok(warp::reply::json(&clips))
}

pub async fn run_server() {
    let endpoints = {
        let cors = warp::cors().allow_any_origin();
        let log = warp::log("streamwatch");

        let dynamic_paths = (warp::get().and(warp::path!("streams")).and_then(streams))
            .or(warp::get()
                .and(warp::path!("processing"))
                .and_then(processing_streams))
            .or(warp::patch()
                .and(warp::path!("streams"))
                .and_then(rescan_streams))
            .or(warp::get()
                .and(warp::path!("persons"))
                .and_then(get_possible_persons))
            .or(warp::get()
                .and(warp::path!("games"))
                .and_then(get_possible_games))
            .or(warp::post()
                .and(warp::path!("games"))
                .and(warp::body::json())
                .and_then(add_possible_game))
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
            .or(warp::get()
                .and(warp::path!("stream" / i64 / "hype"))
                .and_then(get_stream_hype))
            .or(warp::post()
                .and(warp::path!("stream" / i64 / "rate"))
                .and(warp::body::json())
                .and_then(rate_stream))
            .or(warp::put()
                .and(warp::path!("stream" / i64 / "title"))
                .and(warp::body::json())
                .and_then(set_custom_title))
            .or(warp::put()
                .and(warp::path!("user" / String / "progress"))
                .and(warp::body::json())
                .and_then(set_streams_progress))
            .or(warp::get()
                .and(warp::path!("user" / String / "progress"))
                .and_then(get_streams_progress))
            .or(warp::get()
                .and(warp::path!("user" / String / "ratings"))
                .and_then(get_stream_ratings))
            .or(warp::get()
                .and(warp::path!("parties"))
                .and_then(get_watch_parties))
            .or(warp::get()
                .and(warp::path!("party" / "ws"))
                .and(warp::query())
                .and(warp::ws())
                .and_then(watch_party_ws))
            .or(warp::get()
                .and(warp::path!("clips"))
                .and_then(get_all_clips))
            .or(warp::get()
                .and(warp::path!("clips" / i64))
                .and_then(get_stream_clips));
        let static_paths = warp::path("video")
            .and(warp::fs::file("./build/index.html"))
            .or(warp::path("login").and(warp::fs::file("./build/index.html")))
            .or(warp::path("watchparty").and(warp::fs::file("./build/index.html")))
            .or(warp::path("static").and(warp::fs::dir("./build/static")))
            .or(warp::path::end().and(warp::fs::file("./build/index.html")))
            .or(warp::path::end().and(warp::fs::dir("./build")));
        let compressed = dynamic_paths
            .or(static_paths)
            .with(warp::compression::gzip());

        let uncompressed = (warp::path("stream").and(warp::fs::dir(STREAMS_DIR)))
            .or(warp::path("preview").and(warp::fs::dir("./previews")))
            .or(warp::path("thumbnail").and(warp::fs::dir("./thumbnails")));

        compressed.or(uncompressed).with(cors).with(log)
    };

    let addr = IpAddr::from_str("::0").unwrap();
    warp::serve(endpoints).run((addr, 6070)).await;
}
