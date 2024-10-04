use crate::chat::handle_chat_request;
use crate::db::Database;
use crate::job_handler::{Job, SENDER};
use crate::scan::scan_streams;
use crate::util::AnyhowError;
use crate::watchparty::{get_watch_parties, watch_party_ws};
use crate::{check, conn, get_conn, DB, STREAMS_DIR};

use chrono::Utc;
use futures::TryStreamExt;
use serde_json::value::RawValue;
use streamwatch_shared::types::{
    Clip, ConversionProgress, CreateClipRequest, GameItem, StreamJson, StreamProgress,
};

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::ops::DerefMut;
use std::str::FromStr;

use warp::http::StatusCode;
use warp::{Filter, Reply};

use anyhow::anyhow;

use serde::{Deserialize, Serialize};

macro_rules! reply_status {
    ($reply:expr, $code:expr) => {
        warp::reply::with_status($reply, $code).into_response()
    };

    ($code:expr) => {
        reply_status!(warp::reply(), $code)
    };
}

macro_rules! check_username_password {
    ($conn:expr, $username:expr, $password:expr, $on_err:expr) => {{
        let user_id = match check!(Database::get_userid_by_username($conn, $username).await) {
            None => return $on_err,
            Some(id) => id,
        };

        if !check!(Database::check_password($conn, user_id, $password).await) {
            return $on_err;
        }

        user_id
    }};
}

async fn _get_clips(
    stream_id: Option<i64>,
    hashes: HashMap<String, String>,
) -> Result<warp::reply::Json, warp::Rejection> {
    #[derive(Serialize)]
    struct ClipJson {
        #[serde(flatten)]
        pub clip: Clip,
        pub watched: bool,
        pub safe_to_watch: bool,
    }

    let mut conn = get_conn!();

    let username = hashes.get("username").cloned().unwrap_or(String::new());
    let user_pass: Option<(String, String)> = if !username.is_empty() {
        let password = hashes.get("password").cloned().unwrap_or(String::new());
        Some((username, password))
    } else {
        None
    };

    let (viewed_set, stream_progress): (HashSet<i64>, HashMap<i64, StreamProgress>) =
        if let Some((username, password)) = user_pass {
            let user_id =
                check_username_password!(&mut conn, &username, &password, Err(warp::reject()));

            let viewed_set = check!(
                sqlx::query!(
                    "SELECT DISTINCT clip_id FROM clip_views WHERE user_id = ?1",
                    user_id
                )
                .map(|row| row.clip_id)
                .fetch(conn.deref_mut())
                .try_collect()
                .await
            );

            let stream_progress = check!(Database::get_streams_progress(&mut conn, user_id).await);

            (viewed_set, stream_progress)
        } else {
            (HashSet::new(), HashMap::new())
        };

    let clips = check!(Database::get_clips(&mut conn, stream_id).await);
    let clips: Vec<_> = clips
        .into_iter()
        .map(|clip| ClipJson {
            watched: viewed_set.contains(&clip.id),
            safe_to_watch: stream_progress
                .get(&clip.stream_id)
                .is_some_and(|p| p.time >= (clip.start_time + clip.duration)),
            clip,
        })
        .collect();
    Ok(warp::reply::json(&clips))
}

#[derive(Clone, Debug, Deserialize)]
pub struct PasswordQuery {
    password: String,
}

async fn streams() -> Result<warp::reply::Json, warp::Rejection> {
    let streams: Vec<StreamJson> = check!(Database::get_streams(conn!()).await);
    Ok(warp::reply::json(&streams))
}

async fn processing_streams() -> Result<warp::reply::Json, warp::Rejection> {
    let streams: Vec<ConversionProgress> = check!(Database::get_processing_streams(conn!()).await);
    Ok(warp::reply::json(&streams))
}

async fn replace_games(
    stream_id: i64,
    items: Vec<GameItem>,
) -> Result<warp::reply::Response, warp::Rejection> {
    check!(Database::replace_games(conn!(), stream_id, items).await);
    Ok(warp::reply().into_response())
}

async fn replace_persons(
    stream_id: i64,
    person_ids: Vec<i64>,
) -> Result<warp::reply::Response, warp::Rejection> {
    check!(Database::replace_persons(conn!(), stream_id, person_ids).await);
    Ok(warp::reply().into_response())
}

async fn get_stream_hype(stream_id: i64) -> Result<warp::reply::Json, warp::Rejection> {
    let datapoints = check!(Database::get_hype_datapoints(conn!(), stream_id).await);
    Ok(warp::reply::json(&datapoints))
}

async fn get_stream_clips(
    stream_id: i64,
    hashes: HashMap<String, String>,
) -> Result<warp::reply::Json, warp::Rejection> {
    _get_clips(Some(stream_id), hashes).await
}

async fn get_stream_ratings(
    username: String,
    password: PasswordQuery,
) -> Result<warp::reply::Response, warp::Rejection> {
    let mut conn = get_conn!();

    let user_id = check_username_password!(
        &mut conn,
        &username,
        &password.password,
        Ok(reply_status!(StatusCode::UNAUTHORIZED))
    );

    let map = check!(Database::get_ratings(&mut conn, user_id).await);

    Ok(reply_status!(warp::reply::json(&map), StatusCode::FOUND))
}

#[derive(Clone, Debug, Deserialize)]
struct RateStreamBody {
    pub username: String,
    pub score: i8,
}
async fn rate_stream(
    stream_id: i64,
    password: PasswordQuery,
    RateStreamBody { username, score }: RateStreamBody,
) -> Result<warp::reply::Response, warp::Rejection> {
    if ![-1, 0, 1].contains(&score) {
        return Ok(reply_status!(StatusCode::BAD_REQUEST));
    }

    let mut conn = get_conn!();

    let user_id = check_username_password!(
        &mut conn,
        &username,
        &password.password,
        Ok(reply_status!(StatusCode::UNAUTHORIZED))
    );

    check!(Database::set_stream_rating(&mut conn, stream_id, user_id, score).await);

    Ok(warp::reply().into_response())
}

async fn set_custom_title(
    stream_id: i64,
    title: String,
) -> Result<warp::reply::Response, warp::Rejection> {
    check!(Database::set_custom_stream_title(conn!(), stream_id, title).await);
    Ok(warp::reply().into_response())
}

async fn get_stream_other_progress(stream_id: i64) -> Result<warp::reply::Json, warp::Rejection> {
    let mut conn = get_conn!();

    let other_progress: HashMap<String, f64> = check!(
        sqlx::query!(
            r#"
                SELECT username,time
                FROM stream_progress
                JOIN users ON users.id=user_id
                WHERE stream_id = ?1"#,
            stream_id,
        )
        .map(|row| (row.username, row.time))
        .fetch(conn.deref_mut())
        .try_collect()
        .await
    );
    Ok(warp::reply::json(&other_progress))
}

async fn get_streams_progress(
    username: String,
    password: PasswordQuery,
) -> Result<warp::reply::Response, warp::Rejection> {
    let mut conn = get_conn!();

    let user_id = check_username_password!(
        &mut conn,
        &username,
        &password.password,
        Ok(reply_status!(StatusCode::UNAUTHORIZED))
    );

    let map = check!(Database::get_streams_progress(&mut conn, user_id).await);

    Ok(reply_status!(warp::reply::json(&map), StatusCode::FOUND))
}
async fn set_streams_progress(
    username: String,
    password: PasswordQuery,
    progress: HashMap<i64, f64>,
) -> Result<warp::reply::Response, warp::Rejection> {
    let mut conn = get_conn!();

    let user_id = check_username_password!(
        &mut conn,
        &username,
        &password.password,
        Ok(reply_status!(StatusCode::UNAUTHORIZED))
    );

    check!(Database::update_streams_progress(&mut conn, user_id, progress, Utc::now()).await);

    Ok(warp::reply().into_response())
}

async fn get_possible_games() -> Result<warp::reply::Json, warp::Rejection> {
    let possible_games = check!(Database::get_possible_games(conn!()).await);
    Ok(warp::reply::json(&possible_games))
}

async fn add_possible_game(
    info: HashMap<String, String>,
) -> Result<warp::reply::Json, warp::Rejection> {
    let game_info = {
        let name = check!(info.get("name").ok_or_else(|| anyhow!("name required")));
        let twitch_name = info.get("twitchName");
        let platform = info.get("platform");

        check!(
            Database::insert_possible_game(
                conn!(),
                name.to_owned(),
                twitch_name.cloned(),
                platform.cloned()
            )
            .await
        )
    };

    Ok(warp::reply::json(&game_info))
}

async fn get_possible_persons() -> Result<warp::reply::Json, warp::Rejection> {
    let possible_persons = check!(Database::get_possible_persons(conn!()).await);
    Ok(warp::reply::json(&possible_persons))
}

async fn rescan_streams() -> Result<impl warp::Reply, warp::Rejection> {
    check!(scan_streams().await);
    Ok("scanned streams")
}

async fn get_all_clips(
    hashes: HashMap<String, String>,
) -> Result<warp::reply::Json, warp::Rejection> {
    _get_clips(None, hashes).await
}

#[derive(Clone, Debug, Deserialize)]
struct ClipViewParams {
    pub username: String,
    pub password: String,
}
async fn add_clip_view(
    clip_id: i64,
    params: ClipViewParams,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut conn = get_conn!();

    let user_id: Option<i64> = match params.username.as_str() {
        "" => None,
        username => match check!(Database::get_userid_by_username(&mut conn, username).await) {
            None => return Err(warp::reject()),
            Some(id) => Some(id),
        },
    };
    let password_match = match user_id {
        None => true,
        Some(user_id) => {
            check!(Database::check_password(&mut conn, user_id, &params.password).await)
        }
    };
    if !password_match {
        return Err(warp::reject());
    }

    check!(Database::add_clip_view(&mut conn, clip_id, user_id).await);

    Ok(warp::reply::with_status(warp::reply(), StatusCode::CREATED))
}

async fn create_clip(
    password: PasswordQuery,
    clip_request: CreateClipRequest,
) -> Result<warp::reply::Json, warp::Rejection> {
    let mut conn = get_conn!();

    let n: Option<String> = None; // HACK
    let user_id = check_username_password!(
        &mut conn,
        &clip_request.author_username,
        &password.password,
        Ok(warp::reply::json(&n))
    );

    let clip = check!(Database::create_clip(&mut conn, user_id, clip_request).await);

    {
        let sender = SENDER.get().unwrap();
        sender.send(Job::ClipPreview { clip_id: clip.id }).unwrap();
        sender
            .send(Job::ClipThumbnail { clip_id: clip.id })
            .unwrap();
    }

    Ok(warp::reply::json(&clip))
}

async fn update_clip(
    clip_id: i64,
    password: PasswordQuery,
    clip_request: CreateClipRequest,
) -> Result<warp::reply::Json, warp::Rejection> {
    let mut conn = get_conn!();

    let n: Option<String> = None; // HACK
    let user_id = check_username_password!(
        &mut conn,
        &clip_request.author_username,
        &password.password,
        Ok(warp::reply::json(&n))
    );

    let updated = check!(Database::update_clip(&mut conn, user_id, clip_id, clip_request).await);
    if updated {
        {
            let sender = SENDER.get().unwrap();
            sender.send(Job::ClipPreview { clip_id }).unwrap();
            sender.send(Job::ClipThumbnail { clip_id }).unwrap();
        }

        Ok(warp::reply::json(&n))
    } else {
        Err(warp::reject()) // TODO
    }
}

async fn add_twitch_progress(
    username: String,
    password: PasswordQuery,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut conn = get_conn!();
    let user_id = check_username_password!(
        &mut conn,
        &username,
        &password.password,
        Err(warp::reject())
    );
    check!(Database::add_twitch_progress(&mut conn, user_id).await);
    Ok(warp::reply::with_status(warp::reply(), StatusCode::CREATED))
}

async fn check_login(
    username: String,
    password: PasswordQuery,
) -> Result<warp::reply::Response, warp::Rejection> {
    check_username_password!(conn!(), &username, &password.password, Err(warp::reject()));
    Ok(reply_status!(StatusCode::OK))
}
async fn signup(
    username: String,
    password: PasswordQuery,
) -> Result<warp::reply::Response, warp::Rejection> {
    check!(Database::signup(conn!(), &username, &password.password).await);
    Ok(reply_status!(StatusCode::CREATED))
}

pub async fn run_server() {
    let endpoints = {
        let cors = warp::cors()
            .allow_any_origin()
            .allow_methods(vec!["GET", "POST", "PUT"]);
        let log = warp::log("streamwatch");

        let api_paths = warp::path("api").and(
            (warp::get().and(warp::path!("streams")).and_then(streams))
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
                .or(warp::get()
                    .and(warp::path!("stream" / i64 / "clips"))
                    .and(warp::query())
                    .and_then(get_stream_clips))
                .or(warp::post()
                    .and(warp::path!("stream" / i64 / "rate"))
                    .and(warp::query())
                    .and(warp::body::json())
                    .and_then(rate_stream))
                .or(warp::put()
                    .and(warp::path!("stream" / i64 / "title"))
                    .and(warp::body::json())
                    .and_then(set_custom_title))
                .or(warp::get()
                    .and(warp::path!("stream" / i64 / "otherProgress"))
                    .and_then(get_stream_other_progress))
                .or(warp::get()
                    .and(warp::path!("user" / String))
                    .and(warp::query())
                    .and_then(check_login))
                .or(warp::post()
                    .and(warp::path!("user" / String))
                    .and(warp::query())
                    .and_then(signup))
                .or(warp::put()
                    .and(warp::path!("user" / String / "progress"))
                    .and(warp::query())
                    .and(warp::body::json())
                    .and_then(set_streams_progress))
                .or(warp::get()
                    .and(warp::path!("user" / String / "progress"))
                    .and(warp::query())
                    .and_then(get_streams_progress))
                .or(warp::get()
                    .and(warp::path!("user" / String / "ratings"))
                    .and(warp::query())
                    .and_then(get_stream_ratings))
                .or(warp::post()
                    .and(warp::path!("user" / String / "twitchProgress"))
                    .and(warp::query())
                    .and_then(add_twitch_progress))
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
                    .and(warp::query())
                    .and_then(get_all_clips))
                .or(warp::post()
                    .and(warp::path!("clips"))
                    .and(warp::query())
                    .and(warp::body::json())
                    .and_then(create_clip))
                .or(warp::put()
                    .and(warp::path!("clips" / i64))
                    .and(warp::query())
                    .and(warp::body::json())
                    .and_then(update_clip))
                .or(warp::post()
                    .and(warp::path!("clips" / i64 / "view"))
                    .and(warp::query())
                    .and_then(add_clip_view)),
        );
        let static_paths = warp::path("video")
            .and(warp::fs::file("./build/index.html"))
            .or(warp::path("login").and(warp::fs::file("./build/index.html")))
            .or(warp::path("watchparty").and(warp::fs::file("./build/index.html")))
            .or(warp::path("clip").and(warp::fs::file("./build/index.html")))
            .or(warp::path("clips").and(warp::fs::file("./build/index.html")))
            .or(warp::path("static").and(warp::fs::dir("./build/static")))
            .or(warp::path::end().and(warp::fs::file("./build/index.html")))
            .or(warp::path::end().and(warp::fs::dir("./build")));
        let compressed = api_paths.or(static_paths).with(warp::compression::gzip());

        let uncompressed = (warp::path("stream").and(warp::fs::dir(STREAMS_DIR)))
            .or(warp::path("preview").and(warp::fs::dir("./previews")))
            .or(warp::path("thumbnail").and(warp::fs::dir("./thumbnails")))
            .or(warp::path("scrub_thumbnail").and(warp::fs::dir("./scrub_thumbnails")));

        compressed.or(uncompressed).with(cors).with(log)
    };

    let addr = IpAddr::from_str("::0").unwrap();
    warp::serve(endpoints).run((addr, 6070)).await;
}
