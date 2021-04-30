use serde::Serialize;

table! {
    clips (id) {
        id -> BigInt,
        stream_id -> BigInt,
        start_time -> BigInt,
        end_time -> BigInt,
        title -> Nullable<Text>,
    }
}

table! {
    game_features (stream_id, game_id) {
        stream_id -> BigInt,
        game_id -> BigInt,
        start_time -> Double,
    }
}

table! {
    games (id) {
        id -> BigInt,
        name -> Text,
        platform -> Nullable<Text>,
        twitch_name -> Nullable<Text>,
    }
}

table! {
    messages (id) {
        id -> BigInt,
        stream_id -> BigInt,
        author_id -> BigInt,
        time -> Double,
        content -> Text,
    }
}

table! {
    meta (key) {
        key -> Text,
        value -> Nullable<Text>,
    }
}

table! {
    person_participations (stream_id, person_id) {
        stream_id -> BigInt,
        person_id -> BigInt,
    }
}

table! {
    persons (id) {
        id -> BigInt,
        name -> Text,
    }
}

table! {
    stream_previews (stream_id) {
        stream_id -> BigInt,
    }
}

table! {
    stream_progress (stream_id, user_id) {
        stream_id -> BigInt,
        user_id -> BigInt,
        time -> Double,
    }
}

table! {
    stream_thumbnails (stream_id, thumb_index) {
        stream_id -> BigInt,
        thumb_index -> BigInt,
    }
}

table! {
    streams (id) {
        id -> BigInt,
        filename -> Text,
        filesize -> BigInt,
        ts -> BigInt,
        duration -> Double,
    }
}

table! {
    users (id) {
        id -> BigInt,
        username -> Text,
    }
}

joinable!(clips -> streams (stream_id));
joinable!(game_features -> games (game_id));
joinable!(game_features -> streams (stream_id));
joinable!(messages -> users (author_id));
joinable!(person_participations -> persons (person_id));
joinable!(person_participations -> streams (stream_id));
joinable!(stream_previews -> streams (stream_id));
joinable!(stream_progress -> users (user_id));
joinable!(stream_thumbnails -> streams (stream_id));

allow_tables_to_appear_in_same_query!(
    clips,
    game_features,
    games,
    messages,
    meta,
    person_participations,
    persons,
    stream_previews,
    stream_progress,
    stream_thumbnails,
    streams,
    users,
);

#[derive(Queryable, Clone, Debug)]
pub struct Stream {
    pub id: i64,
    pub filename: String,
    pub filesize: i64,
    pub ts: i64,
    pub duration: f64,
}

#[derive(Clone, Debug, Queryable, Serialize)]
pub struct Game {
    pub id: i64,
    pub name: String,
    pub platform: Option<String>,
    pub twitch_name: Option<String>,
}

#[derive(Clone, Debug, Queryable, Serialize)]
pub struct User {
    pub id: i64,
    pub username: String,
}

#[derive(Clone, Debug, Queryable, Serialize)]
pub struct Person {
    pub id: i64,
    pub name: String,
}

#[derive(Clone, Debug, Queryable, Serialize)]
pub struct PersonParticipation {
    pub stream_id: i64,
    pub person_id: i64,
}
