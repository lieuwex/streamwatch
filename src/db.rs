use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::params;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use super::schema::*;
use super::types::*;

pub struct Database {
    pub conn: rusqlite::Connection,
    pub conn2: SqliteConnection,
}

impl Database {
    pub fn new() -> Arc<Mutex<Self>> {
        let conn = rusqlite::Connection::open("./db.db").unwrap();
        //conn.execute("PRAGMA foreign_keys = ?1", params![1])
        //    .unwrap();

        let conn2 = SqliteConnection::establish("./db.db").unwrap();

        Arc::new(Mutex::new(Database { conn, conn2 }))
    }

    /*
    pub fn get_streams(&self) -> Vec<StreamInfo> {
        use diesel::dsl::{count_star, select};

        let mut streams: Vec<StreamInfo> = {
            use crate::schema;
            use crate::schema::stream_previews::dsl::stream_previews;
            use crate::schema::stream_thumbnails::dsl::stream_thumbnails;
            use crate::schema::streams::dsl::streams;

            let stream_previews_q = stream_previews
                .filter(schema::stream_previews::stream_id.eq(schema::streams::id))
                .select(count_star().gt(0))
                .single_value();
            let stream_thumbnails_q = stream_thumbnails
                .filter(schema::stream_thumbnails::stream_id.eq(schema::streams::id))
                .count()
                .single_value();

            streams
                .inner_join(stream_previews)
                .inner_join(stream_thumbnails)
                .load(&self.conn2)
                .unwrap()
                .into_iter()
                .map(
                    |(
                        (id, file_name, file_size, ts, duration),
                        (preview_count, thumbnail_count),
                        (preview_count, thumbnail_count),
                    )| {
                        StreamInfo {
                            id,
                            file_name,
                            file_size: file_size as u64,
                            timestamp: ts as u64,
                            duration,
                            has_preview: preview_count > 0,
                            thumbnail_count: thumbnail_count as usize,

                            games: vec![],
                            persons: vec![],
                            has_chat: false,
                        }
                    },
                )
                .collect()

            /*
            select((streams, stream_previews_q, stream_thumbnails_q))
                .load::<(i64, String, i64, i64, f64, bool, i32)>(&self.conn2)
                .unwrap()
                .into_iter()
                .map(
                    |(id, file_name, file_size, ts, duration, has_preview, thumbnail_count)| {
                        StreamInfo {
                            id,
                            file_name,
                            file_size: file_size as u64,
                            timestamp: ts as u64,
                            duration,
                            has_preview,
                            thumbnail_count: thumbnail_count as usize,

                            games: vec![],
                            persons: vec![],
                            has_chat: false,
                        }
                    },
                )
                .collect()
            */
        };

        let mut games_stmt = self.conn
            .prepare("SELECT game_id,games.name,games.platform,start_time FROM game_features INNER JOIN games ON games.id = game_id WHERE stream_id = ?1 ORDER BY start_time;")
            .unwrap();

        let mut persons_stmt = self.conn
            .prepare("SELECT person_id,persons.name FROM person_participations INNER JOIN persons ON persons.id = person_id WHERE stream_id = ?1 ORDER BY persons.name;")
            .unwrap();

        for stream in &mut streams {
            let it = games_stmt
                .query_map(params![stream.id as i64], |row| {
                    let id: i64 = row.get(0).unwrap();
                    let name: String = row.get(1).unwrap();
                    let platform: Option<String> = row.get(2).unwrap();
                    let start_time: f64 = row.get(3).unwrap();

                    Ok(GameInfo {
                        id,
                        name,
                        platform,
                        start_time,
                    })
                })
                .unwrap();
            stream.games = it.map(|x| x.unwrap()).collect();

            let it = persons_stmt
                .query_map(params![stream.id as i64], |row| {
                    let id: i64 = row.get(0).unwrap();
                    let name: String = row.get(1).unwrap();

                    Ok(PersonInfo { id, name })
                })
                .unwrap();
            stream.persons = it.map(|x| x.unwrap()).collect();

            stream.has_chat = stream.chat_file_path().exists();
        }

        streams
    }
    */

    pub fn get_streams(&self) -> Vec<StreamInfo> {
        let mut stmt = self
                .conn
                .prepare("SELECT id,filename,filesize,ts,duration,(SELECT COUNT(*) > 0 FROM stream_previews WHERE stream_id = id),(SELECT COUNT(*) FROM stream_thumbnails WHERE stream_id  = id) FROM streams")
                .unwrap();
        let it = stmt
            .query_map(params![], |row| {
                let id: i64 = row.get(0).unwrap();
                let file_name: String = row.get(1).unwrap();
                let file_size: i64 = row.get(2).unwrap();
                let ts: i64 = row.get(3).unwrap();
                let duration: f64 = row.get(4).unwrap();
                let has_preview: bool = row.get(5).unwrap();
                let thumbnail_count: i64 = row.get(6).unwrap();

                Ok(StreamInfo {
                    id,
                    file_name: StreamFileName::from_string(file_name),
                    file_size: file_size as u64,
                    timestamp: ts,
                    duration,
                    has_preview,
                    thumbnail_count: thumbnail_count as usize,

                    games: vec![],
                    persons: vec![],
                    has_chat: false,

                    datapoints: vec![],
                    jumpcuts: vec![],
                })
            })
            .unwrap();
        let mut streams: Vec<_> = it.map(|x| x.unwrap()).collect();

        let mut games_stmt = self.conn
            .prepare("SELECT game_id,games.name,games.platform,start_time,games.twitch_name FROM game_features INNER JOIN games ON games.id = game_id WHERE stream_id = ?1 ORDER BY start_time;")
            .unwrap();

        let mut persons_stmt = self.conn
                .prepare("SELECT person_id,persons.name FROM person_participations INNER JOIN persons ON persons.id = person_id WHERE stream_id = ?1 ORDER BY persons.name;")
                .unwrap();

        for stream in &mut streams {
            let it = games_stmt
                .query_map(params![stream.id as i64], |row| {
                    let id: i64 = row.get(0).unwrap();
                    let name: String = row.get(1).unwrap();
                    let platform: Option<String> = row.get(2).unwrap();
                    let start_time: f64 = row.get(3).unwrap();
                    let twitch_name: Option<String> = row.get(4).unwrap();

                    Ok(GameInfo {
                        id,
                        name,
                        platform,
                        start_time,
                        twitch_name,
                    })
                })
                .unwrap();
            stream.games = it.map(|x| x.unwrap()).collect();

            let it = persons_stmt
                .query_map(params![stream.id as i64], |row| {
                    let id: i64 = row.get(0).unwrap();
                    let name: String = row.get(1).unwrap();

                    Ok(PersonInfo { id, name })
                })
                .unwrap();
            stream.persons = it.map(|x| x.unwrap()).collect();

            stream.has_chat = stream.file_name.chat_file_path().exists();

            if let Some(res) = stream.get_extra_info() {
                (stream.datapoints, stream.jumpcuts) = res;
            }
        }

        streams
    }

    pub fn get_stream_id_by_filename(&self, file_name: &str) -> Option<i64> {
        use crate::schema::streams::dsl::*;

        streams
            .select(id)
            .filter(filename.eq(file_name))
            .first(&self.conn2)
            .ok()
    }

    pub fn remove_stream(&self, stream_id: i64) {
        use crate::schema::streams::dsl::*;

        diesel::delete(streams.filter(id.eq(stream_id)))
            .execute(&self.conn2)
            .unwrap();
    }

    pub fn get_possible_games(&self) -> Vec<Game> {
        use crate::schema::games::dsl::*;

        games.order(name).load(&self.conn2).unwrap()
    }
    pub fn insert_possible_game(&self, mut game: Game) -> Game {
        self.conn
            .execute(
                "INSERT INTO GAMES(name, platform, twitch_name) VALUES(?1, ?2, ?3)",
                params![game.name, game.platform, game.twitch_name],
            )
            .unwrap();
        game.id = self.conn.last_insert_rowid();
        game
    }

    pub fn replace_games(&self, stream_id_p: i64, items: Vec<GameItem>) {
        use crate::schema::game_features::dsl::*;

        diesel::delete(game_features.filter(stream_id.eq(stream_id_p)))
            .execute(&self.conn2)
            .unwrap();

        let records: Vec<_> = items
            .into_iter()
            .map(|g| {
                (
                    stream_id.eq(stream_id_p),
                    game_id.eq(g.id),
                    start_time.eq(g.start_time),
                )
            })
            .collect();
        diesel::insert_into(game_features)
            .values(&records)
            .execute(&self.conn2)
            .unwrap();
    }

    pub fn get_possible_persons(&self) -> Vec<Person> {
        use crate::schema::persons::dsl::*;

        persons.order_by(name).load(&self.conn2).unwrap()
    }
    pub fn replace_persons(&self, stream_id_p: i64, person_ids: Vec<i64>) {
        use crate::schema::person_participations::dsl::*;

        diesel::delete(person_participations.filter(stream_id.eq(stream_id_p)))
            .execute(&self.conn2)
            .unwrap();

        let pairs: Vec<_> = person_ids
            .into_iter()
            .map(|id| (stream_id.eq(stream_id_p), person_id.eq(id)))
            .collect();
        diesel::insert_into(person_participations)
            .values(&pairs)
            .execute(&self.conn2)
            .unwrap();
    }

    pub fn get_userid_by_username(&self, username_p: &str) -> Option<i64> {
        use crate::schema::users::dsl::*;

        users
            .select(id)
            .filter(username.eq(username_p))
            .first(&self.conn2)
            .ok()
    }

    pub fn get_streams_progress(&self, user_id_p: i64) -> HashMap<i64, f64> {
        use crate::schema::stream_progress::dsl::*;

        stream_progress
            .select((stream_id, time))
            .filter(user_id.eq(user_id_p))
            .load(&self.conn2)
            .unwrap()
            .into_iter()
            .collect()
    }

    /*
    pub fn update_streams_progress(&self, user_id_p: i64, progress: HashMap<i64, f64>) {
        use crate::schema::stream_progress::dsl::*;

        let records: Vec<_> = progress
            .into_iter()
            .map(|(stream_id_p, time_p)| {
                (
                    user_id.eq(user_id_p),
                    stream_id.eq(stream_id_p),
                    time.eq(time_p),
                )
            })
            .collect();
        diesel::insert_into(stream_progress)
            .values(&records)
            .on_conflict()
            .do_update()
            .execute(&self.conn2)
            .unwrap();
    }
    */

    pub fn update_streams_progress(&self, user_id: i64, progress: HashMap<i64, f64>) {
        for (stream_id, time) in progress {
            self.conn
                .execute(
                    "INSERT INTO stream_progress(user_id, stream_id, time) VALUES(?1, ?2, ?3) ON CONFLICT DO UPDATE SET time = ?3",
                    params![user_id, stream_id, time],
                )
               .unwrap();
        }
    }
}
