use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection};

use super::types::*;

pub struct Database {
    pub conn: Connection,
}

impl Database {
    pub fn new() -> Arc<Mutex<Self>> {
        let conn = Connection::open("./db.db").unwrap();
        //conn.execute("PRAGMA foreign_keys = ?1", params![1])
        //    .unwrap();

        Arc::new(Mutex::new(Database { conn }))
    }

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
        self.conn
            .query_row(
                "SELECT id from streams where filename = ?1",
                params![file_name],
                |row| row.get(0),
            )
            .unwrap()
    }

    pub fn remove_stream(&self, stream_id: i64) {
        self.conn
            .execute("DELETE FROM streams WHERE id = ?1", params![stream_id])
            .unwrap();
    }

    pub fn get_possible_games(&self) -> Vec<GameInfo> {
        let mut stmt = self
            .conn
            .prepare("SELECT id,name,platform,twitch_name FROM games ORDER BY name")
            .unwrap();

        stmt.query_map(params![], |row| {
            let id: i64 = row.get(0).unwrap();
            let name: String = row.get(1).unwrap();
            let platform: Option<String> = row.get(2).unwrap();
            let twitch_name: Option<String> = row.get(3).unwrap();

            Ok(GameInfo {
                id,
                name,
                platform,
                twitch_name,
                start_time: 0.0, // HACK
            })
        })
        .unwrap()
        .map(|x| x.unwrap())
        .collect()
    }
    pub fn insert_possible_game(&self, mut game: GameInfo) -> GameInfo {
        self.conn
            .execute(
                "INSERT INTO GAMES(name, platform, twitch_name) VALUES(?1, ?2, ?3)",
                params![game.name, game.platform, game.twitch_name],
            )
            .unwrap();
        game.id = self.conn.last_insert_rowid();
        game
    }

    pub fn replace_games(&self, stream_id: i64, items: Vec<GameItem>) {
        self.conn
            .execute(
                "DELETE FROM game_features WHERE stream_id = ?1",
                params![stream_id],
            )
            .unwrap();

        for item in items {
            self.conn
                .execute(
                    "INSERT INTO game_features(stream_id, game_id, start_time) VALUES(?1, ?2, ?3)",
                    params![stream_id, item.id, item.start_time],
                )
                .unwrap();
        }
    }

    pub fn get_possible_persons(&self) -> Vec<PersonInfo> {
        let mut stmt = self
            .conn
            .prepare("SELECT id,name FROM persons ORDER BY name")
            .unwrap();

        stmt.query_map(params![], |row| {
            let id: i64 = row.get(0).unwrap();
            let name: String = row.get(1).unwrap();

            Ok(PersonInfo { id, name })
        })
        .unwrap()
        .map(|x| x.unwrap())
        .collect()
    }
    pub fn replace_persons(&self, stream_id: u64, person_ids: Vec<i64>) {
        self.conn
            .execute(
                "DELETE FROM person_participations WHERE stream_id = ?1",
                params![stream_id as i64],
            )
            .unwrap();

        for id in person_ids {
            self.conn
                .execute(
                    "INSERT INTO person_participations(stream_id, person_id) VALUES(?1, ?2)",
                    params![stream_id as i64, id],
                )
                .unwrap();
        }
    }

    pub fn get_userid_by_username(&self, username: &str) -> Option<i64> {
        self.conn
            .query_row(
                "SELECT id FROM users where username = ?1",
                params![username],
                |row| row.get(0),
            )
            .ok()
    }

    pub fn get_streams_progress(&self, user_id: i64) -> HashMap<i64, f64> {
        let mut stmt = self
            .conn
            .prepare("SELECT stream_id,time FROM stream_progress WHERE user_id = ?1")
            .unwrap();

        stmt.query_map(params![user_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

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
