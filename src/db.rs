use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use sqlx::{Connection, SqliteConnection};

use super::types::*;

pub struct Database {
    pub conn: sqlx::SqliteConnection,
}

impl Database {
    pub async fn new() -> Arc<Mutex<Self>> {
        let conn = SqliteConnection::connect("sqlite:./db.db").await.unwrap();
        Arc::new(Mutex::new(Database { conn }))
    }

    pub async fn get_streams(&mut self) -> Vec<StreamInfo> {
        let mut streams = sqlx::query!("SELECT id,filename,filesize,ts,duration,(SELECT COUNT(*) FROM stream_previews WHERE stream_id = id) as preview_count,(SELECT COUNT(*) FROM stream_thumbnails WHERE stream_id  = id) as thumbnail_count FROM streams")
            .map(|row| {
                StreamInfo {
                    id : row.id,
                    file_name: StreamFileName::from_string(row.filename),
                    file_size: row.filesize as u64,
                    timestamp: row.ts,
                    duration: row.duration as f64,
                    has_preview: row.preview_count > 0,
                    thumbnail_count: row.thumbnail_count as usize,

                    games: vec![],
                    persons: vec![],
                    has_chat: false,

                    datapoints: vec![],
                    jumpcuts: vec![],
                }
            })
            .fetch_all(&mut self.conn)
            .await
            .unwrap();

        for stream in &mut streams {
            stream.games = sqlx::query!("SELECT game_id,games.name,games.platform,start_time,games.twitch_name FROM game_features INNER JOIN games ON games.id = game_id WHERE stream_id = ?1 ORDER BY start_time;", stream.id)
                .map(|row| {
                    GameInfo {
                        id: row.game_id,
                        name: row.name,
                        platform: row.platform,
                        twitch_name: row.twitch_name,
                        start_time: row.start_time as f64,
                    }
                })
                .fetch_all(&mut self.conn)
                .await
                .unwrap();

            stream.persons = sqlx::query!("SELECT person_id,persons.name FROM person_participations INNER JOIN persons ON persons.id = person_id WHERE stream_id = ?1 ORDER BY persons.name;", stream.id)
                .map(|row| PersonInfo {
                    id: row.person_id,
                    name: row.name,
                })
                .fetch_all(&mut self.conn)
                .await
                .unwrap();

            stream.has_chat = stream.file_name.chat_file_path().exists();

            if let Some(res) = stream.get_extra_info() {
                (stream.datapoints, stream.jumpcuts) = res;
            }
        }

        streams
    }

    pub async fn get_stream_id_by_filename(&mut self, file_name: &str) -> Option<i64> {
        sqlx::query!("SELECT id from streams where filename = ?1", file_name)
            .map(|row| row.id)
            .fetch_one(&mut self.conn)
            .await
            .ok()
    }

    pub async fn remove_stream(&mut self, stream_id: i64) {
        sqlx::query!("DELETE FROM streams WHERE id = ?1", stream_id)
            .execute(&mut self.conn)
            .await
            .unwrap();
    }

    pub async fn get_possible_games(&mut self) -> Vec<GameInfo> {
        sqlx::query!("SELECT id,name,platform,twitch_name FROM games ORDER BY name")
            .map(|row| GameInfo {
                id: row.id,
                name: row.name,
                twitch_name: row.twitch_name,
                platform: row.platform,
                start_time: 0.0,
            })
            .fetch_all(&mut self.conn)
            .await
            .unwrap()
    }
    pub async fn insert_possible_game(&mut self, mut game: GameInfo) -> GameInfo {
        let res = sqlx::query!(
            "INSERT INTO GAMES(name, platform, twitch_name) VALUES(?1, ?2, ?3)",
            game.name,
            game.platform,
            game.twitch_name
        )
        .execute(&mut self.conn)
        .await
        .unwrap();
        game.id = res.last_insert_rowid();
        game
    }

    pub async fn replace_games(&mut self, stream_id: i64, items: Vec<GameItem>) {
        sqlx::query!("DELETE FROM game_features WHERE stream_id = ?1", stream_id)
            .execute(&mut self.conn)
            .await
            .unwrap();

        for item in items {
            sqlx::query!(
                "INSERT INTO game_features(stream_id, game_id, start_time) VALUES(?1, ?2, ?3)",
                stream_id,
                item.id,
                item.start_time
            )
            .execute(&mut self.conn)
            .await
            .unwrap();
        }
    }

    pub async fn get_possible_persons(&mut self) -> Vec<PersonInfo> {
        sqlx::query_as!(PersonInfo, "SELECT id,name FROM persons ORDER BY name")
            .fetch_all(&mut self.conn)
            .await
            .unwrap()
    }
    pub async fn replace_persons(&mut self, stream_id: i64, person_ids: Vec<i64>) {
        sqlx::query!(
            "DELETE FROM person_participations WHERE stream_id = ?1",
            stream_id
        )
        .execute(&mut self.conn)
        .await
        .unwrap();

        for id in person_ids {
            sqlx::query!(
                "INSERT INTO person_participations(stream_id, person_id) VALUES(?1, ?2)",
                stream_id,
                id
            )
            .execute(&mut self.conn)
            .await
            .unwrap();
        }
    }

    pub async fn get_userid_by_username(&mut self, username: &str) -> Option<i64> {
        sqlx::query!("SELECT id FROM users where username = ?1", username)
            .map(|row| row.id)
            .fetch_one(&mut self.conn)
            .await
            .ok()
    }

    pub async fn get_streams_progress(&mut self, user_id: i64) -> HashMap<i64, f64> {
        sqlx::query!(
            "SELECT stream_id,time FROM stream_progress WHERE user_id = ?1",
            user_id
        )
        .map(|row| (row.stream_id, row.time as f64))
        .fetch_all(&mut self.conn)
        .await
        .unwrap()
        .into_iter()
        .collect()
    }

    pub async fn update_streams_progress(&mut self, user_id: i64, progress: HashMap<i64, f64>) {
        for (stream_id, time) in progress {
            sqlx::query!("INSERT INTO stream_progress(user_id, stream_id, time) VALUES(?1, ?2, ?3) ON CONFLICT DO UPDATE SET time = ?3", user_id, stream_id, time)
            .execute(&mut self.conn)
            .await
            .unwrap();
        }
    }
}
