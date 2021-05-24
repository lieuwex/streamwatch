use crate::types::{GameFeature, GameInfo, GameItem, PersonInfo, StreamFileName, StreamInfo};

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use sqlx::{Connection, SqliteConnection};

use anyhow::Result;

pub struct Database {
    pub conn: sqlx::SqliteConnection,
}

impl Database {
    pub async fn new() -> Result<Arc<Mutex<Self>>> {
        let conn = SqliteConnection::connect("sqlite:./db.db").await?;
        Ok(Arc::new(Mutex::new(Self { conn })))
    }

    pub async fn get_streams(&mut self) -> Result<Vec<StreamInfo>> {
        let mut tx = self.conn.begin().await?;

        let streams = sqlx::query!("SELECT id,filename,filesize,ts,duration,(SELECT COUNT(*) FROM stream_previews WHERE stream_id = id) as preview_count,(SELECT COUNT(*) FROM stream_thumbnails WHERE stream_id  = id) as thumbnail_count FROM streams")
            .map(|row| {
                let file_name: StreamFileName = row.filename.into();
                let has_chat = file_name.chat_file_path().exists();

                StreamInfo {
                    id : row.id,
                    file_name,
                    file_size: row.filesize as u64,
                    timestamp: row.ts,
                    duration: f64::from(row.duration),
                    has_chat,
                    has_preview: row.preview_count > 0,
                    thumbnail_count: row.thumbnail_count as usize,
                }
            })
            .fetch_all(&mut tx)
            .await?;

        tx.commit().await?;

        Ok(streams)
    }

    pub async fn get_stream_games(&mut self, stream_id: i64) -> Result<Vec<GameFeature>> {
        let res =sqlx::query!("SELECT game_id,games.name,games.platform,start_time,games.twitch_name FROM game_features INNER JOIN games ON games.id = game_id WHERE stream_id = ?1 ORDER BY start_time;", stream_id)
            .map(|row| {
                GameFeature {
                    info: GameInfo {
                        id: row.game_id,
                        name: row.name,
                        platform: row.platform,
                        twitch_name: row.twitch_name,
                    },
                    start_time: f64::from(row.start_time),
                }
            })
            .fetch_all(&mut self.conn)
            .await?;
        Ok(res)
    }

    pub async fn get_stream_participations(&mut self, stream_id: i64) -> Result<Vec<PersonInfo>> {
        let res = sqlx::query!("SELECT person_id,persons.name FROM person_participations INNER JOIN persons ON persons.id = person_id WHERE stream_id = ?1 ORDER BY persons.name;", stream_id)
            .map(|row| PersonInfo {
                id: row.person_id,
                name: row.name,
            })
            .fetch_all(&mut self.conn)
            .await?;
        Ok(res)
    }

    pub async fn get_stream_id_by_filename(&mut self, file_name: &str) -> Option<i64> {
        sqlx::query!("SELECT id from streams where filename = ?1", file_name)
            .map(|row| row.id)
            .fetch_one(&mut self.conn)
            .await
            .ok()
    }

    pub async fn remove_stream(&mut self, stream_id: i64) -> Result<()> {
        sqlx::query!("DELETE FROM streams WHERE id = ?1", stream_id)
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn get_possible_games(&mut self) -> Result<Vec<GameInfo>> {
        let res = sqlx::query!("SELECT id,name,platform,twitch_name FROM games ORDER BY name")
            .map(|row| GameInfo {
                id: row.id,
                name: row.name,
                twitch_name: row.twitch_name,
                platform: row.platform,
            })
            .fetch_all(&mut self.conn)
            .await?;
        Ok(res)
    }
    pub async fn insert_possible_game(
        &mut self,
        name: String,
        twitch_name: Option<String>,
        platform: Option<String>,
    ) -> Result<GameInfo> {
        let res = sqlx::query!(
            "INSERT INTO GAMES(name, platform, twitch_name) VALUES(?1, ?2, ?3)",
            name,
            platform,
            twitch_name
        )
        .execute(&mut self.conn)
        .await?;
        Ok(GameInfo {
            name,
            twitch_name,
            platform,
            id: res.last_insert_rowid(),
        })
    }

    pub async fn replace_games(&mut self, stream_id: i64, items: Vec<GameItem>) -> Result<()> {
        let mut tx = self.conn.begin().await?;

        sqlx::query!("DELETE FROM game_features WHERE stream_id = ?1", stream_id)
            .execute(&mut tx)
            .await?;

        for item in items {
            sqlx::query!(
                "INSERT INTO game_features(stream_id, game_id, start_time) VALUES(?1, ?2, ?3)",
                stream_id,
                item.id,
                item.start_time
            )
            .execute(&mut tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_possible_persons(&mut self) -> Result<Vec<PersonInfo>> {
        let res = sqlx::query_as!(PersonInfo, "SELECT id,name FROM persons ORDER BY name")
            .fetch_all(&mut self.conn)
            .await?;
        Ok(res)
    }
    pub async fn replace_persons(&mut self, stream_id: i64, person_ids: Vec<i64>) -> Result<()> {
        let mut tx = self.conn.begin().await?;

        sqlx::query!(
            "DELETE FROM person_participations WHERE stream_id = ?1",
            stream_id
        )
        .execute(&mut tx)
        .await?;

        for id in person_ids {
            sqlx::query!(
                "INSERT INTO person_participations(stream_id, person_id) VALUES(?1, ?2)",
                stream_id,
                id
            )
            .execute(&mut tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_userid_by_username(&mut self, username: &str) -> Option<i64> {
        sqlx::query!("SELECT id FROM users where username = ?1", username)
            .map(|row| row.id)
            .fetch_one(&mut self.conn)
            .await
            .ok()
    }

    pub async fn get_streams_progress(&mut self, user_id: i64) -> Result<HashMap<i64, f64>> {
        let res = sqlx::query!(
            "SELECT stream_id,time FROM stream_progress WHERE user_id = ?1",
            user_id
        )
        .map(|row| (row.stream_id, f64::from(row.time)))
        .fetch_all(&mut self.conn)
        .await?
        .into_iter()
        .collect();
        Ok(res)
    }

    pub async fn update_streams_progress(
        &mut self,
        user_id: i64,
        progress: HashMap<i64, f64>,
    ) -> Result<()> {
        let mut tx = self.conn.begin().await?;

        for (stream_id, time) in progress {
            sqlx::query!("INSERT INTO stream_progress(user_id, stream_id, time) VALUES(?1, ?2, ?3) ON CONFLICT DO UPDATE SET time = ?3", user_id, stream_id, time)
            .execute(&mut tx)
            .await
            ?;
        }

        tx.commit().await?;
        Ok(())
    }
}
