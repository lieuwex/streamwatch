use crate::types::DbMessage;
use crate::types::StreamJson;
use crate::types::{
    GameFeature, GameInfo, GameItem, PersonInfo, StreamFileName, StreamInfo, StreamProgress,
};

use std::collections::HashMap;
use std::time::Instant;

use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use sqlx::SqlitePool;

use anyhow::Result;

use chrono::{DateTime, Utc};

use futures::TryStreamExt;

pub struct Database {
    pub pool: sqlx::SqlitePool,
}

impl Database {
    pub async fn new() -> Result<Self> {
        let pool = SqlitePool::connect("sqlite:./db.db").await?;
        Ok(Self { pool })
    }

    fn map_stream(row: SqliteRow) -> StreamJson {
        StreamJson {
            info: StreamInfo {
                id: row.get("id"),
                title: row.get("title"),
                title_type: row.get("title_type"),
                file_name: {
                    let val: String = row.get("filename");
                    val.into()
                },
                file_size: {
                    let x: i64 = row.get("filesize");
                    x as u64
                },
                timestamp: row.get("ts"),
                duration: {
                    let x: f32 = row.get("duration");
                    f64::from(x)
                },
                has_preview: {
                    let x: i64 = row.get("preview_count");
                    x > 0
                },
                thumbnail_count: {
                    let x: i64 = row.get("thumbnail_count");
                    x as usize
                },
                has_chat: row.get("has_chat"),
            },

            persons: {
                let json: Option<String> = row.get("persons");
                json.map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap_or(vec![])
            },
            games: {
                let json: Option<String> = row.get("games");
                json.map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap_or(vec![])
            },

            datapoints: {
                let json: Option<String> = row.get("datapoints");
                json.map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap_or(vec![])
            },
            jumpcuts: {
                let json: Option<String> = row.get("jumpcuts");
                json.map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap_or(vec![])
            },
        }
    }

    pub async fn get_stream_by_id(&self, stream_id: i64) -> Result<Option<StreamJson>> {
        let instant = Instant::now();
        let stream = sqlx::query(
            r#"
        SELECT
            id,
            title,
            title_type,
            filename,
            filesize,
            ts,
            duration,
            preview_count,
            thumbnail_count,
            has_chat,
            datapoints,
            jumpcuts,
            persons,
            games
        FROM streams_view
        WHERE id = ?
        LIMIT 1
        "#,
        )
        .bind(stream_id)
        .map(Self::map_stream)
        .fetch_optional(&self.pool)
        .await?;
        println!("get_stream_by_id took {:?}", instant.elapsed());

        Ok(stream)
    }

    pub async fn get_streams(&self) -> Result<Vec<StreamJson>> {
        let instant = Instant::now();
        let streams = sqlx::query(
            r#"
        SELECT
            id,
            title,
            title_type,
            filename,
            filesize,
            ts,
            duration,
            preview_count,
            thumbnail_count,
            has_chat,
            datapoints,
            jumpcuts,
            persons,
            games
        FROM streams_view
        "#,
        )
        .map(Self::map_stream)
        .fetch_all(&self.pool)
        .await?;
        println!("get_streams took {:?}", instant.elapsed());

        Ok(streams)
    }

    pub async fn get_stream_id_by_filename(&self, file_name: &str) -> Option<i64> {
        sqlx::query!("SELECT id from streams where filename = ?1", file_name)
            .map(|row| row.id)
            .fetch_one(&self.pool)
            .await
            .ok()
    }

    pub async fn remove_stream(&self, stream_id: i64) -> Result<()> {
        sqlx::query!("DELETE FROM streams WHERE id = ?1", stream_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_possible_games(&self) -> Result<Vec<GameInfo>> {
        let res = sqlx::query!("SELECT id,name,platform,twitch_name FROM games ORDER BY name")
            .map(|row| GameInfo {
                id: row.id,
                name: row.name,
                twitch_name: row.twitch_name,
                platform: row.platform,
            })
            .fetch_all(&self.pool)
            .await?;
        Ok(res)
    }
    pub async fn insert_possible_game(
        &self,
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
        .execute(&self.pool)
        .await?;
        Ok(GameInfo {
            name,
            twitch_name,
            platform,
            id: res.last_insert_rowid(),
        })
    }

    pub async fn replace_games<I>(&self, stream_id: i64, items: I) -> Result<()>
    where
        I: IntoIterator<Item = GameItem>,
    {
        let mut tx = self.pool.begin().await?;

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

    pub async fn get_possible_persons(&self) -> Result<Vec<PersonInfo>> {
        let res = sqlx::query_as!(PersonInfo, "SELECT id,name FROM persons ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(res)
    }
    pub async fn replace_persons(&self, stream_id: i64, person_ids: Vec<i64>) -> Result<()> {
        let mut tx = self.pool.begin().await?;

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

    pub async fn get_userid_by_username(&self, username: &str) -> Option<i64> {
        sqlx::query!("SELECT id FROM users where username = ?1", username)
            .map(|row| row.id)
            .fetch_one(&self.pool)
            .await
            .ok()
    }

    pub async fn get_streams_progress(&self, user_id: i64) -> Result<HashMap<i64, StreamProgress>> {
        let res: sqlx::Result<HashMap<i64, StreamProgress>> = sqlx::query!(
            "SELECT stream_id,time,real_time FROM stream_progress WHERE user_id = ?1",
            user_id
        )
        .map(|row| {
            (
                row.stream_id,
                StreamProgress {
                    time: f64::from(row.time),
                    real_time: row.real_time,
                },
            )
        })
        .fetch(&self.pool)
        .try_collect()
        .await;
        Ok(res?)
    }

    pub async fn update_streams_progress(
        &self,
        user_id: i64,
        progress: HashMap<i64, f64>,
    ) -> Result<()> {
        let real_time = Utc::now().timestamp();

        let mut tx = self.pool.begin().await?;
        for (stream_id, time) in progress {
            sqlx::query!(
                r#"
                INSERT INTO stream_progress
                    (user_id, stream_id, time, real_time)
                VALUES
                    (?1, ?2, ?3, ?4)
                ON CONFLICT DO UPDATE SET
                    time = ?3,
                    real_time = ?4
                "#,
                user_id,
                stream_id,
                time,
                real_time
            )
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;

        Ok(())
    }

    pub async fn get_messages(
        &self,
        stream_id: i64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<DbMessage>> {
        let start = start.timestamp();
        let end = end.timestamp();

        let query = sqlx::query(
            r#"
            SELECT
                messages.id,
                author_id,
                users.username AS author_name,
                time,
                real_time,
                content
            FROM messages
            JOIN users
                ON users.id = author_id
            WHERE stream_id = ?
                AND ? <= time
                AND time <= ?;
            "#,
        )
        .bind(stream_id)
        .bind(start)
        .bind(end);

        let items = query
            .map(|row: SqliteRow| DbMessage {
                id: row.get("id"),
                author_id: row.get("author_id"),
                author_name: row.get("author_name"),
                message: row.get("content"),
                time: row.get("time"),
                real_time: row.get("real_time"),
            })
            .fetch_all(&self.pool)
            .await?;
        Ok(items)
    }
}
