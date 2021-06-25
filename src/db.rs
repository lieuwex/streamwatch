use crate::types::{
    GameFeature, GameInfo, GameItem, PersonInfo, StreamDatapoint, StreamInfo, StreamJumpcut,
    StreamProgress,
};

use std::collections::HashMap;

use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use sqlx::SqlitePool;

use anyhow::Result;

use chrono::Utc;

pub struct Database {
    pub pool: sqlx::SqlitePool,
}

impl Database {
    pub async fn new() -> Result<Self> {
        let pool = SqlitePool::connect("sqlite:./db.db").await?;
        Ok(Self { pool })
    }

    pub async fn get_streams(&self) -> Result<Vec<StreamInfo>> {
        let streams = sqlx::query(
            r#"
        SELECT
            s.id,
            TRIM(
                CASE
                    WHEN custom.title IS NOT NULL then custom.title
                    WHEN dp.title IS NOT NULL THEN dp.title
                    WHEN COUNT(gf.game_id) > 0 THEN GROUP_CONCAT(g.name, ", ")
                    ELSE NULL
                END
            , ' ' || char(10)) AS title,
            s.filename,
            s.filesize,
            s.ts,
            s.duration,
            (SELECT COUNT(*) FROM stream_previews WHERE stream_id = s.id) AS preview_count,
            (SELECT COUNT(*) FROM stream_thumbnails WHERE stream_id  = s.id) AS thumbnail_count,
            s.has_chat
        FROM streams AS s
        LEFT JOIN custom_stream_titles AS custom
            ON custom.stream_id = s.id
        LEFT JOIN stream_datapoints AS dp
            ON dp.stream_id = s.id AND LENGTH(dp.title) > 0
        LEFT JOIN game_features AS gf
            ON gf.stream_id = s.id AND gf.game_id <> 7
        LEFT JOIN games AS g
            ON gf.game_id = g.id
        GROUP BY s.id;
        "#,
        )
        .map(|row: SqliteRow| StreamInfo {
            id: row.get("id"),
            title: row.get("title"),
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
        })
        .fetch_all(&self.pool)
        .await?;
        Ok(streams)
    }

    pub async fn get_stream_games(&self, stream_id: i64) -> Result<Vec<GameFeature>> {
        let res = sqlx::query!("SELECT game_id,games.name,games.platform,start_time,games.twitch_name FROM game_features INNER JOIN games ON games.id = game_id WHERE stream_id = ?1 ORDER BY start_time;", stream_id)
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
            .fetch_all(&self.pool)
            .await?;
        Ok(res)
    }

    pub async fn get_stream_participations(&self, stream_id: i64) -> Result<Vec<PersonInfo>> {
        let res = sqlx::query!("SELECT person_id,persons.name FROM person_participations INNER JOIN persons ON persons.id = person_id WHERE stream_id = ?1 ORDER BY persons.name;", stream_id)
            .map(|row| PersonInfo {
                id: row.person_id,
                name: row.name,
            })
            .fetch_all(&self.pool)
            .await?;
        Ok(res)
    }

    pub async fn get_stream_datapoints(&self, stream_id: i64) -> Result<Vec<StreamDatapoint>> {
        let res = sqlx::query!(
            "SELECT * FROM stream_datapoints WHERE stream_id = ?",
            stream_id
        )
        .map(|row| StreamDatapoint {
            title: row.title,
            viewcount: row.viewcount,
            game: String::new(),
            timestamp: row.timestamp,
        })
        .fetch_all(&self.pool)
        .await?;
        Ok(res)
    }

    pub async fn get_stream_jumpcuts(&self, stream_id: i64) -> Result<Vec<StreamJumpcut>> {
        let res = sqlx::query!(
            "SELECT * FROM stream_jumpcuts WHERE stream_id = ?",
            stream_id
        )
        .map(|row| StreamJumpcut {
            at: row.at,
            duration: row.duration,
        })
        .fetch_all(&self.pool)
        .await?;
        Ok(res)
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
        let res = sqlx::query!(
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
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect();
        Ok(res)
    }

    pub async fn update_streams_progress(
        &self,
        user_id: i64,
        progress: HashMap<i64, f64>,
    ) -> Result<()> {
        let real_time = Utc::now().timestamp();

        let mut tx = self.pool.begin().await?;

        for (stream_id, time) in progress {
            sqlx::query!("INSERT INTO stream_progress(user_id, stream_id, time, real_time) VALUES(?1, ?2, ?3, ?4) ON CONFLICT DO UPDATE SET time = ?3, real_time = ?4", user_id, stream_id, time, real_time)
            .execute(&mut tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
