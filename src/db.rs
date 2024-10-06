use crate::create_preview::SCRUB_PER_SECS;
use crate::loudness::LoudnessDatapoint;
use crate::util::timestamp;

use streamwatch_shared::types::{
    Clip, ConversionProgress, CreateClipRequest, DbMessage, GameInfo, GameItem, HypeDatapoint,
    PersonInfo, StreamInfo, StreamJson, StreamProgress,
};

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::time::{Duration, Instant};

use sqlx::sqlite::{SqliteConnection, SqlitePool, SqliteRow};
use sqlx::{Connection, Row};

use anyhow::{bail, Result};

use chrono::{DateTime, Utc};

use futures::TryStreamExt;

#[derive(Debug)]
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
                timestamp: timestamp(row.get("ts")),
                inserted_at: {
                    let inserted_at: Option<i64> = row.get("inserted_at");
                    inserted_at.map(|x| timestamp(x))
                },
                duration: Duration::from_secs_f64(row.get("duration")),
                has_preview: {
                    let x: i64 = row.get("preview_count");
                    x > 0
                },
                thumbnail_count: {
                    let x: i64 = row.get("thumbnail_count");
                    x as usize
                },
                scrub_thumbnail_count: {
                    let duration: f64 = row.get("duration");
                    (duration / SCRUB_PER_SECS) as usize
                },
                has_chat: row.get("has_chat"),
                hype_average: row.get("hype_average"),
            },

            persons: {
                let json: Option<String> = row.get("persons");
                json.map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap_or_default()
            },
            games: {
                let json: Option<String> = row.get("games");
                json.map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap_or_default()
            },

            datapoints: {
                let json: Option<String> = row.get("datapoints");
                json.map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap_or_default()
            },
            jumpcuts: {
                let json: Option<String> = row.get("jumpcuts");
                json.map(|json| serde_json::from_str(&json).unwrap())
                    .unwrap_or_default()
            },
        }
    }

    pub async fn get_stream_by_id(
        conn: &mut SqliteConnection,
        stream_id: i64,
    ) -> Result<Option<StreamJson>> {
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
            inserted_at,
            duration,
            preview_count,
            thumbnail_count,
            has_chat,
            hype_average,
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
        .fetch_optional(conn.borrow_mut())
        .await?;
        println!("get_stream_by_id took {:?}", instant.elapsed());

        Ok(stream)
    }

    pub async fn get_streams(conn: &mut SqliteConnection) -> Result<Vec<StreamJson>> {
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
            inserted_at,
            duration,
            preview_count,
            thumbnail_count,
            has_chat,
            hype_average,
            datapoints,
            jumpcuts,
            persons,
            games
        FROM streams_view
        "#,
        )
        .map(Self::map_stream)
        .fetch_all(conn.borrow_mut())
        .await?;
        println!("get_streams took {:?}", instant.elapsed());

        Ok(streams)
    }

    pub async fn get_stream_id_by_filename(
        conn: &mut SqliteConnection,
        file_name: &str,
    ) -> Option<i64> {
        sqlx::query!("SELECT id from streams where filename = ?1", file_name)
            .map(|row| row.id)
            .fetch_one(conn.borrow_mut())
            .await
            .ok()
    }

    pub async fn remove_stream(conn: &mut SqliteConnection, stream_id: i64) -> Result<()> {
        sqlx::query!("DELETE FROM streams WHERE id = ?1", stream_id)
            .execute(conn.borrow_mut())
            .await?;
        Ok(())
    }

    pub async fn get_processing_streams(
        conn: &mut SqliteConnection,
    ) -> Result<Vec<ConversionProgress>> {
        let items = sqlx::query!("SELECT * FROM stream_conversion_progress")
            .map(|row| ConversionProgress {
                id: row.id,
                filename: row.filename,
                total: Duration::from_secs_f64(row.total),
                started_at: row.started_at.map(|ts| timestamp(ts)),
                updated_at: row.updated_at.map(|ts| timestamp(ts)),
                datapoint_title: row.datapoint_title,
                games: row.games,
                progress: Duration::from_secs_f64(row.progress),
                eta: row.eta,
                finished: row.finished,
                ts: timestamp(row.ts),
            })
            .fetch_all(conn.borrow_mut())
            .await?;
        Ok(items)
    }

    pub async fn get_possible_games(conn: &mut SqliteConnection) -> Result<Vec<GameInfo>> {
        let res = sqlx::query!("SELECT id,name,platform,twitch_name FROM games ORDER BY name")
            .map(|row| GameInfo {
                id: row.id,
                name: row.name,
                twitch_name: row.twitch_name,
                platform: row.platform,
            })
            .fetch_all(conn.borrow_mut())
            .await?;
        Ok(res)
    }
    pub async fn insert_possible_game<'c>(
        conn: &mut SqliteConnection,
        name: String,
        twitch_name: Option<String>,
        platform: Option<String>,
    ) -> Result<GameInfo> {
        let inserted_at = Utc::now().timestamp();
        let res = sqlx::query!(
            "INSERT INTO games(name, platform, twitch_name, inserted_at) VALUES(?1, ?2, ?3, ?4)",
            name,
            platform,
            twitch_name,
            inserted_at
        )
        .execute(conn)
        .await?;
        Ok(GameInfo {
            name,
            twitch_name,
            platform,
            id: res.last_insert_rowid(),
        })
    }

    pub async fn replace_games<I>(
        conn: &mut SqliteConnection,
        stream_id: i64,
        items: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = GameItem>,
    {
        let real_time = Utc::now().timestamp();
        let mut tx = conn.begin().await?;

        sqlx::query!("DELETE FROM game_features WHERE stream_id = ?1", stream_id)
            .execute(tx.deref_mut())
            .await?;

        for item in items {
            let start_time = item.start_time.as_secs_f64();

            sqlx::query!(
                "INSERT INTO game_features(stream_id, game_id, start_time, inserted_at) VALUES(?1, ?2, ?3, ?4)",
                stream_id,
                item.id,
                start_time,
                real_time,
            )
            .execute(tx.deref_mut())
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_possible_persons(conn: &mut SqliteConnection) -> Result<Vec<PersonInfo>> {
        let res = sqlx::query_as!(PersonInfo, "SELECT id,name FROM persons ORDER BY name")
            .fetch_all(conn.borrow_mut())
            .await?;
        Ok(res)
    }
    pub async fn replace_persons(
        conn: &mut SqliteConnection,
        stream_id: i64,
        person_ids: Vec<i64>,
    ) -> Result<()> {
        let real_time = Utc::now().timestamp();
        let mut tx = conn.begin().await?;

        sqlx::query!(
            "DELETE FROM person_participations WHERE stream_id = ?1",
            stream_id
        )
        .execute(tx.deref_mut())
        .await?;

        for id in person_ids {
            sqlx::query!(
                "INSERT INTO person_participations(stream_id, person_id, inserted_at) VALUES(?1, ?2, ?3)",
                stream_id,
                id,
                real_time,
            )
            .execute(tx.deref_mut())
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn signup(conn: &mut SqliteConnection, username: &str, password: &str) -> Result<()> {
        let mut tx = conn.begin().await?;

        let username_taken = Self::get_userid_by_username(tx.borrow_mut(), username)
            .await?
            .is_some();
        if username_taken {
            bail!("username already taken");
        }

        let created_at = Utc::now().timestamp();

        sqlx::query!(
            r#"
            INSERT INTO users
                (username, password, inserted_at)
            VALUES
                (?1, ?2, ?3)
            "#,
            username,
            password,
            created_at,
        )
        .execute(tx.deref_mut())
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_userid_by_username(
        conn: &mut SqliteConnection,
        username: &str,
    ) -> Result<Option<i64>> {
        let res = sqlx::query!("SELECT id FROM users where username = ?1", username)
            .map(|row| row.id)
            .fetch_optional(conn.borrow_mut())
            .await?;
        Ok(res)
    }

    pub async fn check_password(
        conn: &mut SqliteConnection,
        user_id: i64,
        password: &str,
    ) -> Result<bool> {
        let db_pass: Option<String> =
            sqlx::query!("SELECT password FROM users WHERE id = ?1", user_id)
                .map(|row| row.password)
                .fetch_one(conn.borrow_mut())
                .await?;

        Ok(db_pass.map(|db_pass| password == db_pass).unwrap_or(true))
    }

    pub async fn get_streams_progress(
        conn: &mut SqliteConnection,
        user_id: i64,
    ) -> Result<HashMap<i64, StreamProgress>> {
        let res: sqlx::Result<HashMap<i64, StreamProgress>> =
            sqlx::query("SELECT stream_id,time,real_time FROM stream_progress WHERE user_id = ?1")
                .bind(user_id)
                .map(|row: SqliteRow| {
                    (
                        row.get("stream_id"),
                        StreamProgress {
                            time: Duration::from_secs_f64(row.get("time")),
                            real_time: timestamp(row.get("real_time")),
                        },
                    )
                })
                .fetch(conn.borrow_mut())
                .try_collect()
                .await;
        Ok(res?)
    }

    pub async fn update_streams_progress(
        conn: &mut SqliteConnection,
        user_id: i64,
        progress: HashMap<i64, f64>,
        real_time: DateTime<Utc>,
    ) -> Result<()> {
        let real_time = real_time.timestamp();

        let mut tx = conn.begin().await?;
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
            .execute(tx.deref_mut())
            .await?;

            sqlx::query!(
                r#"
                INSERT INTO stream_progress_updates
                    (user_id, stream_id, time, real_time)
                VALUES
                    (?1, ?2, ?3, ?4)
                "#,
                user_id,
                stream_id,
                time,
                real_time
            )
            .execute(tx.deref_mut())
            .await?;
        }
        tx.commit().await?;

        Ok(())
    }

    pub async fn get_messages(
        conn: &mut SqliteConnection,
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
                time: timestamp(row.get("time")),
                real_time: timestamp(row.get("real_time")),
            })
            .fetch_all(conn.borrow_mut())
            .await?;
        Ok(items)
    }

    pub async fn get_ratings(
        conn: &mut SqliteConnection,
        user_id: i64,
    ) -> Result<HashMap<i64, i8>> {
        let map: HashMap<i64, i8> = sqlx::query!(
            "SELECT stream_id,rating FROM stream_ratings WHERE user_id = ?1",
            user_id
        )
        .map(|row| (row.stream_id, row.rating as i8))
        .fetch(conn.borrow_mut())
        .try_collect()
        .await?;
        Ok(map)
    }

    pub async fn set_stream_rating(
        conn: &mut SqliteConnection,
        stream_id: i64,
        user_id: i64,
        score: i8,
    ) -> Result<()> {
        if score == 0 {
            sqlx::query!(
                "DELETE FROM stream_ratings WHERE user_id = ?1 AND stream_id = ?2",
                user_id,
                stream_id
            )
            .execute(conn.borrow_mut())
            .await?;

            return Ok(());
        }

        let real_time = Utc::now().timestamp();

        sqlx::query!(
            r#"
            INSERT INTO stream_ratings
                (user_id, stream_id, rating, real_time)
            VALUES
                (?1, ?2, ?3, ?4)
            ON CONFLICT DO UPDATE SET
                rating = ?3,
                real_time = ?4
            "#,
            user_id,
            stream_id,
            score,
            real_time
        )
        .execute(conn.borrow_mut())
        .await?;

        Ok(())
    }

    pub async fn set_custom_stream_title(
        conn: &mut SqliteConnection,
        stream_id: i64,
        title: String,
    ) -> Result<()> {
        let inserted_at = Utc::now().timestamp();

        if title.is_empty() {
            sqlx::query!(
                "DELETE FROM custom_stream_titles WHERE stream_id = ?1",
                stream_id
            )
            .execute(conn.borrow_mut())
            .await?;
        } else {
            sqlx::query!(
                r#"
                INSERT INTO custom_stream_titles
                    (stream_id, title, inserted_at)
                VALUES
                    (?1, ?2, ?3)
                ON CONFLICT DO UPDATE SET
                    title = ?2,
                    inserted_at = ?3
                "#,
                stream_id,
                title,
                inserted_at
            )
            .execute(conn.borrow_mut())
            .await?;
        }

        Ok(())
    }

    pub async fn get_hype_datapoints(
        conn: &mut SqliteConnection,
        stream_id: i64,
    ) -> Result<Vec<HypeDatapoint>> {
        let res = sqlx::query(
            "SELECT ts,loudness,messages FROM stream_hype_datapoints_sad WHERE stream_id = ?",
        )
        .bind(stream_id)
        .map(|row: SqliteRow| HypeDatapoint {
            ts: timestamp(row.get("ts")),
            loudness: row.get("loudness"),
            chat_hype: row.get("messages"),
            hype: {
                let loudness: Option<f64> = row.get("loudness");
                let messages: Option<i32> = row.get("messages");

                loudness
                    .map(|m| 1.0 / 8.0 * (1.0 + (5.0 * (m + 75.0 / 2.0) / 80.0).tanh()))
                    .unwrap_or(0.0)
                    + messages.map(|m| (m as f64) / 5.0).unwrap_or(0.0)
            },
        })
        .fetch_all(conn.borrow_mut())
        .await?;

        Ok(res)
    }

    pub async fn set_stream_decibels(
        conn: &mut SqliteConnection,
        stream_id: i64,
        datapoints: Vec<(i64, f64)>,
    ) -> Result<()> {
        let mut tx = conn.begin().await?;

        sqlx::query!(
            "DELETE FROM stream_decibels WHERE stream_id = ?1",
            stream_id
        )
        .execute(tx.deref_mut())
        .await?;

        for (ts, db) in datapoints {
            sqlx::query!(
                "INSERT INTO stream_decibels(stream_id, ts, db) VALUES(?1, ?2, ?3)",
                stream_id,
                ts,
                db,
            )
            .execute(tx.deref_mut())
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn set_stream_loudness(
        conn: &mut SqliteConnection,
        stream_id: i64,
        datapoints: Vec<LoudnessDatapoint>,
    ) -> Result<()> {
        let mut tx = conn.begin().await?;

        sqlx::query!(
            "DELETE FROM stream_loudness WHERE stream_id = ?1",
            stream_id
        )
        .execute(tx.deref_mut())
        .await?;

        for dp in datapoints {
            let ts = dp.ts.timestamp();

            sqlx::query!(
                "INSERT INTO stream_loudness(stream_id, ts, momentary, short_term, integrated, lra) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                stream_id,
                ts,
                dp.momentary,
                dp.short_term,
                dp.integrated,
                dp.lra,
            )
            .execute(tx.deref_mut())
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn set_stream_chatspeed_datapoints<I>(
        conn: &mut SqliteConnection,
        stream_id: i64,
        datapoints: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (DateTime<Utc>, i64)>,
    {
        let mut tx = conn.begin().await?;

        sqlx::query!(
            "DELETE FROM stream_chatspeed_datapoints WHERE stream_id = ?1",
            stream_id
        )
        .execute(tx.deref_mut())
        .await?;

        for (ts, messages) in datapoints {
            let ts = ts.timestamp();

            sqlx::query!(
                "INSERT INTO stream_chatspeed_datapoints(stream_id, ts, messages) VALUES(?1, ?2, ?3)",
                stream_id,
                ts,
                messages,
            )
            .execute(tx.deref_mut())
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_clips(
        conn: &mut SqliteConnection,
        stream_id: Option<i64>,
    ) -> Result<Vec<Clip>> {
        let mut sql = String::from(
            r#"
                SELECT
                    clips.*,
                    users.username,
                    (SELECT COUNT(*) FROM clip_views WHERE clip_id=clips.id) AS view_count
                FROM clips
                JOIN users
                    ON users.id=clips.author_id
            "#,
        );

        let query = if let Some(stream_id) = stream_id {
            sql += " WHERE stream_id = ?";
            sqlx::query(&sql).bind(stream_id)
        } else {
            sqlx::query(&sql)
        };

        let items = query
            .map(|row: SqliteRow| Clip {
                id: row.get("id"),
                author_id: row.get("author_id"),
                author_username: row.get("username"),
                stream_id: row.get("stream_id"),
                start_time: Duration::from_millis(row.get::<i64, _>("start_time") as u64),
                duration: Duration::from_millis(row.get::<i64, _>("duration") as u64),
                title: row.get("title"),
                created_at: row.get("created_at"),
                view_count: row.get("view_count"),
            })
            .fetch_all(conn.borrow_mut())
            .await?;
        Ok(items)
    }

    pub async fn create_clip(
        conn: &mut SqliteConnection,
        author_id: i64,
        clip_request: CreateClipRequest,
    ) -> Result<Clip> {
        let created_at = Utc::now().timestamp();
        let start_time = clip_request.start_time.as_millis() as i64;
        let duration = clip_request.duration.as_millis() as i64;

        let res = sqlx::query!(
            r#"
            INSERT INTO clips
                (author_id, stream_id, start_time, duration, title, created_at)
            VALUES
                (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            author_id,
            clip_request.stream_id,
            start_time,
            duration,
            clip_request.title,
            created_at,
        )
        .execute(conn.borrow_mut())
        .await?;

        Ok(Clip {
            id: res.last_insert_rowid(),
            author_id,
            author_username: clip_request.author_username,
            stream_id: clip_request.stream_id,
            start_time: clip_request.start_time,
            duration: clip_request.duration,
            title: clip_request.title,
            created_at,
            view_count: 0, // we just created the clip, so view_count=0 is always valid.
        })
    }

    pub async fn update_clip(
        conn: &mut SqliteConnection,
        author_id: i64,
        clip_id: i64,
        clip_request: CreateClipRequest,
    ) -> Result<bool> {
        let start_time = clip_request.start_time.as_millis() as i64;
        let duration = clip_request.duration.as_millis() as i64;

        let res = sqlx::query!(
            r#"
            UPDATE clips
            SET
                start_time=?1,
                duration=?2,
                title=?3
            WHERE
                id=?4 AND author_id=?5
            "#,
            start_time,
            duration,
            clip_request.title,
            clip_id,
            author_id,
        )
        .execute(conn.borrow_mut())
        .await?;

        Ok(res.rows_affected() > 0)
    }

    pub async fn add_clip_view(
        conn: &mut SqliteConnection,
        clip_id: i64,
        user_id: Option<i64>,
    ) -> Result<()> {
        let timestamp = Utc::now().timestamp();

        sqlx::query!(
            r#"
            INSERT INTO clip_views
                (clip_id, user_id, real_time)
            VALUES
                (?1, ?2, ?3)
            "#,
            clip_id,
            user_id,
            timestamp,
        )
        .execute(conn.borrow_mut())
        .await?;

        Ok(())
    }

    pub async fn add_twitch_progress(conn: &mut SqliteConnection, user_id: i64) -> Result<()> {
        let timestamp = Utc::now().timestamp();

        sqlx::query!(
            r#"
            INSERT INTO twitch_progress
                (user_id, real_time)
            VALUES
                (?1, ?2)
            "#,
            user_id,
            timestamp,
        )
        .execute(conn.borrow_mut())
        .await?;

        Ok(())
    }

    pub async fn convert_twitch_progress(
        conn: &mut SqliteConnection,
        stream_id: i64,
    ) -> Result<()> {
        let mut tx = conn.begin().await?;

        let stream = Database::get_stream_by_id(&mut tx, stream_id)
            .await?
            .unwrap();

        let stream_start = stream.info.timestamp.timestamp();
        let stream_end = stream_start + (stream.info.duration.as_secs() as i64);

        let items = sqlx::query!(
            "SELECT user_id,real_time FROM twitch_progress WHERE real_time BETWEEN ?1 AND ?2 ORDER BY real_time ASC",
            stream_start,
            stream_end
        )
        .map(|row| (row.user_id, row.real_time))
        .fetch_all(tx.deref_mut())
        .await?;

        for (user_id, ts) in items {
            let time = ts - stream_start;
            assert!(time >= 0);
            assert!(time as u64 <= stream.info.duration.as_secs());

            Database::update_streams_progress(
                &mut tx,
                user_id,
                HashMap::from([(stream_id, time as f64)]),
                timestamp(ts),
            )
            .await?;

            println!("[{stream_id}] set progress for {user_id} to {time}");
        }

        // TODO: remove twitch_progress we imported

        tx.commit().await?;
        Ok(())
    }

    pub async fn insert_api_call(
        conn: &mut SqliteConnection,
        username: &str,
        function_name: &str,
    ) -> Result<()> {
        let ts = Utc::now().timestamp();
        sqlx::query!(
            "INSERT INTO api_calls(username, function_name, ts) values(?1, ?2, ?3)",
            username,
            function_name,
            ts,
        )
        .execute(conn)
        .await?;
        Ok(())
    }
}
