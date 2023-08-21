use std::borrow::BorrowMut;
use std::ops::DerefMut;

use crate::{
    db::Database,
    job_handler::{Job, SENDER},
    util::get_conn,
    DB, STREAMS_DIR,
};

use anyhow::Result;

macro_rules! version_check {
    ($version:expr) => {{
        const VERSION: i64 = $version;
        if get_version().await? >= VERSION {
            return Ok(());
        }
        println!("running migration {}", VERSION);

        || {
            let db = DB.get().unwrap();
            sqlx::query!(
                "UPDATE meta SET value = ? WHERE key = 'schema_version'",
                VERSION
            )
            .execute(&db.pool)
        }
    }};
}

async fn get_version() -> Result<i64> {
    let db = DB.get().unwrap();

    let res = sqlx::query!("select value from meta where key = 'schema_version'")
        .fetch_one(&db.pool)
        .await?
        .value
        .unwrap()
        .parse()?;
    Ok(res)
}

async fn three() -> Result<()> {
    let done = version_check!(3);

    let db = DB.get().unwrap();

    let streams = Database::get_streams(get_conn().await?.borrow_mut()).await?;
    for stream in streams {
        let (datapoints, jumpcuts) = match stream
            .info
            .file_name
            .get_extra_info_from_file(STREAMS_DIR)
            .await?
        {
            None => continue,
            Some((datapoints, jumpcuts)) => (datapoints, jumpcuts),
        };

        let mut tx = db.pool.begin().await?;

        for datapoint in datapoints {
            sqlx::query(
                "INSERT INTO stream_datapoints(stream_id, timestamp, title, viewcount) VALUES(?, ?, ?, ?)",
            )
            .bind(stream.info.id)
            .bind(datapoint.timestamp.timestamp())
            .bind(datapoint.title)
            .bind(datapoint.viewcount)
            .execute(tx.deref_mut())
            .await?;
        }

        for jumpcut in jumpcuts {
            sqlx::query("INSERT INTO stream_jumpcuts(stream_id, at, duration) VALUES(?, ?, ?)")
                .bind(stream.info.id)
                .bind(jumpcut.at.timestamp())
                .bind(jumpcut.duration.as_secs() as i64)
                .execute(tx.deref_mut())
                .await?;
        }

        tx.commit().await?;
    }

    done().await?;

    Ok(())
}

async fn four() -> Result<()> {
    let done = version_check!(4);

    let db = DB.get().unwrap();

    let streams = Database::get_streams(get_conn().await?.borrow_mut()).await?;

    {
        let mut tx = db.pool.begin().await?;
        for stream in streams {
            let has_chat = stream.info.file_name.has_chat(STREAMS_DIR).await?;

            sqlx::query("UPDATE streams SET has_chat = ? WHERE id = ?")
                .bind(has_chat)
                .bind(stream.info.id)
                .execute(tx.deref_mut())
                .await?;
        }
        tx.commit().await?;
    }

    done().await?;

    Ok(())
}

async fn five() -> Result<()> {
    let done = version_check!(5);

    let clips = Database::get_clips(get_conn().await?.borrow_mut(), None).await?;

    let total_count = clips.len();
    for (i, clip) in clips.into_iter().enumerate() {
        println!("migration 5: clip {}/{}", i + 1, total_count);

        let sender = SENDER.get().unwrap();
        sender.send(Job::ClipPreview { clip_id: clip.id })?;
        sender.send(Job::ClipThumbnail { clip_id: clip.id })?;
    }

    done().await?;

    Ok(())
}

pub async fn run() -> Result<()> {
    three().await?;
    four().await?;
    five().await?;

    Ok(())
}
