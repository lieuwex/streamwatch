use crate::{DB, STREAMS_DIR};

use anyhow::Result;

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
    const VERSION: i64 = 3;

    if get_version().await? >= VERSION {
        return Ok(());
    }
    println!("running migration {}", VERSION);

    let db = DB.get().unwrap();

    let streams = db.get_streams().await?;
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
            .execute(&mut tx)
            .await?;
        }

        for jumpcut in jumpcuts {
            sqlx::query("INSERT INTO stream_jumpcuts(stream_id, at, duration) VALUES(?, ?, ?)")
                .bind(stream.info.id)
                .bind(jumpcut.at.timestamp())
                .bind(jumpcut.duration.num_seconds())
                .execute(&mut tx)
                .await?;
        }

        tx.commit().await?;
    }

    sqlx::query!(
        "UPDATE meta SET value = ? WHERE key = 'schema_version'",
        VERSION
    )
    .execute(&db.pool)
    .await?;

    Ok(())
}

async fn four() -> Result<()> {
    const VERSION: i64 = 4;

    if get_version().await? >= VERSION {
        return Ok(());
    }
    println!("running migration {}", VERSION);

    let db = DB.get().unwrap();

    let streams = db.get_streams().await?;

    {
        let mut tx = db.pool.begin().await?;
        for stream in streams {
            let has_chat = stream.info.file_name.has_chat(STREAMS_DIR).await?;

            sqlx::query("UPDATE streams SET has_chat = ? WHERE id = ?")
                .bind(has_chat)
                .bind(stream.info.id)
                .execute(&mut tx)
                .await?;
        }
        tx.commit().await?;
    }

    sqlx::query!(
        "UPDATE meta SET value = ? WHERE key = 'schema_version'",
        VERSION
    )
    .execute(&db.pool)
    .await?;

    Ok(())
}

pub async fn run() -> Result<()> {
    three().await?;
    four().await?;

    Ok(())
}
