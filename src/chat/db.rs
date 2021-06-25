use crate::{types::DbMessageJson, DB};

use anyhow::Result;

use chrono::{DateTime, Utc};

use super::types::Item;

pub async fn get_messages(
    stream_id: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Item>> {
    let items = {
        let db = DB.get().unwrap();
        let db = db.lock().await;
        db.get_messages(stream_id, start, end).await?
    };

    Ok(items.into_iter().map(|item| Item {
        ts: item.message.time,
    }))
}
