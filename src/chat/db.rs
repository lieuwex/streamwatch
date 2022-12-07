use std::borrow::BorrowMut;

use super::types::Item;

use crate::db::Database;
use crate::util::get_conn;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{json, value::to_raw_value};

pub async fn get_messages(
    stream_id: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Item>> {
    let items =
        Database::get_messages(get_conn().await?.borrow_mut(), stream_id, start, end).await?;

    let items = items
        .into_iter()
        .map(|item| {
            let content = json!({
                "type": "chat",
                "tags": {
                    "user-id": format!("db_{}", item.author_id),
                    "display-name": item.author_name,
                    "id": format!("db_{}", item.id),
                },
                "message": item.message,
            });
            Item {
                ts: item.time,
                content: to_raw_value(&content).unwrap(),
            }
        })
        .collect();
    Ok(items)
}
