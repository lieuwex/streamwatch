use chrono::{serde::ts_milliseconds, DateTime, Utc};
use serde::Serialize;
use serde_json::value::RawValue;

#[derive(Clone, Debug, Serialize)]
pub struct Item {
    #[serde(with = "ts_milliseconds")]
    pub ts: DateTime<Utc>,
    pub content: Box<RawValue>, // lazy response so we don't have to parse the json blob
}
