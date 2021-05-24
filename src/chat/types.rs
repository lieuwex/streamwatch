use serde::Serialize;
use serde_json::value::RawValue;

#[derive(Clone, Debug, Serialize)]
pub struct Item {
    pub ts: usize,
    pub content: Box<RawValue>, // lazy response so we don't have to parse the json blob
}
