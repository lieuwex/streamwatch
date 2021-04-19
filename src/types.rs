use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::STREAMS_DIR;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersonInfo {
    pub id: i64,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GameInfo {
    pub id: i64,
    pub name: String,
    pub platform: Option<String>,
    pub start_time: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamInfo {
    pub id: i64,
    pub file_name: String,
    pub file_size: u64,
    pub timestamp: i64,
    pub duration: f64,
    pub has_chat: bool,
    pub has_preview: bool,
    pub thumbnail_count: usize,

    pub persons: Vec<PersonInfo>,
    pub games: Vec<GameInfo>,
}

impl StreamInfo {
    pub fn preview_url(&self) -> Option<String> {
        if self.has_preview {
            Some(format!("/preview/{}/preview.webm", self.id))
        } else {
            None
        }
    }

    pub fn thumbnail_urls(&self) -> Vec<String> {
        (0..self.thumbnail_count)
            .map(|i| format!("/thumbnail/{}/{}.webp", self.id, i))
            .collect()
    }

    pub fn datetime(&self) -> DateTime<Utc> {
        Utc.timestamp(self.timestamp, 0)
    }

    pub fn chat_file_path(&self) -> PathBuf {
        let mut res = Path::new(STREAMS_DIR).join(&self.file_name);
        res.set_extension("txt.zst");
        res
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct GameItem {
    pub id: i64,
    pub start_time: f64,
}
