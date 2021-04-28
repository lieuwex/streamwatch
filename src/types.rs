use std::fs::read_to_string;
use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone, Utc};

use serde::{Deserialize, Serialize};

use serde_yaml;

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
    pub twitch_name: Option<String>,
    pub platform: Option<String>,
    pub start_time: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamDatapoint {
    pub title: String,
    pub viewcount: i64,
    pub game: String,
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamJumpcut {
    pub timestamp: i64,
    pub amount: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamFileName(String);
impl StreamFileName {
    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn chat_file_path(&self) -> PathBuf {
        let mut res = Path::new(STREAMS_DIR).join(&self.0);
        res.set_extension("txt.zst");
        res
    }

    fn extra_info_file_path(&self) -> PathBuf {
        let mut res = Path::new(STREAMS_DIR).join(&self.0);
        res.set_extension("yaml");
        res
    }

    pub fn get_extra_info(&self) -> Option<(Vec<StreamDatapoint>, Vec<StreamJumpcut>)> {
        read_to_string(self.extra_info_file_path())
            .ok()
            .and_then(|s| serde_yaml::from_str(&s).ok())
            .map(|info: serde_yaml::Value| {
                let info = info.as_mapping().unwrap();

                let datapoints = info.get(&"datapoints".into()).unwrap();
                let datapoints = serde_yaml::from_value(datapoints.to_owned()).unwrap();

                let jumpcuts = info.get(&"jumpcuts".into()).unwrap();
                let jumpcuts = serde_yaml::from_value(jumpcuts.to_owned()).unwrap();

                (datapoints, jumpcuts)
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamInfo {
    pub id: i64,
    pub file_name: StreamFileName,
    pub file_size: u64,
    pub timestamp: i64,
    pub duration: f64,
    pub has_chat: bool,
    pub has_preview: bool,
    pub thumbnail_count: usize,

    pub persons: Vec<PersonInfo>,
    pub games: Vec<GameInfo>,

    pub datapoints: Vec<StreamDatapoint>,
    pub jumpcuts: Vec<StreamJumpcut>,
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

    pub fn get_extra_info(&self) -> Option<(Vec<StreamDatapoint>, Vec<StreamJumpcut>)> {
        self.file_name.get_extra_info()
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct GameItem {
    pub id: i64,
    pub start_time: f64,
}
