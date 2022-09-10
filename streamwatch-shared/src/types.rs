use crate::serde::{duration_seconds, duration_seconds_float};

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use tokio::fs::{metadata, read_to_string};

use chrono::{
    serde::{ts_seconds, ts_seconds_option},
    DateTime, Duration, Utc,
};

use serde::{Deserialize, Serialize};

use anyhow::{anyhow, Result};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PersonInfo {
    pub id: i64,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GameInfo {
    pub id: i64,
    pub name: String,
    pub twitch_name: Option<String>,
    pub platform: Option<String>,
}
impl From<GameFeature> for GameInfo {
    fn from(item: GameFeature) -> Self {
        item.info
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GameFeature {
    #[serde(flatten)]
    pub info: GameInfo,
    #[serde(with = "duration_seconds_float")]
    pub start_time: Duration,
}
impl GameFeature {
    pub const fn from_game_info(info: GameInfo, start_time: Duration) -> Self {
        Self { info, start_time }
    }
}

#[derive(Deserialize)]
pub struct GameItem {
    pub id: i64,
    #[serde(with = "duration_seconds_float")]
    pub start_time: Duration,
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamFileName(String);
impl StreamFileName {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn stream_path(&self, streams_dir: &str) -> PathBuf {
        Path::new(streams_dir).join(&self.0)
    }

    pub fn chat_file_path(&self, streams_dir: &str) -> PathBuf {
        let mut res = Path::new(streams_dir).join(&self.0);
        res.set_extension("txt.zst");
        res
    }

    pub async fn has_chat(&self, streams_dir: &str) -> Result<bool> {
        let res = metadata(self.chat_file_path(streams_dir))
            .await
            .map(|_| true)
            .or_else(|error| {
                if error.kind() == ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(error)
                }
            })?;
        Ok(res)
    }

    fn extra_info_file_path(&self, streams_dir: &str) -> PathBuf {
        let mut res = Path::new(streams_dir).join(&self.0);
        res.set_extension("yaml");
        res
    }

    pub async fn get_extra_info_from_file(
        &self,
        streams_dir: &str,
    ) -> Result<Option<(Vec<StreamDatapoint>, Vec<StreamJumpcut>)>> {
        let s = match read_to_string(self.extra_info_file_path(streams_dir)).await {
            Ok(s) => s,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(anyhow::Error::from(e)),
        };

        let info: serde_yaml::Value = serde_yaml::from_str(&s)?;
        let info = info
            .as_mapping()
            .ok_or(anyhow!("parsing error: expected mapping"))?;

        let datapoints = match info.get(&"datapoints".into()) {
            Some(value) => serde_yaml::from_value(value.to_owned())?,
            None => vec![],
        };

        let jumpcuts = match info.get(&"jumpcuts".into()) {
            Some(value) => serde_yaml::from_value(value.to_owned())?,
            None => vec![],
        };

        Ok(Some((datapoints, jumpcuts)))
    }
}

impl From<String> for StreamFileName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<StreamFileName> for String {
    fn from(s: StreamFileName) -> Self {
        s.0
    }
}

impl AsRef<str> for StreamFileName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamInfo {
    pub id: i64,
    pub title: Option<String>,
    pub title_type: String,
    pub file_name: StreamFileName,
    pub file_size: u64,
    #[serde(with = "ts_seconds")]
    pub timestamp: DateTime<Utc>,
    #[serde(with = "ts_seconds_option")]
    pub inserted_at: Option<DateTime<Utc>>,
    #[serde(with = "duration_seconds_float")]
    pub duration: Duration,
    pub has_preview: bool,
    pub thumbnail_count: usize,
    pub has_chat: bool,
    pub hype_average: Option<f64>,
}

impl StreamInfo {
    pub fn preview_path(id: i64) -> PathBuf {
        Path::new("./previews").join(id.to_string() + ".webm")
    }
    pub fn preview_url(&self) -> Option<String> {
        self.has_preview
            .then(|| format!("/preview/{}.webm", self.id))
    }

    pub fn thumbnails_path(id: i64) -> PathBuf {
        Path::new("./thumbnails").join(id.to_string())
    }
    pub fn thumbnail_urls(&self) -> Vec<String> {
        (0..self.thumbnail_count)
            .map(|i| format!("/thumbnail/{}/{}.webp", self.id, i))
            .collect()
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamDatapoint {
    pub title: String,
    pub viewcount: i64,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub game: String,
    #[serde(with = "ts_seconds")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamJumpcut {
    #[serde(with = "ts_seconds")]
    pub at: DateTime<Utc>,
    #[serde(with = "duration_seconds")]
    pub duration: Duration,
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamJson {
    #[serde(flatten)]
    pub info: StreamInfo,

    pub persons: Vec<PersonInfo>,
    pub games: Vec<GameFeature>,

    pub datapoints: Vec<StreamDatapoint>,
    pub jumpcuts: Vec<StreamJumpcut>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamProgress {
    #[serde(with = "duration_seconds_float")]
    pub time: Duration,
    #[serde(with = "ts_seconds")]
    pub real_time: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbMessage {
    pub id: i64,
    pub author_id: i64,
    pub message: String,
    #[serde(with = "ts_seconds")]
    pub time: DateTime<Utc>,
    #[serde(with = "ts_seconds")]
    pub real_time: DateTime<Utc>,
    pub author_name: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConversionProgress {
    pub id: i64,
    pub filename: String,
    #[serde(with = "ts_seconds")]
    pub ts: DateTime<Utc>,
    pub datapoint_title: Option<String>,
    pub games: Option<String>,
    #[serde(with = "duration_seconds_float")]
    pub progress: Duration,
    pub eta: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct HypeDatapoint {
    #[serde(with = "ts_seconds")]
    pub ts: DateTime<Utc>,
    pub loudness: Option<f64>,
    pub chat_hype: Option<i32>,
    pub hype: f64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreateClipRequest {
    pub author_username: String,
    pub stream_id: i64,
    pub start_time: i64, // in seconds
    pub duration: i64,   // in seconds
    pub title: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct Clip {
    pub id: i64,
    pub author_id: i64,
    pub author_username: String,
    pub stream_id: i64,
    pub start_time: i64, // in seconds
    pub duration: i64,   // in seconds
    pub title: Option<String>,
    pub created_at: i64,
    pub view_count: i64,
}
impl Clip {
    pub fn preview_path(id: i64) -> PathBuf {
        Path::new("./previews/clips").join(id.to_string() + ".webm")
    }
    pub fn preview_url(&self) -> String {
        format!("/preview/clips/{}.webm", self.id)
        /*
        self.has_preview
            .then(|| format!("/preview/{}.webm", self.id))
        */
    }

    pub fn thumbnail_path(id: i64) -> PathBuf {
        Path::new("./thumbnails/clips").join(id.to_string() + ".webp")
    }
    pub fn thumbnail_url(&self) -> String {
        format!("/thumbnail/clips/{}.webp", self.id)
        /*
        (0..self.thumbnail_count)
            .map(|i| format!("/thumbnail/{}/{}.webp", self.id, i))
            .collect()
        */
    }
}
