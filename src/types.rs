use super::STREAMS_DIR;

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use tokio::fs::{metadata, read_to_string};

use chrono::{DateTime, TimeZone, Utc};

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
    pub start_time: f64,
}
impl GameFeature {
    pub const fn from_game_info(info: GameInfo, start_time: f64) -> Self {
        Self { info, start_time }
    }
}

#[derive(Deserialize)]
pub struct GameItem {
    pub id: i64,
    pub start_time: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamFileName(String);
impl StreamFileName {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn stream_path(&self) -> PathBuf {
        Path::new(STREAMS_DIR).join(&self.0)
    }

    pub fn chat_file_path(&self) -> PathBuf {
        let mut res = Path::new(STREAMS_DIR).join(&self.0);
        res.set_extension("txt.zst");
        res
    }

    pub async fn has_chat(&self) -> Result<bool> {
        let res = metadata(self.chat_file_path())
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

    fn extra_info_file_path(&self) -> PathBuf {
        let mut res = Path::new(STREAMS_DIR).join(&self.0);
        res.set_extension("yaml");
        res
    }

    pub async fn get_extra_info_from_file(
        &self,
    ) -> Result<Option<(Vec<StreamDatapoint>, Vec<StreamJumpcut>)>> {
        let s = match read_to_string(self.extra_info_file_path()).await {
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
    pub timestamp: i64,
    pub duration: f64,
    pub has_preview: bool,
    pub thumbnail_count: usize,
    pub has_chat: bool,
    pub hype_average: Option<f32>,
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

    pub fn datetime(&self) -> DateTime<Utc> {
        Utc.timestamp(self.timestamp, 0)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamDatapoint {
    pub title: String,
    pub viewcount: i64,
    #[serde(skip_serializing, default)]
    pub game: String, // this field was later removed
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamJumpcut {
    pub at: i64,
    pub duration: i64,
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
    pub time: f64,
    pub real_time: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct DbMessage {
    pub id: i64,
    pub author_id: i64,
    pub message: String,
    pub time: i64,
    pub real_time: i64,
    pub author_name: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConversionProgress {
    pub id: i64,
    pub filename: String,
    pub ts: i64,
    pub datapoint_title: Option<String>,
    pub games: Option<String>,
    pub progress: f32,
    pub eta: Option<f32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct HypeDatapoint {
    pub ts: i64,
    pub loudness: Option<f32>,
    pub chat_hype: Option<i32>,
    pub hype: f32,
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
}
