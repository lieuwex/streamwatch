use super::{db::Database, STREAMS_DIR};

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use tokio::fs::{metadata, read_to_string};
use tokio::try_join;

use chrono::{DateTime, TimeZone, Utc};

use serde::{Deserialize, Serialize};

use anyhow::{anyhow, Result};

#[derive(Clone, Debug, Serialize)]
pub struct PersonInfo {
    pub id: i64,
    pub name: String,
}

#[derive(Clone, Debug, Serialize)]
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

#[derive(Clone, Debug, Serialize)]
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

        let datapoints = info
            .get(&"datapoints".into())
            .ok_or(anyhow!("field not found"))?;
        let datapoints = serde_yaml::from_value(datapoints.to_owned())?;

        let jumpcuts = info
            .get(&"jumpcuts".into())
            .ok_or(anyhow!("field not found"))?;
        let jumpcuts = serde_yaml::from_value(jumpcuts.to_owned())?;

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
#[derive(Clone, Debug, Serialize)]
pub struct StreamInfo {
    pub id: i64,
    pub title: Option<String>,
    pub file_name: StreamFileName,
    pub file_size: u64,
    pub timestamp: i64,
    pub duration: f64,
    pub has_preview: bool,
    pub thumbnail_count: usize,
    pub has_chat: bool,
}

impl StreamInfo {
    pub fn preview_path(id: i64) -> PathBuf {
        Path::new("./previews")
            .join(id.to_string())
            .join("preview.webm")
    }
    pub fn preview_url(&self) -> Option<String> {
        if self.has_preview {
            Some(format!("/preview/{}/preview.webm", self.id))
        } else {
            None
        }
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

    pub async fn into_stream_json(self, db: &Database) -> Result<StreamJson> {
        let (games, persons, datapoints, jumpcuts) = try_join!(
            db.get_stream_games(self.id),
            db.get_stream_participations(self.id),
            db.get_stream_datapoints(self.id),
            db.get_stream_jumpcuts(self.id),
        )?;

        Ok(StreamJson {
            info: self,

            persons,
            games,

            datapoints,
            jumpcuts,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamDatapoint {
    pub title: String,
    pub viewcount: i64,
    #[serde(skip_serializing)]
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
