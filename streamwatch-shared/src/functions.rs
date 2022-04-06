use std::path::Path;

use tokio::process::Command;

use once_cell::sync::Lazy;

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone};

use regex::Regex;

use anyhow::Result;

pub async fn get_video_duration_in_secs(path: &Path) -> Result<f32> {
    let output = Command::new("ffprobe")
        .args(&[
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "format=duration",
            "-of",
            "csv=p=0",
        ])
        .arg(path.as_os_str())
        .output()
        .await?;

    let s = String::from_utf8(output.stdout)?;
    Ok(s.trim().parse::<f32>()?)
}

pub enum DateType {
    Full,
    DateOnly,
}

pub fn parse_filename(path: &Path) -> Option<(DateTime<Local>, DateType)> {
    static FILE_STEM_REGEX_DATETIME: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap());
    static FILE_STEM_REGEX_DATE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap());

    let stem = path.file_stem().unwrap().to_str().unwrap();

    let (naive_datetime, typ) = FILE_STEM_REGEX_DATETIME
        .find(stem)
        .and_then(|m| NaiveDateTime::parse_from_str(m.as_str(), "%Y-%m-%d %H:%M:%S").ok())
        .map(|d| (d, DateType::Full))
        .or_else(|| {
            FILE_STEM_REGEX_DATE
                .find(stem)
                .and_then(|m| NaiveDate::parse_from_str(m.as_str(), "%Y-%m-%d").ok())
                .map(|d| (d.and_hms(0, 0, 0), DateType::DateOnly))
        })?;

    Some((Local.from_local_datetime(&naive_datetime).unwrap(), typ))
}
