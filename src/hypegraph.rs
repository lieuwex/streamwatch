use std::collections::HashMap;
use std::io;
use std::path::Path;

use chrono::Duration;
use tokio::process::Command;

use itertools::Itertools;

use anyhow::Result;

use serde::Serialize;

use regex::Regex;

use crate::chat::FileReader;

use streamwatch_shared::types::StreamInfo;

pub async fn get_volume_points(stream: StreamInfo) -> Result<Vec<(f32, f32)>> {
    let mut cmd = {
        let mut cmd = Command::new("nice");
        cmd.args(&["-n10", "ffmpeg", "-i"]);
        cmd.arg(stream.file_name.stream_path());
        cmd.args(&[
            "-vn",
            "-af",
            "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level:file=/dev/stdout",
            "-f",
            "null",
            "-",
        ]);
        cmd
    };

    let output = {
        let output = cmd.output().await?;
        String::from_utf8(output.stdout)?
    };

    let res: Vec<(f32, f32)> = output
        .lines()
        .tuple_windows()
        .step_by(2)
        .map(|(pos_line, info_line)| {
            let pos = {
                let splitted = pos_line.split(' ').filter(|s| !s.is_empty());
                let pts_info = splitted.last().unwrap();
                pts_info.splitn(2, ':').nth(1).unwrap()
            };
            let pos = pos.parse().unwrap();

            let db = info_line.splitn(2, '=').nth(1).unwrap();
            let db = db.parse().unwrap();

            (pos, db)
        })
        .collect();

    Ok(res)
}

pub async fn get_loudness_points(stream: StreamInfo) -> Result<Vec<(f32, f32)>> {
    let mut cmd = {
        let mut cmd = Command::new("nice");
        cmd.args(&["-n10", "ffmpeg"]);
        cmd.args(&["-hide_banner", "-nostats"]);
        cmd.arg("-i");
        cmd.arg(stream.file_name.stream_path());
        cmd.args(&["-vn", "-filter_complex", "abur128", "-f", "null", "-"]);
        cmd
    };

    let output = {
        let output = cmd.output().await?;
        String::from_utf8(output.stderr)?
    };

    let re = Regex::new(r"([a-zA-Z]):\s*([-0-9.]+)").unwrap();
    let res: Vec<(f32, f32)> = output
        .lines()
        .filter(|line| line.starts_with("[Parsed_ebur1280"))
        .map(|line| {
            let rest = line.splitn(2, ']').nth(1).unwrap();
            let hash_map: HashMap<String, f32> = re
                .captures_iter(rest)
                .map(|cap| (cap[1].to_string(), cap[2].parse().unwrap()))
                .collect();

            let pos = *hash_map.get("t").unwrap();
            let loudness = *hash_map.get("M").unwrap();

            (pos, loudness)
        })
        .collect();

    Ok(res)
}

async fn get_chat_hype(stream: StreamInfo) -> Result<Vec<(i64, f32)>> {
    return Ok(vec![]);
    if !stream.has_chat {
        return Ok(vec![]);
    }

    let duration = stream.duration as usize;
    let timestamp = (stream.timestamp * 1_000) as usize;
    let (start, end) = (
        stream.datetime(),
        stream.datetime() + Duration::seconds(stream.duration as i64),
    );

    let items = FileReader::new(stream)
        .await?
        .get_between(start, end)
        .await?;

    let res = (0..=duration)
        .map(|s| {
            let s = s as i64;
            let start = (s - 10).max(0);
            let end = (s + 10).max(duration as i64);

            let total_time = (end - start + 1) as f32;
            let n_messages = items
                .iter()
                .filter(|item| {
                    let ts = item.ts as i64;
                    start <= ts && ts <= end
                })
                .count() as f32;
            (s, n_messages / total_time)
        })
        .collect();

    Ok(res)
}

pub struct HypeInfoDatapoint {
    pub ts: i64,
    pub decibels: Option<f32>,
    pub loudness: Option<f32>,
    pub messages_per_second: Option<f32>,
}

pub async fn get_hype(stream: StreamInfo) -> Result<Vec<HypeInfoDatapoint>> {
    let ts = stream.timestamp;
    let duration = stream.duration as usize;

    let volume_points: HashMap<i64, f32> = get_volume_points(stream.clone())
        .await?
        .into_iter()
        .group_by(|(pos, _)| pos.round())
        .into_iter()
        .map(|(pos, xs)| {
            let avg = {
                let xs: Vec<_> = xs.map(|(_, x)| x).collect();
                let len = xs.len() as f32;
                let sum = xs.into_iter().sum::<f32>();
                sum / len
            };
            (pos as i64, avg)
        })
        .collect();

    let loudness_points: HashMap<i64, f32> = get_loudness_points(stream.clone())
        .await?
        .into_iter()
        .group_by(|(pos, _)| pos.round())
        .into_iter()
        .map(|(pos, xs)| {
            let avg = {
                let xs: Vec<_> = xs.map(|(_, x)| x).collect();
                let len = xs.len() as f32;
                let sum = xs.into_iter().sum::<f32>();
                sum / len
            };
            (pos as i64, avg)
        })
        .collect();

    let chat_hype_points: HashMap<i64, f32> = get_chat_hype(stream).await?.into_iter().collect();

    let res = (0..=duration)
        .map(|i| {
            let i = i as i64;

            HypeInfoDatapoint {
                ts: ts + i,
                decibels: volume_points.get(&i).cloned(),
                loudness: loudness_points.get(&i).cloned(),
                messages_per_second: chat_hype_points.get(&i).cloned(),
            }
        })
        .collect();
    Ok(res)
}
