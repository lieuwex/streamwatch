use std::collections::HashMap;
use std::io;
use std::path::Path;

use chrono::Duration;
use tokio::process::Command;

use itertools::Itertools;

use anyhow::Result;

use serde::Serialize;

use crate::chat::FileReader;
use crate::types::StreamInfo;

async fn get_loudness(stream: StreamInfo) -> Result<Vec<(f32, f32)>> {
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

    let sum: f32 = res.iter().map(|(_, x)| *x).sum();
    let avg: f32 = sum / (res.len() as f32);

    let res = res
        .into_iter()
        .map(|(pos, db)| (pos, 1.0 / (db / avg)))
        .collect();

    Ok(res)
}

async fn get_chat_hype(stream: StreamInfo) -> Result<Vec<(f32, f32)>> {
    let timestamp = (stream.timestamp * 1_000) as usize;
    let (start, end) = (
        stream.datetime(),
        stream.datetime() + Duration::seconds(stream.duration as i64),
    );

    let items = FileReader::new(stream)
        .await
        .unwrap()
        .get_between(start, end)
        .await?;

    let res = (0..items.len())
        .map(|i| {
            let (start, end) = {
                let i = i as i64;
                let start = (i - 5).max(0);
                let end = (i + 5).min(items.len() as i64 - 1);

                (start as usize, end as usize)
            };
            let count = end - start + 1;

            let pos = (items[i].ts - timestamp) as f32 / 1e3f32;

            let messages_per_second = {
                let (start_ts, end_ts) = (items[start].ts, items[end].ts);
                let delta = (end_ts - start_ts) as f32 / 1e3;
                let time_per_message = delta / count as f32;
                1.0 / time_per_message
            };

            (pos, messages_per_second)
        })
        .collect();

    Ok(res)
}

#[derive(Clone, Debug, Serialize)]
pub struct HypeDatapoint {
    pub ts: i64,
    pub loudness: Option<f32>,
    pub chat_hype: Option<f32>,
    pub hype: f32,
}

pub async fn get_hype(stream: StreamInfo) -> Result<Vec<HypeDatapoint>> {
    let ts = stream.timestamp;
    let duration = stream.duration as usize;

    let loudness: HashMap<i64, f32> = {
        let loudness = get_loudness(stream.clone()).await?;
        loudness
            .into_iter()
            .group_by(|(pos, _)| pos.floor())
            .into_iter()
            .map(|(pos, xs)| {
                let xs: Vec<_> = xs.map(|(_, x)| x).collect();
                let len = xs.len() as f32;
                let avg = xs.into_iter().sum::<f32>() / len;
                (pos as i64, avg)
            })
            .collect()
    };

    let chat_hype: HashMap<i64, f32> = {
        let chat_hype = get_chat_hype(stream).await?;

        chat_hype
            .into_iter()
            .group_by(|(pos, _)| pos.floor())
            .into_iter()
            .map(|(pos, xs)| {
                let xs: Vec<_> = xs.map(|(_, x)| x).collect();
                let len = xs.len() as f32;
                let avg = xs.into_iter().sum::<f32>() / len;
                (pos as i64, avg)
            })
            .collect()
    };

    let res = (0..=duration)
        .map(|i| {
            let i = i as i64;

            HypeDatapoint {
                ts: ts + i,
                loudness: loudness.get(&i).cloned(),
                chat_hype: chat_hype.get(&i).cloned(),
                hype: 0.0,
            }
        })
        .collect();
    Ok(res)
}
