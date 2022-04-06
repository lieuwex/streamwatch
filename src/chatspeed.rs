use anyhow::Result;
use chrono::Duration;
use itertools::Itertools;
use std::collections::HashMap;

use crate::chat::FileReader;

use streamwatch_shared::types::StreamInfo;

pub async fn get_chatspeed_points(stream: StreamInfo) -> Result<Vec<(usize, usize)>> {
    if !stream.has_chat || vec![614].contains(&stream.id) {
        return Ok(vec![]);
    }

    let ts = stream.timestamp;
    let duration = stream.duration as i64;
    let (start, end) = (
        stream.datetime(),
        stream.datetime() + Duration::seconds(duration),
    );

    let items = FileReader::new(stream)
        .await?
        .get_between(start, end)
        .await?;

    let map: HashMap<usize, usize> = items
        .into_iter()
        .group_by(|item| item.ts / 1000)
        .into_iter()
        .map(|(ts, xs)| (ts, xs.count()))
        .collect();

    let res: Vec<(usize, usize)> = (0..=duration)
        .map(|s| {
            let ts = (s + ts) as usize;
            let count = map.get(&ts).cloned().unwrap_or(0);

            (ts, count)
        })
        .collect();

    Ok(res)
}
