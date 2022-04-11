use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use std::collections::HashMap;

use crate::chat::FileReader;

use streamwatch_shared::types::StreamInfo;

pub async fn get_chatspeed_points(stream: StreamInfo) -> Result<Vec<(DateTime<Utc>, usize)>> {
    if !stream.has_chat || vec![614].contains(&stream.id) {
        return Ok(vec![]);
    }

    let ts = stream.timestamp;
    let duration = stream.duration as i64;
    let (start, end) = (
        stream.timestamp,
        stream.timestamp + Duration::seconds(duration),
    );

    let items = FileReader::new(stream)
        .await?
        .get_between(start, end)
        .await?;

    let map: HashMap<i64, usize> = items
        .into_iter()
        .group_by(|item| item.ts.timestamp())
        .into_iter()
        .map(|(ts, xs)| (ts, xs.count()))
        .collect();

    let res: Vec<(DateTime<Utc>, usize)> = (0..=duration)
        .map(|s| {
            let ts = ts + Duration::seconds(s);

            let count = map.get(&ts.timestamp()).cloned().unwrap_or(0);

            (ts, count)
        })
        .collect();

    Ok(res)
}
