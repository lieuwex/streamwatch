use crate::STREAMS_DIR;

use tokio::process::Command;

use itertools::Itertools;

use anyhow::Result;

use streamwatch_shared::types::StreamInfo;

async fn _get_volume_points(stream: StreamInfo) -> Result<Vec<(f32, f32)>> {
    let mut cmd = {
        let mut cmd = Command::new("nice");
        cmd.args(&["-n10", "ffmpeg", "-i"]);
        cmd.arg(stream.file_name.stream_path(STREAMS_DIR));
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

pub async fn get_volume_points(stream: StreamInfo) -> Result<Vec<(i64, f32)>> {
    let ts = stream.timestamp;

    let res = _get_volume_points(stream)
        .await?
        .into_iter()
        .group_by(|(pos, _)| pos.round())
        .into_iter()
        .map(|(pos, xs)| {
            let pos = pos as i64;

            let avg = {
                let xs: Vec<_> = xs.map(|(_, x)| x).collect();
                let len = xs.len() as f32;
                let sum = xs.into_iter().sum::<f32>();
                sum / len
            };

            (ts + pos, avg)
        })
        .collect();
    Ok(res)
}
