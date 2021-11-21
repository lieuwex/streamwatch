use std::{ascii::AsciiExt, collections::HashMap, iter::Sum, panic};

use tokio::process::Command;

use itertools::Itertools;

use anyhow::Result;

use regex::Regex;

use crate::types::{StreamFileName, StreamInfo};

struct LoudnessInformation {
    pub momentary: f32,
    pub short_term: f32,
    pub integrated: f32,
    pub lra: f32,
}

impl LoudnessInformation {
    pub fn div(&self, scalar: f32) -> LoudnessInformation {
        LoudnessInformation {
            momentary: self.momentary / scalar,
            short_term: self.short_term / scalar,
            integrated: self.integrated / scalar,
            lra: self.lra / scalar,
        }
    }
}

impl Sum for LoudnessInformation {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(
            LoudnessInformation {
                momentary: 0.0,
                short_term: 0.0,
                integrated: 0.0,
                lra: 0.0,
            },
            |mut acc, x| {
                acc.momentary += x.momentary;
                acc.short_term += x.short_term;
                acc.integrated += x.integrated;
                acc.lra += x.lra;
                acc
            },
        )
    }
}

async fn _get_loudness_points(
    stream_filename: &StreamFileName,
) -> Result<Vec<(f32, LoudnessInformation)>> {
    let mut cmd = {
        let mut cmd = Command::new("nice");
        cmd.args(&["-n10", "ffmpeg"]);
        cmd.args(&["-hide_banner", "-nostats"]);
        cmd.arg("-i");
        cmd.arg(stream_filename.stream_path());
        cmd.args(&["-vn", "-filter_complex", "ebur128", "-f", "null", "-"]);
        cmd
    };

    let output = {
        let output = cmd.output().await?;
        String::from_utf8(output.stderr)?
    };

    let re = Regex::new(r"([a-zA-Z]+):\s*(nan|[-0-9.]+)").unwrap();
    let res = output
        .lines()
        .filter(|line| line.starts_with("[Parsed_ebur128"))
        .filter_map(|line| {
            let result = panic::catch_unwind(|| {
                let parse = |s: &str| {
                    if s == "nan" {
                        Ok(f32::NAN)
                    } else {
                        s.parse()
                    }
                };

                let hash_map: HashMap<String, f32> = re
                    .captures_iter(line)
                    .map(|cap| (cap[1].to_string(), parse(&cap[2]).unwrap()))
                    .collect();

                hash_map.get("t").map(|pos| {
                    let info = LoudnessInformation {
                        momentary: *hash_map.get("M").unwrap(),
                        short_term: *hash_map.get("S").unwrap(),
                        integrated: *hash_map.get("I").unwrap(),
                        lra: *hash_map.get("LRA").unwrap(),
                    };

                    (*pos, info)
                })
            });

            match result {
                Err(e) => {
                    println!("{:#?}\n{}", e, line);
                    panic::resume_unwind(e);
                }
                Ok(r) => r,
            }
        })
        .collect();
    Ok(res)
}

pub struct LoudnessDatapoint {
    pub ts: i64,
    pub momentary: f32,
    pub short_term: f32,
    pub integrated: f32,
    pub lra: f32,
}
pub async fn get_loudness_points(stream: &StreamInfo) -> Result<Vec<LoudnessDatapoint>> {
    let ts = stream.timestamp;

    let res = _get_loudness_points(&stream.file_name)
        .await?
        .into_iter()
        .group_by(|(pos, _)| pos.round())
        .into_iter()
        .map(|(pos, xs)| {
            let pos = pos as i64;

            let avg = {
                let xs: Vec<_> = xs.map(|(_, x)| x).collect();
                let len = xs.len() as f32;
                let sum = xs.into_iter().sum::<LoudnessInformation>();
                sum.div(len)
            };

            LoudnessDatapoint {
                ts: ts + pos,
                momentary: avg.momentary,
                short_term: avg.short_term,
                integrated: avg.integrated,
                lra: avg.lra,
            }
        })
        .collect();
    Ok(res)
}
