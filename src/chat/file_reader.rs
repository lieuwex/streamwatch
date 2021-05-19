use super::types::Item;
use crate::types::StreamInfo;
use crate::util::split_tuple;

use chrono::{DateTime, Utc};

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};

use serde_json::value::RawValue;

use async_compression::tokio::bufread::ZstdDecoder;

use anyhow::{anyhow, Error};

type LinesReader = Lines<BufReader<ZstdDecoder<BufReader<File>>>>;

pub struct FileReader {
    orphan: Option<(DateTime<Utc>, Box<RawValue>)>,
    prev_datetime: DateTime<Utc>,
    stream: StreamInfo,
    lines: LinesReader,
}

impl FileReader {
    async fn create_lines(stream: &StreamInfo) -> Result<LinesReader, Error> {
        let f = File::open(stream.file_name.chat_file_path()).await?;
        let reader = BufReader::new(ZstdDecoder::new(BufReader::new(f)));
        Ok(reader.lines())
    }

    fn parse_line(line: &str) -> Result<(DateTime<Utc>, &str), Error> {
        let (date, json) = split_tuple(line, ' ').ok_or_else(|| anyhow!("failed to parse line"))?;
        let date = DateTime::parse_from_rfc3339(date)?.with_timezone(&Utc);
        Ok((date, json))
    }

    pub async fn new(stream: StreamInfo) -> Result<Self, Error> {
        let lines = Self::create_lines(&stream).await?;

        Ok(Self {
            orphan: None,
            prev_datetime: stream.datetime(),
            stream,
            lines,
        })
    }

    pub async fn get_between(
        &mut self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Item>, Error> {
        let mut res = Vec::new();
        macro_rules! push {
            ($datetime:expr, $json:expr) => {
                res.push(Item {
                    ts: $datetime.timestamp_millis() as usize,
                    content: $json,
                });
            };
        }

        if start < self.prev_datetime {
            // we are going back to the past, so we have to reopen the file to seek to the file
            // start.
            eprintln!("!!! seeking back from {} to {}", self.prev_datetime, start);
            self.lines = Self::create_lines(&self.stream).await?;
            self.orphan = None;
        }

        if let Some((datetime, json)) = self.orphan.take() {
            if start <= datetime && datetime <= end {
                push!(datetime, json);
            } else if start <= datetime && end < datetime {
                self.orphan = Some((datetime, json));
                return Ok(res);
            }
        }

        while let Some(line) = self.lines.next_line().await? {
            let (datetime, json) = Self::parse_line(&line)?;
            self.prev_datetime = datetime;

            // loop until we reached the starting point
            if datetime < start {
                continue;
            }

            let json = RawValue::from_string(json.to_string())?;
            if datetime > end {
                self.orphan = Some((datetime, json));
                break;
            } else {
                push!(datetime, json);
            }
        }

        Ok(res)
    }
}
