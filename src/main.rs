#![recursion_limit = "256"]
#![feature(iter_intersperse)]
#![feature(hash_extract_if)]
#![feature(try_blocks)]
#![feature(async_closure)]

mod chat;
mod chatspeed;
mod create_preview;
mod db;
//mod hypegraph;
mod job_handler;
mod loudness;
mod migrations;
mod scan;
mod util;
mod volume;
mod watchparty;
mod web;

use crate::chat::cache_pruner;
use crate::job_handler::spawn_job_watchers;
use crate::scan::generate_missing_info;
use crate::web::run_server;

use anyhow::Result;
use db::Database;
use once_cell::sync::OnceCell;
use serde_json::value::{to_raw_value, RawValue};
use tokio::sync::RwLock;
use util::get_conn;

const PREVIEW_WORKERS: usize = 4;
pub const STREAMS_DIR: &str = "/streams/lekkerspelen";

pub static DB: OnceCell<db::Database> = OnceCell::new();

pub static STREAMS_JSON_CACHE: OnceCell<RwLock<Box<RawValue>>> = OnceCell::new();

pub async fn update_cache() -> anyhow::Result<()> {
    let mut conn = get_conn().await.unwrap();

    let cache = STREAMS_JSON_CACHE.get().unwrap();
    let mut cache = cache.write().await;

    let streams = Database::get_streams(&mut conn).await?;
    *cache = to_raw_value(&streams)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    okky!(DB, db::Database::new().await?);

    spawn_job_watchers(PREVIEW_WORKERS);

    migrations::run().await.unwrap();

    generate_missing_info().await?;

    tokio::spawn(async {
        cache_pruner().await;
    });

    okky!(STREAMS_JSON_CACHE, {
        let mut conn = get_conn().await.unwrap();
        let streams = Database::get_streams(&mut conn).await.unwrap();
        let val = to_raw_value(&streams).unwrap();
        RwLock::new(val)
    });

    run_server().await;

    Ok(())
}
