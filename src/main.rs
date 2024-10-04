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
use once_cell::sync::OnceCell;

const PREVIEW_WORKERS: usize = 4;
pub const STREAMS_DIR: &str = "/streams/lekkerspelen";

pub static DB: OnceCell<db::Database> = OnceCell::new();

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

    run_server().await;

    Ok(())
}
