#![feature(iter_intersperse)]
#![feature(hash_drain_filter)]
#![feature(destructuring_assignment)]
#![feature(pattern)]

mod chat;
mod chatspeed;
mod create_preview;
mod db;
//mod hypegraph;
mod job_handler;
mod loudness;
mod migrations;
mod scan;
mod types;
mod util;
mod volume;
mod web;

use crate::chat::cache_pruner;
use crate::job_handler::spawn_jobs;
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

    migrations::run().await.unwrap();

    spawn_jobs(PREVIEW_WORKERS);

    tokio::spawn(async {
        cache_pruner().await;
    });

    run_server().await;

    Ok(())
}
