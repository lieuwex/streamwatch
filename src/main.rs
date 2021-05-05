#![feature(iter_intersperse)]
#![feature(hash_drain_filter)]
#![feature(destructuring_assignment)]

mod chat_stream;
mod create_preview;
mod db;
mod job_handler;
mod scan;
mod types;
mod util;
mod web;

use std::io;
use std::sync::Arc;

use crate::chat_stream::cache_pruner;
use crate::job_handler::spawn_jobs;
use crate::web::run_server;

use once_cell::sync::OnceCell;

const PREVIEW_WORKERS: usize = 4;
pub const STREAMS_DIR: &str = "/streams/lekkerspelen";

pub static DB: OnceCell<Arc<tokio::sync::Mutex<db::Database>>> = OnceCell::new();

#[tokio::main]
async fn main() -> io::Result<()> {
    okky!(DB, db::Database::new().await);

    spawn_jobs(PREVIEW_WORKERS);

    tokio::spawn(async {
        cache_pruner().await;
    });

    run_server().await;

    Ok(())
}
