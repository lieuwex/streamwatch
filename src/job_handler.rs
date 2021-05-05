use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::create_preview::*;
use crate::types::*;
use crate::{okky, DB};

use tokio::sync;

use once_cell::sync::OnceCell;

use sqlx::Connection;

pub static SENDER: OnceCell<tokio::sync::mpsc::Sender<Job>> = OnceCell::new();

#[derive(Clone, Debug)]
pub enum Job {
    Preview { stream_id: i64, path: PathBuf },
    Thumbnails { stream_id: i64, path: PathBuf },
}

async fn make_preview(stream_id: i64, path: PathBuf) {
    let sections = get_sections_from_file(&path).await.unwrap();
    println!("[{}] sections are: {:?}", stream_id, sections);

    let start = Instant::now();

    let preview_path = StreamInfo::preview_path(stream_id);
    create_preview(&path, &preview_path, &sections)
        .await
        .unwrap();

    let db = DB.get().unwrap();
    let mut db = db.lock().await;
    sqlx::query!(
        "INSERT INTO stream_previews(stream_id) values(?1)",
        stream_id
    )
    .execute(&mut db.conn)
    .await
    .unwrap();

    println!("[{}] made preview in {:?}", stream_id, start.elapsed());
}

async fn make_thumbnails(stream_id: i64, path: PathBuf) {
    let sections = get_sections_from_file(&path).await.unwrap();
    println!("[{}] sections are: {:?}", stream_id, sections);

    let start = Instant::now();

    let thumbnail_path = StreamInfo::thumbnails_path(stream_id);
    let ts: Vec<_> = sections.iter().map(|(a, _)| *a).collect();
    let items = create_thumbnails(&path, &thumbnail_path, &ts)
        .await
        .unwrap();

    let db = DB.get().unwrap();
    let mut db = db.lock().await;
    let mut tx = db.conn.begin().await.unwrap();

    for (i, _) in items.iter().enumerate() {
        let i = i as i64;
        sqlx::query!(
            "INSERT INTO stream_thumbnails(stream_id, thumb_index) values(?1, ?2)",
            stream_id,
            i
        )
        .execute(&mut tx)
        .await
        .unwrap();
    }

    tx.commit().await.unwrap();

    println!(
        "[{}] made {} thumbnails in {:?}",
        stream_id,
        sections.len(),
        start.elapsed()
    );
}

async fn job_watcher(receiver: Arc<sync::Mutex<sync::mpsc::Receiver<Job>>>) {
    loop {
        let job = match {
            let mut receiver = receiver.lock().await;
            receiver.recv().await
        } {
            None => break,
            Some(j) => j,
        };

        match job {
            Job::Preview { stream_id, path } => make_preview(stream_id, path).await,
            Job::Thumbnails { stream_id, path } => make_thumbnails(stream_id, path).await,
        }
    }
}

pub fn spawn_jobs(count: usize) {
    let (sender, receiver) = sync::mpsc::channel(1);
    let receiver = Arc::new(sync::Mutex::new(receiver));
    okky!(SENDER, sender);

    for _ in 0..count {
        let receiver_cloned = receiver.clone();
        tokio::spawn(async move {
            job_watcher(receiver_cloned).await;
        });
    }
}
