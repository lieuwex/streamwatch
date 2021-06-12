use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::create_preview::{create_preview, create_thumbnails, get_sections_from_file};
use crate::types::StreamInfo;
use crate::{okky, DB};

use tokio::sync::{self, mpsc};

use once_cell::sync::OnceCell;

use sqlx::Connection;

use anyhow::Result;

pub static SENDER: OnceCell<JobSender> = OnceCell::new();

pub struct JobSender {
    thumbnail_jobs: mpsc::UnboundedSender<Job>,
    preview_jobs: mpsc::UnboundedSender<Job>,
}
impl JobSender {
    pub fn send(&self, job: Job) -> Result<(), mpsc::error::SendError<Job>> {
        match job {
            j @ Job::Thumbnails { .. } => self.thumbnail_jobs.send(j),
            j @ Job::Preview { .. } => self.preview_jobs.send(j),
        }
    }
}

pub struct JobReceiver {
    thumbnail_jobs: mpsc::UnboundedReceiver<Job>,
    preview_jobs: mpsc::UnboundedReceiver<Job>,
}
impl JobReceiver {
    pub async fn recv(&mut self) -> Option<Job> {
        let thumbnails = self.thumbnail_jobs.recv();
        tokio::pin!(thumbnails);
        let previews = self.preview_jobs.recv();
        tokio::pin!(previews);

        tokio::select! {
            biased;
            Some(job) = &mut thumbnails => Some(job),
            Some(job) = &mut previews => Some(job),
            else => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Job {
    Preview { stream_id: i64, path: PathBuf },
    Thumbnails { stream_id: i64, path: PathBuf },
}

async fn make_preview(stream_id: i64, path: PathBuf) -> Result<()> {
    let sections = get_sections_from_file(&path).await?;
    println!("[{}] sections are: {:?}", stream_id, sections);

    let start = Instant::now();

    let preview_path = StreamInfo::preview_path(stream_id);
    create_preview(&path, &preview_path, &sections).await?;

    let db = DB.get().unwrap();
    let mut db = db.lock().await;
    sqlx::query!(
        "INSERT INTO stream_previews(stream_id) values(?1)",
        stream_id
    )
    .execute(&mut db.conn)
    .await?;

    println!("[{}] made preview in {:?}", stream_id, start.elapsed());

    Ok(())
}

async fn make_thumbnails(stream_id: i64, path: PathBuf) -> Result<()> {
    let sections = get_sections_from_file(&path).await?;
    println!("[{}] sections are: {:?}", stream_id, sections);

    let start = Instant::now();

    let thumbnail_path = StreamInfo::thumbnails_path(stream_id);
    let ts: Vec<_> = sections.iter().map(|(a, _)| *a).collect();
    let items = create_thumbnails(&path, &thumbnail_path, &ts).await?;

    let db = DB.get().unwrap();
    let mut db = db.lock().await;
    let mut tx = db.conn.begin().await?;

    for (i, _) in items.iter().enumerate() {
        let i = i as i64;
        sqlx::query!(
            "INSERT INTO stream_thumbnails(stream_id, thumb_index) values(?1, ?2)",
            stream_id,
            i
        )
        .execute(&mut tx)
        .await?;
    }

    tx.commit().await?;

    println!(
        "[{}] made {} thumbnails in {:?}",
        stream_id,
        sections.len(),
        start.elapsed()
    );

    Ok(())
}

async fn job_watcher(receiver: Arc<sync::Mutex<JobReceiver>>) {
    loop {
        let job = match {
            let mut receiver = receiver.lock().await;
            receiver.recv().await
        } {
            None => break,
            Some(j) => j,
        };

        let res = match job {
            Job::Preview { stream_id, path } => make_preview(stream_id, path).await,
            Job::Thumbnails { stream_id, path } => make_thumbnails(stream_id, path).await,
        };
        match res {
            Ok(_) => {}
            Err(e) => eprintln!("error while executing job: {:?}", e),
        }
    }
}

pub fn spawn_jobs(count: usize) {
    let (sender, receiver) = {
        let (thumb_sender, thumb_receiver) = sync::mpsc::unbounded_channel();
        let (prev_sender, prev_receiver) = sync::mpsc::unbounded_channel();

        let sender = JobSender {
            thumbnail_jobs: thumb_sender,
            preview_jobs: prev_sender,
        };

        let receiver = JobReceiver {
            thumbnail_jobs: thumb_receiver,
            preview_jobs: prev_receiver,
        };

        (sender, receiver)
    };
    let receiver = Arc::new(sync::Mutex::new(receiver));
    okky!(SENDER, sender);

    for _ in 0..count {
        let receiver_cloned = receiver.clone();
        tokio::spawn(async move {
            job_watcher(receiver_cloned).await;
        });
    }
}
