use std::borrow::BorrowMut;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::chatspeed::get_chatspeed_points;
use crate::create_preview::{
    create_clip_preview, create_clip_thumbnail, create_preview, create_thumbnails,
    get_sections_from_file,
};
use crate::db::Database;
use crate::loudness::get_loudness_points;
use crate::util::get_conn;
use crate::{okky, DB, STREAMS_DIR};

use sqlx::SqliteConnection;
use streamwatch_shared::types::{Clip, StreamInfo, StreamJson};

use tokio::sync::{self, mpsc};

use once_cell::sync::OnceCell;

use anyhow::{anyhow, Result};

async fn expect_stream(conn: &mut SqliteConnection, stream_id: i64) -> Result<StreamJson> {
    match Database::get_stream_by_id(conn, stream_id).await? {
        None => Err(anyhow!("stream {} not found", stream_id)),
        Some(s) => Ok(s),
    }
}

async fn expect_clip(conn: &mut SqliteConnection, clip_id: i64) -> Result<Clip> {
    let clips = Database::get_clips(conn, None).await?;
    match clips.into_iter().find(|c| c.id == clip_id) {
        None => Err(anyhow!("clip {} not found", clip_id)),
        Some(s) => Ok(s),
    }
}

async fn expect_clip_stream(clip_id: i64) -> Result<(Clip, StreamJson)> {
    let db = DB.get().unwrap();
    let mut conn = db.pool.acquire().await?;

    let clip = expect_clip(&mut conn, clip_id).await?;
    let stream = expect_stream(&mut conn, clip.stream_id).await?;

    Ok((clip, stream))
}

pub static SENDER: OnceCell<JobSender> = OnceCell::new();

#[derive(Debug)]
pub struct JobSender {
    thumbnail_jobs: mpsc::UnboundedSender<Job>,
    preview_jobs: mpsc::UnboundedSender<Job>,
    clip_thumbnail_jobs: mpsc::UnboundedSender<Job>,
    clip_preview_jobs: mpsc::UnboundedSender<Job>,
    loudness_jobs: mpsc::UnboundedSender<Job>,
    chatspeed_jobs: mpsc::UnboundedSender<Job>,
}
impl JobSender {
    pub fn send(&self, job: Job) -> Result<(), mpsc::error::SendError<Job>> {
        match job {
            j @ Job::Thumbnails { .. } => self.thumbnail_jobs.send(j),
            j @ Job::Preview { .. } => self.preview_jobs.send(j),
            j @ Job::ClipThumbnail { .. } => self.clip_thumbnail_jobs.send(j),
            j @ Job::ClipPreview { .. } => self.clip_preview_jobs.send(j),
            j @ Job::Loudness { .. } => self.loudness_jobs.send(j),
            j @ Job::Chatspeed { .. } => self.chatspeed_jobs.send(j),
        }
    }
}

pub struct JobReceiver {
    thumbnail_jobs: mpsc::UnboundedReceiver<Job>,
    preview_jobs: mpsc::UnboundedReceiver<Job>,
    clip_thumbnail_jobs: mpsc::UnboundedReceiver<Job>,
    clip_preview_jobs: mpsc::UnboundedReceiver<Job>,
    loudness_jobs: mpsc::UnboundedReceiver<Job>,
    chatspeed_jobs: mpsc::UnboundedReceiver<Job>,
}
impl JobReceiver {
    pub async fn recv(&mut self) -> Option<Job> {
        let thumbnails = self.thumbnail_jobs.recv();
        let previews = self.preview_jobs.recv();
        let clip_thumbnails = self.clip_thumbnail_jobs.recv();
        let clip_previews = self.clip_preview_jobs.recv();
        let chatspeed = self.chatspeed_jobs.recv();
        let loudness = self.loudness_jobs.recv();

        tokio::select! {
            biased;
            Some(job) = thumbnails => Some(job),
            Some(job) = previews => Some(job),
            Some(job) = clip_thumbnails => Some(job),
            Some(job) = clip_previews => Some(job),
            Some(job) = chatspeed => Some(job),
            Some(job) = loudness => Some(job),
            else => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Job {
    Preview { stream_id: i64, path: PathBuf },
    Thumbnails { stream_id: i64, path: PathBuf },
    ClipPreview { clip_id: i64 },
    ClipThumbnail { clip_id: i64 },
    Loudness { stream_id: i64 },
    Chatspeed { stream_id: i64 },
}

async fn make_preview(stream_id: i64, path: PathBuf) -> Result<()> {
    let sections = get_sections_from_file(&path).await?;
    println!("[{}] sections are: {:?}", stream_id, sections);

    let start = Instant::now();

    let preview_path = StreamInfo::preview_path(stream_id);
    create_preview(&path, &preview_path, &sections).await?;

    let db = DB.get().unwrap();
    sqlx::query!(
        "UPDATE streams SET preview_count = 1 WHERE id = ?1",
        stream_id,
    )
    .execute(&db.pool)
    .await?;

    println!("[{}] made preview in {:?}", stream_id, start.elapsed());

    Ok(())
}

async fn make_clip_preview(clip_id: i64) -> Result<()> {
    let (clip, stream) = expect_clip_stream(clip_id).await?;

    let start = Instant::now();

    let preview_path = Clip::preview_path(clip_id);
    create_clip_preview(
        &stream.info.file_name.stream_path(STREAMS_DIR),
        &preview_path,
        clip.start_time,
        clip.duration,
    )
    .await?;

    /*
    let db = DB.get().unwrap();
    sqlx::query!(
        "UPDATE streams SET preview_count = 1 WHERE id = ?1",
        stream_id,
    )
    .execute(&Database::pool)
    .await?;
    */

    println!("[{}] made clip preview in {:?}", clip_id, start.elapsed());

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
    let thumbnail_count = items.len() as i64;
    sqlx::query!(
        "UPDATE streams SET thumbnail_count = ?1 WHERE id = ?2",
        thumbnail_count,
        stream_id,
    )
    .execute(&db.pool)
    .await?;

    println!(
        "[{}] made {} thumbnails in {:?}",
        stream_id,
        sections.len(),
        start.elapsed()
    );

    Ok(())
}

async fn make_clip_thumbnail(clip_id: i64) -> Result<()> {
    let (clip, stream) = expect_clip_stream(clip_id).await?;

    let start = Instant::now();

    let thumbnail_path = Clip::thumbnail_path(clip_id);
    create_clip_thumbnail(
        &stream.info.file_name.stream_path(STREAMS_DIR),
        &thumbnail_path,
        clip.start_time,
    )
    .await?;

    /*
    let db = DB.get().unwrap();
    let thumbnail_count = items.len() as i64;
    sqlx::query!(
        "UPDATE streams SET thumbnail_count = ?1 WHERE id = ?2",
        thumbnail_count,
        stream_id,
    )
    .execute(&Database::pool)
    .await?;
    */

    println!("[{}] made thumbnail in {:?}", clip_id, start.elapsed());

    Ok(())
}

async fn update_loudness(stream_id: i64) -> Result<()> {
    let stream = expect_stream(get_conn().await?.borrow_mut(), stream_id).await?;
    let loudness = get_loudness_points(&stream.info).await?;
    Database::set_stream_loudness(get_conn().await?.borrow_mut(), stream_id, loudness).await?;

    Ok(())
}

async fn update_chatspeed(stream_id: i64) -> Result<()> {
    let stream = expect_stream(get_conn().await?.borrow_mut(), stream_id).await?;
    if !stream.info.has_chat {
        return Ok(());
    }

    let chatspeed = get_chatspeed_points(stream.info)
        .await?
        .into_iter()
        .map(|(ts, cnt)| (ts, cnt as i64));
    Database::set_stream_chatspeed_datapoints(get_conn().await?.borrow_mut(), stream_id, chatspeed)
        .await?;

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
            Job::ClipPreview { clip_id } => make_clip_preview(clip_id).await,
            Job::ClipThumbnail { clip_id } => make_clip_thumbnail(clip_id).await,
            Job::Loudness { stream_id } => update_loudness(stream_id).await,
            Job::Chatspeed { stream_id } => update_chatspeed(stream_id).await,
        };
        if let Err(e) = res {
            eprintln!("error while executing job: {:?}", e);
        }
    }
}

pub fn spawn_jobs(count: usize) {
    let (sender, receiver) = {
        let (thumb_sender, thumb_receiver) = sync::mpsc::unbounded_channel();
        let (prev_sender, prev_receiver) = sync::mpsc::unbounded_channel();
        let (clip_thumb_sender, clip_thumb_receiver) = sync::mpsc::unbounded_channel();
        let (clip_prev_sender, clip_prev_receiver) = sync::mpsc::unbounded_channel();
        let (loudness_sender, loudness_receiver) = sync::mpsc::unbounded_channel();
        let (chatspeed_sender, chatspeed_receiver) = sync::mpsc::unbounded_channel();

        let sender = JobSender {
            thumbnail_jobs: thumb_sender,
            preview_jobs: prev_sender,
            clip_thumbnail_jobs: clip_thumb_sender,
            clip_preview_jobs: clip_prev_sender,
            loudness_jobs: loudness_sender,
            chatspeed_jobs: chatspeed_sender,
        };

        let receiver = JobReceiver {
            thumbnail_jobs: thumb_receiver,
            preview_jobs: prev_receiver,
            clip_thumbnail_jobs: clip_thumb_receiver,
            clip_preview_jobs: clip_prev_receiver,
            loudness_jobs: loudness_receiver,
            chatspeed_jobs: chatspeed_receiver,
        };

        (sender, receiver)
    };
    let receiver = Arc::new(sync::Mutex::new(receiver));
    okky!(SENDER, sender);

    itertools::repeat_n(receiver, count).for_each(|receiver| {
        tokio::spawn(async move {
            job_watcher(receiver).await;
        });
    });
}
