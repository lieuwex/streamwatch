use streamwatch_shared::functions::get_video_duration;

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::fs::create_dir_all;
use tokio::process::Command;

use anyhow::Result;

const SECTION_DURATION_SECS: i32 = 1;
pub const PREVIEW_PER_SECS: f64 = 900.0;
pub const SCRUB_PER_SECS: f64 = 30.0;

fn get_sections(duration: &Duration, per_n_secs: f64) -> Vec<(i32, i32)> {
    let duration = duration.as_secs_f64();
    let count = std::cmp::max((duration / per_n_secs) as usize, 1);
    (1..=count)
        .map(|i| {
            let frac = (i as f64) / ((count + 1) as f64);
            let start = (frac * duration) as i32;
            (start, start + SECTION_DURATION_SECS)
        })
        .collect()
}

pub async fn get_sections_from_file(path: &Path, per_n_secs: f64) -> Result<Vec<(i32, i32)>> {
    let duration = get_video_duration(path).await?;
    Ok(get_sections(&duration, per_n_secs))
}

/// Creates webp thumbnails
pub async fn create_thumbnails(
    path: &Path,
    output_dir: &Path,
    time_locations: &[i32],
) -> io::Result<Vec<PathBuf>> {
    create_dir_all(output_dir).await?;

    let mut outputs = Vec::with_capacity(time_locations.len());

    for (i, loc) in time_locations.iter().enumerate() {
        let output = output_dir.join(format!("{}.webp", i));

        let mut handle = Command::new("ffmpeg")
            .args(&[
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                &loc.to_string(),
                "-i",
            ])
            .arg(path.as_os_str())
            .args(&["-frames:v", "1", "-y"])
            .arg(output.as_os_str())
            .spawn()?;
        handle.wait().await?;

        outputs.push(output);
    }

    Ok(outputs)

    /*
    let mut outputs = Vec::with_capacity(time_locations.len());
    let mut futures = Vec::with_capacity(time_locations.len());

    for loc in time_locations {
        let output = output_dir.join(format!("{}.webp", loc));

        let mut handle = Command::new("ffmpeg")
            .args(&[
                "-ss",
                &loc.to_string(),
                "-i",
                path.to_str().unwrap(),
                "-frames:v",
                "1",
                output.to_str().unwrap(),
            ])
            .spawn()?;

        outputs.push(output);
        futures.push(handle.wait());
    }

    join_all(futures).await;
    Ok(outputs)
    */
}

/// Creates webp thumbnail for a clip
pub async fn create_clip_thumbnail(path: &Path, output: &Path, begin: Duration) -> io::Result<()> {
    create_dir_all(output.ancestors().nth(1).unwrap()).await?;

    let mut handle = Command::new("ffmpeg")
        .args(&[
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            &begin.as_secs_f64().to_string(),
            "-i",
        ])
        .arg(path.as_os_str())
        .args(&["-frames:v", "1", "-y"])
        .arg(output.as_os_str())
        .spawn()?;
    handle.wait().await?;

    Ok(())
}

/// Create a low-resolution av1 preview
pub async fn create_preview(path: &Path, output: &Path, sections: &[(i32, i32)]) -> io::Result<()> {
    create_dir_all(output.ancestors().nth(1).unwrap()).await?;

    let path_string = path.as_os_str();

    let mut cmd = Command::new("nice");
    cmd.args(&["-n10", "ffmpeg", "-hide_banner", "-loglevel", "error"]);

    for (begin, end) in sections {
        cmd.arg("-ss");
        cmd.arg(begin.to_string());
        cmd.arg("-t");
        cmd.arg((end - begin).to_string());
        cmd.arg("-i");
        cmd.arg(path_string);
    }

    let filter_complex = {
        let mut s: String = (0..sections.len()).map(|i| format!("[{}:v]", i)).collect();
        s += &format!(
            "concat=n={}[out];[out]scale=432:243[out2];[out2]fps=30",
            sections.len()
        );
        s
    };
    cmd.args(&[
        "-filter_complex",
        &filter_complex,
        "-c:v",
        "libaom-av1",
        "-cpu-used",
        "1",
        "-row-mt",
        "1",
        "-threads",
        "8",
        "-b:v",
        "150k",
        "-minrate",
        "0k",
        "-maxrate",
        "180k",
        "-an",
        "-y",
    ]);
    cmd.arg(output.as_os_str());

    let mut handle = cmd.spawn()?;
    handle.wait().await?;
    Ok(())
}
/*
pub async fn create_preview(path: &Path, output: &Path, sections: &[(i32, i32)]) -> io::Result<()> {
    create_dir_all(output.ancestors().nth(1).unwrap())?;

    let mut select_filter = String::new();
    for (i, (begin, end)) in sections.iter().enumerate() {
        if i > 0 {
            select_filter.push_str(" + ");
        }
        select_filter.push_str(&format!("between(t\\, {}\\, {})", begin, end));
    }

    let mut handle = Command::new("nice")
        .args(&[
            "-n19",
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            path.to_str().unwrap(),
            "-vf",
            &format!(
                "select={},setpts=N/FRAME_RATE/TB,scale=432:243,fps=30",
                select_filter
            ),
            "-c:v",
            "libaom-av1",
            "-cpu-used",
            "1",
            "-threads",
            "1",
            "-b:v",
            "150k",
            "-minrate",
            "0k",
            "-maxrate",
            "180k",
            "-an",
            "-y",
            output.to_str().unwrap(),
        ])
        .spawn()?;
    handle.wait().await?;
    Ok(())
}
*/

/// Create a low-resolution av1 preview for a clip
pub async fn create_clip_preview(
    path: &Path,
    output: &Path,
    begin: Duration,
    duration: Duration,
) -> io::Result<()> {
    create_dir_all(output.ancestors().nth(1).unwrap()).await?;

    let path_string = path.as_os_str();

    let mut cmd = Command::new("nice");
    cmd.args(&["-n10", "ffmpeg", "-hide_banner", "-loglevel", "error"]);

    cmd.arg("-ss");
    cmd.arg(begin.as_secs_f64().to_string());
    cmd.arg("-t");
    cmd.arg(duration.as_secs_f64().to_string());
    cmd.arg("-i");
    cmd.arg(path_string);

    let filter_complex = "[0:v]scale=432:243[out2];[out2]fps=30";

    cmd.args(&[
        "-filter_complex",
        &filter_complex,
        "-c:v",
        "libaom-av1",
        "-cpu-used",
        "1",
        "-row-mt",
        "1",
        "-threads",
        "8",
        "-b:v",
        "150k",
        "-minrate",
        "0k",
        "-maxrate",
        "180k",
        "-an",
        "-y",
    ]);
    cmd.arg(output.as_os_str());

    let mut handle = cmd.spawn()?;
    handle.wait().await?;
    Ok(())
}
