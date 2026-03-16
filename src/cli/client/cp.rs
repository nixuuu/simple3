use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::Semaphore;

use super::progress::count_bar;
use super::{is_s3_uri, list_all_objects, S3Uri, Transport};

pub async fn run(
    transport: Arc<dyn Transport>,
    src: &str,
    dest: &str,
    recursive: bool,
    concurrency: usize,
    version_id: Option<&str>,
) -> anyhow::Result<()> {
    let src_s3 = is_s3_uri(src);
    let dest_s3 = is_s3_uri(dest);

    if version_id.is_some() && (!src_s3 || dest_s3) {
        anyhow::bail!("--version-id is only supported when downloading from S3 to local");
    }

    match (src_s3, dest_s3) {
        (false, true) => upload(&transport, src, dest, recursive, concurrency).await,
        (true, false) => {
            download(&transport, src, dest, recursive, concurrency, version_id).await
        }
        (true, true) => copy_s3_to_s3(&transport, src, dest, recursive, concurrency).await,
        (false, false) => anyhow::bail!("at least one path must be an s3:// URI"),
    }
}

async fn upload(
    transport: &Arc<dyn Transport>,
    src: &str,
    dest: &str,
    recursive: bool,
    concurrency: usize,
) -> anyhow::Result<()> {
    let parsed = S3Uri::parse(dest)?;
    let bucket = parsed.bucket();
    let dest_prefix = parsed.key().unwrap_or_default();
    let src_path = Path::new(src);

    if !recursive || src_path.is_file() {
        let key = if dest_prefix.is_empty() || dest_prefix.ends_with('/') {
            let name = src_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("file");
            format!("{dest_prefix}{name}")
        } else {
            dest_prefix.to_owned()
        };
        let ct = mime_guess::from_path(src_path)
            .first_raw()
            .unwrap_or("application/octet-stream");
        transport.put_object(bucket, &key, src_path, Some(ct)).await?;
        println!("upload: {src} -> s3://{bucket}/{key}");
        return Ok(());
    }

    let entries = collect_local_files(src_path);
    let sem = Arc::new(Semaphore::new(concurrency));
    let pb = Arc::new(count_bar(entries.len() as u64, "upload"));
    let errors = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(entries.len());

    for (rel_key, full_path) in &entries {
        let permit = Arc::clone(&sem).acquire_owned().await?;
        let transport = Arc::clone(transport);
        let pb = Arc::clone(&pb);
        let errors = Arc::clone(&errors);
        let bucket = bucket.to_owned();
        let key = if dest_prefix.is_empty() {
            rel_key.clone()
        } else {
            format!("{}/{rel_key}", dest_prefix.trim_end_matches('/'))
        };
        let ct = mime_guess::from_path(full_path)
            .first_raw()
            .unwrap_or("application/octet-stream")
            .to_owned();
        let full_path = full_path.clone();
        let rel_key = rel_key.clone();

        handles.push(tokio::spawn(async move {
            if let Err(e) = transport.put_object(&bucket, &key, &full_path, Some(&ct)).await {
                pb.suspend(|| eprintln!("ERROR {rel_key}: {e}"));
                errors.fetch_add(1, Ordering::Relaxed);
            }
            pb.inc(1);
            drop(permit);
        }));
    }
    for handle in handles {
        handle.await?;
    }
    pb.finish_and_clear();
    let err_count = errors.load(Ordering::Relaxed);
    #[allow(clippy::cast_possible_truncation)]
    let succeeded = entries.len() - err_count as usize;
    println!("upload: {succeeded} files, {err_count} errors");
    if err_count > 0 {
        anyhow::bail!("{err_count} upload(s) failed");
    }
    Ok(())
}

async fn download(
    transport: &Arc<dyn Transport>,
    src: &str,
    dest: &str,
    recursive: bool,
    concurrency: usize,
    version_id: Option<&str>,
) -> anyhow::Result<()> {
    let parsed = S3Uri::parse(src)?;
    let bucket = parsed.bucket();
    let src_prefix = parsed.key();
    let dest_path = Path::new(dest);

    if !recursive {
        let key = src_prefix.ok_or_else(|| anyhow::anyhow!("no object key in source URI"))?;
        let target = if dest_path.is_dir() {
            let name = key.rsplit('/').next().unwrap_or(key);
            dest_path.join(name)
        } else {
            dest_path.to_owned()
        };
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let bytes = if let Some(vid) = version_id {
            transport
                .get_object_version(bucket, key, vid, &target)
                .await?
        } else {
            transport.get_object(bucket, key, &target).await?
        };
        println!(
            "download: s3://{bucket}/{key} -> {} ({bytes} bytes)",
            target.display()
        );
        return Ok(());
    }

    if version_id.is_some() {
        anyhow::bail!("--version-id cannot be used with --recursive");
    }

    let objects = list_all_objects(transport.as_ref(), bucket, src_prefix).await?;
    let prefix_strip = src_prefix.unwrap_or_default().to_owned();
    let sem = Arc::new(Semaphore::new(concurrency));
    let pb = Arc::new(count_bar(objects.len() as u64, "download"));
    let errors = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(objects.len());

    for obj in &objects {
        let permit = Arc::clone(&sem).acquire_owned().await?;
        let transport = Arc::clone(transport);
        let pb = Arc::clone(&pb);
        let errors = Arc::clone(&errors);
        let bucket = bucket.to_owned();
        let obj_key = obj.key.clone();
        let rel = obj.key.strip_prefix(&prefix_strip).unwrap_or(&obj.key);
        let rel = rel.strip_prefix('/').unwrap_or(rel);
        let target: PathBuf = dest_path.join(rel);

        handles.push(tokio::spawn(async move {
            if let Some(parent) = target.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            if let Err(e) = transport.get_object(&bucket, &obj_key, &target).await {
                pb.suspend(|| eprintln!("ERROR {obj_key}: {e}"));
                errors.fetch_add(1, Ordering::Relaxed);
            }
            pb.inc(1);
            drop(permit);
        }));
    }
    for handle in handles {
        handle.await?;
    }
    pb.finish_and_clear();
    let err_count = errors.load(Ordering::Relaxed);
    #[allow(clippy::cast_possible_truncation)]
    let succeeded = objects.len() - err_count as usize;
    println!("download: {succeeded} files, {err_count} errors");
    if err_count > 0 {
        anyhow::bail!("{err_count} download(s) failed");
    }
    Ok(())
}

async fn copy_s3_to_s3(
    transport: &Arc<dyn Transport>,
    src: &str,
    dest: &str,
    recursive: bool,
    concurrency: usize,
) -> anyhow::Result<()> {
    let src_parsed = S3Uri::parse(src)?;
    let dest_parsed = S3Uri::parse(dest)?;
    let src_bucket = src_parsed.bucket();
    let dest_bucket = dest_parsed.bucket();
    let src_prefix = src_parsed.key();
    let dest_prefix = dest_parsed.key().unwrap_or_default();

    if !recursive {
        let key = src_prefix.ok_or_else(|| anyhow::anyhow!("no object key in source URI"))?;
        let dest_key = if dest_prefix.is_empty() || dest_prefix.ends_with('/') {
            let name = key.rsplit('/').next().unwrap_or(key);
            format!("{dest_prefix}{name}")
        } else {
            dest_prefix.to_owned()
        };
        transport
            .copy_object(src_bucket, key, dest_bucket, &dest_key)
            .await?;
        println!("copy: s3://{src_bucket}/{key} -> s3://{dest_bucket}/{dest_key}");
        return Ok(());
    }

    let objects = list_all_objects(transport.as_ref(), src_bucket, src_prefix).await?;
    let prefix_strip = src_prefix.unwrap_or_default().to_owned();
    let sem = Arc::new(Semaphore::new(concurrency));
    let pb = Arc::new(count_bar(objects.len() as u64, "copy"));
    let errors = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(objects.len());

    for obj in &objects {
        let permit = Arc::clone(&sem).acquire_owned().await?;
        let transport = Arc::clone(transport);
        let pb = Arc::clone(&pb);
        let errors = Arc::clone(&errors);
        let src_bucket = src_bucket.to_owned();
        let dest_bucket = dest_bucket.to_owned();
        let obj_key = obj.key.clone();
        let rel = obj.key.strip_prefix(&prefix_strip).unwrap_or(&obj.key);
        let rel = rel.strip_prefix('/').unwrap_or(rel);
        let dest_key = if dest_prefix.is_empty() {
            rel.to_owned()
        } else {
            format!("{}/{rel}", dest_prefix.trim_end_matches('/'))
        };

        handles.push(tokio::spawn(async move {
            let result = transport
                .copy_object(&src_bucket, &obj_key, &dest_bucket, &dest_key)
                .await;
            if let Err(e) = result {
                pb.suspend(|| eprintln!("ERROR {obj_key}: {e}"));
                errors.fetch_add(1, Ordering::Relaxed);
            }
            pb.inc(1);
            drop(permit);
        }));
    }
    for handle in handles {
        handle.await?;
    }
    pb.finish_and_clear();
    let err_count = errors.load(Ordering::Relaxed);
    #[allow(clippy::cast_possible_truncation)]
    let succeeded = objects.len() - err_count as usize;
    println!("copy: {succeeded} files, {err_count} errors");
    if err_count > 0 {
        anyhow::bail!("{err_count} copy(s) failed");
    }
    Ok(())
}

fn collect_local_files(root: &Path) -> Vec<(String, PathBuf)> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        if entry.file_type().is_file() {
            let rel = entry
                .path()
                .strip_prefix(root)
                .unwrap_or_else(|_| entry.path())
                .to_string_lossy()
                .replace('\\', "/");
            files.push((rel, entry.path().to_owned()));
        }
    }
    files
}
