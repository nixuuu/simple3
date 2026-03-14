use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;

use super::progress::count_bar;
use super::{is_s3_uri, list_all_objects, S3Uri, Transport};

pub async fn run(
    transport: &dyn Transport,
    src: &str,
    dest: &str,
    delete: bool,
    dryrun: bool,
    size_only: bool,
    concurrency: usize,
) -> anyhow::Result<()> {
    let src_s3 = is_s3_uri(src);
    let dest_s3 = is_s3_uri(dest);

    match (src_s3, dest_s3) {
        (false, true) => {
            sync_up(transport, src, dest, delete, dryrun, size_only, concurrency).await
        }
        (true, false) => {
            sync_down(transport, src, dest, delete, dryrun, size_only, concurrency).await
        }
        (true, true) => {
            sync_s3(transport, src, dest, delete, dryrun, size_only, concurrency).await
        }
        (false, false) => anyhow::bail!("at least one path must be an s3:// URI"),
    }
}

/// File metadata used for comparison.
struct FileMeta {
    size: u64,
    last_modified: u64,
}

/// Build local file manifest: `relative_key` -> (size, `mtime_epoch_secs`).
fn local_manifest(root: &Path) -> anyhow::Result<HashMap<String, FileMeta>> {
    let mut map = HashMap::new();
    for entry in walkdir::WalkDir::new(root).into_iter().filter_map(std::result::Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let rel = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        let meta = entry.metadata()?;
        let mtime = meta
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        map.insert(
            rel,
            FileMeta {
                size: meta.len(),
                last_modified: mtime,
            },
        );
    }
    Ok(map)
}

/// Build remote file manifest: key -> (size, `last_modified_epoch`).
async fn remote_manifest(
    transport: &dyn Transport,
    bucket: &str,
    prefix: Option<&str>,
) -> anyhow::Result<HashMap<String, FileMeta>> {
    let objects = list_all_objects(transport, bucket, prefix).await?;
    let prefix_str = prefix.unwrap_or_default();
    let mut map = HashMap::new();
    for obj in objects {
        let rel = obj.key.strip_prefix(prefix_str).unwrap_or(&obj.key);
        let rel = rel.strip_prefix('/').unwrap_or(rel);
        map.insert(
            rel.to_owned(),
            FileMeta {
                size: obj.size,
                last_modified: obj.last_modified,
            },
        );
    }
    Ok(map)
}

const fn needs_sync(src: &FileMeta, dest: &FileMeta, size_only: bool) -> bool {
    if src.size != dest.size {
        return true;
    }
    if size_only {
        return false;
    }
    src.last_modified > dest.last_modified
}

/// local -> S3
async fn sync_up(
    transport: &dyn Transport,
    src: &str,
    dest: &str,
    delete: bool,
    dryrun: bool,
    size_only: bool,
    concurrency: usize,
) -> anyhow::Result<()> {
    let parsed = S3Uri::parse(dest)?;
    let bucket = parsed.bucket();
    let prefix = parsed.key().unwrap_or_default();
    let src_path = Path::new(src);

    let local = local_manifest(src_path)?;
    let remote = remote_manifest(transport, bucket, Some(prefix)).await?;

    let to_upload = compute_uploads(&local, &remote, size_only);
    let to_delete = if delete {
        compute_deletes(&local, &remote)
    } else {
        vec![]
    };

    println!(
        "sync: {} to upload, {} to delete, {} unchanged",
        to_upload.len(),
        to_delete.len(),
        local.len() - to_upload.len()
    );

    if dryrun {
        print_plan(&to_upload, &to_delete, "(upload)", "(delete)");
        return Ok(());
    }

    exec_uploads(transport, bucket, prefix, src_path, &to_upload, concurrency).await?;
    exec_remote_deletes(transport, bucket, prefix, &to_delete).await?;
    Ok(())
}

/// S3 -> local
async fn sync_down(
    transport: &dyn Transport,
    src: &str,
    dest: &str,
    delete: bool,
    dryrun: bool,
    size_only: bool,
    concurrency: usize,
) -> anyhow::Result<()> {
    let parsed = S3Uri::parse(src)?;
    let bucket = parsed.bucket();
    let prefix = parsed.key().unwrap_or_default();
    let dest_path = Path::new(dest);

    let remote = remote_manifest(transport, bucket, Some(prefix)).await?;
    let local = if dest_path.exists() {
        local_manifest(dest_path)?
    } else {
        HashMap::new()
    };

    let to_download = compute_uploads(&remote, &local, size_only);
    let to_delete = if delete {
        compute_deletes(&remote, &local)
    } else {
        vec![]
    };

    println!(
        "sync: {} to download, {} to delete, {} unchanged",
        to_download.len(),
        to_delete.len(),
        remote.len() - to_download.len()
    );

    if dryrun {
        print_plan(&to_download, &to_delete, "(download)", "(delete)");
        return Ok(());
    }

    exec_downloads(transport, bucket, prefix, dest_path, &to_download, concurrency).await?;
    exec_local_deletes(dest_path, &to_delete).await?;
    Ok(())
}

/// S3 -> S3 (same server)
async fn sync_s3(
    transport: &dyn Transport,
    src: &str,
    dest: &str,
    delete: bool,
    dryrun: bool,
    size_only: bool,
    concurrency: usize,
) -> anyhow::Result<()> {
    let src_parsed = S3Uri::parse(src)?;
    let dest_parsed = S3Uri::parse(dest)?;
    let src_bucket = src_parsed.bucket();
    let dest_bucket = dest_parsed.bucket();
    let src_prefix = src_parsed.key().unwrap_or_default();
    let dest_prefix = dest_parsed.key().unwrap_or_default();

    let src_manifest = remote_manifest(transport, src_bucket, Some(src_prefix)).await?;
    let dest_manifest = remote_manifest(transport, dest_bucket, Some(dest_prefix)).await?;

    let to_copy = compute_uploads(&src_manifest, &dest_manifest, size_only);
    let to_delete = if delete {
        compute_deletes(&src_manifest, &dest_manifest)
    } else {
        vec![]
    };

    println!(
        "sync: {} to copy, {} to delete, {} unchanged",
        to_copy.len(),
        to_delete.len(),
        src_manifest.len() - to_copy.len()
    );

    if dryrun {
        print_plan(&to_copy, &to_delete, "(copy)", "(delete)");
        return Ok(());
    }

    let pb = count_bar(to_copy.len() as u64, "sync");
    let errors = AtomicU64::new(0);
    futures::stream::iter(to_copy.iter().map(|key| {
        let pb = &pb;
        let errors = &errors;
        let src_key = make_full_key(src_prefix, key);
        let dest_key = make_full_key(dest_prefix, key);
        async move {
            let Ok(tmp) = tempfile::NamedTempFile::new() else {
                errors.fetch_add(1, Ordering::Relaxed);
                pb.inc(1);
                return;
            };
            match transport.get_object(src_bucket, &src_key, tmp.path()).await {
                Ok(_) => {
                    if let Err(e) = transport
                        .put_object(dest_bucket, &dest_key, tmp.path(), None)
                        .await
                    {
                        pb.suspend(|| eprintln!("ERROR put {dest_key}: {e}"));
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    pb.suspend(|| eprintln!("ERROR get {src_key}: {e}"));
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            pb.inc(1);
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<()>>()
    .await;
    pb.finish_and_clear();

    let err_count = errors.load(Ordering::Relaxed);
    if err_count > 0 {
        anyhow::bail!("{err_count} copy(s) failed");
    }

    if !to_delete.is_empty() {
        let full_keys: Vec<String> = to_delete
            .iter()
            .map(|k| make_full_key(dest_prefix, k))
            .collect();
        transport.delete_objects(dest_bucket, &full_keys).await?;
    }

    Ok(())
}

// === Helpers ===

fn compute_uploads(
    src: &HashMap<String, FileMeta>,
    dest: &HashMap<String, FileMeta>,
    size_only: bool,
) -> Vec<String> {
    src.iter()
        .filter(|(key, src_meta)| {
            dest.get(*key)
                .is_none_or(|dest_meta| needs_sync(src_meta, dest_meta, size_only))
        })
        .map(|(key, _)| key.clone())
        .collect()
}

fn compute_deletes(
    src: &HashMap<String, FileMeta>,
    dest: &HashMap<String, FileMeta>,
) -> Vec<String> {
    dest.keys()
        .filter(|key| !src.contains_key(*key))
        .cloned()
        .collect()
}

fn print_plan(to_sync: &[String], to_delete: &[String], sync_label: &str, del_label: &str) {
    for key in to_sync {
        println!("  {sync_label} {key}");
    }
    for key in to_delete {
        println!("  {del_label} {key}");
    }
}

fn make_full_key(prefix: &str, rel: &str) -> String {
    if prefix.is_empty() {
        rel.to_owned()
    } else {
        format!("{}/{rel}", prefix.trim_end_matches('/'))
    }
}

async fn exec_uploads(
    transport: &dyn Transport,
    bucket: &str,
    prefix: &str,
    src_root: &Path,
    keys: &[String],
    concurrency: usize,
) -> anyhow::Result<()> {
    let pb = count_bar(keys.len() as u64, "upload");
    let errors = AtomicU64::new(0);
    futures::stream::iter(keys.iter().map(|key| {
        let pb = &pb;
        let errors = &errors;
        let full_key = make_full_key(prefix, key);
        let local_path = src_root.join(key);
        let ct = mime_guess::from_path(&local_path)
            .first_raw()
            .unwrap_or("application/octet-stream");
        async move {
            if let Err(e) = transport
                .put_object(bucket, &full_key, &local_path, Some(ct))
                .await
            {
                pb.suspend(|| eprintln!("ERROR {key}: {e}"));
                errors.fetch_add(1, Ordering::Relaxed);
            }
            pb.inc(1);
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<()>>()
    .await;
    pb.finish_and_clear();
    let err_count = errors.load(Ordering::Relaxed);
    if err_count > 0 {
        anyhow::bail!("{err_count} upload(s) failed");
    }
    Ok(())
}

async fn exec_downloads(
    transport: &dyn Transport,
    bucket: &str,
    prefix: &str,
    dest_root: &Path,
    keys: &[String],
    concurrency: usize,
) -> anyhow::Result<()> {
    let pb = count_bar(keys.len() as u64, "download");
    let errors = AtomicU64::new(0);
    futures::stream::iter(keys.iter().map(|key| {
        let pb = &pb;
        let errors = &errors;
        let full_key = make_full_key(prefix, key);
        let local_path = dest_root.join(key);
        async move {
            if let Some(parent) = local_path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            if let Err(e) = transport
                .get_object(bucket, &full_key, &local_path)
                .await
            {
                pb.suspend(|| eprintln!("ERROR {key}: {e}"));
                errors.fetch_add(1, Ordering::Relaxed);
            }
            pb.inc(1);
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<()>>()
    .await;
    pb.finish_and_clear();
    let err_count = errors.load(Ordering::Relaxed);
    if err_count > 0 {
        anyhow::bail!("{err_count} download(s) failed");
    }
    Ok(())
}

async fn exec_remote_deletes(
    transport: &dyn Transport,
    bucket: &str,
    prefix: &str,
    keys: &[String],
) -> anyhow::Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let full_keys: Vec<String> = keys.iter().map(|k| make_full_key(prefix, k)).collect();
    transport.delete_objects(bucket, &full_keys).await
}

async fn exec_local_deletes(dest_root: &Path, keys: &[String]) -> anyhow::Result<()> {
    for key in keys {
        let path = dest_root.join(key);
        if path.exists() {
            tokio::fs::remove_file(&path).await?;
            println!("  delete: {}", path.display());
        }
    }
    Ok(())
}
