use std::path::{Path, PathBuf};

use super::progress::count_bar;
use super::{is_s3_uri, list_all_objects, S3Uri, Transport};

pub async fn run(
    transport: &dyn Transport,
    src: &str,
    dest: &str,
    recursive: bool,
) -> anyhow::Result<()> {
    let src_s3 = is_s3_uri(src);
    let dest_s3 = is_s3_uri(dest);

    match (src_s3, dest_s3) {
        (false, true) => upload(transport, src, dest, recursive).await,
        (true, false) => download(transport, src, dest, recursive).await,
        (true, true) => copy_s3_to_s3(transport, src, dest, recursive).await,
        (false, false) => anyhow::bail!("at least one path must be an s3:// URI"),
    }
}

async fn upload(
    transport: &dyn Transport,
    src: &str,
    dest: &str,
    recursive: bool,
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
    let pb = count_bar(entries.len() as u64, "upload");
    let mut errors = 0u64;
    for (rel_key, full_path) in &entries {
        let key = if dest_prefix.is_empty() {
            rel_key.clone()
        } else {
            format!("{}/{rel_key}", dest_prefix.trim_end_matches('/'))
        };
        let ct = mime_guess::from_path(full_path)
            .first_raw()
            .unwrap_or("application/octet-stream");
        pb.set_message(rel_key.clone());
        if let Err(e) = transport.put_object(bucket, &key, full_path, Some(ct)).await {
            pb.suspend(|| eprintln!("ERROR {rel_key}: {e}"));
            errors += 1;
        }
        pb.inc(1);
    }
    pb.finish_and_clear();
    #[allow(clippy::cast_possible_truncation)]
    let succeeded = entries.len() - errors as usize;
    println!("upload: {succeeded} files, {errors} errors");
    if errors > 0 {
        anyhow::bail!("{errors} upload(s) failed");
    }
    Ok(())
}

async fn download(
    transport: &dyn Transport,
    src: &str,
    dest: &str,
    recursive: bool,
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
        let bytes = transport.get_object(bucket, key, &target).await?;
        println!("download: s3://{bucket}/{key} -> {} ({bytes} bytes)", target.display());
        return Ok(());
    }

    let objects = list_all_objects(transport, bucket, src_prefix).await?;
    let prefix_strip = src_prefix.unwrap_or_default();
    let pb = count_bar(objects.len() as u64, "download");
    let mut errors = 0u64;
    for obj in &objects {
        let rel = obj.key.strip_prefix(prefix_strip).unwrap_or(&obj.key);
        let rel = rel.strip_prefix('/').unwrap_or(rel);
        let target = dest_path.join(rel);
        pb.set_message(rel.to_owned());
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if let Err(e) = transport.get_object(bucket, &obj.key, &target).await {
            pb.suspend(|| eprintln!("ERROR {}: {e}", obj.key));
            errors += 1;
        }
        pb.inc(1);
    }
    pb.finish_and_clear();
    #[allow(clippy::cast_possible_truncation)]
    let succeeded = objects.len() - errors as usize;
    println!("download: {succeeded} files, {errors} errors");
    if errors > 0 {
        anyhow::bail!("{errors} download(s) failed");
    }
    Ok(())
}

async fn copy_s3_to_s3(
    transport: &dyn Transport,
    src: &str,
    dest: &str,
    recursive: bool,
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
        let tmp = tempfile::NamedTempFile::new()?;
        transport.get_object(src_bucket, key, tmp.path()).await?;
        transport
            .put_object(dest_bucket, &dest_key, tmp.path(), None)
            .await?;
        println!("copy: s3://{src_bucket}/{key} -> s3://{dest_bucket}/{dest_key}");
        return Ok(());
    }

    let objects = list_all_objects(transport, src_bucket, src_prefix).await?;
    let prefix_strip = src_prefix.unwrap_or_default();
    let pb = count_bar(objects.len() as u64, "copy");
    let mut errors = 0u64;
    for obj in &objects {
        let rel = obj.key.strip_prefix(prefix_strip).unwrap_or(&obj.key);
        let rel = rel.strip_prefix('/').unwrap_or(rel);
        let dest_key = if dest_prefix.is_empty() {
            rel.to_owned()
        } else {
            format!("{}/{rel}", dest_prefix.trim_end_matches('/'))
        };
        pb.set_message(rel.to_owned());
        let tmp = tempfile::NamedTempFile::new()?;
        match transport.get_object(src_bucket, &obj.key, tmp.path()).await {
            Ok(_) => {
                if let Err(e) = transport
                    .put_object(dest_bucket, &dest_key, tmp.path(), None)
                    .await
                {
                    pb.suspend(|| eprintln!("ERROR put {dest_key}: {e}"));
                    errors += 1;
                }
            }
            Err(e) => {
                pb.suspend(|| eprintln!("ERROR get {}: {e}", obj.key));
                errors += 1;
            }
        }
        pb.inc(1);
    }
    pb.finish_and_clear();
    #[allow(clippy::cast_possible_truncation)]
    let succeeded = objects.len() - errors as usize;
    println!("copy: {succeeded} files, {errors} errors");
    if errors > 0 {
        anyhow::bail!("{errors} copy(s) failed");
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
