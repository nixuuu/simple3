use crate::cli::util::format_epoch;

use super::{S3Uri, Transport};

pub async fn run(
    transport: &dyn Transport,
    uri: Option<&str>,
    recursive: bool,
    versions: bool,
) -> anyhow::Result<()> {
    if versions {
        return list_versions(transport, uri).await;
    }

    let Some(uri) = uri else {
        return list_buckets(transport).await;
    };

    if !uri.starts_with("s3://") {
        anyhow::bail!("expected s3:// URI, got: {uri}");
    }

    let parsed = S3Uri::parse(uri)?;
    let bucket = parsed.bucket();
    let prefix = parsed.key();
    let delimiter = if recursive { None } else { Some("/") };

    let mut token: Option<String> = None;
    loop {
        let result = transport
            .list_objects(bucket, prefix, delimiter, token.as_deref())
            .await?;

        for cp in &result.common_prefixes {
            println!("                           PRE {cp}");
        }

        for obj in &result.objects {
            let dt = format_epoch(obj.last_modified);
            let size = obj.size;
            println!("{dt} {size:>10} {}", obj.key);
        }

        if result.is_truncated {
            token = result.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(())
}

async fn list_buckets(transport: &dyn Transport) -> anyhow::Result<()> {
    let buckets = transport.list_buckets().await?;
    for b in &buckets {
        println!("{}", b.name);
    }
    Ok(())
}

async fn list_versions(transport: &dyn Transport, uri: Option<&str>) -> anyhow::Result<()> {
    let uri = uri.ok_or_else(|| anyhow::anyhow!("--versions requires an s3:// URI"))?;
    let parsed = S3Uri::parse(uri)?;
    let bucket = parsed.bucket();
    let prefix = parsed.key();

    let mut key_marker: Option<String> = None;
    let mut vid_marker: Option<String> = None;
    loop {
        let result = transport
            .list_object_versions(
                bucket,
                prefix,
                key_marker.as_deref(),
                vid_marker.as_deref(),
            )
            .await?;

        for entry in &result.entries {
            let dt = format_epoch(entry.last_modified);
            let latest = if entry.is_latest { " [LATEST]" } else { "" };
            if entry.is_delete_marker {
                println!(
                    "{}           0  {}  {}  [DELETE MARKER]{}",
                    entry.version_id, dt, entry.key, latest,
                );
            } else {
                println!(
                    "{}  {:>10}  {}  {}{}",
                    entry.version_id, entry.size, dt, entry.key, latest,
                );
            }
        }

        if result.is_truncated {
            key_marker = result.next_key_marker;
            vid_marker = result.next_version_id_marker;
        } else {
            break;
        }
    }

    Ok(())
}
