use crate::cli::util::format_epoch;

use super::{S3Uri, Transport, build_s3_client, ClientArgs};

pub async fn run(
    transport: &dyn Transport,
    uri: Option<&str>,
    recursive: bool,
    versions: bool,
    client_args: Option<&ClientArgs>,
) -> anyhow::Result<()> {
    if versions {
        return list_versions(uri, client_args).await;
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

async fn list_versions(
    uri: Option<&str>,
    client_args: Option<&ClientArgs>,
) -> anyhow::Result<()> {
    let uri = uri.ok_or_else(|| anyhow::anyhow!("--versions requires an s3:// URI"))?;
    let parsed = S3Uri::parse(uri)?;
    let bucket = parsed.bucket();
    let prefix = parsed.key();

    let args = client_args.ok_or_else(|| {
        anyhow::anyhow!("--versions requires HTTP transport (client args missing)")
    })?;
    let resolved = args.clone().resolve(8080);
    let client = build_s3_client(
        &resolved.endpoint,
        &resolved.access_key,
        &resolved.secret_key,
        &resolved.region,
    );

    let mut key_marker: Option<String> = None;
    let mut vid_marker: Option<String> = None;
    loop {
        let mut req = client.list_object_versions().bucket(bucket);
        if let Some(p) = prefix {
            req = req.prefix(p);
        }
        if let Some(km) = &key_marker {
            req = req.key_marker(km);
        }
        if let Some(vm) = &vid_marker {
            req = req.version_id_marker(vm);
        }
        let resp = req.send().await?;

        for ver in resp.versions() {
            print_version_entry(ver);
        }
        for dm in resp.delete_markers() {
            print_delete_marker(dm);
        }

        if resp.is_truncated().unwrap_or_default() {
            key_marker = resp.next_key_marker().map(String::from);
            vid_marker = resp.next_version_id_marker().map(String::from);
        } else {
            break;
        }
    }

    Ok(())
}

#[allow(clippy::cast_sign_loss)] // millis since epoch are always positive
fn aws_timestamp_to_epoch(ts: Option<&aws_sdk_s3::primitives::DateTime>) -> u64 {
    ts.and_then(|t| t.to_millis().ok())
        .map_or(0, |ms| (ms / 1000) as u64)
}

fn print_version_entry(ver: &aws_sdk_s3::types::ObjectVersion) {
    let vid = ver.version_id().unwrap_or("null");
    let size = ver.size().unwrap_or_default();
    let dt = format_epoch(aws_timestamp_to_epoch(ver.last_modified()));
    let key = ver.key().unwrap_or_default();
    let latest = if ver.is_latest().unwrap_or_default() {
        " [LATEST]"
    } else {
        ""
    };
    println!("{vid}  {size:>10}  {dt}  {key}{latest}");
}

fn print_delete_marker(dm: &aws_sdk_s3::types::DeleteMarkerEntry) {
    let vid = dm.version_id().unwrap_or("null");
    let dt = format_epoch(aws_timestamp_to_epoch(dm.last_modified()));
    let key = dm.key().unwrap_or_default();
    let latest = if dm.is_latest().unwrap_or_default() {
        " [LATEST]"
    } else {
        ""
    };
    println!("{vid}           0  {dt}  {key}  [DELETE MARKER]{latest}");
}
