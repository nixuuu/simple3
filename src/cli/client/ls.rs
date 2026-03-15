use crate::cli::util::format_epoch;

use super::{S3Uri, Transport};

pub async fn run(
    transport: &dyn Transport,
    uri: Option<&str>,
    recursive: bool,
) -> anyhow::Result<()> {
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
