use super::{list_all_objects, S3Uri, Transport};

pub async fn run(
    transport: &dyn Transport,
    uri: &str,
    recursive: bool,
    version_id: Option<&str>,
) -> anyhow::Result<()> {
    let parsed = S3Uri::parse(uri)?;
    let bucket = parsed.bucket();

    if version_id.is_some() && recursive {
        anyhow::bail!("--version-id cannot be used with --recursive");
    }

    if let Some(vid) = version_id {
        let key = parsed
            .key()
            .ok_or_else(|| anyhow::anyhow!("no object key in URI for version delete"))?;
        transport.delete_object_version(bucket, key, vid).await?;
        println!("delete: s3://{bucket}/{key} (version {vid})");
        return Ok(());
    }

    if recursive {
        let prefix = parsed.key();
        let objects = list_all_objects(transport, bucket, prefix).await?;
        if objects.is_empty() {
            println!("delete: no objects found");
            return Ok(());
        }
        let keys: Vec<String> = objects.into_iter().map(|o| o.key).collect();
        let count = keys.len();
        transport.delete_objects(bucket, &keys).await?;
        println!("delete: {count} objects");
    } else {
        let key = parsed
            .key()
            .ok_or_else(|| anyhow::anyhow!("no object key in URI, use --recursive for prefix"))?;
        transport.delete_object(bucket, key).await?;
        println!("delete: s3://{bucket}/{key}");
    }

    Ok(())
}
