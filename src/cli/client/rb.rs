use super::{list_all_objects, S3Uri, Transport};

pub async fn run(transport: &dyn Transport, uri: &str, force: bool) -> anyhow::Result<()> {
    let parsed = S3Uri::parse(uri)?;
    let bucket = parsed.bucket();

    if force {
        let objects = list_all_objects(transport, bucket, None).await?;
        if !objects.is_empty() {
            let keys: Vec<String> = objects.into_iter().map(|o| o.key).collect();
            let count = keys.len();
            transport.delete_objects(bucket, &keys).await?;
            println!("delete: {count} objects");
        }
    }

    transport.delete_bucket(bucket).await?;
    println!("remove_bucket: {uri}");
    Ok(())
}
