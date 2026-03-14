use super::{S3Uri, Transport};

pub async fn run(transport: &dyn Transport, uri: &str) -> anyhow::Result<()> {
    let parsed = S3Uri::parse(uri)?;
    transport.create_bucket(parsed.bucket()).await?;
    println!("make_bucket: {uri}");
    Ok(())
}
