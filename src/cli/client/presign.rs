use std::time::Duration;

use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation};
use aws_sdk_s3::presigning::PresigningConfig;

use super::S3Uri;

const MAX_TTL_SECS: u64 = 604_800; // 7 days

pub fn parse_ttl(s: &str) -> anyhow::Result<Duration> {
    // Pure seconds: "3600"
    if let Ok(secs) = s.parse::<u64>() {
        return validate_ttl(secs);
    }

    let s_trimmed = s.trim();
    if s_trimmed.len() < 2 {
        anyhow::bail!("invalid TTL: {s}");
    }

    let (num_str, suffix) = s_trimmed.split_at(s_trimmed.len() - 1);
    let n: u64 = num_str
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid TTL: {s}"))?;
    let secs = match suffix {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        _ => anyhow::bail!("unknown TTL suffix '{suffix}', expected s/m/h/d"),
    };
    validate_ttl(secs)
}

fn validate_ttl(secs: u64) -> anyhow::Result<Duration> {
    if secs == 0 {
        anyhow::bail!("TTL must be greater than 0");
    }
    if secs > MAX_TTL_SECS {
        anyhow::bail!("TTL must not exceed 7 days (604800s), got {secs}s");
    }
    Ok(Duration::from_secs(secs))
}

pub async fn run(
    uri: &str,
    method: &str,
    ttl_str: &str,
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<()> {
    let parsed = S3Uri::parse(uri)?;
    let key = parsed
        .key()
        .ok_or_else(|| anyhow::anyhow!("presign requires an object key: s3://bucket/key"))?;
    let bucket = parsed.bucket();

    let ttl = parse_ttl(ttl_str)?;
    let presign_config = PresigningConfig::expires_in(ttl)?;

    let creds = Credentials::new(access_key, secret_key, None, None, "simple3-cli");
    let config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .region(Region::new(region.to_owned()))
        .force_path_style(true)
        .behavior_version_latest()
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .build();
    let client = aws_sdk_s3::Client::from_conf(config);

    let presigned = match method.to_uppercase().as_str() {
        "GET" => {
            client
                .get_object()
                .bucket(bucket)
                .key(key)
                .presigned(presign_config)
                .await?
        }
        "PUT" => {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .presigned(presign_config)
                .await?
        }
        other => anyhow::bail!("unsupported method: {other}, expected GET or PUT"),
    };

    println!("{}", presigned.uri());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ttl_seconds() {
        assert_eq!(parse_ttl("3600").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_ttl("1").unwrap(), Duration::from_secs(1));
        assert_eq!(parse_ttl("604800").unwrap(), Duration::from_secs(604_800));
    }

    #[test]
    fn test_parse_ttl_suffixes() {
        assert_eq!(parse_ttl("60s").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_ttl("30m").unwrap(), Duration::from_secs(1800));
        assert_eq!(parse_ttl("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_ttl("7d").unwrap(), Duration::from_secs(604_800));
    }

    #[test]
    fn test_parse_ttl_zero_rejected() {
        assert!(parse_ttl("0").is_err());
        assert!(parse_ttl("0s").is_err());
    }

    #[test]
    fn test_parse_ttl_over_7_days_rejected() {
        assert!(parse_ttl("604801").is_err());
        assert!(parse_ttl("8d").is_err());
    }

    #[test]
    fn test_parse_ttl_invalid() {
        assert!(parse_ttl("abc").is_err());
        assert!(parse_ttl("10x").is_err());
        assert!(parse_ttl("").is_err());
    }
}
