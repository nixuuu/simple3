use std::time::Duration;

use aws_sdk_s3::presigning::PresigningConfig;
use clap::ValueEnum;

use super::{ResolvedArgs, S3Uri, build_s3_client};

const MAX_TTL_SECS: u64 = 604_800; // 7 days

#[derive(Clone, Copy, ValueEnum)]
pub enum PresignMethod {
    Get,
    Put,
}

pub fn parse_ttl(s: &str) -> anyhow::Result<Duration> {
    let s_trimmed = s.trim();
    if let Ok(secs) = s_trimmed.parse::<u64>() {
        return validate_ttl(secs);
    }

    let suffix = s_trimmed
        .chars()
        .next_back()
        .ok_or_else(|| anyhow::anyhow!("invalid TTL: {s}"))?;
    let num_str = &s_trimmed[..s_trimmed.len() - suffix.len_utf8()];
    let n: u64 = num_str
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid TTL: {s}"))?;
    let multiplier: u64 = match suffix {
        's' => 1,
        'm' => 60,
        'h' => 3600,
        'd' => 86400,
        _ => anyhow::bail!("unknown TTL suffix '{suffix}', expected s/m/h/d"),
    };
    let secs = n
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow::anyhow!("TTL overflow: {s}"))?;
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
    method: PresignMethod,
    ttl_str: &str,
    resolved: ResolvedArgs,
) -> anyhow::Result<()> {
    let parsed = S3Uri::parse(uri)?;
    let key = parsed
        .key()
        .ok_or_else(|| anyhow::anyhow!("presign requires an object key: s3://bucket/key"))?;
    let bucket = parsed.bucket();

    let ttl = parse_ttl(ttl_str)?;
    let presign_config = PresigningConfig::expires_in(ttl)?;
    let client = build_s3_client(
        &resolved.endpoint,
        &resolved.access_key,
        &resolved.secret_key,
        &resolved.region,
    );

    let presigned = match method {
        PresignMethod::Get => {
            client
                .get_object()
                .bucket(bucket)
                .key(key)
                .presigned(presign_config)
                .await?
        }
        PresignMethod::Put => {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .presigned(presign_config)
                .await?
        }
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

    #[test]
    fn test_parse_ttl_overflow() {
        assert!(parse_ttl("99999999999999999h").is_err());
        assert!(parse_ttl("99999999999999999d").is_err());
    }

    #[test]
    fn test_parse_ttl_multibyte_suffix() {
        assert!(parse_ttl("1秒").is_err());
        assert!(parse_ttl("5分").is_err());
    }
}
