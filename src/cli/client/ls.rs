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
            let dt = format_timestamp(obj.last_modified);
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

fn format_timestamp(epoch_secs: u64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let dt = UNIX_EPOCH + Duration::from_secs(epoch_secs);
    let elapsed = dt
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let secs = elapsed % 60;
    let mins = (elapsed / 60) % 60;
    let hours = (elapsed / 3600) % 24;
    let days = elapsed / 86400;
    // Simple date formatting: days since epoch -> Y-M-D
    let (year, month, day) = days_to_ymd(days);
    format!("{year:04}-{month:02}-{day:02} {hours:02}:{mins:02}:{secs:02}")
}

#[allow(clippy::missing_const_for_fn)]
fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    let mut year = 1970;
    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let leap = is_leap(year);
    let months: [u64; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1;
    for &m in &months {
        if days < m {
            break;
        }
        days -= m;
        month += 1;
    }
    (year, month, days + 1)
}

const fn is_leap(y: u64) -> bool {
    (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400)
}
