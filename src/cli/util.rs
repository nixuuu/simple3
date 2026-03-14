/// Format an epoch timestamp as a human-readable date string.
pub fn format_epoch(epoch: u64) -> String {
    let days = epoch / 86400;
    let remaining = epoch % 86400;
    let hours = remaining / 3600;
    let mins = (remaining % 3600) / 60;
    let (y, m, d) = days_to_date(days);
    format!("{y:04}-{m:02}-{d:02} {hours:02}:{mins:02}")
}

/// Convert days since Unix epoch to (year, month, day) using the civil calendar algorithm.
fn days_to_date(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
