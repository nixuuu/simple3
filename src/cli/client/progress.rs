use indicatif::{ProgressBar, ProgressStyle};

#[allow(dead_code)]
pub fn transfer_bar(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec})",
        )
        .unwrap()
        .progress_chars("=> "),
    );
    pb
}

pub fn count_bar(total: u64, verb: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(&format!(
            "{{spinner:.green}} {verb}: {{pos}}/{{len}} {{wide_msg}}"
        ))
        .unwrap(),
    );
    pb
}

#[allow(dead_code)]
pub fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    #[allow(clippy::cast_precision_loss)]
    let mut size = n as f64;
    for unit in UNITS {
        if size < 1024.0 {
            return format!("{size:.1} {unit}");
        }
        size /= 1024.0;
    }
    format!("{size:.1} PiB")
}
