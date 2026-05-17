use indicatif::{ProgressBar, ProgressStyle};

pub fn count_bar(total: u64, verb: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(&format!(
            "{{spinner:.green}} {verb}: {{pos}}/{{len}} {{wide_msg}}"
        ))
        .expect("static template literal must parse"),
    );
    pb
}
