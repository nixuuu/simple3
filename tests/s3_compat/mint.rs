use std::time::Duration;

use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

#[path = "../common/mod.rs"]
mod common;

use crate::helpers::{apply_network_config, build_mint_report, parse_mint_log};
use crate::known_failures::mint_known_failures;

/// Marker echoed before catting the JSON log so we can split stdout cleanly.
const LOG_MARKER: &str = "___MINT_LOG_START___";

/// Run MinIO Mint core test suite against an in-process simple3 server.
///
/// Requires Docker. Run with: `cargo test --test s3_compat compat_mint -- --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn compat_mint_core() {
    let dir = tempfile::tempdir().unwrap();
    let srv = common::start_server_external(dir.path()).await;
    let host = common::container_host();
    let endpoint = format!("{host}:{}", srv.port);

    // Mint writes JSON to /mint/log/log.json but also emits text to stdout.
    // Override ENTRYPOINT so we can chain: run Mint, echo a marker, then cat
    // the log file. We split on the marker to get clean JSON.
    let cmd = format!(
        "/mint/entrypoint.sh; echo '{LOG_MARKER}'; cat /mint/log/log.json"
    );
    let image = GenericImage::new("minio/mint", "edge")
        .with_entrypoint("/bin/sh")
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(2),
        });

    let container = image
        .with_env_var("SERVER_ENDPOINT", &endpoint)
        .with_env_var("ACCESS_KEY", &srv.access_key)
        .with_env_var("SECRET_KEY", &srv.secret_key)
        .with_env_var("ENABLE_HTTPS", "0")
        .with_env_var("MINT_MODE", "core")
        .with_cmd(["-c", &cmd])
        .with_startup_timeout(Duration::from_secs(60));

    let container = apply_network_config(container).start().await.unwrap();

    // Wait for Mint to finish (poll exit code with timeout).
    tokio::time::timeout(Duration::from_secs(2400), async {
        loop {
            if container.exit_code().await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    })
    .await
    .expect("Mint did not finish within 40 minutes");

    // Read stdout and extract only the JSON log section after our marker.
    let stdout = container.stdout_to_vec().await.unwrap();
    let stderr = container.stderr_to_vec().await.unwrap();
    let full_output = String::from_utf8_lossy(&stdout);

    let log_section = full_output
        .split_once(LOG_MARKER)
        .map(|(_, after)| after.trim())
        .unwrap_or_else(|| {
            let stderr_str = String::from_utf8_lossy(&stderr);
            panic!(
                "Mint log marker not found in stdout — container may have crashed.\n\
                 --- stdout ---\n{full_output}\n--- stderr ---\n{stderr_str}\n--- end ---"
            );
        });

    let results = parse_mint_log(log_section);
    assert!(
        !results.is_empty(),
        "no Mint JSON records found after marker.\n--- log section ---\n{log_section}\n--- end ---"
    );

    let known = mint_known_failures();
    let report = build_mint_report(&results, &known);

    print!("{report}");

    // Report-only: don't fail CI on known compatibility gaps.
    // Once known_failures is populated, switch to assertion mode to catch regressions.
    if report.failed > 0 {
        eprintln!(
            "note: {}/{} Mint tests failed (report-only, not blocking CI)",
            report.failed, report.total
        );
    }
}
