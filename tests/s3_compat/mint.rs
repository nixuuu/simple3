use std::time::Duration;

use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

#[path = "../common/mod.rs"]
mod common;

use crate::helpers::{apply_network_config, build_mint_report, parse_mint_log};
use crate::known_failures::mint_known_failures;

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

    // Mint runs its test suite, writes JSON logs to /mint/log/log.json, then exits.
    // The image has ENTRYPOINT ["/mint/entrypoint.sh"] — override it to /bin/sh
    // so we can chain: run Mint, then cat the JSON log to stdout for capture.
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
        .with_cmd(["-c", "/mint/entrypoint.sh; cat /mint/log/log.json"])
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

    // Read stdout which contains the JSON log (from our cat command).
    let stdout = container.stdout_to_vec().await.unwrap();
    let stderr = container.stderr_to_vec().await.unwrap();
    let log_content = String::from_utf8_lossy(&stdout);

    if log_content.trim().is_empty() {
        let stderr_str = String::from_utf8_lossy(&stderr);
        panic!(
            "Mint produced no stdout.\n--- stderr ---\n{stderr_str}\n--- end ---"
        );
    }

    let results = parse_mint_log(&log_content);
    assert!(
        !results.is_empty(),
        "no Mint JSON records found in output.\n--- raw stdout ---\n{log_content}\n--- end ---"
    );

    let known = mint_known_failures();
    let report = build_mint_report(&results, &known);

    // Print report.
    print!("{report}");

    // Fail on unexpected failures only.
    let unexpected: Vec<_> = report
        .failures
        .iter()
        .filter(|f| f.known_issue.is_none())
        .collect();

    assert!(
        unexpected.is_empty(),
        "{} unexpected Mint failures (see output above)",
        unexpected.len()
    );
}
