use std::time::Duration;

use testcontainers::core::{ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

#[path = "../common/mod.rs"]
mod common;

use crate::helpers::{apply_network_config, parse_pytest_summary, CompatReport, FailureDetail};
use crate::known_failures::ceph_known_failures;

/// Generate s3tests.conf content for ceph s3-tests.
fn generate_ceph_config(
    host: &str,
    port: u16,
    main_key: &str,
    main_secret: &str,
    alt_key: &str,
    alt_secret: &str,
) -> String {
    format!(
        r#"[DEFAULT]
host = {host}
port = {port}
is_secure = no
ssl_verify = no

[fixtures]
bucket prefix = s3test-{{random}}-

[s3 main]
display_name = Main User
user_id = main
email = main@example.com
access_key = {main_key}
secret_key = {main_secret}
api_name = default

[s3 alt]
display_name = Alt User
user_id = alt
email = alt@example.com
access_key = {alt_key}
secret_key = {alt_secret}

[s3 tenant]
display_name = Tenant
user_id = tenant
email = tenant@example.com
access_key = {main_key}
secret_key = {main_secret}
"#
    )
}

/// Run pytest inside the container and return raw stdout.
async fn run_pytest(container: &ContainerAsync<GenericImage>) -> String {
    let pytest_cmd = concat!(
        "cd /s3-tests && S3TEST_CONF=/s3-tests/s3tests.conf tox -e py -- ",
        "s3tests_boto3/functional/test_s3.py ",
        "-m 'not encryption and not sse_s3 and not bucket_logging ",
        "and not fails_on_aws and not s3select and not storage_class ",
        "and not tagging and not object_lock' ",
        "--tb=short --no-header -q 2>&1; echo \"EXIT:$?\""
    );

    let mut exec_result = container
        .exec(ExecCommand::new(["bash", "-c", pytest_cmd]))
        .await
        .unwrap();

    let stdout = exec_result.stdout_to_vec().await.unwrap();
    String::from_utf8_lossy(&stdout).into_owned()
}

/// Build a compat report from pytest output.
fn build_ceph_report(output: &str) -> CompatReport {
    let (passed, failed, skipped) = parse_pytest_summary(output);
    let total = passed + failed + skipped;

    let known = ceph_known_failures();
    let failures: Vec<FailureDetail> = output
        .lines()
        .filter(|l| l.starts_with("FAILED ") || l.starts_with("ERROR "))
        .map(|l| {
            let name = l
                .strip_prefix("FAILED ")
                .or_else(|| l.strip_prefix("ERROR "))
                .unwrap_or(l)
                .split(" - ")
                .next()
                .unwrap_or(l)
                .to_owned();
            let known_issue = known.get(name.as_str()).copied();
            FailureDetail {
                test_name: name,
                error: String::new(),
                known_issue,
            }
        })
        .collect();

    CompatReport {
        suite: "Ceph s3-tests (boto3)".to_owned(),
        total,
        passed,
        failed,
        skipped,
        failures,
    }
}

/// Run Ceph s3-tests (boto3) against an in-process simple3 server.
///
/// Requires Docker with the `simple3-ceph-s3-tests` image built:
///   docker build -t simple3-ceph-s3-tests docker/ceph-s3-tests/
///
/// Run with: `cargo test --test s3_compat compat_ceph -- --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn compat_ceph_s3_tests() {
    let dir = tempfile::tempdir().unwrap();
    let srv = common::start_server_external(dir.path()).await;
    let host = common::container_host();

    let config = generate_ceph_config(
        host,
        srv.port,
        &srv.access_key,
        &srv.secret_key,
        &srv.alt_access_key,
        &srv.alt_secret_key,
    );

    let image = GenericImage::new("simple3-ceph-s3-tests", "latest").with_wait_for(
        WaitFor::Duration {
            length: Duration::from_secs(2),
        },
    );

    let container = apply_network_config(image.with_startup_timeout(Duration::from_secs(120)))
        .start()
        .await
        .unwrap();

    // Write config into the container.
    let write_cmd = format!(
        "cat > /s3-tests/s3tests.conf << 'CEPH_CONF'\n{config}\nCEPH_CONF"
    );
    container
        .exec(ExecCommand::new(["bash", "-c", &write_cmd]))
        .await
        .unwrap();

    let output = run_pytest(&container).await;

    // Validate pytest actually ran (exit 0 = all passed, 1 = some failed, >=2 = execution error).
    let exit_code = output
        .lines()
        .rev()
        .find_map(|l| l.strip_prefix("EXIT:"))
        .and_then(|s| s.trim().parse::<i32>().ok())
        .expect("missing pytest EXIT marker in output");
    assert!(
        matches!(exit_code, 0 | 1),
        "pytest execution failed (exit code {exit_code}); check container setup"
    );

    let report = build_ceph_report(&output);
    print!("{report}");

    let unexpected: Vec<_> = report
        .failures
        .iter()
        .filter(|f| f.known_issue.is_none())
        .collect();

    assert!(
        unexpected.is_empty(),
        "{} unexpected Ceph s3-tests failures (see output above)",
        unexpected.len()
    );
}
