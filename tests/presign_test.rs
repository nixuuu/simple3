use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use hyper_util::rt::TokioIo;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;

use simple3::auth::policy::{Effect, OneOrMany, PolicyDocument, Statement};
use simple3::auth::s3_auth::AuthProvider;
use simple3::auth::AuthStore;
use simple3::s3impl::SimpleStorage;
use simple3::storage::Storage;

struct TestServer {
    port: u16,
    access_key: String,
    secret_key: String,
    auth_store: Arc<AuthStore>,
}

/// Spin up a minimal S3 server on a random port.
async fn start_server(dir: &std::path::Path) -> TestServer {
    let storage = Arc::new(Storage::open(dir).unwrap());
    let (auth_store, bootstrap) = AuthStore::open(dir).unwrap();
    let auth_store = Arc::new(auth_store);

    let (access_key, secret_key) = match bootstrap {
        simple3::auth::types::BootstrapResult::NewRootKey {
            access_key_id,
            secret_key,
        } => (access_key_id, secret_key),
        simple3::auth::types::BootstrapResult::Existing => {
            panic!("expected new root key for fresh data dir");
        }
    };

    let s3 = SimpleStorage::new(Arc::clone(&storage));
    let auth_provider = AuthProvider::new(Arc::clone(&auth_store));
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(auth_provider.clone());
    builder.set_access(auth_provider);
    let s3_service = builder.build();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let svc = s3_service.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });

    TestServer {
        port,
        access_key,
        secret_key,
        auth_store,
    }
}

fn make_client(port: u16, access_key: &str, secret_key: &str) -> aws_sdk_s3::Client {
    let creds = Credentials::new(access_key, secret_key, None, None, "test");
    let config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .force_path_style(true)
        .behavior_version_latest()
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

#[tokio::test]
async fn test_presign_get() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    // Create bucket and put object
    client.create_bucket().bucket("test").send().await.unwrap();
    client
        .put_object()
        .bucket("test")
        .key("hello.txt")
        .body(ByteStream::from_static(b"hello world"))
        .send()
        .await
        .unwrap();

    // Generate presigned GET URL
    let presign_config = PresigningConfig::expires_in(Duration::from_secs(3600)).unwrap();
    let presigned = client
        .get_object()
        .bucket("test")
        .key("hello.txt")
        .presigned(presign_config)
        .await
        .unwrap();

    // Fetch with unsigned HTTP client
    let http_client = reqwest::Client::new();
    let resp = http_client.get(presigned.uri()).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(&body[..], b"hello world");
}

#[tokio::test]
async fn test_presign_put() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    // Generate presigned PUT URL
    let presign_config = PresigningConfig::expires_in(Duration::from_secs(3600)).unwrap();
    let presigned = client
        .put_object()
        .bucket("test")
        .key("upload.txt")
        .presigned(presign_config)
        .await
        .unwrap();

    // PUT with unsigned HTTP client
    let http_client = reqwest::Client::new();
    let resp = http_client
        .put(presigned.uri())
        .body("uploaded content")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "PUT failed: {}", resp.status());

    // Verify via normal GET
    let get_resp = client
        .get_object()
        .bucket("test")
        .key("upload.txt")
        .send()
        .await
        .unwrap();
    let data = get_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&data[..], b"uploaded content");
}

#[tokio::test]
async fn test_presign_expired() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();
    client
        .put_object()
        .bucket("test")
        .key("file.txt")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    // Generate presigned URL that is already expired (start_time far in the past + 1s TTL)
    let past = SystemTime::now() - Duration::from_secs(3600);
    let presign_config = PresigningConfig::builder()
        .start_time(past)
        .expires_in(Duration::from_secs(1))
        .build()
        .unwrap();
    let presigned = client
        .get_object()
        .bucket("test")
        .key("file.txt")
        .presigned(presign_config)
        .await
        .unwrap();

    // Should be rejected
    let http_client = reqwest::Client::new();
    let resp = http_client.get(presigned.uri()).send().await.unwrap();
    assert_eq!(resp.status(), 403, "expired presigned URL should return 403");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<Code>AccessDenied</Code>"),
        "expected AccessDenied error code, got: {body}"
    );
}

#[tokio::test]
async fn test_presign_policy_denied() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    // Create two buckets
    client.create_bucket().bucket("allowed").send().await.unwrap();
    client.create_bucket().bucket("denied").send().await.unwrap();

    // Put objects in both
    client
        .put_object()
        .bucket("allowed")
        .key("file.txt")
        .body(ByteStream::from_static(b"allowed-data"))
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("denied")
        .key("file.txt")
        .body(ByteStream::from_static(b"denied-data"))
        .send()
        .await
        .unwrap();

    // Create restricted key (non-admin) with policy allowing only "allowed" bucket
    let restricted_key = srv.auth_store.create_key("restricted", false).unwrap();

    let policy_doc = PolicyDocument {
        version: "2024-01-01".to_owned(),
        statements: vec![Statement {
            sid: None,
            effect: Effect::Allow,
            action: OneOrMany(vec!["s3:GetObject".to_owned()]),
            resource: OneOrMany(vec!["arn:s3:::allowed/*".to_owned()]),
        }],
    };
    srv.auth_store.create_policy("restricted-policy", policy_doc).unwrap();
    srv.auth_store
        .attach_policy(&restricted_key.access_key_id, "restricted-policy")
        .unwrap();

    // Build client with restricted credentials
    let restricted_client = make_client(srv.port, &restricted_key.access_key_id, &restricted_key.secret_key);

    // Presigned GET for allowed bucket should work
    let presign_config = PresigningConfig::expires_in(Duration::from_secs(3600)).unwrap();
    let presigned_allowed = restricted_client
        .get_object()
        .bucket("allowed")
        .key("file.txt")
        .presigned(presign_config)
        .await
        .unwrap();

    let http_client = reqwest::Client::new();
    let resp = http_client.get(presigned_allowed.uri()).send().await.unwrap();
    assert_eq!(resp.status(), 200, "allowed bucket should return 200");

    // Presigned GET for denied bucket should fail
    let presign_config = PresigningConfig::expires_in(Duration::from_secs(3600)).unwrap();
    let presigned_denied = restricted_client
        .get_object()
        .bucket("denied")
        .key("file.txt")
        .presigned(presign_config)
        .await
        .unwrap();

    let resp = http_client.get(presigned_denied.uri()).send().await.unwrap();
    assert_eq!(resp.status(), 403, "denied bucket should return 403");
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<Code>AccessDenied</Code>"),
        "expected AccessDenied error code, got: {body}"
    );
}

/// Exercise the `simple3 presign` CLI command end-to-end.
#[tokio::test]
async fn test_presign_cli_path() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("cli-test").send().await.unwrap();
    client
        .put_object()
        .bucket("cli-test")
        .key("doc.txt")
        .body(ByteStream::from_static(b"cli content"))
        .send()
        .await
        .unwrap();

    // Run the binary's presign subcommand
    let bin = env!("CARGO_BIN_EXE_simple3");
    let output = std::process::Command::new(bin)
        .args([
            "presign",
            "s3://cli-test/doc.txt",
            "--ttl", "1h",
            "--method", "get",
            "--endpoint-url", &format!("http://127.0.0.1:{}", srv.port),
            "--access-key", &srv.access_key,
            "--secret-key", &srv.secret_key,
        ])
        .output()
        .expect("failed to run simple3 presign");

    assert!(output.status.success(), "presign failed: {}", String::from_utf8_lossy(&output.stderr));
    let url = String::from_utf8(output.stdout).unwrap().trim().to_owned();
    assert!(url.starts_with("http://"), "expected URL, got: {url}");

    // Fetch the presigned URL with an unsigned client
    let http_client = reqwest::Client::new();
    let resp = http_client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(&body[..], b"cli content");
}
