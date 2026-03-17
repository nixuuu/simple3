// Included by multiple test binaries; not all items used by each.
#![allow(dead_code)]

use std::sync::Arc;

use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation};
use hyper_util::rt::TokioIo;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;

use simple3::auth::policy::{Effect, OneOrMany, PolicyDocument, Statement};
use simple3::auth::s3_auth::AuthProvider;
use simple3::auth::AuthStore;
use simple3::s3impl::SimpleStorage;
use simple3::storage::Storage;

pub struct TestServer {
    pub port: u16,
    pub access_key: String,
    pub secret_key: String,
    pub auth_store: Arc<AuthStore>,
}

/// Server accessible from Docker containers, with two users for cross-user tests.
pub struct ExternalTestServer {
    pub port: u16,
    pub access_key: String,
    pub secret_key: String,
    pub alt_access_key: String,
    pub alt_secret_key: String,
    pub auth_store: Arc<AuthStore>,
}

/// Bootstrap storage + auth and build an S3 service.
/// Returns (service, auth_store, root_access_key, root_secret_key).
fn build_s3_service(
    dir: &std::path::Path,
) -> (s3s::service::S3Service, Arc<AuthStore>, String, String) {
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
    let svc = builder.build();

    (svc, auth_store, access_key, secret_key)
}

/// Spawn a background task that accepts connections on `listener` and serves `svc`.
fn spawn_accept_loop(listener: TcpListener, svc: s3s::service::S3Service) {
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let svc = svc.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
}

pub async fn start_server(dir: &std::path::Path) -> TestServer {
    let (svc, auth_store, access_key, secret_key) = build_s3_service(dir);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    spawn_accept_loop(listener, svc);

    TestServer {
        port,
        access_key,
        secret_key,
        auth_store,
    }
}

/// Start a server bound to 0.0.0.0 (accessible from Docker containers) with
/// a second non-admin user for cross-user compatibility tests.
pub async fn start_server_external(dir: &std::path::Path) -> ExternalTestServer {
    let (svc, auth_store, access_key, secret_key) = build_s3_service(dir);

    // Create alt user with full S3 access for cross-user tests.
    let alt_key = auth_store.create_key("alt-user", false).unwrap();
    let full_access = PolicyDocument {
        version: "2024-01-01".to_owned(),
        statements: vec![Statement {
            sid: None,
            effect: Effect::Allow,
            action: OneOrMany(vec!["s3:*".to_owned()]),
            resource: OneOrMany(vec!["arn:s3:::*".to_owned()]),
        }],
    };
    auth_store
        .create_policy("full-access", full_access)
        .unwrap();
    auth_store
        .attach_policy(&alt_key.access_key_id, "full-access")
        .unwrap();

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    spawn_accept_loop(listener, svc);

    ExternalTestServer {
        port,
        access_key,
        secret_key,
        alt_access_key: alt_key.access_key_id,
        alt_secret_key: alt_key.secret_key,
        auth_store,
    }
}

/// Returns the hostname that a Docker container should use to reach the host.
pub fn container_host() -> &'static str {
    if cfg!(target_os = "linux") {
        "127.0.0.1"
    } else {
        "host.docker.internal"
    }
}

pub fn make_client(port: u16, access_key: &str, secret_key: &str) -> aws_sdk_s3::Client {
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
