#![allow(dead_code)]

use std::sync::Arc;

use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation};
use hyper_util::rt::TokioIo;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;

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

pub async fn start_server(dir: &std::path::Path) -> TestServer {
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
