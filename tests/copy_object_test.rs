use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::MetadataDirective;
use hyper_util::rt::TokioIo;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;

use simple3::auth::s3_auth::AuthProvider;
use simple3::auth::AuthStore;
use simple3::s3impl::SimpleStorage;
use simple3::storage::Storage;

struct TestServer {
    port: u16,
    access_key: String,
    secret_key: String,
}

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
async fn test_copy_same_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    let body = b"hello copy";
    client
        .put_object()
        .bucket("test")
        .key("src.txt")
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();

    let resp = client
        .copy_object()
        .bucket("test")
        .key("dest.txt")
        .copy_source("test/src.txt")
        .send()
        .await
        .unwrap();

    assert!(resp.copy_object_result().is_some());
    let result = resp.copy_object_result().unwrap();
    assert!(result.e_tag().is_some());

    let get = client
        .get_object()
        .bucket("test")
        .key("dest.txt")
        .send()
        .await
        .unwrap();
    let data = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(&data[..], body);
}

#[tokio::test]
async fn test_copy_cross_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("src").send().await.unwrap();
    client.create_bucket().bucket("dest").send().await.unwrap();

    let body = b"cross bucket data";
    client
        .put_object()
        .bucket("src")
        .key("file.bin")
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket("dest")
        .key("file.bin")
        .copy_source("src/file.bin")
        .send()
        .await
        .unwrap();

    let get = client
        .get_object()
        .bucket("dest")
        .key("file.bin")
        .send()
        .await
        .unwrap();
    let data = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(&data[..], body);
}

#[tokio::test]
async fn test_copy_metadata_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    let mut meta = HashMap::new();
    meta.insert("x-custom".to_owned(), "value123".to_owned());

    client
        .put_object()
        .bucket("test")
        .key("src.txt")
        .body(ByteStream::from_static(b"meta test"))
        .content_type("text/plain")
        .set_metadata(Some(meta))
        .send()
        .await
        .unwrap();

    // Default directive = COPY, metadata should be preserved
    client
        .copy_object()
        .bucket("test")
        .key("dest.txt")
        .copy_source("test/src.txt")
        .send()
        .await
        .unwrap();

    let head = client
        .head_object()
        .bucket("test")
        .key("dest.txt")
        .send()
        .await
        .unwrap();

    assert_eq!(head.content_type().unwrap(), "text/plain");
    let dest_meta = head.metadata().unwrap();
    assert_eq!(dest_meta.get("x-custom").unwrap(), "value123");
}

#[tokio::test]
async fn test_copy_metadata_replace() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    let mut meta = HashMap::new();
    meta.insert("x-original".to_owned(), "old".to_owned());

    client
        .put_object()
        .bucket("test")
        .key("src.txt")
        .body(ByteStream::from_static(b"replace test"))
        .content_type("text/plain")
        .set_metadata(Some(meta))
        .send()
        .await
        .unwrap();

    let mut new_meta = HashMap::new();
    new_meta.insert("x-new".to_owned(), "fresh".to_owned());

    client
        .copy_object()
        .bucket("test")
        .key("dest.txt")
        .copy_source("test/src.txt")
        .metadata_directive(MetadataDirective::Replace)
        .content_type("application/json")
        .set_metadata(Some(new_meta))
        .send()
        .await
        .unwrap();

    let head = client
        .head_object()
        .bucket("test")
        .key("dest.txt")
        .send()
        .await
        .unwrap();

    assert_eq!(head.content_type().unwrap(), "application/json");
    let dest_meta = head.metadata().unwrap();
    assert!(dest_meta.get("x-original").is_none());
    assert_eq!(dest_meta.get("x-new").unwrap(), "fresh");
}

#[tokio::test]
async fn test_copy_nonexistent_source() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    let err = client
        .copy_object()
        .bucket("test")
        .key("dest.txt")
        .copy_source("test/nonexistent.txt")
        .send()
        .await;

    assert!(err.is_err());
}

#[tokio::test]
async fn test_copy_overwrite_existing() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    client
        .put_object()
        .bucket("test")
        .key("src.txt")
        .body(ByteStream::from_static(b"new data"))
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket("test")
        .key("dest.txt")
        .body(ByteStream::from_static(b"old data"))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket("test")
        .key("dest.txt")
        .copy_source("test/src.txt")
        .send()
        .await
        .unwrap();

    let get = client
        .get_object()
        .bucket("test")
        .key("dest.txt")
        .send()
        .await
        .unwrap();
    let data = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(&data[..], b"new data");
}

#[tokio::test]
async fn test_copy_empty_object() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    client
        .put_object()
        .bucket("test")
        .key("empty.txt")
        .body(ByteStream::from_static(b""))
        .content_type("text/plain")
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket("test")
        .key("empty-copy.txt")
        .copy_source("test/empty.txt")
        .send()
        .await
        .unwrap();

    let head = client
        .head_object()
        .bucket("test")
        .key("empty-copy.txt")
        .send()
        .await
        .unwrap();

    assert_eq!(head.content_length().unwrap(), 0);
    assert_eq!(head.content_type().unwrap(), "text/plain");
}

#[tokio::test]
async fn test_copy_special_chars_key() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    let body = b"special chars data";
    let key = "path/to/file with spaces & symbols.txt";

    client
        .put_object()
        .bucket("test")
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket("test")
        .key("dest/copied file.txt")
        .copy_source(format!("test/{key}"))
        .send()
        .await
        .unwrap();

    let get = client
        .get_object()
        .bucket("test")
        .key("dest/copied file.txt")
        .send()
        .await
        .unwrap();
    let data = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(&data[..], body);
}
