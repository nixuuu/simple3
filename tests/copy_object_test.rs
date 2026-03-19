mod common;
use common::{make_client, start_server};

use std::collections::HashMap;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::MetadataDirective;

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

/// Regression test: keys containing characters that require percent-encoding
/// (?, #, %) must round-trip through CopyObject correctly. The s3s library
/// decodes these at parse time; this test guards against regressions.
#[tokio::test]
async fn test_copy_percent_encoded_chars_key() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    let body = b"percent encoded data";
    let key = "docs/file?version=2#section&tag=100%done.txt";

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
        .key("backup/encoded-copy.txt")
        .copy_source(format!("test/{key}"))
        .send()
        .await
        .unwrap();

    let get = client
        .get_object()
        .bucket("test")
        .key("backup/encoded-copy.txt")
        .send()
        .await
        .unwrap();
    let data = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(&data[..], body);
}
