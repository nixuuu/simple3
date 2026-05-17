//! Integration tests for #14 — request size limits and list page caps.

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;

mod common;

use simple3::limits::Limits;

fn small_object_limit() -> Limits {
    Limits {
        max_object_size: 1024,
        max_list_keys: 5,
    }
}

#[tokio::test]
async fn put_object_rejected_when_above_max_size() {
    let tmp = tempfile::tempdir().unwrap();
    let server = common::start_server_with_limits(tmp.path(), small_object_limit()).await;
    let client = common::make_client(server.port, &server.access_key, &server.secret_key);

    client.create_bucket().bucket("bucket-a").send().await.unwrap();

    let body = ByteStream::from(Bytes::from(vec![0u8; 4096]));
    let err = client
        .put_object()
        .bucket("bucket-a")
        .key("big")
        .body(body)
        .send()
        .await
        .expect_err("upload above limit must fail");
    let raw = err.into_service_error();
    assert_eq!(raw.code(), Some("EntityTooLarge"), "got: {raw:?}");
}

#[tokio::test]
async fn put_object_accepts_at_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let server = common::start_server_with_limits(tmp.path(), small_object_limit()).await;
    let client = common::make_client(server.port, &server.access_key, &server.secret_key);

    client.create_bucket().bucket("bucket-a").send().await.unwrap();

    let body = ByteStream::from(Bytes::from(vec![1u8; 1024]));
    client
        .put_object()
        .bucket("bucket-a")
        .key("ok")
        .body(body)
        .send()
        .await
        .expect("upload at exact limit must succeed");
}

#[tokio::test]
async fn multipart_total_above_limit_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let server = common::start_server_with_limits(
        tmp.path(),
        Limits {
            max_object_size: 6 * 1024 * 1024,
            max_list_keys: 100,
        },
    )
    .await;
    let client = common::make_client(server.port, &server.access_key, &server.secret_key);

    client.create_bucket().bucket("bucket-a").send().await.unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("bucket-a")
        .key("mp")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id.unwrap();

    // Two 5 MiB parts → 10 MiB > 6 MiB limit
    let part_size: usize = 5 * 1024 * 1024;
    let mut parts = Vec::new();
    for n in 1..=2 {
        let p = client
            .upload_part()
            .bucket("bucket-a")
            .key("mp")
            .upload_id(&upload_id)
            .part_number(n)
            .body(ByteStream::from(Bytes::from(vec![0u8; part_size])))
            .send()
            .await
            .unwrap();
        parts.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(p.e_tag.unwrap())
                .build(),
        );
    }

    let completed = CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    let err = client
        .complete_multipart_upload()
        .bucket("bucket-a")
        .key("mp")
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect_err("multipart total above limit must fail");
    let raw = err.into_service_error();
    assert_eq!(raw.code(), Some("EntityTooLarge"), "got: {raw:?}");
}

#[tokio::test]
async fn list_objects_v2_clamps_to_max_list_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let server = common::start_server_with_limits(
        tmp.path(),
        Limits {
            max_object_size: u64::MAX,
            max_list_keys: 3,
        },
    )
    .await;
    let client = common::make_client(server.port, &server.access_key, &server.secret_key);

    client.create_bucket().bucket("bucket-a").send().await.unwrap();
    for i in 0..10 {
        client
            .put_object()
            .bucket("bucket-a")
            .key(format!("k-{i:02}"))
            .body(ByteStream::from(Bytes::from_static(b"x")))
            .send()
            .await
            .unwrap();
    }

    let listed = client
        .list_objects_v2()
        .bucket("bucket-a")
        .max_keys(100)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.contents().len(), 3, "must clamp to server max_list_keys");
    assert_eq!(listed.is_truncated(), Some(true));
}

#[test]
fn limits_default_matches_documented_values() {
    let d = Limits::default();
    assert_eq!(d.max_object_size, 5 * 1024 * 1024 * 1024);
    assert_eq!(d.max_list_keys, 1000);
}
