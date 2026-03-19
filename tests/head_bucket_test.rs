mod common;
use aws_sdk_s3::error::SdkError;
use common::{make_client, start_server};

#[tokio::test]
async fn test_head_bucket_exists() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    let resp = client.head_bucket().bucket("test").send().await;
    assert!(resp.is_ok());
}

#[tokio::test]
async fn test_head_bucket_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    let err = client
        .head_bucket()
        .bucket("nonexistent")
        .send()
        .await
        .expect_err("head_bucket should fail for missing bucket");
    assert!(
        err.into_service_error().is_not_found(),
        "expected NotFound error"
    );
}

#[tokio::test]
async fn test_head_bucket_after_delete() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();
    client.head_bucket().bucket("test").send().await.unwrap();

    client.delete_bucket().bucket("test").send().await.unwrap();

    let err = client
        .head_bucket()
        .bucket("test")
        .send()
        .await
        .expect_err("head_bucket should fail after deletion");
    assert!(
        err.into_service_error().is_not_found(),
        "expected NotFound error"
    );
}

#[tokio::test]
async fn test_head_bucket_auth_failure() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start_server(dir.path()).await;
    let client = make_client(srv.port, &srv.access_key, &srv.secret_key);

    client.create_bucket().bucket("test").send().await.unwrap();

    // Create non-admin key with no policies (no permissions at all)
    let restricted = srv.auth_store.create_key("no-perms", false).unwrap();
    let bad_client = make_client(srv.port, &restricted.access_key_id, &restricted.secret_key);

    let err = bad_client
        .head_bucket()
        .bucket("test")
        .send()
        .await
        .expect_err("head_bucket should fail with insufficient permissions");

    // HeadBucket returns 403 for auth failures (no XML body, so check HTTP status)
    let status = match &err {
        SdkError::ServiceError(e) => e.raw().status().as_u16(),
        SdkError::ResponseError(e) => e.raw().status().as_u16(),
        other => panic!("unexpected error variant: {other:?}"),
    };
    assert_eq!(status, 403, "expected 403 Forbidden, got {status}");
}
