use super::*;

fn build_admin_service(dir: &std::path::Path) -> AdminService {
    let storage = Arc::new(Storage::open(dir).unwrap());
    let s3 = SimpleStorage::new(Arc::clone(&storage));
    let (auth_store, _) = simple3::auth::AuthStore::open(dir).unwrap();
    let auth_store = Arc::new(auth_store);
    let auth_provider = AuthProvider::new(Arc::clone(&auth_store));
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(auth_provider.clone());
    builder.set_access(auth_provider);
    let s3_service = builder.build();
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let prometheus_handle = recorder.handle();

    AdminService {
        s3: s3_service,
        storage,
        auth_store,
        min_disk_free_mb: 0,
        prometheus_handle,
        metrics_auth: None,
    }
}

/// Spawn an `AdminService` on a random port, accepting multiple connections.
/// Returns the port number.
async fn spawn_test_server(service: AdminService) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            let svc = service.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    port
}

fn build_admin_service_with_metrics_auth(
    dir: &std::path::Path,
    metrics_auth: Option<(String, String)>,
) -> AdminService {
    let mut svc = build_admin_service(dir);
    svc.metrics_auth = metrics_auth.map(Arc::new);
    svc
}

#[tokio::test]
async fn test_request_id_header() {
    let dir = tempfile::tempdir().unwrap();
    let port = spawn_test_server(build_admin_service(dir.path())).await;
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/health"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let header = resp
        .headers()
        .get("x-request-id")
        .expect("response must include x-request-id header");
    let id_str = header.to_str().unwrap();
    uuid::Uuid::parse_str(id_str).expect("x-request-id must be a valid UUID");
}

#[tokio::test]
async fn test_request_id_unique_per_request() {
    let dir = tempfile::tempdir().unwrap();
    let port = spawn_test_server(build_admin_service(dir.path())).await;
    let client = reqwest::Client::new();
    let r1 = client
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await
        .unwrap();
    let r2 = client
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await
        .unwrap();
    let id1 = r1.headers().get("x-request-id").unwrap().to_str().unwrap();
    let id2 = r2.headers().get("x-request-id").unwrap().to_str().unwrap();
    assert_ne!(id1, id2, "each request must get a unique request ID");
}

#[test]
fn test_ready_ok() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("test-bucket").unwrap();
    let resp = handle_ready(&storage, 0);
    assert_eq!(resp.status(), 200);
}

#[test]
fn test_ready_disk_low() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let resp = handle_ready(&storage, u64::MAX / (1024 * 1024));
    assert_eq!(resp.status(), 503);
}

#[test]
fn test_collect_ready_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b1").unwrap();
    storage.create_bucket("b2").unwrap();
    let m = collect_ready_metrics(&storage).unwrap();
    assert_eq!(m.bucket_count, 2);
    assert_eq!(m.total_size, 0);
    assert_eq!(m.total_dead, 0);
    assert!(!m.compaction_running);
}

#[test]
fn test_disk_free_nonzero() {
    let dir = tempfile::tempdir().unwrap();
    assert!(disk_free(dir.path()) > 0);
}

#[tokio::test]
async fn test_metrics_no_auth_open() {
    let dir = tempfile::tempdir().unwrap();
    let port = spawn_test_server(build_admin_service(dir.path())).await;
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_metrics_auth_rejects_no_header() {
    let dir = tempfile::tempdir().unwrap();
    let service = build_admin_service_with_metrics_auth(
        dir.path(),
        Some(("user".into(), "pass".into())),
    );
    let port = spawn_test_server(service).await;
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    assert_eq!(
        resp.headers().get("www-authenticate").unwrap().to_str().unwrap(),
        "Basic realm=\"metrics\""
    );
}

#[tokio::test]
async fn test_metrics_auth_accepts_valid() {
    let dir = tempfile::tempdir().unwrap();
    let service = build_admin_service_with_metrics_auth(
        dir.path(),
        Some(("user".into(), "pass".into())),
    );
    let port = spawn_test_server(service).await;
    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .basic_auth("user", Some("pass"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_metrics_auth_rejects_wrong_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let service = build_admin_service_with_metrics_auth(
        dir.path(),
        Some(("user".into(), "pass".into())),
    );
    let port = spawn_test_server(service).await;
    let client = reqwest::Client::new();

    // Wrong password
    let resp = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .basic_auth("user", Some("wrong"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Wrong username
    let resp = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .basic_auth("wrong", Some("pass"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Malformed header (not base64)
    let resp = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .header("authorization", "Basic %%%notbase64%%%")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_metrics_auth_password_with_colons() {
    let dir = tempfile::tempdir().unwrap();
    let service = build_admin_service_with_metrics_auth(
        dir.path(),
        Some(("user".into(), "p:a:ss".into())),
    );
    let port = spawn_test_server(service).await;
    let resp = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .basic_auth("user", Some("p:a:ss"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[test]
fn test_compacting_counter() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    assert!(!storage.is_compacting());
    storage.begin_compacting();
    assert!(storage.is_compacting());
    storage.begin_compacting();
    assert!(storage.is_compacting());
    storage.end_compacting();
    assert!(storage.is_compacting());
    storage.end_compacting();
    assert!(!storage.is_compacting());
}
