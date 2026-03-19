use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use s3s::service::{S3Service, S3ServiceBuilder};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::signal::unix::SignalKind;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

use tracing::Instrument;

use simple3::auth::s3_auth::AuthProvider;
use simple3::auth::types::BootstrapResult;
use simple3::auth::AuthStore;
use simple3::s3impl::SimpleStorage;
use simple3::storage::Storage;

fn run_scrub(storage: &Arc<Storage>) {
    let Ok(buckets) = storage.list_buckets() else {
        return;
    };
    for name in &buckets {
        let Some(store) = storage.get_bucket(name).ok().flatten() else {
            continue;
        };
        let start = std::time::Instant::now();
        match store.verify_integrity() {
            Ok(result) => {
                let elapsed = start.elapsed();
                if result.errors.is_empty() {
                    tracing::info!(
                        "scrub: {name} — {} objects OK in {elapsed:.2?}",
                        result.verified_ok,
                    );
                } else {
                    for e in &result.errors {
                        tracing::warn!(
                            "scrub: {name}/{} — {:?}: {}",
                            e.key, e.kind, e.detail,
                        );
                    }
                    tracing::warn!(
                        "scrub: {name} — {} errors in {elapsed:.2?}",
                        result.errors.len(),
                    );
                }
            }
            Err(e) => {
                let elapsed = start.elapsed();
                tracing::error!("scrub: {name} — verify failed in {elapsed:.2?}: {e}");
            }
        }
    }
}

fn run_autovacuum(storage: &Arc<Storage>, threshold: f64) {
    let Ok(buckets) = storage.list_buckets() else {
        return;
    };
    for name in &buckets {
        let Some(store) = storage.get_bucket(name).ok().flatten() else {
            continue;
        };
        let Ok(stats) = store.segment_stats() else {
            continue;
        };
        for stat in stats {
            if stat.dead_bytes == 0 || stat.size == 0 {
                continue;
            }
            #[allow(clippy::cast_precision_loss)]
            let ratio = stat.dead_bytes as f64 / stat.size as f64;
            if ratio >= threshold {
                tracing::info!(
                    "autovacuum: {name}/seg_{:06} — {} dead bytes ({:.0}%), compacting...",
                    stat.id,
                    stat.dead_bytes,
                    ratio * 100.0
                );
                match store.compact_segment(stat.id) {
                    Ok(()) => tracing::info!("autovacuum: {name}/seg_{:06} — done", stat.id),
                    Err(e) => {
                        tracing::error!(
                            "autovacuum: {name}/seg_{:06} — compact failed: {e}",
                            stat.id
                        );
                    }
                }
            }
        }
    }
}

// === Admin endpoints ===

#[derive(Serialize)]
struct StatsResponse {
    bucket: String,
    segments: Vec<SegmentStatJson>,
    total_size: u64,
    total_dead_bytes: u64,
}

#[derive(Serialize)]
struct CompactResponse {
    bucket: String,
    compacted: bool,
    segments_before: Vec<SegmentStatJson>,
    segments_after: Vec<SegmentStatJson>,
}

#[derive(Serialize)]
struct SegmentStatJson {
    id: u32,
    size: u64,
    dead_bytes: u64,
}

#[derive(Clone)]
struct AdminService {
    s3: S3Service,
    storage: Arc<Storage>,
    auth_store: Arc<AuthStore>,
    min_disk_free_mb: u64,
    prometheus_handle: metrics_exporter_prometheus::PrometheusHandle,
    metrics_auth: Option<(String, String)>,
}

type ServiceFuture =
    Pin<Box<dyn Future<Output = Result<s3s::HttpResponse, s3s::HttpError>> + Send>>;

impl hyper::service::Service<hyper::Request<hyper::body::Incoming>> for AdminService {
    type Response = s3s::HttpResponse;
    type Error = s3s::HttpError;
    type Future = ServiceFuture;

    fn call(&self, req: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        let request_id = super::request_id::generate_request_id();
        let method = req.method().clone();
        let path = req.uri().path().to_owned();
        let span = tracing::info_span!(
            "http_request",
            request_id = %request_id,
            method = %method,
            path = %path,
        );

        let fut = self.dispatch(req, &method, &path);
        Box::pin(
            async move {
                let mut resp = fut.await?;
                super::request_id::set_request_id_header(&mut resp, &request_id);
                Ok(resp)
            }
            .instrument(span),
        )
    }
}

impl AdminService {
    fn dispatch(
        &self,
        req: hyper::Request<hyper::body::Incoming>,
        method: &hyper::Method,
        path: &str,
    ) -> ServiceFuture {
        if *method == hyper::Method::GET && path == "/metrics" {
            if let Some((ref user, ref pass)) = self.metrics_auth
                && !super::metrics_auth::check(&req, user, pass)
            {
                let resp = super::metrics_auth::unauthorized_response();
                return Box::pin(async { Ok(resp) });
            }
            let body = self.prometheus_handle.render();
            Box::pin(async move {
                Ok(hyper::Response::builder()
                    .status(200)
                    .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                    .body(s3s::Body::from(body))
                    .unwrap_or_else(|e| {
                        json_response(500, &serde_json::json!({"error": format!("metrics render: {e}")}))
                    }))
            })
        } else if *method == hyper::Method::GET && path == "/health" {
            Box::pin(async {
                Ok(json_response(200, &serde_json::json!({"status": "ok"})))
            })
        } else if *method == hyper::Method::GET && path == "/ready" {
            let storage = Arc::clone(&self.storage);
            let min_free = self.min_disk_free_mb;
            Box::pin(async move {
                Ok(tokio::task::spawn_blocking(move || handle_ready(&storage, min_free))
                    .await
                    .unwrap_or_else(|e| {
                        json_response(500, &serde_json::json!({"error": e.to_string()}))
                    }))
            })
        } else if path.starts_with("/_/keys") || path.starts_with("/_/policies") {
            let auth_store = Arc::clone(&self.auth_store);
            let method = method.clone();
            let path = path.to_owned();
            Box::pin(async move {
                Ok(super::admin_auth::handle_admin_auth(req, &path, &method, auth_store).await)
            })
        } else if path.starts_with("/_/") {
            let storage = Arc::clone(&self.storage);
            let auth_store = Arc::clone(&self.auth_store);
            Box::pin(async move { Ok(handle_admin(req, storage, auth_store).await) })
        } else {
            hyper::service::Service::call(&self.s3, req)
        }
    }
}

pub fn json_response(status: u16, body: &impl Serialize) -> s3s::HttpResponse {
    let json = serde_json::to_string(body).unwrap_or_else(|e| format!(r#"{{"error":"{e}"}}"#));
    hyper::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(s3s::Body::from(json))
        .unwrap_or_else(|e| {
            hyper::Response::builder()
                .status(500)
                .body(s3s::Body::from(format!(r#"{{"error":"{e}"}}"#)))
                .expect("fallback response with no custom headers cannot fail")
        })
}

fn stats_to_json(stats: &[simple3::storage::SegmentStat]) -> Vec<SegmentStatJson> {
    stats
        .iter()
        .map(|s| SegmentStatJson {
            id: s.id,
            size: s.size,
            dead_bytes: s.dead_bytes,
        })
        .collect()
}

async fn handle_admin(
    req: hyper::Request<hyper::body::Incoming>,
    storage: Arc<Storage>,
    _auth_store: Arc<AuthStore>,
) -> s3s::HttpResponse {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();

    if method == hyper::Method::GET && path.starts_with("/_/stats/") {
        let bucket = path["/_/stats/".len()..].trim_end_matches('/').to_owned();
        tokio::task::spawn_blocking(move || admin_stats(&storage, &bucket))
            .await
            .unwrap_or_else(|e| {
                json_response(500, &serde_json::json!({"error": e.to_string()}))
            })
    } else if method == hyper::Method::POST && path.starts_with("/_/compact/") {
        let bucket = path["/_/compact/".len()..].trim_end_matches('/').to_owned();
        tokio::task::spawn_blocking(move || admin_compact(&storage, &bucket))
            .await
            .unwrap_or_else(|e| {
                json_response(500, &serde_json::json!({"error": e.to_string()}))
            })
    } else if method == hyper::Method::GET && path.starts_with("/_/verify/") {
        let bucket = path["/_/verify/".len()..].trim_end_matches('/').to_owned();
        tokio::task::spawn_blocking(move || admin_verify(&storage, &bucket))
            .await
            .unwrap_or_else(|e| {
                json_response(500, &serde_json::json!({"error": e.to_string()}))
            })
    } else {
        json_response(404, &serde_json::json!({"error": "not found"}))
    }
}

fn admin_stats(storage: &Storage, bucket: &str) -> s3s::HttpResponse {
    let Some(store) = storage.get_bucket(bucket).ok().flatten() else {
        return json_response(404, &serde_json::json!({"error": "bucket not found"}));
    };
    let Ok(stats) = store.segment_stats() else {
        return json_response(500, &serde_json::json!({"error": "failed to read stats"}));
    };
    let total_size: u64 = stats.iter().map(|s| s.size).sum();
    let total_dead_bytes: u64 = stats.iter().map(|s| s.dead_bytes).sum();
    json_response(
        200,
        &StatsResponse {
            bucket: bucket.to_owned(),
            segments: stats_to_json(&stats),
            total_size,
            total_dead_bytes,
        },
    )
}

fn admin_compact(storage: &Storage, bucket: &str) -> s3s::HttpResponse {
    let Some(store) = storage.get_bucket(bucket).ok().flatten() else {
        return json_response(404, &serde_json::json!({"error": "bucket not found"}));
    };
    let before = store.segment_stats().unwrap_or_default();
    let before_json = stats_to_json(&before);
    storage.begin_compacting();
    let _guard = CompactingGuard(storage);
    if let Err(e) = store.compact() {
        return json_response(500, &serde_json::json!({"error": e.to_string()}));
    }
    let after = store.segment_stats().unwrap_or_default();
    json_response(
        200,
        &CompactResponse {
            bucket: bucket.to_owned(),
            compacted: true,
            segments_before: before_json,
            segments_after: stats_to_json(&after),
        },
    )
}

fn admin_verify(storage: &Storage, bucket: &str) -> s3s::HttpResponse {
    let Some(store) = storage.get_bucket(bucket).ok().flatten() else {
        return json_response(404, &serde_json::json!({"error": "bucket not found"}));
    };
    match store.verify_integrity() {
        Ok(result) => json_response(200, &result),
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}

struct ReadyMetrics {
    bucket_count: usize,
    total_size: u64,
    total_dead: u64,
    compaction_running: bool,
}

fn collect_ready_metrics(storage: &Storage) -> io::Result<ReadyMetrics> {
    let buckets = storage.list_buckets()?;
    let mut total_size: u64 = 0;
    let mut total_dead: u64 = 0;
    for name in &buckets {
        let store = storage
            .get_bucket(name)?
            .ok_or_else(|| io::Error::other(format!("bucket disappeared: {name}")))?;
        total_dead += store.dead_bytes();
        total_size += store.data_file_size()?;
    }
    Ok(ReadyMetrics {
        bucket_count: buckets.len(),
        total_size,
        total_dead,
        compaction_running: storage.is_compacting(),
    })
}

fn handle_ready(storage: &Storage, min_disk_free_mb: u64) -> s3s::HttpResponse {
    let metrics = match collect_ready_metrics(storage) {
        Ok(m) => m,
        Err(e) => {
            let msg = format!("storage not accessible: {e}");
            return json_response(
                503,
                &serde_json::json!({"status": "unavailable", "error": msg}),
            );
        }
    };

    #[allow(clippy::cast_precision_loss)] // u64 -> f64 for ratio; sub-ULP precision irrelevant
    let dead_ratio = if metrics.total_size > 0 {
        metrics.total_dead as f64 / metrics.total_size as f64
    } else {
        0.0
    };

    let disk_free_bytes = disk_free(storage.data_dir());
    let min_free_bytes = min_disk_free_mb.saturating_mul(1024 * 1024);

    let degraded = min_free_bytes > 0 && disk_free_bytes < min_free_bytes;
    let (status_code, status) = if degraded { (503, "unavailable") } else { (200, "ok") };

    let mut body = serde_json::json!({
        "status": status,
        "bucket_count": metrics.bucket_count,
        "total_size_bytes": metrics.total_size,
        "total_dead_bytes": metrics.total_dead,
        "dead_space_ratio": (dead_ratio * 10000.0).round() / 10000.0,
        "disk_free_bytes": disk_free_bytes,
        "compaction_running": metrics.compaction_running,
    });
    if degraded {
        body["error"] = serde_json::json!(format!(
            "disk free {}MB < minimum {}MB",
            disk_free_bytes / (1024 * 1024),
            min_disk_free_mb
        ));
    }

    json_response(status_code, &body)
}

fn disk_free(path: &Path) -> u64 {
    match nix::sys::statvfs::statvfs(path) {
        #[allow(clippy::useless_conversion)] // blocks_available() is u32 on macOS, u64 on Linux
        Ok(s) => u64::from(s.blocks_available()) * s.fragment_size(),
        Err(e) => {
            tracing::warn!("statvfs failed for {}: {e}", path.display());
            0
        }
    }
}

struct CompactingGuard<'a>(&'a Storage);

impl Drop for CompactingGuard<'_> {
    fn drop(&mut self) {
        self.0.end_compacting();
    }
}

use super::metrics::{ConnectionGuard, spawn_metrics_updater};

fn spawn_signal_handler(
    mut sigterm: tokio::signal::unix::Signal,
    mut sigint: tokio::signal::unix::Signal,
    shutdown_tx: watch::Sender<bool>,
) {
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("received SIGTERM, shutting down"),
            _ = sigint.recv() => tracing::info!("received SIGINT, shutting down"),
        }
        let _ = shutdown_tx.send(true);

        // Second signal: force exit
        tokio::select! {
            _ = sigterm.recv() => tracing::warn!("received second SIGTERM, forcing exit"),
            _ = sigint.recv() => tracing::warn!("received second SIGINT, forcing exit"),
        }
        std::process::exit(1);
    });
}

fn spawn_autovacuum(
    storage: &Arc<Storage>,
    interval_secs: u64,
    threshold: f64,
    shutdown_rx: &watch::Receiver<bool>,
) -> JoinHandle<()> {
    let av_storage = Arc::clone(storage);
    let interval = Duration::from_secs(interval_secs);
    let mut rx = shutdown_rx.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(interval) => {}
                _ = rx.changed() => break,
            }
            av_storage.begin_compacting();
            let guard = CompactingGuard(&av_storage);
            let storage = Arc::clone(&av_storage);
            if let Err(e) =
                tokio::task::spawn_blocking(move || run_autovacuum(&storage, threshold)).await
            {
                tracing::error!("autovacuum task panicked: {e}");
            }
            drop(guard);
        }
    });
    tracing::info!(
        "autovacuum enabled: interval={}s, threshold={:.0}%",
        interval_secs,
        threshold * 100.0
    );
    handle
}

fn spawn_scrub(
    storage: &Arc<Storage>,
    interval_secs: u64,
    shutdown_rx: &watch::Receiver<bool>,
) -> JoinHandle<()> {
    let scrub_storage = Arc::clone(storage);
    let interval = Duration::from_secs(interval_secs);
    let mut rx = shutdown_rx.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(interval) => {}
                _ = rx.changed() => break,
            }
            let storage = Arc::clone(&scrub_storage);
            if let Err(e) = tokio::task::spawn_blocking(move || run_scrub(&storage)).await {
                tracing::error!("scrub task panicked: {e}");
            }
        }
    });
    tracing::info!("background scrub enabled: interval={}s", interval_secs);
    handle
}

async fn spawn_grpc(
    storage: &Arc<Storage>,
    auth_store: &Arc<AuthStore>,
    host: &str,
    port: u16,
    shutdown_rx: &watch::Receiver<bool>,
) -> anyhow::Result<JoinHandle<()>> {
    let grpc_svc =
        simple3::grpc::GrpcService::new(Arc::clone(storage), Some(Arc::clone(auth_store)));
    let listener = TcpListener::bind(format!("{host}:{port}")).await?;
    tracing::info!("gRPC listening on {}:{}", host, port);
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let mut rx = shutdown_rx.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .layer(super::request_id::RequestIdLayer)
            .add_service(
                simple3::grpc::proto::simple3_server::Simple3Server::new(grpc_svc)
                    .max_decoding_message_size(64 * 1024 * 1024)
                    .max_encoding_message_size(64 * 1024 * 1024),
            )
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = rx.changed().await;
            })
            .await
        {
            tracing::error!("gRPC server error: {e}");
        }
    });
    Ok(handle)
}

async fn await_bg_tasks(tasks: &mut [JoinHandle<()>], timeout_secs: u64) {
    let timeout = Duration::from_secs(timeout_secs);
    if tokio::time::timeout(timeout, async {
        for handle in tasks.iter_mut() {
            let _ = handle.await;
        }
    })
    .await
    .is_err()
    {
        tracing::warn!("background tasks did not finish within timeout, aborting");
        for handle in tasks.iter() {
            handle.abort();
        }
    }
}

async fn drain_connections(connections: &mut JoinSet<()>, timeout_secs: u64) {
    tracing::info!(
        "draining {} in-flight connection(s) (timeout {}s)",
        connections.len(),
        timeout_secs
    );
    let timeout = Duration::from_secs(timeout_secs);
    if tokio::time::timeout(timeout, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tracing::warn!(
            "shutdown timeout reached, dropping {} connection(s)",
            connections.len()
        );
        connections.shutdown().await;
    }
}

/// Accept HTTP connections until a shutdown signal is received.
/// Returns the in-flight connection set for draining by the caller.
async fn accept_loop(
    listener: TcpListener,
    service: AdminService,
    shutdown_rx: &watch::Receiver<bool>,
) -> anyhow::Result<JoinSet<()>> {
    let mut connections: JoinSet<()> = JoinSet::new();
    let mut shutdown_rx_accept = shutdown_rx.clone();

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _addr) = result?;
                let conn_guard = ConnectionGuard::new();
                let svc = service.clone();
                let mut rx = shutdown_rx.clone();
                connections.spawn(async move {
                    let _conn_guard = conn_guard;
                    let io = TokioIo::new(stream);
                    let conn = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc);
                    tokio::pin!(conn);
                    tokio::select! {
                        result = conn.as_mut() => {
                            if let Err(e) = result {
                                tracing::error!("connection error: {e}");
                            }
                        }
                        _ = rx.changed() => {
                            conn.as_mut().graceful_shutdown();
                            if let Err(e) = conn.await {
                                tracing::error!("connection error during drain: {e}");
                            }
                        }
                    }
                });
            }
            _ = shutdown_rx_accept.changed() => break,
        }
    }

    drop(listener);
    Ok(connections)
}

pub struct ServeConfig {
    pub host: String,
    pub port: u16,
    pub grpc_port: u16,
    pub autovacuum_interval: u64,
    pub autovacuum_threshold: f64,
    pub max_segment_size_mb: u64,
    pub scrub_interval: u64,
    pub shutdown_timeout: u64,
    pub min_disk_free_mb: u64,
    pub metrics_user: Option<String>,
    pub metrics_password: Option<String>,
}

pub async fn run(data_dir: &Path, cfg: ServeConfig) -> anyhow::Result<()> {
    let max_seg_bytes = cfg.max_segment_size_mb * 1024 * 1024;
    let storage = Arc::new(Storage::open_with_segment_size(data_dir, max_seg_bytes)?);
    let s3 = SimpleStorage::new(Arc::clone(&storage));

    // Open auth database and bootstrap root key if needed
    let (auth_store, bootstrap) = AuthStore::open(data_dir)?;
    let auth_store = Arc::new(auth_store);
    if let BootstrapResult::NewRootKey {
        access_key_id,
        secret_key,
    } = bootstrap
    {
        eprintln!("================================================================");
        eprintln!("  root admin key created (save this, it won't be shown again):");
        eprintln!();
        eprintln!("  Access Key ID: {access_key_id}");
        eprintln!("  Secret Key:    {secret_key}");
        eprintln!("================================================================");
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
    let sigint = tokio::signal::unix::signal(SignalKind::interrupt())?;
    spawn_signal_handler(sigterm, sigint, shutdown_tx);

    let mut bg_tasks: Vec<JoinHandle<()>> = Vec::new();

    if cfg.autovacuum_interval > 0 {
        bg_tasks.push(spawn_autovacuum(
            &storage, cfg.autovacuum_interval, cfg.autovacuum_threshold, &shutdown_rx,
        ));
    }

    if cfg.scrub_interval > 0 {
        bg_tasks.push(spawn_scrub(&storage, cfg.scrub_interval, &shutdown_rx));
    }

    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| anyhow::anyhow!("prometheus recorder: {e}"))?;

    bg_tasks.push(spawn_metrics_updater(&storage, &shutdown_rx));

    let auth_provider = AuthProvider::new(Arc::clone(&auth_store));
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(auth_provider.clone());
    builder.set_access(auth_provider);
    let s3_service = builder.build();

    let metrics_auth = match (cfg.metrics_user, cfg.metrics_password) {
        (Some(u), Some(p)) => {
            tracing::info!("metrics endpoint auth enabled");
            Some((u, p))
        }
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!(
                "metrics auth partially configured: both --metrics-user and --metrics-password must be set"
            );
        }
        _ => None,
    };

    let service = AdminService {
        s3: s3_service,
        storage: Arc::clone(&storage),
        auth_store: Arc::clone(&auth_store),
        min_disk_free_mb: cfg.min_disk_free_mb,
        prometheus_handle,
        metrics_auth,
    };

    if cfg.grpc_port > 0 {
        bg_tasks.push(spawn_grpc(&storage, &auth_store, &cfg.host, cfg.grpc_port, &shutdown_rx).await?);
    }

    let listener = TcpListener::bind((&*cfg.host, cfg.port)).await?;
    tracing::info!("S3 HTTP listening on {}:{}", cfg.host, cfg.port);

    let mut connections = accept_loop(listener, service, &shutdown_rx).await?;

    tokio::join!(
        drain_connections(&mut connections, cfg.shutdown_timeout),
        await_bg_tasks(&mut bg_tasks, cfg.shutdown_timeout),
    );

    match tokio::task::spawn_blocking(move || storage.sync_all()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::error!("failed to sync storage on shutdown: {e}");
            return Err(e.into());
        }
        Err(e) => {
            tracing::error!("sync_all task panicked: {e}");
            return Err(e.into());
        }
    }

    tracing::info!("shutdown complete");
    Ok(())
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;
