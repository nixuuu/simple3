//! HTTP admin endpoints under `/_/`.
//!
//! Extracted from `serve.rs` so the main server module stays under the project's
//! 800-line file budget. Routing lives in `serve::AdminService::dispatch`; the
//! handlers below are pure functions over `Storage`.

use std::sync::Arc;

use http_body_util::BodyExt;
use serde::Serialize;

use simple3::auth::AuthStore;
use simple3::storage::{SegmentStat, Storage};

use super::admin_auth::check_admin_auth;
use super::serve::{json_response, CompactingGuard};

#[derive(Serialize)]
pub(super) struct StatsResponse {
    pub bucket: String,
    pub segments: Vec<SegmentStatJson>,
    pub total_size: u64,
    pub total_dead_bytes: u64,
}

#[derive(Serialize)]
pub(super) struct CompactResponse {
    pub bucket: String,
    pub compacted: bool,
    pub segments_before: Vec<SegmentStatJson>,
    pub segments_after: Vec<SegmentStatJson>,
}

#[derive(Serialize)]
pub(super) struct SegmentStatJson {
    pub id: u32,
    pub size: u64,
    pub dead_bytes: u64,
}

pub(super) fn stats_to_json(stats: &[SegmentStat]) -> Vec<SegmentStatJson> {
    stats
        .iter()
        .map(|s| SegmentStatJson {
            id: s.id,
            size: s.size,
            dead_bytes: s.dead_bytes,
        })
        .collect()
}

/// Dispatch `/_/{stats,compact,verify,lifecycle}/...` to the corresponding handler.
pub(super) async fn handle_admin(
    req: hyper::Request<hyper::body::Incoming>,
    storage: Arc<Storage>,
    auth_store: Arc<AuthStore>,
) -> s3s::HttpResponse {
    if let Err(resp) = check_admin_auth(&req, &auth_store) {
        return resp;
    }

    let path = req.uri().path().to_owned();
    let method = req.method().clone();

    if method == hyper::Method::GET && path.starts_with("/_/stats/") {
        let bucket = path["/_/stats/".len()..].trim_end_matches('/').to_owned();
        spawn_blocking_admin(move || admin_stats(&storage, &bucket)).await
    } else if method == hyper::Method::POST && path.starts_with("/_/compact/") {
        let bucket = path["/_/compact/".len()..].trim_end_matches('/').to_owned();
        spawn_blocking_admin(move || admin_compact(&storage, &bucket)).await
    } else if method == hyper::Method::GET && path.starts_with("/_/verify/") {
        let bucket = path["/_/verify/".len()..].trim_end_matches('/').to_owned();
        spawn_blocking_admin(move || admin_verify(&storage, &bucket)).await
    } else if let Some(rest) = path.strip_prefix("/_/lifecycle/") {
        let bucket = rest.trim_end_matches('/').to_owned();
        if bucket.is_empty() {
            return json_response(404, &serde_json::json!({"error": "bucket required"}));
        }
        handle_lifecycle(req, method, storage, bucket).await
    } else {
        json_response(404, &serde_json::json!({"error": "not found"}))
    }
}

async fn handle_lifecycle(
    req: hyper::Request<hyper::body::Incoming>,
    method: hyper::Method,
    storage: Arc<Storage>,
    bucket: String,
) -> s3s::HttpResponse {
    if method == hyper::Method::GET {
        spawn_blocking_admin(move || admin_lifecycle_get(&storage, &bucket)).await
    } else if method == hyper::Method::PUT {
        let body_bytes = match req.into_body().collect().await {
            Ok(c) => c.to_bytes(),
            Err(e) => {
                return json_response(
                    400,
                    &serde_json::json!({"error": format!("failed to read request body: {e}")}),
                );
            }
        };
        let body_str = String::from_utf8_lossy(&body_bytes).into_owned();
        spawn_blocking_admin(move || admin_lifecycle_put(&storage, &bucket, &body_str)).await
    } else if method == hyper::Method::DELETE {
        spawn_blocking_admin(move || admin_lifecycle_delete(&storage, &bucket)).await
    } else {
        json_response(405, &serde_json::json!({"error": "method not allowed"}))
    }
}

/// Resolve a bucket, mapping storage errors to 500 instead of folding them into 404.
#[allow(clippy::result_large_err)] // Err variant carries the JSON response we return directly to the caller
fn resolve_bucket(
    storage: &Storage,
    bucket: &str,
) -> Result<Arc<simple3::storage::BucketStore>, s3s::HttpResponse> {
    match storage.get_bucket(bucket) {
        Ok(Some(store)) => Ok(store),
        Ok(None) => Err(json_response(
            404,
            &serde_json::json!({"error": "bucket not found"}),
        )),
        Err(e) => Err(json_response(
            500,
            &serde_json::json!({"error": e.to_string()}),
        )),
    }
}

async fn spawn_blocking_admin(
    f: impl FnOnce() -> s3s::HttpResponse + Send + 'static,
) -> s3s::HttpResponse {
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| json_response(500, &serde_json::json!({"error": e.to_string()})))
}

fn admin_lifecycle_get(storage: &Storage, bucket: &str) -> s3s::HttpResponse {
    let store = match resolve_bucket(storage, bucket) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.get_lifecycle() {
        Ok(Some(cfg)) => json_response(200, &cfg),
        Ok(None) => json_response(404, &serde_json::json!({"error": "no lifecycle configured"})),
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}

fn admin_lifecycle_put(storage: &Storage, bucket: &str, body: &str) -> s3s::HttpResponse {
    let store = match resolve_bucket(storage, bucket) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let cfg: simple3::storage::LifecycleConfig = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return json_response(400, &serde_json::json!({"error": e.to_string()})),
    };
    match store.set_lifecycle(&cfg) {
        Ok(()) => json_response(200, &cfg),
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}

fn admin_lifecycle_delete(storage: &Storage, bucket: &str) -> s3s::HttpResponse {
    let store = match resolve_bucket(storage, bucket) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.delete_lifecycle() {
        Ok(removed) => json_response(200, &serde_json::json!({"removed": removed})),
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}

fn admin_stats(storage: &Storage, bucket: &str) -> s3s::HttpResponse {
    let store = match resolve_bucket(storage, bucket) {
        Ok(s) => s,
        Err(resp) => return resp,
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
    let store = match resolve_bucket(storage, bucket) {
        Ok(s) => s,
        Err(resp) => return resp,
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
    let store = match resolve_bucket(storage, bucket) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.verify_integrity() {
        Ok(result) => json_response(200, &result),
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}
