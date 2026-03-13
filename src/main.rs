#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use hyper_util::rt::TokioIo;
use s3s::auth::SimpleAuth;
use s3s::service::{S3Service, S3ServiceBuilder};
use serde::Serialize;
use tokio::net::TcpListener;

use simple3::s3impl::SimpleStorage;
use simple3::storage::Storage;

#[derive(Parser)]
#[command(name = "simple3", about = "Simple S3-compatible storage service")]
struct Cli {
    #[arg(long, default_value = "./data", global = true)]
    data_dir: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the S3 server (default)
    Serve {
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
        #[arg(long, default_value_t = 8080)]
        port: u16,
        /// Autovacuum interval in seconds (0 = disabled)
        #[arg(long, default_value_t = 300)]
        autovacuum_interval: u64,
        /// Autovacuum threshold: compact when `dead_bytes` > threshold fraction of file size (0.0-1.0)
        #[arg(long, default_value_t = 0.5)]
        autovacuum_threshold: f64,
        /// Maximum segment file size in megabytes
        #[arg(long, default_value_t = 4096)]
        max_segment_size_mb: u64,
    },
    /// Compact buckets to reclaim dead space
    Compact {
        /// Compact only this bucket (default: all buckets)
        bucket: Option<String>,
    },
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
}

type ServiceFuture =
    Pin<Box<dyn Future<Output = Result<s3s::HttpResponse, s3s::HttpError>> + Send>>;

impl hyper::service::Service<hyper::Request<hyper::body::Incoming>> for AdminService {
    type Response = s3s::HttpResponse;
    type Error = s3s::HttpError;
    type Future = ServiceFuture;

    fn call(&self, req: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        if req.uri().path().starts_with("/_/") {
            let storage = Arc::clone(&self.storage);
            Box::pin(async move { Ok(handle_admin(req, storage).await) })
        } else {
            hyper::service::Service::call(&self.s3, req)
        }
    }
}

fn json_response(status: u16, body: &impl Serialize) -> s3s::HttpResponse {
    let json = serde_json::to_string(body).unwrap_or_else(|e| format!(r#"{{"error":"{e}"}}"#));
    hyper::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(s3s::Body::from(json))
        .unwrap()
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Command::Compact { bucket }) => {
            let storage = Storage::open(&cli.data_dir)?;
            let buckets = bucket.map_or_else(|| storage.list_buckets(), |name| Ok(vec![name]))?;
            for name in &buckets {
                let store = storage
                    .get_bucket(name)?
                    .ok_or_else(|| anyhow::anyhow!("bucket '{name}' not found"))?;
                let dead = store.dead_bytes();
                if dead == 0 {
                    println!("{name}: no dead bytes, skipping");
                    continue;
                }
                println!("{name}: compacting ({dead} dead bytes)...");
                store.compact()?;
                println!("{name}: done");
            }
            Ok(())
        }
        cmd => {
            let (host, port, autovacuum_interval, autovacuum_threshold, max_segment_size_mb) =
                match cmd {
                    Some(Command::Serve {
                        host,
                        port,
                        autovacuum_interval,
                        autovacuum_threshold,
                        max_segment_size_mb,
                    }) => (
                        host,
                        port,
                        autovacuum_interval,
                        autovacuum_threshold,
                        max_segment_size_mb,
                    ),
                    _ => ("0.0.0.0".into(), 8080, 300, 0.5, 4096),
                };

            let max_seg_bytes = max_segment_size_mb * 1024 * 1024;
            let storage = Arc::new(Storage::open_with_segment_size(
                &cli.data_dir,
                max_seg_bytes,
            )?);
            let s3 = SimpleStorage::new(Arc::clone(&storage));

            // Start autovacuum background task
            if autovacuum_interval > 0 {
                let av_storage = Arc::clone(&storage);
                let interval = Duration::from_secs(autovacuum_interval);
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(interval).await;
                        let st = Arc::clone(&av_storage);
                        let threshold = autovacuum_threshold;
                        // Run compaction on blocking thread (it does file I/O)
                        if let Err(e) =
                            tokio::task::spawn_blocking(move || run_autovacuum(&st, threshold)).await
                        {
                            tracing::error!("autovacuum task panicked: {e}");
                        }
                    }
                });
                tracing::info!(
                    "autovacuum enabled: interval={}s, threshold={:.0}%",
                    autovacuum_interval,
                    autovacuum_threshold * 100.0
                );
            }

            let mut builder = S3ServiceBuilder::new(s3);
            builder.set_auth(SimpleAuth::from_single("test", "test"));
            let s3_service = builder.build();

            let service = AdminService {
                s3: s3_service,
                storage: Arc::clone(&storage),
            };

            let listener = TcpListener::bind((&*host, port)).await?;
            tracing::info!("listening on {}:{}", host, port);

            loop {
                let (stream, _addr) = listener.accept().await?;
                let svc = service.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    if let Err(e) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await
                    {
                        tracing::error!("connection error: {e}");
                    }
                });
            }
        }
    }
}
