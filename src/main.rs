use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use hyper_util::rt::TokioIo;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
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
        let dead = store.dead_bytes();
        if dead == 0 {
            continue;
        }
        let file_size = store
            .data_file_size()
            .unwrap_or(0);
        if file_size == 0 {
            continue;
        }
        #[allow(clippy::cast_precision_loss)]
        let ratio = dead as f64 / file_size as f64;
        if ratio >= threshold {
            tracing::info!(
                "autovacuum: {name} — {dead} dead bytes ({:.0}%), compacting...",
                ratio * 100.0
            );
            match store.compact() {
                Ok(()) => tracing::info!("autovacuum: {name} — done"),
                Err(e) => tracing::error!("autovacuum: {name} — compact failed: {e}"),
            }
        }
    }
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
            let (host, port, autovacuum_interval, autovacuum_threshold) = match cmd {
                Some(Command::Serve {
                    host,
                    port,
                    autovacuum_interval,
                    autovacuum_threshold,
                }) => (host, port, autovacuum_interval, autovacuum_threshold),
                _ => ("0.0.0.0".into(), 8080, 300, 0.5),
            };

            let storage = Arc::new(Storage::open(&cli.data_dir)?);
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
            let service = builder.build();

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
