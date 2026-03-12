use std::path::PathBuf;

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
    },
    /// Compact buckets to reclaim dead space
    Compact {
        /// Compact only this bucket (default: all buckets)
        bucket: Option<String>,
    },
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
            let buckets = match bucket {
                Some(name) => vec![name],
                None => storage.list_buckets(),
            };
            for name in &buckets {
                let store = storage
                    .get_bucket(name)
                    .ok_or_else(|| anyhow::anyhow!("bucket '{}' not found", name))?;
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
            let (host, port) = match cmd {
                Some(Command::Serve { host, port }) => (host, port),
                _ => ("0.0.0.0".into(), 8080),
            };

            let storage = Storage::open(&cli.data_dir)?;
            let s3 = SimpleStorage::new(storage);

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
