use std::path::PathBuf;

use clap::Parser;
use hyper_util::rt::TokioIo;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;

use simple3::s3impl::SimpleStorage;
use simple3::storage::Storage;

#[derive(Parser)]
#[command(name = "simple3", about = "Simple S3-compatible storage service")]
struct Cli {
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
    #[arg(long, default_value_t = 8080)]
    port: u16,
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let storage = Storage::open(&cli.data_dir)?;
    let s3 = SimpleStorage::new(storage);

    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(SimpleAuth::from_single("test", "test"));
    let service = builder.build();

    let listener = TcpListener::bind((&*cli.host, cli.port)).await?;
    tracing::info!("listening on {}:{}", cli.host, cli.port);

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
