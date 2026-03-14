use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod compact;
pub mod client;
mod serve;
mod verify;

#[derive(Parser)]
#[command(name = "simple3", about = "Simple S3-compatible storage service")]
pub struct Cli {
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
        /// gRPC server port (0 = disabled)
        #[arg(long, default_value_t = 50051)]
        grpc_port: u16,
    },
    /// Compact buckets to reclaim dead space
    Compact {
        /// Compact only this bucket (default: all buckets)
        bucket: Option<String>,
    },
    /// Verify data integrity of stored objects
    Verify {
        /// Verify only this bucket (default: all buckets)
        bucket: Option<String>,
    },
    /// Create a bucket
    #[command(name = "mb")]
    Mb {
        /// Target bucket (<s3://bucket>)
        uri: String,
        #[command(flatten)]
        client: client::ClientArgs,
    },
    /// Remove a bucket
    #[command(name = "rb")]
    Rb {
        /// Target bucket (<s3://bucket>)
        uri: String,
        /// Delete all objects before removing the bucket
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        client: client::ClientArgs,
    },
    /// List buckets or objects
    Ls {
        /// Optional target (<s3://bucket>[/prefix])
        uri: Option<String>,
        /// List all objects recursively
        #[arg(long)]
        recursive: bool,
        #[command(flatten)]
        client: client::ClientArgs,
    },
    /// Copy files to/from S3
    Cp {
        /// Source path (local path or <s3://bucket/key>)
        src: String,
        /// Destination path (local path or <s3://bucket/key>)
        dest: String,
        /// Copy directories recursively
        #[arg(long)]
        recursive: bool,
        /// Number of concurrent transfers
        #[arg(long, short = 'j', default_value_t = 5)]
        concurrency: usize,
        #[command(flatten)]
        client: client::ClientArgs,
    },
    /// Remove objects from S3
    #[command(name = "rm")]
    Rm {
        /// Object to delete (<s3://bucket/key> or <s3://bucket/prefix/> with --recursive)
        uri: String,
        /// Delete all objects under prefix recursively
        #[arg(long)]
        recursive: bool,
        #[command(flatten)]
        client: client::ClientArgs,
    },
    /// Sync files between local filesystem and S3
    Sync {
        /// Source (local path or <s3://bucket/prefix>)
        src: String,
        /// Destination (local path or <s3://bucket/prefix>)
        dest: String,
        /// Delete files at destination that don't exist at source
        #[arg(long)]
        delete: bool,
        /// Show what would be done without making changes
        #[arg(long)]
        dryrun: bool,
        /// Compare by size only, ignore timestamps
        #[arg(long)]
        size_only: bool,
        /// Number of concurrent transfers
        #[arg(long, short = 'j', default_value_t = 5)]
        concurrency: usize,
        #[command(flatten)]
        client: client::ClientArgs,
    },
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Compact { bucket }) => compact::run(&cli.data_dir, bucket),
        Some(Command::Verify { bucket }) => verify::run(&cli.data_dir, bucket),
        Some(Command::Mb { uri, client: args }) => {
            let transport = args.build_transport().await?;
            client::mb::run(&*transport, &uri).await
        }
        Some(Command::Rb {
            uri,
            force,
            client: args,
        }) => {
            let transport = args.build_transport().await?;
            client::rb::run(&*transport, &uri, force).await
        }
        Some(Command::Ls {
            uri,
            recursive,
            client: args,
        }) => {
            let transport = args.build_transport().await?;
            client::ls::run(&*transport, uri.as_deref(), recursive).await
        }
        Some(Command::Rm {
            uri,
            recursive,
            client: args,
        }) => {
            let transport = args.build_transport().await?;
            client::rm::run(&*transport, &uri, recursive).await
        }
        Some(Command::Cp {
            src,
            dest,
            recursive,
            concurrency,
            client: args,
        }) => {
            let transport = args.build_transport().await?;
            client::cp::run(&*transport, &src, &dest, recursive, concurrency).await
        }
        Some(Command::Sync {
            src,
            dest,
            delete,
            dryrun,
            size_only,
            concurrency,
            client: args,
        }) => {
            let transport = args.build_transport().await?;
            client::sync_cmd::run(&*transport, &src, &dest, delete, dryrun, size_only, concurrency)
                .await
        }
        cmd => {
            let (host, port, av_interval, av_threshold, max_seg_mb, grpc_port) = match cmd {
                Some(Command::Serve {
                    host,
                    port,
                    autovacuum_interval,
                    autovacuum_threshold,
                    max_segment_size_mb,
                    grpc_port,
                }) => (
                    host,
                    port,
                    autovacuum_interval,
                    autovacuum_threshold,
                    max_segment_size_mb,
                    grpc_port,
                ),
                _ => ("0.0.0.0".into(), 8080, 300, 0.5, 4096, 50051),
            };
            serve::run(
                &cli.data_dir,
                &host,
                port,
                grpc_port,
                av_interval,
                av_threshold,
                max_seg_mb,
            )
            .await
        }
    }
}
