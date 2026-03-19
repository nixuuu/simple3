use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

mod admin_auth;
mod compact;
mod health;
mod metrics;
mod metrics_auth;
mod request_id;
pub mod client;
pub mod config;
pub mod keys;
pub mod policy_cmd;
pub mod serve;
pub mod util;
mod verify;

#[derive(Parser)]
#[command(name = "simple3", about = "Simple S3-compatible storage service")]
pub struct Cli {
    #[arg(long, default_value = "./data", global = true)]
    data_dir: PathBuf,

    /// Path to TOML config file (default: `<data_dir>/simple3.toml`)
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Log output format
    #[arg(long, global = true, value_enum)]
    log_format: Option<LogFormat>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

fn init_logging(format: LogFormat) {
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    match format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        LogFormat::Text => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .init();
        }
    }
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
        /// Background scrub interval in seconds (0 = disabled)
        #[arg(long, default_value_t = 3600)]
        scrub_interval: u64,
        /// Graceful shutdown timeout in seconds
        #[arg(long)]
        shutdown_timeout: Option<u64>,
        /// Minimum free disk space in MB; /ready returns 503 below this (0 = disabled)
        #[arg(long)]
        min_disk_free_mb: Option<u64>,
        /// Metrics endpoint HTTP Basic Auth username
        #[arg(long, env = "SIMPLE3_METRICS_USER")]
        metrics_user: Option<String>,
        /// Metrics endpoint HTTP Basic Auth password
        #[arg(long, env = "SIMPLE3_METRICS_PASSWORD")]
        metrics_password: Option<String>,
    },
    /// Check if the running server is healthy
    Health,
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
        /// List all object versions
        #[arg(long)]
        versions: bool,
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
        /// Download a specific object version
        #[arg(long)]
        version_id: Option<String>,
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
        /// Delete a specific object version
        #[arg(long)]
        version_id: Option<String>,
        #[command(flatten)]
        client: client::ClientArgs,
    },
    /// Manage access keys
    Keys {
        #[command(subcommand)]
        cmd: keys::KeysCommand,
        #[command(flatten)]
        client: client::ClientArgs,
    },
    /// Manage IAM policies
    Policy {
        #[command(subcommand)]
        cmd: policy_cmd::PolicyCommand,
        #[command(flatten)]
        client: client::ClientArgs,
    },
    /// Generate a presigned URL for GET or PUT
    Presign {
        /// Object URI (<s3://bucket/key>)
        uri: String,
        /// HTTP method
        #[arg(long, value_enum, default_value_t = client::presign::PresignMethod::Get)]
        method: client::presign::PresignMethod,
        /// Time-to-live for the URL (e.g. 3600, 1h, 30m, 7d)
        #[arg(long, default_value = "3600")]
        ttl: String,
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

#[allow(clippy::too_many_lines)] // single dispatch for all subcommands, splitting would hurt readability
pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // For non-serve commands, init text logging immediately.
    // Serve path defers init until after config load to support JSON format from TOML.
    if !matches!(&cli.command, None | Some(Command::Serve { .. })) {
        init_logging(cli.log_format.unwrap_or_default());
    }

    match cli.command {
        Some(Command::Health) => health::run(&cli.data_dir, cli.config.as_deref()).await,
        Some(Command::Compact { bucket }) => compact::run(&cli.data_dir, bucket),
        Some(Command::Verify { bucket }) => verify::run(&cli.data_dir, bucket),
        Some(Command::Keys { cmd, client: args }) => keys::run(args, cmd).await,
        Some(Command::Policy { cmd, client: args }) => policy_cmd::run(args, cmd).await,
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
            versions,
            client: args,
        }) => {
            let transport = args.build_transport().await?;
            client::ls::run(&*transport, uri.as_deref(), recursive, versions).await
        }
        Some(Command::Rm {
            uri,
            recursive,
            version_id,
            client: args,
        }) => {
            let transport = args.build_transport().await?;
            client::rm::run(&*transport, &uri, recursive, version_id.as_deref()).await
        }
        Some(Command::Cp {
            src,
            dest,
            recursive,
            concurrency,
            version_id,
            client: args,
        }) => {
            let transport = args.build_transport().await?;
            client::cp::run(
                transport,
                &src,
                &dest,
                recursive,
                concurrency,
                version_id.as_deref(),
            )
            .await
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
            client::sync_cmd::run(transport, &src, &dest, delete, dryrun, size_only, concurrency)
                .await
        }
        Some(Command::Presign {
            uri,
            method,
            ttl,
            client: args,
        }) => {
            if args.grpc {
                anyhow::bail!("`simple3 presign` only supports S3 HTTP endpoints; remove --grpc");
            }
            let resolved = args.resolve(8080);
            client::presign::run(&uri, method, &ttl, resolved).await
        }
        cmd => {
            let cfg = config::load_config(cli.config.as_deref(), &cli.data_dir)?;
            let log_format = cli
                .log_format
                .or(cfg.server.log_format)
                .unwrap_or_default();
            init_logging(log_format);
            let (
                host,
                port,
                av_interval,
                av_threshold,
                max_seg_mb,
                grpc_port,
                scrub_interval,
                shutdown_timeout,
                min_disk_free_mb,
                metrics_user,
                metrics_password,
            ) = match cmd {
                Some(Command::Serve {
                    host,
                    port,
                    autovacuum_interval,
                    autovacuum_threshold,
                    max_segment_size_mb,
                    grpc_port,
                    scrub_interval,
                    shutdown_timeout,
                    min_disk_free_mb,
                    metrics_user,
                    metrics_password,
                }) => (
                    host,
                    port,
                    autovacuum_interval,
                    autovacuum_threshold,
                    max_segment_size_mb,
                    grpc_port,
                    scrub_interval,
                    shutdown_timeout
                        .or(cfg.server.shutdown_timeout)
                        .unwrap_or(30),
                    min_disk_free_mb
                        .or(cfg.storage.min_disk_free_mb)
                        .unwrap_or(0),
                    metrics_user.or(cfg.metrics.username),
                    metrics_password.or(cfg.metrics.password),
                ),
                _ => (
                    cfg.server.host.unwrap_or_else(|| "0.0.0.0".into()),
                    cfg.server.port.unwrap_or(8080),
                    cfg.storage.autovacuum_interval.unwrap_or(300),
                    cfg.storage.autovacuum_threshold.unwrap_or(0.5),
                    cfg.storage.max_segment_size_mb.unwrap_or(4096),
                    cfg.server.grpc_port.unwrap_or(50051),
                    cfg.storage.scrub_interval.unwrap_or(3600),
                    cfg.server.shutdown_timeout.unwrap_or(30),
                    cfg.storage.min_disk_free_mb.unwrap_or(0),
                    cfg.metrics.username,
                    cfg.metrics.password,
                ),
            };
            serve::run(
                &cli.data_dir,
                &host,
                port,
                grpc_port,
                av_interval,
                av_threshold,
                max_seg_mb,
                scrub_interval,
                shutdown_timeout,
                min_disk_free_mb,
                metrics_user,
                metrics_password,
            )
            .await
        }
    }
}
