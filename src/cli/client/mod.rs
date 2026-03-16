pub mod cp;
pub mod grpc;
pub mod http;
pub mod ls;
pub mod mb;
pub mod presign;
pub mod progress;
pub mod rb;
pub mod rm;
pub mod sync_cmd;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use clap::Args;

/// Shared CLI arguments for all client commands.
#[derive(Args, Clone)]
pub struct ClientArgs {
    /// Use gRPC transport instead of S3 HTTP
    #[arg(long)]
    pub grpc: bool,

    /// Override endpoint URL (env: `AWS_ENDPOINT_URL`)
    #[arg(long, env = "AWS_ENDPOINT_URL")]
    pub endpoint_url: Option<String>,

    /// Access key (env: `AWS_ACCESS_KEY_ID`)
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    pub access_key: Option<String>,

    /// Secret key (env: `AWS_SECRET_ACCESS_KEY`)
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub secret_key: Option<String>,

    /// Region (env: `AWS_DEFAULT_REGION` or `AWS_REGION`)
    #[arg(long, env = "AWS_DEFAULT_REGION")]
    pub region: Option<String>,
}

/// Resolved connection parameters (no more `Option`s).
pub struct ResolvedArgs {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

impl ClientArgs {
    /// Resolve optional CLI/env values into concrete defaults.
    pub fn resolve(self, default_port: u16) -> ResolvedArgs {
        let region = self
            .region
            .or_else(|| std::env::var("AWS_REGION").ok())
            .unwrap_or_else(|| "us-east-1".into());
        let endpoint = self
            .endpoint_url
            .unwrap_or_else(|| format!("http://localhost:{default_port}"));
        let access_key = self.access_key.unwrap_or_else(|| "test".into());
        let secret_key = self.secret_key.unwrap_or_else(|| "test".into());
        ResolvedArgs {
            endpoint,
            access_key,
            secret_key,
            region,
        }
    }

    pub async fn build_transport(self) -> anyhow::Result<Arc<dyn Transport>> {
        if self.grpc {
            let r = self.resolve(50051);
            let transport = grpc::GrpcTransport::connect(
                &r.endpoint,
                Some(r.access_key.as_str()),
                Some(r.secret_key.as_str()),
            )
            .await?;
            Ok(Arc::new(transport))
        } else {
            let r = self.resolve(8080);
            let transport =
                http::HttpTransport::new(&r.endpoint, &r.access_key, &r.secret_key, &r.region);
            Ok(Arc::new(transport))
        }
    }
}

/// Parsed S3 URI.
pub enum S3Uri {
    Bucket(String),
    Object { bucket: String, key: String },
}

impl S3Uri {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let stripped = s
            .strip_prefix("s3://")
            .ok_or_else(|| anyhow::anyhow!("expected s3:// URI, got: {s}"))?;
        if stripped.is_empty() {
            anyhow::bail!("empty S3 URI");
        }
        stripped.find('/').map_or_else(
            || Ok(Self::Bucket(stripped.to_owned())),
            |idx| {
                let bucket = &stripped[..idx];
                let key = &stripped[idx + 1..];
                if key.is_empty() {
                    Ok(Self::Bucket(bucket.to_owned()))
                } else {
                    Ok(Self::Object {
                        bucket: bucket.to_owned(),
                        key: key.to_owned(),
                    })
                }
            },
        )
    }

    pub fn bucket(&self) -> &str {
        match self {
            Self::Bucket(b) | Self::Object { bucket: b, .. } => b,
        }
    }

    pub fn key(&self) -> Option<&str> {
        match self {
            Self::Bucket(_) => None,
            Self::Object { key, .. } => Some(key),
        }
    }
}

/// Build an `aws_sdk_s3::Client` with path-style addressing and static credentials.
pub fn build_s3_client(endpoint: &str, access_key: &str, secret_key: &str, region: &str) -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation};

    let creds = Credentials::new(access_key, secret_key, None, None, "simple3-cli");
    let config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .region(Region::new(region.to_owned()))
        .force_path_style(true)
        .behavior_version_latest()
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

/// Returns true if the string looks like an S3 URI.
pub fn is_s3_uri(s: &str) -> bool {
    s.starts_with("s3://")
}

// === Transport trait ===

pub struct BucketEntry {
    pub name: String,
}

#[allow(dead_code)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub last_modified: u64,
    pub etag: String,
}

#[allow(dead_code)]
pub struct ObjectHead {
    pub size: u64,
    pub last_modified: u64,
    pub etag: String,
    pub content_type: Option<String>,
}

pub struct ListResult {
    pub objects: Vec<ObjectEntry>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

pub struct VersionEntryInfo {
    pub key: String,
    pub version_id: String,
    pub size: u64,
    pub last_modified: u64,
    pub is_latest: bool,
    pub is_delete_marker: bool,
}

pub struct ListVersionsResult {
    pub entries: Vec<VersionEntryInfo>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn create_bucket(&self, bucket: &str) -> anyhow::Result<()>;
    async fn delete_bucket(&self, bucket: &str) -> anyhow::Result<()>;
    async fn list_buckets(&self) -> anyhow::Result<Vec<BucketEntry>>;
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
    ) -> anyhow::Result<ListResult>;
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: &Path,
        content_type: Option<&str>,
    ) -> anyhow::Result<()>;
    async fn get_object(&self, bucket: &str, key: &str, dest: &Path) -> anyhow::Result<u64>;
    #[allow(dead_code)]
    async fn head_object(&self, bucket: &str, key: &str) -> anyhow::Result<Option<ObjectHead>>;
    #[allow(dead_code)]
    async fn delete_object(&self, bucket: &str, key: &str) -> anyhow::Result<()>;
    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> anyhow::Result<()>;
    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        dest: &Path,
    ) -> anyhow::Result<u64>;
    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> anyhow::Result<()>;
    #[allow(dead_code)]
    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
    ) -> anyhow::Result<ListVersionsResult>;
}

/// List ALL objects under a prefix by paginating through continuation tokens.
pub async fn list_all_objects(
    transport: &dyn Transport,
    bucket: &str,
    prefix: Option<&str>,
) -> anyhow::Result<Vec<ObjectEntry>> {
    let mut all = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let result =
            transport
                .list_objects(bucket, prefix, None, token.as_deref())
                .await?;
        all.extend(result.objects);
        if result.is_truncated {
            token = result.next_continuation_token;
        } else {
            break;
        }
    }
    Ok(all)
}
