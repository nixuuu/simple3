use std::collections::HashMap;
use std::path::Path;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReceiverStream;

use simple3::grpc::proto::simple3_client::Simple3Client;
#[allow(clippy::wildcard_imports)]
use simple3::grpc::proto::*;

use super::{
    BucketEntry, ListResult, ListVersionsResult, ObjectEntry, ObjectHead, Transport,
    VersionEntryInfo,
};

const CHUNK_SIZE: usize = 256 * 1024;

pub struct GrpcTransport {
    client: Simple3Client<tonic::service::interceptor::InterceptedService<tonic::transport::Channel, AuthInterceptor>>,
}

const MAX_MSG_SIZE: usize = 64 * 1024 * 1024;

#[derive(Clone)]
struct AuthInterceptor {
    access_key: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    secret_key: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(ak) = &self.access_key {
            request.metadata_mut().insert("x-access-key", ak.clone());
        }
        if let Some(sk) = &self.secret_key {
            request.metadata_mut().insert("x-secret-key", sk.clone());
        }
        Ok(request)
    }
}

impl GrpcTransport {
    pub async fn connect(
        endpoint: &str,
        access_key: Option<&str>,
        secret_key: Option<&str>,
    ) -> anyhow::Result<Self> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_owned())?
            .connect()
            .await?;
        let interceptor = AuthInterceptor {
            access_key: access_key.and_then(|k| k.parse().ok()),
            secret_key: secret_key.and_then(|k| k.parse().ok()),
        };
        let client = Simple3Client::with_interceptor(channel, interceptor)
            .max_decoding_message_size(MAX_MSG_SIZE)
            .max_encoding_message_size(MAX_MSG_SIZE);
        Ok(Self { client })
    }

    async fn download_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        dest: &Path,
    ) -> anyhow::Result<u64> {
        let resp = self
            .client
            .clone()
            .get_object(GetObjectRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                range_start: None,
                range_end: None,
                version_id: version_id.map(str::to_owned),
            })
            .await?;
        let mut stream = resp.into_inner();
        let mut file = tokio::fs::File::create(dest).await?;
        let mut total: u64 = 0;

        while let Some(msg) = stream.message().await? {
            if let Some(get_object_response::Response::Data(chunk)) = msg.response {
                file.write_all(&chunk).await?;
                total += chunk.len() as u64;
            }
        }
        file.flush().await?;
        Ok(total)
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn create_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        self.client
            .clone()
            .create_bucket(CreateBucketRequest {
                name: bucket.to_owned(),
            })
            .await?;
        Ok(())
    }

    async fn delete_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        self.client
            .clone()
            .delete_bucket(DeleteBucketRequest {
                name: bucket.to_owned(),
            })
            .await?;
        Ok(())
    }

    async fn list_buckets(&self) -> anyhow::Result<Vec<BucketEntry>> {
        let resp = self
            .client
            .clone()
            .list_buckets(ListBucketsRequest {})
            .await?;
        Ok(resp
            .into_inner()
            .buckets
            .into_iter()
            .map(|name| BucketEntry { name })
            .collect())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
    ) -> anyhow::Result<ListResult> {
        let resp = self
            .client
            .clone()
            .list_objects(ListObjectsRequest {
                bucket: bucket.to_owned(),
                prefix: prefix.unwrap_or_default().to_owned(),
                delimiter: delimiter.unwrap_or_default().to_owned(),
                max_keys: 1000,
                continuation_token: continuation_token.unwrap_or_default().to_owned(),
            })
            .await?;
        let inner = resp.into_inner();
        Ok(ListResult {
            objects: inner
                .objects
                .into_iter()
                .map(|o| ObjectEntry {
                    key: o.key,
                    size: o.size,
                    last_modified: o.last_modified,
                    etag: o.etag,
                })
                .collect(),
            common_prefixes: inner.common_prefixes,
            is_truncated: inner.is_truncated,
            next_continuation_token: if inner.next_continuation_token.is_empty() {
                None
            } else {
                Some(inner.next_continuation_token)
            },
        })
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: &Path,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(8);

        // Spawn producer — reads file in chunks concurrently with the RPC consumer
        let body = body.to_owned();
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let ct = content_type.unwrap_or_default().to_owned();
        tokio::spawn(async move {
            let init = PutObjectRequest {
                request: Some(put_object_request::Request::Init(PutObjectInit {
                    bucket,
                    key,
                    content_type: ct,
                    user_metadata: HashMap::default(),
                })),
            };
            if tx.send(init).await.is_err() {
                return;
            }
            let Ok(mut file) = tokio::fs::File::open(&body).await else {
                return;
            };
            let mut buf = vec![0u8; CHUNK_SIZE];
            loop {
                let n = match tokio::io::AsyncReadExt::read(&mut file, &mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let msg = PutObjectRequest {
                    request: Some(put_object_request::Request::Data(buf[..n].to_vec())),
                };
                if tx.send(msg).await.is_err() {
                    return;
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        self.client.clone().put_object(stream).await?;
        Ok(())
    }

    async fn get_object(&self, bucket: &str, key: &str, dest: &Path) -> anyhow::Result<u64> {
        self.download_object(bucket, key, None, dest).await
    }

    async fn head_object(&self, bucket: &str, key: &str) -> anyhow::Result<Option<ObjectHead>> {
        match self
            .client
            .clone()
            .head_object(HeadObjectRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                version_id: None,
            })
            .await
        {
            Ok(resp) => {
                let meta = resp.into_inner().metadata;
                Ok(meta.map(|m| ObjectHead {
                    size: m.size,
                    last_modified: m.last_modified,
                    etag: m.etag,
                    content_type: if m.content_type.is_empty() {
                        None
                    } else {
                        Some(m.content_type)
                    },
                }))
            }
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> anyhow::Result<()> {
        self.client
            .clone()
            .delete_object(DeleteObjectRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                version_id: None,
            })
            .await?;
        Ok(())
    }

    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        dest: &Path,
    ) -> anyhow::Result<u64> {
        self.download_object(bucket, key, Some(version_id), dest)
            .await
    }

    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> anyhow::Result<()> {
        self.client
            .clone()
            .delete_object(DeleteObjectRequest {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                version_id: Some(version_id.to_owned()),
            })
            .await?;
        Ok(())
    }

    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> anyhow::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        for chunk in keys.chunks(1000) {
            self.client
                .clone()
                .delete_objects(DeleteObjectsRequest {
                    bucket: bucket.to_owned(),
                    items: chunk
                        .iter()
                        .map(|k| DeleteObjectIdentifier {
                            key: k.clone(),
                            version_id: None,
                        })
                        .collect(),
                })
                .await?;
        }
        Ok(())
    }

    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
    ) -> anyhow::Result<ListVersionsResult> {
        let resp = self
            .client
            .clone()
            .list_object_versions(ListObjectVersionsRequest {
                bucket: bucket.to_owned(),
                prefix: prefix.unwrap_or_default().to_owned(),
                key_marker: key_marker.unwrap_or_default().to_owned(),
                version_id_marker: version_id_marker.unwrap_or_default().to_owned(),
                max_keys: 1000,
                delimiter: String::new(),
            })
            .await?;
        let inner = resp.into_inner();

        let mut entries = Vec::new();

        for ver in inner.versions {
            entries.push(VersionEntryInfo {
                key: ver.key,
                version_id: ver.version_id,
                size: ver.size,
                last_modified: ver.last_modified,
                is_latest: ver.is_latest,
                is_delete_marker: false,
            });
        }

        for dm in inner.delete_markers {
            entries.push(VersionEntryInfo {
                key: dm.key,
                version_id: dm.version_id,
                size: 0,
                last_modified: dm.last_modified,
                is_latest: dm.is_latest,
                is_delete_marker: true,
            });
        }

        Ok(ListVersionsResult {
            entries,
            is_truncated: inner.is_truncated,
            next_key_marker: if inner.next_key_marker.is_empty() {
                None
            } else {
                Some(inner.next_key_marker)
            },
            next_version_id_marker: if inner.next_version_id_marker.is_empty() {
                None
            } else {
                Some(inner.next_version_id_marker)
            },
        })
    }
}
