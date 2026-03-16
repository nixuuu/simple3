use std::path::Path;

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};

use super::{
    BucketEntry, ListResult, ListVersionsResult, ObjectEntry, ObjectHead, Transport,
    VersionEntryInfo, build_s3_client,
};

pub struct HttpTransport {
    client: aws_sdk_s3::Client,
}

impl HttpTransport {
    pub fn new(endpoint: &str, access_key: &str, secret_key: &str, region: &str) -> Self {
        Self {
            client: build_s3_client(endpoint, access_key, secret_key, region),
        }
    }
}

#[allow(clippy::cast_sign_loss)]
fn millis_to_epoch_secs(t: &aws_sdk_s3::primitives::DateTime) -> u64 {
    t.to_millis().map_or(0, |ms| (ms / 1000) as u64)
}

#[async_trait]
impl Transport for HttpTransport {
    async fn create_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        self.client.create_bucket().bucket(bucket).send().await?;
        Ok(())
    }

    async fn delete_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        self.client.delete_bucket().bucket(bucket).send().await?;
        Ok(())
    }

    async fn list_buckets(&self) -> anyhow::Result<Vec<BucketEntry>> {
        let resp = self.client.list_buckets().send().await?;
        let buckets = resp
            .buckets()
            .iter()
            .filter_map(|b| {
                b.name().map(|name| BucketEntry {
                    name: name.to_owned(),
                })
            })
            .collect();
        Ok(buckets)
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
    ) -> anyhow::Result<ListResult> {
        let mut req = self.client.list_objects_v2().bucket(bucket);
        if let Some(p) = prefix {
            req = req.prefix(p);
        }
        if let Some(d) = delimiter {
            req = req.delimiter(d);
        }
        if let Some(c) = continuation_token {
            req = req.continuation_token(c);
        }
        let resp = req.send().await?;
        let objects = resp
            .contents()
            .iter()
            .map(|obj| ObjectEntry {
                key: obj.key().unwrap_or_default().to_owned(),
                #[allow(clippy::cast_sign_loss)]
                size: obj.size().unwrap_or_default() as u64,
                last_modified: obj
                    .last_modified()
                    .map_or(0, millis_to_epoch_secs),
                etag: obj.e_tag().unwrap_or_default().to_owned(),
            })
            .collect();
        let common_prefixes = resp
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix().map(String::from))
            .collect();
        Ok(ListResult {
            objects,
            common_prefixes,
            is_truncated: resp.is_truncated().unwrap_or_default(),
            next_continuation_token: resp.next_continuation_token().map(String::from),
        })
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: &Path,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        let stream = ByteStream::from_path(body).await?;
        let mut req = self.client.put_object().bucket(bucket).key(key).body(stream);
        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }
        req.send().await?;
        Ok(())
    }

    async fn get_object(&self, bucket: &str, key: &str, dest: &Path) -> anyhow::Result<u64> {
        let resp = self.client.get_object().bucket(bucket).key(key).send().await?;
        let data = resp.body.collect().await?;
        let bytes = data.into_bytes();
        let len = bytes.len() as u64;
        tokio::fs::write(dest, bytes).await?;
        Ok(len)
    }

    async fn head_object(&self, bucket: &str, key: &str) -> anyhow::Result<Option<ObjectHead>> {
        match self.client.head_object().bucket(bucket).key(key).send().await {
            Ok(resp) => Ok(Some(ObjectHead {
                #[allow(clippy::cast_sign_loss)]
                size: resp.content_length().unwrap_or_default() as u64,
                last_modified: resp
                    .last_modified()
                    .map_or(0, millis_to_epoch_secs),
                etag: resp.e_tag().unwrap_or_default().to_owned(),
                content_type: resp.content_type().map(String::from),
            })),
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_not_found() {
                    Ok(None)
                } else {
                    Err(service_err.into())
                }
            }
        }
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
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
        let resp = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await?;
        let data = resp.body.collect().await?;
        let bytes = data.into_bytes();
        let len = bytes.len() as u64;
        tokio::fs::write(dest, bytes).await?;
        Ok(len)
    }

    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await?;
        Ok(())
    }

    async fn delete_objects(&self, bucket: &str, keys: &[String]) -> anyhow::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        for chunk in keys.chunks(1000) {
            let objects: Vec<ObjectIdentifier> = chunk
                .iter()
                .filter_map(|k| ObjectIdentifier::builder().key(k).build().ok())
                .collect();
            let delete = Delete::builder().set_objects(Some(objects)).build()?;
            self.client
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
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
        let mut req = self.client.list_object_versions().bucket(bucket);
        if let Some(p) = prefix {
            req = req.prefix(p);
        }
        if let Some(km) = key_marker {
            req = req.key_marker(km);
        }
        if let Some(vm) = version_id_marker {
            req = req.version_id_marker(vm);
        }
        let resp = req.send().await?;

        let mut entries = Vec::new();

        for ver in resp.versions() {
            entries.push(VersionEntryInfo {
                key: ver.key().unwrap_or_default().to_owned(),
                version_id: ver.version_id().unwrap_or("null").to_owned(),
                #[allow(clippy::cast_sign_loss)]
                size: ver.size().unwrap_or_default() as u64,
                last_modified: ver
                    .last_modified()
                    .map_or(0, millis_to_epoch_secs),
                is_latest: ver.is_latest().unwrap_or_default(),
                is_delete_marker: false,
            });
        }

        for dm in resp.delete_markers() {
            entries.push(VersionEntryInfo {
                key: dm.key().unwrap_or_default().to_owned(),
                version_id: dm.version_id().unwrap_or("null").to_owned(),
                size: 0,
                last_modified: dm
                    .last_modified()
                    .map_or(0, millis_to_epoch_secs),
                is_latest: dm.is_latest().unwrap_or_default(),
                is_delete_marker: true,
            });
        }

        Ok(ListVersionsResult {
            entries,
            is_truncated: resp.is_truncated().unwrap_or_default(),
            next_key_marker: resp.next_key_marker().map(String::from),
            next_version_id_marker: resp.next_version_id_marker().map(String::from),
        })
    }
}
