use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use md5::{Digest, Md5};
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};

use crate::storage::Storage;
use crate::types::ObjectMeta;

pub struct SimpleStorage {
    inner: Arc<Storage>,
}

impl SimpleStorage {
    pub fn new(storage: Storage) -> Self {
        Self {
            inner: Arc::new(storage),
        }
    }
}

#[async_trait::async_trait]
impl S3 for SimpleStorage {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = &req.input.bucket;
        let existed = self
            .inner
            .create_bucket(bucket)
            .map_err(|e| s3_error!(e, InternalError))?;
        if existed {
            return Err(s3_error!(BucketAlreadyOwnedByYou));
        }
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let names = self.inner.list_buckets();
        let buckets: Vec<Bucket> = names
            .into_iter()
            .map(|name| Bucket {
                name: Some(name),
                creation_date: Some(Timestamp::from(UNIX_EPOCH)),
                ..Default::default()
            })
            .collect();
        let output = ListBucketsOutput {
            buckets: Some(buckets),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        let store = self
            .inner
            .get_bucket(&input.bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?;

        let body = input.body.ok_or_else(|| s3_error!(IncompleteBody))?;

        let mut data = Vec::new();
        let mut stream = body;
        while let Some(chunk) = stream.try_next().await.map_err(|_| s3_error!(InternalError))? {
            data.extend_from_slice(&chunk);
        }

        let mut hasher = Md5::new();
        hasher.update(&data);
        let etag_hex = format!("{:x}", hasher.finalize());

        let (offset, length) = store
            .append_data(&data)
            .map_err(|e| s3_error!(e, InternalError))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        let meta = ObjectMeta {
            offset,
            length,
            content_type: input.content_type,
            etag: etag_hex.clone(),
            last_modified: now,
            user_metadata: input.metadata.unwrap_or_default(),
        };

        store
            .put_meta(&input.key, &meta)
            .map_err(|e| s3_error!(e, InternalError))?;

        let output = PutObjectOutput {
            e_tag: Some(ETag::Strong(etag_hex)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let store = self
            .inner
            .get_bucket(&input.bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?;

        let meta = store
            .get_meta(&input.key)
            .map_err(|e| s3_error!(e, InternalError))?
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        let data = store
            .read_data(meta.offset, meta.length)
            .map_err(|e| s3_error!(e, InternalError))?;

        let stream =
            futures::stream::once(async { Ok::<_, std::io::Error>(bytes::Bytes::from(data)) });
        let body = StreamingBlob::wrap(stream);

        let last_modified = Timestamp::from(UNIX_EPOCH + Duration::from_secs(meta.last_modified));

        let metadata = if meta.user_metadata.is_empty() {
            None
        } else {
            Some(meta.user_metadata)
        };

        let output = GetObjectOutput {
            body: Some(body),
            content_length: Some(meta.length as i64),
            content_type: meta.content_type,
            e_tag: Some(ETag::Strong(meta.etag)),
            last_modified: Some(last_modified),
            metadata,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let store = self
            .inner
            .get_bucket(&input.bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?;

        let meta = store
            .get_meta(&input.key)
            .map_err(|e| s3_error!(e, InternalError))?
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        let last_modified = Timestamp::from(UNIX_EPOCH + Duration::from_secs(meta.last_modified));

        let metadata = if meta.user_metadata.is_empty() {
            None
        } else {
            Some(meta.user_metadata)
        };

        let output = HeadObjectOutput {
            content_length: Some(meta.length as i64),
            content_type: meta.content_type,
            e_tag: Some(ETag::Strong(meta.etag)),
            last_modified: Some(last_modified),
            metadata,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        let store = self
            .inner
            .get_bucket(&input.bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?;

        store
            .delete_and_compact(&input.key)
            .map_err(|e| s3_error!(e, InternalError))?;

        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let store = self
            .inner
            .get_bucket(&input.bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket))?;

        let max_keys = input.max_keys.unwrap_or(1000) as usize;
        let prefix = input.prefix.as_deref();
        let continuation = input.continuation_token.as_deref();

        let (entries, truncated) = store
            .list_objects(prefix, max_keys, continuation)
            .map_err(|e| s3_error!(e, InternalError))?;

        let next_token = if truncated {
            entries.last().map(|(k, _)| k.clone())
        } else {
            None
        };

        let objects: Vec<Object> = entries
            .into_iter()
            .map(|(key, meta)| {
                let last_modified =
                    Timestamp::from(UNIX_EPOCH + Duration::from_secs(meta.last_modified));
                Object {
                    key: Some(key),
                    size: Some(meta.length as i64),
                    e_tag: Some(ETag::Strong(meta.etag)),
                    last_modified: Some(last_modified),
                    ..Default::default()
                }
            })
            .collect();

        let key_count = objects.len() as i32;

        let output = ListObjectsV2Output {
            contents: Some(objects),
            is_truncated: Some(truncated),
            key_count: Some(key_count),
            max_keys: Some(max_keys as i32),
            name: Some(input.bucket),
            prefix: input.prefix,
            next_continuation_token: next_token,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}
