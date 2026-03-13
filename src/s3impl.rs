use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use md5::{Digest, Md5};
use s3s::dto::{CreateBucketInput, CreateBucketOutput, DeleteBucketInput, DeleteBucketOutput, ListBucketsInput, ListBucketsOutput, Bucket, Timestamp, PutObjectInput, PutObjectOutput, ETag, GetObjectInput, GetObjectOutput, StreamingBlob, HeadObjectInput, HeadObjectOutput, DeleteObjectInput, DeleteObjectOutput, ListObjectsV2Input, ListObjectsV2Output, Object, CommonPrefix, CreateMultipartUploadInput, CreateMultipartUploadOutput, UploadPartInput, UploadPartOutput, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, AbortMultipartUploadInput, AbortMultipartUploadOutput};
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};

use crate::storage::{BucketStore, Storage};

pub struct SimpleStorage {
    inner: Arc<Storage>,
}

impl SimpleStorage {
    pub const fn new(storage: Arc<Storage>) -> Self {
        Self { inner: storage }
    }

    fn bucket(&self, name: &str) -> S3Result<Arc<BucketStore>> {
        self.inner
            .get_bucket(name)
            .map_err(|e| s3_error!(e, InternalError))?
            .ok_or_else(|| s3_error!(NoSuchBucket))
    }
}

/// Stream request body to a temp file, returning the MD5 hex digest.
async fn stream_body_to_tmp(body: StreamingBlob, tmp_path: &Path) -> S3Result<String> {
    let mut tmp_file =
        std::fs::File::create(tmp_path).map_err(|e| s3_error!(e, InternalError))?;
    let mut hasher = Md5::new();
    let mut stream = body;

    while let Some(chunk) = stream.try_next().await.map_err(|_| s3_error!(InternalError))? {
        tmp_file
            .write_all(&chunk)
            .map_err(|e| s3_error!(e, InternalError))?;
        hasher.update(&chunk);
    }
    tmp_file.flush().map_err(|e| s3_error!(e, InternalError))?;

    Ok(format!("{:x}", hasher.finalize()))
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

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket = &req.input.bucket;
        let store = self.bucket(bucket)?;

        if !store.is_empty().map_err(|e| s3_error!(e, InternalError))? {
            return Err(s3_error!(BucketNotEmpty));
        }
        drop(store);

        self.inner
            .delete_bucket(bucket)
            .map_err(|e| s3_error!(e, InternalError))?;

        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let names = self
            .inner
            .list_buckets()
            .map_err(|e| s3_error!(e, InternalError))?;
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
        let store = self.bucket(&input.bucket)?;

        let body = input.body.ok_or_else(|| s3_error!(IncompleteBody))?;

        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let tmp_path = store.bucket_dir().join(format!(".tmp_{now_nanos:020}"));

        let etag_hex = match stream_body_to_tmp(body, &tmp_path).await {
            Ok(etag) => etag,
            Err(e) => {
                std::fs::remove_file(&tmp_path).ok();
                return Err(e);
            }
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        store
            .put_object_streamed(
                &input.key,
                &tmp_path,
                input.content_type,
                etag_hex.clone(),
                now,
                input.metadata.unwrap_or_default(),
            )
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
        let store = self.bucket(&input.bucket)?;

        let meta = store
            .get_meta(&input.key)
            .map_err(|e| s3_error!(e, InternalError))?
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        let data = store
            .read_data(meta.segment_id, meta.offset, meta.length)
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
            content_length: Some(meta.length.cast_signed()),
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
        let store = self.bucket(&input.bucket)?;

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
            content_length: Some(meta.length.cast_signed()),
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
        let store = self.bucket(&input.bucket)?;

        store
            .delete_object(&input.key)
            .map_err(|e| s3_error!(e, InternalError))?;

        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;

        #[allow(clippy::cast_sign_loss)] // max(0) guarantees non-negative
        let max_keys = input.max_keys.unwrap_or(1000).max(0) as usize;
        let prefix = input.prefix.as_deref();
        let delimiter = input.delimiter.as_deref();
        let continuation = input.continuation_token.as_deref();

        let (entries, common_prefixes, truncated) = store
            .list_objects_with_delimiter(prefix, delimiter, max_keys, continuation)
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
                    size: Some(meta.length.cast_signed()),
                    e_tag: Some(ETag::Strong(meta.etag)),
                    last_modified: Some(last_modified),
                    ..Default::default()
                }
            })
            .collect();

        let cp: Option<Vec<CommonPrefix>> = if common_prefixes.is_empty() {
            None
        } else {
            Some(
                common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix {
                        prefix: Some(p),
                    })
                    .collect(),
            )
        };

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let key_count = objects.len() as i32;
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let max_keys_i32 = max_keys as i32;

        let output = ListObjectsV2Output {
            contents: Some(objects),
            common_prefixes: cp,
            delimiter: input.delimiter,
            is_truncated: Some(truncated),
            key_count: Some(key_count),
            max_keys: Some(max_keys_i32),
            name: Some(input.bucket),
            prefix: input.prefix,
            next_continuation_token: next_token,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    // === Multipart upload ===

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;

        let upload_id = store.create_multipart_upload();

        let output = CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;

        let body = input.body.ok_or_else(|| s3_error!(IncompleteBody))?;

        let mut data = Vec::new();
        let mut stream = body;
        while let Some(chunk) = stream.try_next().await.map_err(|_| s3_error!(InternalError))? {
            data.extend_from_slice(&chunk);
        }

        let etag = store
            .upload_part(&input.upload_id, input.part_number, &data)
            .map_err(|e| s3_error!(e, InternalError))?;

        let output = UploadPartOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;

        let completed = input
            .multipart_upload
            .ok_or_else(|| s3_error!(MalformedXML))?;
        let parts_list = completed.parts.ok_or_else(|| s3_error!(MalformedXML))?;

        let parts: Vec<(i32, String)> = parts_list
            .iter()
            .map(|p| {
                let num = p.part_number.unwrap_or(0);
                let etag = p
                    .e_tag
                    .as_ref()
                    .map(|e| e.value().to_owned())
                    .unwrap_or_default();
                (num, etag)
            })
            .collect();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        let (_meta, etag) = store
            .complete_multipart_upload(
                &input.upload_id,
                &input.key,
                &parts,
                None, // content_type from CreateMultipartUpload not stored yet
                now,
                HashMap::new(),
            )
            .map_err(|e| s3_error!(e, InternalError))?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;

        store
            .abort_multipart_upload(&input.upload_id)
            .map_err(|e| s3_error!(e, InternalError))?;

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }
}
