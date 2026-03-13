use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use md5::{Digest, Md5};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CommonPrefix,
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
    DeleteBucketInput, DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput,
    DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, ETag, GetObjectInput,
    GetObjectOutput, HeadObjectInput, HeadObjectOutput, ListBucketsInput, ListBucketsOutput,
    ListObjectsV2Input, ListObjectsV2Output, Object, PutObjectInput, PutObjectOutput,
    StreamingBlob, Timestamp, UploadPartInput, UploadPartOutput,
};
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};

use crate::storage::{BucketStore, Storage};

/// Monotonic counter for unique temp file names across concurrent requests.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Run a blocking closure on the tokio blocking thread pool.
async fn blocking<F, T>(f: F) -> Result<T, io::Error>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| io::Error::other(format!("task panicked: {e}")))?
}

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
            .map_err(|e| { tracing::error!("get_bucket({name}): {e}"); s3_error!(e, InternalError) })?
            .ok_or_else(|| s3_error!(NoSuchBucket))
    }
}

/// Stream request body to a temp file, returning the MD5 hex digest.
async fn stream_body_to_tmp(body: StreamingBlob, tmp_path: &Path) -> S3Result<String> {
    let mut tmp_file =
        std::fs::File::create(tmp_path).map_err(|e| { tracing::error!("create tmp file: {e}"); s3_error!(e, InternalError) })?;
    let mut hasher = Md5::new();
    let mut stream = body;

    while let Some(chunk) = stream.try_next().await.map_err(|e| { tracing::error!("read request body: {e}"); s3_error!(InternalError) })? {
        tmp_file
            .write_all(&chunk)
            .map_err(|e| { tracing::error!("write tmp file: {e}"); s3_error!(e, InternalError) })?;
        hasher.update(&chunk);
    }
    tmp_file.flush().map_err(|e| { tracing::error!("flush tmp file: {e}"); s3_error!(e, InternalError) })?;

    Ok(format!("{:x}", hasher.finalize()))
}

#[async_trait::async_trait]
impl S3 for SimpleStorage {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = req.input.bucket;
        let storage = Arc::clone(&self.inner);
        let existed = blocking(move || storage.create_bucket(&bucket))
            .await
            .map_err(|e| { tracing::error!("create_bucket: {e}"); s3_error!(e, InternalError) })?;
        if existed {
            return Err(s3_error!(BucketAlreadyOwnedByYou));
        }
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket = req.input.bucket;
        let store = self.bucket(&bucket)?;
        let storage = Arc::clone(&self.inner);

        blocking(move || {
            if !store.is_empty()? {
                return Err(io::Error::other("BucketNotEmpty"));
            }
            drop(store);
            storage.delete_bucket(&bucket)?;
            Ok(())
        })
        .await
        .map_err(|e| {
            if e.to_string() == "BucketNotEmpty" {
                return s3_error!(BucketNotEmpty);
            }
            tracing::error!("delete_bucket: {e}");
            s3_error!(e, InternalError)
        })?;

        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let storage = Arc::clone(&self.inner);
        let names = blocking(move || storage.list_buckets())
            .await
            .map_err(|e| { tracing::error!("list_buckets: {e}"); s3_error!(e, InternalError) })?;
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

        let tmp_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = store.bucket_dir().join(format!(".tmp_{tmp_id:020}"));

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

        let key = input.key;
        let content_type = input.content_type;
        let etag_clone = etag_hex.clone();
        let metadata = input.metadata.unwrap_or_default();
        blocking(move || {
            store.put_object_streamed(&key, &tmp_path, content_type, etag_clone, now, metadata)
                .map(|_| ())
        })
        .await
        .map_err(|e| { tracing::error!("put_object: {e}"); s3_error!(e, InternalError) })?;

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

        let key = input.key;
        let (meta, data) = blocking(move || {
            let meta = store.get_meta(&key)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "NoSuchKey"))?;
            let data = store.read_data(meta.segment_id, meta.offset, meta.length)?;
            Ok((meta, data))
        })
        .await
        .map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                return s3_error!(NoSuchKey);
            }
            tracing::error!("get_object: {e}");
            s3_error!(e, InternalError)
        })?;

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

        let key = input.key;
        let meta = blocking(move || {
            store.get_meta(&key)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "NoSuchKey"))
        })
        .await
        .map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                return s3_error!(NoSuchKey);
            }
            tracing::error!("head_object: {e}");
            s3_error!(e, InternalError)
        })?;

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

        let key = input.key;
        blocking(move || store.delete_object(&key).map(|_| ()))
            .await
            .map_err(|e| { tracing::error!("delete_object: {e}"); s3_error!(e, InternalError) })?;

        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;
        let quiet = input.delete.quiet.unwrap_or(false);
        let objects = input.delete.objects;

        let (deleted_keys, errors) = blocking(move || {
            let mut deleted_keys = Vec::new();
            let mut errors: Vec<s3s::dto::Error> = Vec::new();
            for obj_id in objects {
                let key = obj_id.key;
                match store.delete_object(&key) {
                    Ok(_) => deleted_keys.push(key),
                    Err(e) => errors.push(s3s::dto::Error {
                        code: Some("InternalError".to_owned()),
                        key: Some(key),
                        message: Some(e.to_string()),
                        version_id: None,
                    }),
                }
            }
            Ok((deleted_keys, errors))
        })
        .await
        .map_err(|e| {
            tracing::error!("delete_objects: {e}");
            s3_error!(e, InternalError)
        })?;

        let deleted = if quiet {
            None
        } else {
            Some(
                deleted_keys
                    .into_iter()
                    .map(|key| DeletedObject {
                        key: Some(key),
                        ..Default::default()
                    })
                    .collect(),
            )
        };

        let errors = if errors.is_empty() {
            None
        } else {
            Some(errors)
        };

        let output = DeleteObjectsOutput {
            deleted,
            errors,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;

        #[allow(clippy::cast_sign_loss)] // max(0) guarantees non-negative
        let max_keys = input.max_keys.unwrap_or(1000).max(0) as usize;
        let prefix = input.prefix.clone();
        let delimiter = input.delimiter.clone();
        let continuation = input.continuation_token.clone();

        let (entries, common_prefixes, truncated) = blocking(move || {
            store.list_objects_with_delimiter(
                prefix.as_deref(),
                delimiter.as_deref(),
                max_keys,
                continuation.as_deref(),
            )
        })
        .await
        .map_err(|e| { tracing::error!("list_objects_v2({}): {e}", input.bucket); s3_error!(e, InternalError) })?;

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
        while let Some(chunk) = stream.try_next().await.map_err(|e| { tracing::error!("upload_part: read body: {e}"); s3_error!(InternalError) })? {
            data.extend_from_slice(&chunk);
        }

        let upload_id = input.upload_id;
        let part_number = input.part_number;
        let etag = blocking(move || store.upload_part(&upload_id, part_number, &data))
            .await
            .map_err(|e| { tracing::error!("upload_part: {e}"); s3_error!(e, InternalError) })?;

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

        let upload_id = input.upload_id;
        let key = input.key;
        let bucket = input.bucket;
        let (_meta, etag) = blocking(move || {
            store.complete_multipart_upload(
                &upload_id,
                &key,
                &parts,
                None, // content_type from CreateMultipartUpload not stored yet
                now,
                HashMap::new(),
            )
        })
        .await
        .map_err(|e| { tracing::error!("complete_multipart_upload: {e}"); s3_error!(e, InternalError) })?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: None,
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

        let upload_id = input.upload_id;
        blocking(move || store.abort_multipart_upload(&upload_id))
            .await
            .map_err(|e| { tracing::error!("abort_multipart_upload: {e}"); s3_error!(e, InternalError) })?;

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }
}
