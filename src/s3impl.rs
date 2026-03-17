use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use md5::{Digest, Md5};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, BucketVersioningStatus,
    CommonPrefix, CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
    CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
    DeleteBucketInput, DeleteBucketOutput, DeleteMarkerEntry, DeleteObjectInput,
    DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, ETag,
    GetBucketVersioningInput, GetBucketVersioningOutput, GetObjectInput, GetObjectOutput,
    HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
    ListBucketsInput, ListBucketsOutput,
    ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsV2Input, ListObjectsV2Output,
    MetadataDirective, Object, ObjectVersion, PutBucketVersioningInput,
    PutBucketVersioningOutput, PutObjectInput, PutObjectOutput, StreamingBlob, Timestamp,
    UploadPartInput, UploadPartOutput,
};
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};

use crate::storage::versioning::VersioningState;
use crate::storage::{BucketStore, Storage};
use crate::types::ObjectMeta;

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

/// Stream request body to a temp file, returning `(md5_hex, crc32c)`.
async fn stream_body_to_tmp(body: StreamingBlob, tmp_path: &Path) -> S3Result<(String, u32)> {
    let file = std::fs::File::create(tmp_path)
        .map_err(|e| { tracing::error!("create tmp file: {e}"); s3_error!(e, InternalError) })?;
    let mut writer = io::BufWriter::with_capacity(1024 * 1024, file);
    let mut hasher = Md5::new();
    let mut crc: u32 = 0;
    let mut stream = body;

    while let Some(chunk) = stream.try_next().await.map_err(|e| { tracing::error!("read request body: {e}"); s3_error!(InternalError) })? {
        writer
            .write_all(&chunk)
            .map_err(|e| { tracing::error!("write tmp file: {e}"); s3_error!(e, InternalError) })?;
        hasher.update(&chunk);
        crc = crc32c::crc32c_append(crc, &chunk);
    }
    writer.flush().map_err(|e| { tracing::error!("flush tmp file: {e}"); s3_error!(e, InternalError) })?;

    Ok((format!("{:x}", hasher.finalize()), crc))
}

fn version_id_string(vid: Option<&str>) -> Option<String> {
    vid.map(str::to_owned)
}

/// Resolve an object by key, optionally looking up a specific version.
/// Uses a single read transaction to avoid TOCTOU between tables.
/// Returns the meta or an `io::Error` with appropriate `ErrorKind`.
fn resolve_object_version(
    store: &BucketStore,
    key: &str,
    version_id: Option<&str>,
) -> io::Result<ObjectMeta> {
    let not_found_msg = if version_id.is_some() { "NoSuchVersion" } else { "NoSuchKey" };
    let meta = store
        .get_object_or_version(key, version_id)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, not_found_msg))?;

    if meta.is_delete_marker {
        return Err(io::Error::new(io::ErrorKind::NotFound, "DeleteMarker"));
    }

    Ok(meta)
}

#[async_trait::async_trait]
impl S3 for SimpleStorage {
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let bucket = req.input.bucket;
        let _store = self.bucket(&bucket)?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

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

    // === Bucket versioning ===

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;

        let status = input.versioning_configuration.status;

        blocking(move || {
            if let Some(s) = status {
                let state = if s.as_str() == BucketVersioningStatus::ENABLED {
                    VersioningState::Enabled
                } else if s.as_str() == BucketVersioningStatus::SUSPENDED {
                    VersioningState::Suspended
                } else {
                    return Err(io::Error::other(format!(
                        "MalformedXML: invalid versioning status '{}'",
                        s.as_str()
                    )));
                };
                store.set_versioning_state(state)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| {
            if e.to_string().starts_with("MalformedXML") {
                return s3_error!(MalformedXML);
            }
            tracing::error!("put_bucket_versioning: {e}");
            s3_error!(e, InternalError)
        })?;

        Ok(S3Response::new(PutBucketVersioningOutput::default()))
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let store = self.bucket(&req.input.bucket)?;

        let state = blocking(move || store.get_versioning_state())
            .await
            .map_err(|e| { tracing::error!("get_bucket_versioning: {e}"); s3_error!(e, InternalError) })?;

        let status = state.map(|s| match s {
            VersioningState::Enabled => {
                BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)
            }
            VersioningState::Suspended => {
                BucketVersioningStatus::from_static(BucketVersioningStatus::SUSPENDED)
            }
        });

        let output = GetBucketVersioningOutput {
            status,
            mfa_delete: None,
        };
        Ok(S3Response::new(output))
    }

    // === Object operations ===

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;

        let body = input.body.ok_or_else(|| s3_error!(IncompleteBody))?;

        let tmp_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = store.bucket_dir().join(format!(".tmp_{tmp_id:020}"));

        let (etag_hex, crc) = match stream_body_to_tmp(body, &tmp_path).await {
            Ok(v) => v,
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
        let meta = blocking(move || {
            store.put_object_streamed(
                &key, &tmp_path, content_type, etag_clone, now, metadata, Some(crc),
            )
        })
        .await
        .map_err(|e| { tracing::error!("put_object: {e}"); s3_error!(e, InternalError) })?;

        let output = PutObjectOutput {
            e_tag: Some(ETag::Strong(etag_hex)),
            version_id: version_id_string(meta.version_id.as_deref()),
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
        let range = input.range;
        let req_version_id = input.version_id;

        let (meta, data, content_range) = blocking(move || {
            let meta = resolve_object_version(
                &store, &key, req_version_id.as_deref(),
            )?;

            let obj_size = meta.data_length();

            if let Some(ref range) = range {
                let byte_range = range
                    .check(obj_size)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "InvalidRange"))?;
                let range_offset = meta.offset + byte_range.start;
                let range_len = byte_range.end - byte_range.start;
                let data = store.read_data(meta.segment_id, range_offset, range_len)?;
                let cr = format!(
                    "bytes {}-{}/{}",
                    byte_range.start,
                    byte_range.end - 1,
                    obj_size
                );
                Ok((meta, data, Some(cr)))
            } else {
                let data = store.read_object(&meta)?;
                Ok((meta, data, None))
            }
        })
        .await
        .map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                return s3_error!(NoSuchKey);
            }
            if e.kind() == io::ErrorKind::InvalidInput {
                return s3_error!(InvalidRange);
            }
            tracing::error!("get_object: {e}");
            s3_error!(e, InternalError)
        })?;

        #[allow(clippy::cast_possible_wrap)]
        let content_length = data.len() as i64;
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
            content_length: Some(content_length),
            content_range,
            content_type: meta.content_type,
            e_tag: Some(ETag::Strong(meta.etag)),
            last_modified: Some(last_modified),
            metadata,
            version_id: version_id_string(meta.version_id.as_deref()),
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
        let req_version_id = input.version_id;

        let meta = blocking(move || {
            resolve_object_version(&store, &key, req_version_id.as_deref())
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
        let content_length = meta.data_length().cast_signed();

        let metadata = if meta.user_metadata.is_empty() {
            None
        } else {
            Some(meta.user_metadata)
        };

        let output = HeadObjectOutput {
            content_length: Some(content_length),
            content_type: meta.content_type,
            e_tag: Some(ETag::Strong(meta.etag)),
            last_modified: Some(last_modified),
            metadata,
            version_id: version_id_string(meta.version_id.as_deref()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;

        let (src_bucket, src_key, src_version_id) = match input.copy_source {
            CopySource::Bucket {
                bucket,
                key,
                version_id,
            } => (
                bucket.to_string(),
                key.to_string(),
                version_id.map(|v| v.to_string()),
            ),
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
        };

        let src_store = self.bucket(&src_bucket)?;
        let dest_store = self.bucket(&input.bucket)?;

        let replace_metadata = input
            .metadata_directive
            .as_ref()
            .is_some_and(|d| d.as_str() == MetadataDirective::REPLACE);

        let req_content_type = input.content_type;
        let req_metadata = input.metadata.unwrap_or_default();

        let tmp_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = dest_store.bucket_dir().join(format!(".tmp_{tmp_id:020}"));

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        let src_vid = src_version_id.clone();
        let ss = Arc::clone(&src_store);
        let dest_key = input.key;
        let result = blocking(move || {
            let src_meta = resolve_object_version(&ss, &src_key, src_vid.as_deref())?;

            let (content_type, user_metadata) = if replace_metadata {
                (req_content_type, req_metadata)
            } else {
                (src_meta.content_type.clone(), src_meta.user_metadata.clone())
            };

            let (etag_hex, crc) = ss.copy_to_tmp_file(&src_meta, &tmp_path)?;
            let data_len = src_meta.data_length();
            match dest_store.put_object_streamed(
                &dest_key, &tmp_path, content_type, etag_hex.clone(), now, user_metadata, Some(crc),
            ) {
                Ok(m) => Ok((etag_hex, m, src_meta.version_id, data_len)),
                Err(e) => {
                    std::fs::remove_file(&tmp_path).ok();
                    Err(e)
                }
            }
        })
        .await
        .map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                return s3_error!(NoSuchKey);
            }
            tracing::error!("copy_object: {e}");
            s3_error!(e, InternalError)
        })?;

        let (etag_hex, meta, src_vid_out, _data_len) = result;

        let last_modified = Timestamp::from(UNIX_EPOCH + Duration::from_secs(now));

        let output = CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(etag_hex)),
                last_modified: Some(last_modified),
                ..Default::default()
            }),
            version_id: version_id_string(meta.version_id.as_deref()),
            copy_source_version_id: src_version_id
                .or(src_vid_out),
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
        let req_version_id = input.version_id;

        let result_meta = blocking(move || {
            if let Some(vid) = req_version_id {
                // Version-specific delete: permanently remove.
                // Return metadata for the response (AWS returns version_id).
                if let Some(meta) = store.delete_current_version(&key, &vid)? {
                    return Ok(Some(meta));
                }
                // Try versions table
                Ok(store.delete_version(&key, &vid)?)
            } else {
                // No version_id: hard delete or create delete marker
                store.delete_object(&key)
            }
        })
        .await
        .map_err(|e| { tracing::error!("delete_object: {e}"); s3_error!(e, InternalError) })?;

        let mut output = DeleteObjectOutput::default();
        if let Some(meta) = result_meta {
            output.version_id = version_id_string(meta.version_id.as_deref());
            if meta.is_delete_marker {
                output.delete_marker = Some(true);
            }
        }
        Ok(S3Response::new(output))
    }

    #[allow(clippy::too_many_lines)] // per-item version/non-version branching in batch delete
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;
        let quiet = input.delete.quiet.unwrap_or(false);
        let objects = input.delete.objects;

        let (deleted_list, errors) = blocking(move || {
            let mut deleted_list: Vec<DeletedObject> = Vec::new();
            let mut errors: Vec<s3s::dto::Error> = Vec::new();

            for obj_id in objects {
                let key = obj_id.key;
                let version_id = obj_id.version_id;

                if let Some(vid) = version_id {
                    // Version-specific delete
                    match store.delete_current_version(&key, &vid) {
                        Ok(Some(meta)) => {
                            deleted_list.push(DeletedObject {
                                key: Some(key),
                                version_id: Some(vid),
                                delete_marker: Some(meta.is_delete_marker),
                                ..Default::default()
                            });
                        }
                        Ok(None) => {
                            // Try versions table
                            match store.delete_version(&key, &vid) {
                                Ok(Some(meta)) => {
                                    deleted_list.push(DeletedObject {
                                        key: Some(key),
                                        version_id: Some(vid),
                                        delete_marker: if meta.is_delete_marker { Some(true) } else { None },
                                        ..Default::default()
                                    });
                                }
                                Ok(None) => {
                                    deleted_list.push(DeletedObject {
                                        key: Some(key),
                                        version_id: Some(vid),
                                        ..Default::default()
                                    });
                                }
                                Err(e) => errors.push(s3s::dto::Error {
                                    code: Some("InternalError".to_owned()),
                                    key: Some(key),
                                    message: Some(e.to_string()),
                                    version_id: Some(vid),
                                }),
                            }
                        }
                        Err(e) => errors.push(s3s::dto::Error {
                            code: Some("InternalError".to_owned()),
                            key: Some(key),
                            message: Some(e.to_string()),
                            version_id: Some(vid),
                        }),
                    }
                } else {
                    // No version_id: hard delete or create delete marker
                    match store.delete_object(&key) {
                        Ok(Some(meta)) => {
                            deleted_list.push(DeletedObject {
                                key: Some(key),
                                version_id: version_id_string(meta.version_id.as_deref()),
                                delete_marker: if meta.is_delete_marker { Some(true) } else { None },
                                ..Default::default()
                            });
                        }
                        Ok(None) => {
                            deleted_list.push(DeletedObject {
                                key: Some(key),
                                ..Default::default()
                            });
                        }
                        Err(e) => errors.push(s3s::dto::Error {
                            code: Some("InternalError".to_owned()),
                            key: Some(key),
                            message: Some(e.to_string()),
                            version_id: None,
                        }),
                    }
                }
            }
            Ok((deleted_list, errors))
        })
        .await
        .map_err(|e| {
            tracing::error!("delete_objects: {e}");
            s3_error!(e, InternalError)
        })?;

        let deleted = if quiet { None } else { Some(deleted_list) };
        let errors = if errors.is_empty() { None } else { Some(errors) };

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
                    size: Some(meta.data_length().cast_signed()),
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

    // === ListObjectVersions ===

    #[allow(clippy::too_many_lines)] // maps storage entries to S3 DTOs with version/delete-marker split
    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let input = req.input;
        let store = self.bucket(&input.bucket)?;

        #[allow(clippy::cast_sign_loss)]
        let max_keys = input.max_keys.unwrap_or(1000).max(0) as usize;
        let prefix = input.prefix.clone();
        let delimiter = input.delimiter.clone();
        let key_marker = input.key_marker.clone();
        let version_id_marker = input.version_id_marker.clone();

        let result = blocking(move || {
            store.list_object_versions(
                prefix.as_deref(),
                delimiter.as_deref(),
                max_keys,
                key_marker.as_deref(),
                version_id_marker.as_deref(),
            )
        })
        .await
        .map_err(|e| { tracing::error!("list_object_versions: {e}"); s3_error!(e, InternalError) })?;

        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();

        for entry in result.entries {
            let last_modified =
                Timestamp::from(UNIX_EPOCH + Duration::from_secs(entry.meta.last_modified));

            if entry.meta.is_delete_marker {
                delete_markers.push(DeleteMarkerEntry {
                    is_latest: Some(entry.is_latest),
                    key: Some(entry.key),
                    last_modified: Some(last_modified),
                    version_id: Some(entry.version_id),
                    ..Default::default()
                });
            } else {
                let size = entry.meta.data_length().cast_signed();
                versions.push(ObjectVersion {
                    e_tag: Some(ETag::Strong(entry.meta.etag)),
                    is_latest: Some(entry.is_latest),
                    key: Some(entry.key),
                    last_modified: Some(last_modified),
                    size: Some(size),
                    version_id: Some(entry.version_id),
                    ..Default::default()
                });
            }
        }

        let cp: Option<Vec<CommonPrefix>> = if result.common_prefixes.is_empty() {
            None
        } else {
            Some(
                result.common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect(),
            )
        };

        let output = ListObjectVersionsOutput {
            versions: if versions.is_empty() { None } else { Some(versions) },
            delete_markers: if delete_markers.is_empty() { None } else { Some(delete_markers) },
            common_prefixes: cp,
            delimiter: input.delimiter,
            is_truncated: Some(result.is_truncated),
            key_marker: input.key_marker,
            version_id_marker: input.version_id_marker,
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            max_keys: Some(max_keys as i32),
            name: Some(input.bucket),
            prefix: input.prefix,
            next_key_marker: result.next_key_marker,
            next_version_id_marker: result.next_version_id_marker,
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
        let (meta, etag) = blocking(move || {
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
            version_id: version_id_string(meta.version_id.as_deref()),
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
