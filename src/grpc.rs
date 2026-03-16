use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::grpc_auth::{
    check_grpc_access, extract_credentials, extract_credentials_from_metadata,
};
use crate::auth::AuthStore;
use crate::grpc_helpers::{
    bulk_get_header, bulk_get_not_found, bulk_put_one_object, map_io_err, meta_to_proto,
    segments_to_proto, spawn_download_stream, stream_to_tmp, TMP_COUNTER,
};
use crate::storage::versioning::VersioningState;
use crate::storage::{BucketStore, Storage};

#[allow(
    clippy::doc_markdown,
    clippy::derive_partial_eq_without_eq,
    clippy::wildcard_imports,
    clippy::missing_const_for_fn,
    clippy::default_trait_access,
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::missing_errors_doc
)]
pub mod proto {
    tonic::include_proto!("simple3");
}

use proto::simple3_server::Simple3;
#[allow(clippy::wildcard_imports)]
use proto::*;

pub struct GrpcService {
    storage: Arc<Storage>,
    auth_store: Option<Arc<AuthStore>>,
}

impl GrpcService {
    pub const fn new(storage: Arc<Storage>, auth_store: Option<Arc<AuthStore>>) -> Self {
        Self {
            storage,
            auth_store,
        }
    }

    fn check_auth<T>(
        &self,
        request: &Request<T>,
        action: &str,
        resource: &str,
    ) -> Result<(), Status> {
        let Some(store) = &self.auth_store else {
            return Ok(());
        };
        let creds = extract_credentials(request)?;
        check_grpc_access(store, &creds, action, resource)
    }

    fn check_auth_meta(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        action: &str,
        resource: &str,
    ) -> Result<(), Status> {
        let Some(store) = &self.auth_store else {
            return Ok(());
        };
        let creds = extract_credentials_from_metadata(metadata)?;
        check_grpc_access(store, &creds, action, resource)
    }

    fn bucket(&self, name: &str) -> Result<Arc<BucketStore>, Status> {
        self.storage
            .get_bucket(name)
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("bucket '{name}' not found")))
    }
}

type GrpcStream<T> = ReceiverStream<Result<T, Status>>;

#[tonic::async_trait]
impl Simple3 for GrpcService {
    // ================================================================
    // Object operations
    // ================================================================

    type GetObjectStream = GrpcStream<GetObjectResponse>;

    async fn put_object(
        &self,
        request: Request<Streaming<PutObjectRequest>>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let (metadata, _, stream) = request.into_parts();
        let mut stream = stream;

        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty stream"))?;
        let Some(proto::put_object_request::Request::Init(init)) = first.request else {
            return Err(Status::invalid_argument("first message must be init"));
        };

        let resource = format!("arn:s3:::{}/{}", init.bucket, init.key);
        self.check_auth_meta(&metadata, "s3:PutObject", &resource)?;

        let store = self.bucket(&init.bucket)?;
        let tmp_id = TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp_path = store.bucket_dir().join(format!(".tmp_grpc_{tmp_id:020}"));

        let (etag, size, crc) = match stream_to_tmp(&mut stream, &tmp_path).await {
            Ok(result) => result,
            Err(e) => {
                std::fs::remove_file(&tmp_path).ok();
                return Err(e);
            }
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        let key = init.key;
        let content_type = if init.content_type.is_empty() {
            None
        } else {
            Some(init.content_type)
        };
        let etag_clone = etag.clone();
        let metadata = init.user_metadata;
        let meta = tokio::task::spawn_blocking(move || {
            store.put_object_streamed(
                &key, &tmp_path, content_type, etag_clone, now, metadata, Some(crc),
            )
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?
        .map_err(map_io_err)?;

        Ok(Response::new(PutObjectResponse {
            etag,
            content_md5: meta.content_md5.unwrap_or_default(),
            size,
            version_id: meta.version_id,
        }))
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let input = request.get_ref();
        let resource = format!("arn:s3:::{}/{}", input.bucket, input.key);
        let action = if input.version_id.is_some() {
            "s3:GetObjectVersion"
        } else {
            "s3:GetObject"
        };
        self.check_auth(&request, action, &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let key = input.key.clone();
        let version_id = input.version_id.clone();
        let s = Arc::clone(&store);
        let meta = tokio::task::spawn_blocking(move || {
            s.get_object_or_version(&key, version_id.as_deref())
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?
        .map_err(map_io_err)?
        .ok_or_else(|| Status::not_found("object not found"))?;

        if meta.is_delete_marker {
            return Err(Status::not_found("object is a delete marker"));
        }

        let obj_size = meta.data_length();
        let (range_offset, range_len) = if let Some(start) = input.range_start {
            let end = input.range_end.unwrap_or(obj_size);
            if start >= obj_size || end > obj_size || start >= end {
                return Err(Status::out_of_range("invalid range"));
            }
            (meta.offset + start, end - start)
        } else {
            (meta.offset, obj_size)
        };

        let (tx, rx) = mpsc::channel(8);

        let metadata_msg = GetObjectResponse {
            response: Some(get_object_response::Response::Metadata(meta_to_proto(
                &input.key, &meta,
            ))),
        };
        tx.send(Ok(metadata_msg))
            .await
            .map_err(|_| Status::internal("channel closed"))?;

        spawn_download_stream(store, meta, range_offset, range_len, tx);

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<HeadObjectResponse>, Status> {
        let input = request.get_ref();
        let resource = format!("arn:s3:::{}/{}", input.bucket, input.key);
        let action = if input.version_id.is_some() {
            "s3:GetObjectVersion"
        } else {
            "s3:GetObject"
        };
        self.check_auth(&request, action, &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let key = input.key.clone();
        let version_id = input.version_id.clone();
        let s = Arc::clone(&store);
        let meta = tokio::task::spawn_blocking(move || {
            s.get_object_or_version(&key, version_id.as_deref())
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?
        .map_err(map_io_err)?
        .ok_or_else(|| Status::not_found("object not found"))?;

        if meta.is_delete_marker {
            return Err(Status::not_found("object is a delete marker"));
        }

        Ok(Response::new(HeadObjectResponse {
            metadata: Some(meta_to_proto(&input.key, &meta)),
        }))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let input = request.get_ref();
        let resource = format!("arn:s3:::{}/{}", input.bucket, input.key);
        let action = if input.version_id.is_some() {
            "s3:DeleteObjectVersion"
        } else {
            "s3:DeleteObject"
        };
        self.check_auth(&request, action, &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let key = input.key;
        let version_id = input.version_id;

        let result_meta = if let Some(vid) = version_id {
            // Versioned delete — permanently remove specific version
            let vid_clone = vid.clone();
            let s = Arc::clone(&store);
            let k = key.clone();
            tokio::task::spawn_blocking(move || {
                // Try deleting from current first, then from versions table
                if let Some(m) = s.delete_current_version(&k, &vid_clone)? {
                    return Ok(Some(m));
                }
                s.delete_version(&k, &vid_clone)
            })
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?
        } else {
            // Normal delete (may create delete marker when versioned)
            tokio::task::spawn_blocking(move || store.delete_object(&key))
                .await
                .map_err(|e| Status::internal(format!("task panicked: {e}")))?
                .map_err(map_io_err)?
        };

        let (resp_vid, resp_is_dm) = result_meta
            .as_ref()
            .map_or((None, None), |m| {
                (m.version_id.clone(), Some(m.is_delete_marker))
            });

        Ok(Response::new(DeleteObjectResponse {
            version_id: resp_vid,
            is_delete_marker: resp_is_dm,
        }))
    }

    async fn delete_objects(
        &self,
        request: Request<DeleteObjectsRequest>,
    ) -> Result<Response<DeleteObjectsResponse>, Status> {
        // Per-item auth: version-specific deletes require s3:DeleteObjectVersion
        let bucket = &request.get_ref().bucket;
        for item in &request.get_ref().items {
            let resource = format!("arn:s3:::{bucket}/{}", item.key);
            let action = if item.version_id.is_some() {
                "s3:DeleteObjectVersion"
            } else {
                "s3:DeleteObject"
            };
            self.check_auth(&request, action, &resource)?;
        }
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let items = input.items;
        let (deleted, errors) = tokio::task::spawn_blocking(move || {
            let mut deleted = Vec::new();
            let mut errors = Vec::new();
            for item in items {
                let result = if let Some(vid) = &item.version_id {
                    let s = Arc::clone(&store);
                    let k = item.key.clone();
                    let v = vid.clone();
                    // Try current version first, then versions table
                    (|| -> io::Result<Option<crate::types::ObjectMeta>> {
                        if let Some(m) = s.delete_current_version(&k, &v)? {
                            return Ok(Some(m));
                        }
                        s.delete_version(&k, &v)
                    })()
                } else {
                    store.delete_object(&item.key)
                };

                match result {
                    Ok(meta) => {
                        let (vid, dm) = meta
                            .as_ref()
                            .map_or((None, None), |m| {
                                (m.version_id.clone(), Some(m.is_delete_marker))
                            });
                        deleted.push(DeletedObjectResult {
                            key: item.key,
                            version_id: vid,
                            delete_marker: dm,
                        });
                    }
                    Err(e) => errors.push(DeleteError {
                        key: item.key,
                        message: e.to_string(),
                        version_id: item.version_id,
                    }),
                }
            }
            (deleted, errors)
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?;

        Ok(Response::new(DeleteObjectsResponse { deleted, errors }))
    }

    async fn copy_object(
        &self,
        request: Request<CopyObjectRequest>,
    ) -> Result<Response<CopyObjectResponse>, Status> {
        let input = request.get_ref();
        let src_resource = format!("arn:s3:::{}/{}", input.source_bucket, input.source_key);
        let dest_resource = format!("arn:s3:::{}/{}", input.dest_bucket, input.dest_key);
        self.check_auth(&request, "s3:GetObject", &src_resource)?;
        self.check_auth(&request, "s3:PutObject", &dest_resource)?;
        let input = request.into_inner();

        let src_store = self.bucket(&input.source_bucket)?;
        let dest_store = self.bucket(&input.dest_bucket)?;

        let replace_metadata = input.metadata_directive == "REPLACE";
        let req_content_type = input.content_type.filter(|s| !s.is_empty());
        let req_metadata = input.metadata;

        let tmp_id = TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp_path = dest_store.bucket_dir().join(format!(".tmp_grpc_{tmp_id:020}"));

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();

        let src_key = input.source_key;
        let src_vid = input.source_version_id;
        let ss = Arc::clone(&src_store);
        let dest_key = input.dest_key;
        let (etag, meta, src_version_id_out, size) = tokio::task::spawn_blocking(move || {
            let src_meta = ss
                .get_object_or_version(&src_key, src_vid.as_deref())?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "source object not found"))?;
            if src_meta.is_delete_marker {
                return Err(io::Error::new(io::ErrorKind::NotFound, "source is a delete marker"));
            }

            let (content_type, user_metadata) = if replace_metadata {
                (req_content_type, req_metadata)
            } else {
                (src_meta.content_type.clone(), src_meta.user_metadata.clone())
            };

            let (etag, crc) = ss.copy_to_tmp_file(&src_meta, &tmp_path)?;
            let data_len = src_meta.data_length();
            match dest_store.put_object_streamed(
                &dest_key, &tmp_path, content_type, etag.clone(), now, user_metadata, Some(crc),
            ) {
                Ok(m) => Ok((etag, m, src_meta.version_id, data_len)),
                Err(e) => {
                    std::fs::remove_file(&tmp_path).ok();
                    Err(e)
                }
            }
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?
        .map_err(map_io_err)?;

        Ok(Response::new(CopyObjectResponse {
            etag,
            content_md5: meta.content_md5.unwrap_or_default(),
            size,
            last_modified: now,
            source_version_id: src_version_id_out,
            version_id: meta.version_id,
        }))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().bucket);
        self.check_auth(&request, "s3:ListBucket", &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let prefix = if input.prefix.is_empty() {
            None
        } else {
            Some(input.prefix)
        };
        let delimiter = if input.delimiter.is_empty() {
            None
        } else {
            Some(input.delimiter)
        };
        #[allow(clippy::cast_sign_loss)]
        let max_keys = if input.max_keys <= 0 {
            1000
        } else {
            input.max_keys as usize
        };
        let continuation = if input.continuation_token.is_empty() {
            None
        } else {
            Some(input.continuation_token)
        };

        let (entries, common_prefixes, truncated) = tokio::task::spawn_blocking(move || {
            store.list_objects_with_delimiter(
                prefix.as_deref(),
                delimiter.as_deref(),
                max_keys,
                continuation.as_deref(),
            )
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?
        .map_err(map_io_err)?;

        let next_token = if truncated {
            entries.last().map(|(k, _)| k.clone()).unwrap_or_default()
        } else {
            String::new()
        };

        let objects: Vec<ObjectInfo> = entries
            .into_iter()
            .map(|(key, meta)| ObjectInfo {
                key,
                size: meta.data_length(),
                etag: meta.etag,
                last_modified: meta.last_modified,
                version_id: meta.version_id,
            })
            .collect();

        Ok(Response::new(ListObjectsResponse {
            objects,
            common_prefixes,
            is_truncated: truncated,
            next_continuation_token: next_token,
        }))
    }

    // ================================================================
    // Bulk streaming
    // ================================================================

    type BulkGetStream = GrpcStream<BulkGetResponse>;

    async fn bulk_get(
        &self,
        request: Request<BulkGetRequest>,
    ) -> Result<Response<Self::BulkGetStream>, Status> {
        let resource = format!("arn:s3:::{}/*", request.get_ref().bucket);
        self.check_auth(&request, "s3:GetObject", &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;
        let keys = input.keys;

        let (tx, rx) = mpsc::channel(8);

        tokio::spawn(async move {
            for key in keys {
                let s = Arc::clone(&store);
                let k = key.clone();
                let meta_result =
                    tokio::task::spawn_blocking(move || s.get_meta(&k)).await;

                let meta = match meta_result {
                    Ok(Ok(Some(m))) => m,
                    Ok(Ok(None)) => {
                        if tx.send(Ok(bulk_get_not_found(key))).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(map_io_err(e))).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        return;
                    }
                };

                if tx.send(Ok(bulk_get_header(&key, &meta))).await.is_err() {
                    return;
                }

                if !crate::grpc_helpers::stream_object_chunks(&store, &meta, &tx).await {
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type BulkPutStream = GrpcStream<BulkPutResponse>;

    async fn bulk_put(
        &self,
        request: Request<Streaming<BulkPutRequest>>,
    ) -> Result<Response<Self::BulkPutStream>, Status> {
        // For bulk_put we check auth at the request level with a wildcard resource
        // since we don't know buckets/keys until we read the stream
        let (metadata, _, stream) = request.into_parts();
        self.check_auth_meta(&metadata, "s3:PutObject", "arn:s3:::*")?;
        let mut stream = stream;
        let storage = Arc::clone(&self.storage);

        let (tx, rx) = mpsc::channel(8);

        tokio::spawn(async move {
            loop {
                let msg = match stream.message().await {
                    Ok(Some(m)) => m,
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        return;
                    }
                };

                let Some(bulk_put_request::Request::Init(init)) = msg.request else {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "expected init message for next object",
                        )))
                        .await;
                    return;
                };

                let store = match storage
                    .get_bucket(&init.bucket)
                    .map_err(|e| Status::internal(e.to_string()))
                    .and_then(|opt| {
                        opt.ok_or_else(|| {
                            Status::not_found(format!("bucket '{}' not found", init.bucket))
                        })
                    }) {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };

                match bulk_put_one_object(&init, &mut stream, &store).await {
                    Ok((etag, content_md5, size)) => {
                        let resp = BulkPutResponse {
                            key: init.key,
                            etag,
                            content_md5,
                            size,
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ================================================================
    // Bucket management
    // ================================================================

    async fn head_bucket(
        &self,
        request: Request<HeadBucketRequest>,
    ) -> Result<Response<HeadBucketResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().name);
        self.check_auth(&request, "s3:HeadBucket", &resource)?;
        let name = request.into_inner().name;
        let _store = self.bucket(&name)?;
        Ok(Response::new(HeadBucketResponse {}))
    }

    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().name);
        self.check_auth(&request, "s3:CreateBucket", &resource)?;
        let name = request.into_inner().name;
        let storage = Arc::clone(&self.storage);
        let existed = tokio::task::spawn_blocking(move || storage.create_bucket(&name))
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?;

        Ok(Response::new(CreateBucketResponse {
            already_existed: existed,
        }))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().name);
        self.check_auth(&request, "s3:DeleteBucket", &resource)?;
        let name = request.into_inner().name;
        let store = self.bucket(&name)?;
        let storage = Arc::clone(&self.storage);

        tokio::task::spawn_blocking(move || {
            if !store.is_empty()? {
                return Err(io::Error::other("bucket not empty"));
            }
            drop(store);
            storage.delete_bucket(&name)?;
            Ok(())
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?
        .map_err(|e: io::Error| {
            if e.to_string() == "bucket not empty" {
                Status::failed_precondition("bucket not empty")
            } else {
                map_io_err(e)
            }
        })?;

        Ok(Response::new(DeleteBucketResponse {}))
    }

    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        self.check_auth(&request, "s3:ListAllMyBuckets", "arn:s3:::*")?;
        let storage = Arc::clone(&self.storage);
        let buckets = tokio::task::spawn_blocking(move || storage.list_buckets())
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?;

        Ok(Response::new(ListBucketsResponse { buckets }))
    }

    // ================================================================
    // Versioning
    // ================================================================

    async fn get_bucket_versioning(
        &self,
        request: Request<GetBucketVersioningRequest>,
    ) -> Result<Response<GetBucketVersioningResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().bucket);
        self.check_auth(&request, "s3:GetBucketVersioning", &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let state = tokio::task::spawn_blocking(move || store.get_versioning_state())
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?;

        let status = match state {
            Some(VersioningState::Enabled) => "Enabled".to_owned(),
            Some(VersioningState::Suspended) => "Suspended".to_owned(),
            None => String::new(),
        };

        Ok(Response::new(GetBucketVersioningResponse { status }))
    }

    async fn put_bucket_versioning(
        &self,
        request: Request<PutBucketVersioningRequest>,
    ) -> Result<Response<PutBucketVersioningResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().bucket);
        self.check_auth(&request, "s3:PutBucketVersioning", &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let state = match input.status.as_str() {
            "Enabled" => VersioningState::Enabled,
            "Suspended" => VersioningState::Suspended,
            other => {
                return Err(Status::invalid_argument(format!(
                    "invalid versioning status: '{other}', expected 'Enabled' or 'Suspended'"
                )));
            }
        };

        tokio::task::spawn_blocking(move || store.set_versioning_state(state))
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?;

        Ok(Response::new(PutBucketVersioningResponse {}))
    }

    async fn list_object_versions(
        &self,
        request: Request<ListObjectVersionsRequest>,
    ) -> Result<Response<ListObjectVersionsResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().bucket);
        self.check_auth(&request, "s3:ListBucketVersions", &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let prefix = if input.prefix.is_empty() {
            None
        } else {
            Some(input.prefix)
        };
        let delimiter = if input.delimiter.is_empty() {
            None
        } else {
            Some(input.delimiter)
        };
        #[allow(clippy::cast_sign_loss)]
        let max_keys = if input.max_keys <= 0 {
            1000
        } else {
            input.max_keys as usize
        };
        let key_marker = if input.key_marker.is_empty() {
            None
        } else {
            Some(input.key_marker)
        };
        let vid_marker = if input.version_id_marker.is_empty() {
            None
        } else {
            Some(input.version_id_marker)
        };

        let result = tokio::task::spawn_blocking(move || {
            store.list_object_versions(
                prefix.as_deref(),
                delimiter.as_deref(),
                max_keys,
                key_marker.as_deref(),
                vid_marker.as_deref(),
            )
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?
        .map_err(map_io_err)?;

        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();

        for entry in result.entries {
            if entry.meta.is_delete_marker {
                delete_markers.push(DeleteMarkerProto {
                    key: entry.key,
                    version_id: entry.version_id,
                    last_modified: entry.meta.last_modified,
                    is_latest: entry.is_latest,
                });
            } else {
                let size = entry.meta.data_length();
                let last_modified = entry.meta.last_modified;
                versions.push(proto::VersionEntry {
                    key: entry.key,
                    version_id: entry.version_id,
                    etag: entry.meta.etag,
                    size,
                    last_modified,
                    is_latest: entry.is_latest,
                    content_type: entry.meta.content_type.unwrap_or_default(),
                });
            }
        }

        Ok(Response::new(ListObjectVersionsResponse {
            versions,
            delete_markers,
            common_prefixes: result.common_prefixes,
            is_truncated: result.is_truncated,
            next_key_marker: result.next_key_marker.unwrap_or_default(),
            next_version_id_marker: result.next_version_id_marker.unwrap_or_default(),
        }))
    }

    // ================================================================
    // Admin
    // ================================================================

    async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().bucket);
        self.check_auth(&request, "admin:Stats", &resource)?;
        let bucket = request.into_inner().bucket;
        let store = self.bucket(&bucket)?;

        let s = Arc::clone(&store);
        let (stats, vstats) = tokio::task::spawn_blocking(move || {
            let seg = s.segment_stats()?;
            let ver = s.version_stats()?;
            Ok::<_, io::Error>((seg, ver))
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?
        .map_err(map_io_err)?;

        let total_size: u64 = stats.iter().map(|s| s.size).sum();
        let total_dead_bytes: u64 = stats.iter().map(|s| s.dead_bytes).sum();
        let (version_count, delete_marker_count, total_versioned_size) = vstats;

        Ok(Response::new(StatsResponse {
            bucket,
            segments: segments_to_proto(stats),
            total_size,
            total_dead_bytes,
            version_count,
            delete_marker_count,
            total_versioned_size,
        }))
    }

    async fn compact(
        &self,
        request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().bucket);
        self.check_auth(&request, "admin:Compact", &resource)?;
        let bucket = request.into_inner().bucket;
        let store = self.bucket(&bucket)?;

        let s = Arc::clone(&store);
        let before = tokio::task::spawn_blocking(move || s.segment_stats())
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .unwrap_or_default();

        let s2 = Arc::clone(&store);
        tokio::task::spawn_blocking(move || s2.compact())
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?;

        let after = tokio::task::spawn_blocking(move || store.segment_stats())
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .unwrap_or_default();

        Ok(Response::new(CompactResponse {
            bucket,
            compacted: true,
            segments_before: segments_to_proto(before),
            segments_after: segments_to_proto(after),
        }))
    }

    async fn verify(
        &self,
        request: Request<VerifyRequest>,
    ) -> Result<Response<VerifyResponse>, Status> {
        let resource = format!("arn:s3:::{}", request.get_ref().bucket);
        self.check_auth(&request, "admin:Verify", &resource)?;
        let bucket = request.into_inner().bucket;
        let store = self.bucket(&bucket)?;

        let result = tokio::task::spawn_blocking(move || store.verify_integrity())
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?;

        let errors = result
            .errors
            .into_iter()
            .map(|e| proto::VerifyError {
                key: e.key,
                kind: format!("{:?}", e.kind),
                detail: e.detail,
            })
            .collect();

        Ok(Response::new(VerifyResponse {
            total_objects: result.total_objects,
            verified_ok: result.verified_ok,
            checksum_errors: result.checksum_errors,
            read_errors: result.read_errors,
            errors,
            crc_errors: result.crc_errors,
        }))
    }
}
