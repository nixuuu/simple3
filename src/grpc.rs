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
        }))
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let resource = format!("arn:s3:::{}/{}", request.get_ref().bucket, request.get_ref().key);
        self.check_auth(&request, "s3:GetObject", &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let key = input.key.clone();
        let s = Arc::clone(&store);
        let meta = tokio::task::spawn_blocking(move || s.get_meta(&key))
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?
            .ok_or_else(|| Status::not_found("object not found"))?;

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
        let resource = format!("arn:s3:::{}/{}", request.get_ref().bucket, request.get_ref().key);
        self.check_auth(&request, "s3:GetObject", &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let key = input.key.clone();
        let meta = tokio::task::spawn_blocking(move || store.get_meta(&key))
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?
            .ok_or_else(|| Status::not_found("object not found"))?;

        Ok(Response::new(HeadObjectResponse {
            metadata: Some(meta_to_proto(&input.key, &meta)),
        }))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let resource = format!("arn:s3:::{}/{}", request.get_ref().bucket, request.get_ref().key);
        self.check_auth(&request, "s3:DeleteObject", &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let key = input.key;
        tokio::task::spawn_blocking(move || store.delete_object(&key))
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?;

        Ok(Response::new(DeleteObjectResponse {}))
    }

    async fn delete_objects(
        &self,
        request: Request<DeleteObjectsRequest>,
    ) -> Result<Response<DeleteObjectsResponse>, Status> {
        let resource = format!("arn:s3:::{}/*", request.get_ref().bucket);
        self.check_auth(&request, "s3:DeleteObject", &resource)?;
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let keys = input.keys;
        let (deleted, errors) = tokio::task::spawn_blocking(move || {
            let mut deleted = Vec::new();
            let mut errors = Vec::new();
            for key in keys {
                match store.delete_object(&key) {
                    Ok(_) => deleted.push(key),
                    Err(e) => errors.push(DeleteError {
                        key,
                        message: e.to_string(),
                    }),
                }
            }
            (deleted, errors)
        })
        .await
        .map_err(|e| Status::internal(format!("task panicked: {e}")))?;

        Ok(Response::new(DeleteObjectsResponse { deleted, errors }))
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

        let stats = tokio::task::spawn_blocking(move || store.segment_stats())
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?;

        let total_size: u64 = stats.iter().map(|s| s.size).sum();
        let total_dead_bytes: u64 = stats.iter().map(|s| s.dead_bytes).sum();

        Ok(Response::new(StatsResponse {
            bucket,
            segments: segments_to_proto(stats),
            total_size,
            total_dead_bytes,
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
