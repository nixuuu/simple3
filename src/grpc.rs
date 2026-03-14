use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

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

/// Monotonic counter for unique temp file names.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Chunk size for streaming downloads (256 KB).
const STREAM_CHUNK_SIZE: u64 = 256 * 1024;

pub struct GrpcService {
    storage: Arc<Storage>,
}

impl GrpcService {
    pub const fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    fn bucket(&self, name: &str) -> Result<Arc<BucketStore>, Status> {
        self.storage
            .get_bucket(name)
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("bucket '{name}' not found")))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_io_err(e: io::Error) -> Status {
    match e.kind() {
        io::ErrorKind::NotFound => Status::not_found(e.to_string()),
        io::ErrorKind::AlreadyExists => Status::already_exists(e.to_string()),
        io::ErrorKind::PermissionDenied => Status::permission_denied(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

fn meta_to_proto(key: &str, meta: &crate::types::ObjectMeta) -> ObjectMetadata {
    ObjectMetadata {
        key: key.to_owned(),
        etag: meta.etag.clone(),
        content_type: meta.content_type.clone().unwrap_or_default(),
        size: meta.length,
        last_modified: meta.last_modified,
        user_metadata: meta.user_metadata.clone(),
        content_md5: meta.content_md5.clone().unwrap_or_default(),
    }
}

fn segments_to_proto(stats: Vec<crate::storage::SegmentStat>) -> Vec<proto::SegmentStat> {
    stats
        .into_iter()
        .map(|s| proto::SegmentStat {
            id: s.id,
            size: s.size,
            dead_bytes: s.dead_bytes,
        })
        .collect()
}

/// Write incoming stream chunks to a temp file, computing MD5.
async fn stream_to_tmp(
    stream: &mut Streaming<PutObjectRequest>,
    tmp_path: &std::path::Path,
) -> Result<(String, u64), Status> {
    let path = tmp_path.to_owned();
    let mut file =
        std::fs::File::create(&path).map_err(|e| Status::internal(format!("create tmp: {e}")))?;
    let mut hasher = Md5::new();
    let mut size = 0u64;

    while let Some(msg) = stream.message().await? {
        let Some(proto::put_object_request::Request::Data(chunk)) = msg.request else {
            return Err(Status::invalid_argument(
                "expected data chunk after init message",
            ));
        };
        file.write_all(&chunk)
            .map_err(|e| Status::internal(format!("write tmp: {e}")))?;
        hasher.update(&chunk);
        #[allow(clippy::cast_possible_truncation)]
        {
            size += chunk.len() as u64;
        }
    }
    file.flush()
        .map_err(|e| Status::internal(format!("flush tmp: {e}")))?;

    let etag = format!("{:x}", hasher.finalize());
    Ok((etag, size))
}

/// Stream object data in chunks via an mpsc channel.
#[allow(clippy::needless_pass_by_value)]
fn spawn_download_stream(
    store: Arc<BucketStore>,
    meta: crate::types::ObjectMeta,
    range_offset: u64,
    range_len: u64,
    tx: mpsc::Sender<Result<GetObjectResponse, Status>>,
) {
    tokio::spawn(async move {
        let mut pos = 0u64;
        while pos < range_len {
            let chunk_size = (range_len - pos).min(STREAM_CHUNK_SIZE);
            let s = Arc::clone(&store);
            let seg_id = meta.segment_id;
            let offset = range_offset + pos;
            let data = tokio::task::spawn_blocking(move || s.read_data(seg_id, offset, chunk_size))
                .await
                .unwrap_or_else(|e| Err(io::Error::other(format!("task panicked: {e}"))));

            match data {
                Ok(bytes) => {
                    let resp = GetObjectResponse {
                        response: Some(get_object_response::Response::Data(bytes)),
                    };
                    if tx.send(Ok(resp)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(map_io_err(e))).await;
                    break;
                }
            }
            pos += chunk_size;
        }
    });
}

/// Stream chunks for a single object during bulk get.
async fn stream_object_chunks(
    store: &Arc<BucketStore>,
    meta: &crate::types::ObjectMeta,
    tx: &mpsc::Sender<Result<BulkGetResponse, Status>>,
) -> bool {
    let mut pos = 0u64;
    while pos < meta.length {
        let chunk_size = (meta.length - pos).min(STREAM_CHUNK_SIZE);
        let s = Arc::clone(store);
        let seg_id = meta.segment_id;
        let offset = meta.offset + pos;
        let data =
            tokio::task::spawn_blocking(move || s.read_data(seg_id, offset, chunk_size)).await;

        match data {
            Ok(Ok(bytes)) => {
                let resp = BulkGetResponse {
                    response: Some(bulk_get_response::Response::Data(bytes)),
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return false;
                }
            }
            Ok(Err(e)) => {
                let _ = tx.send(Err(map_io_err(e))).await;
                return false;
            }
            Err(e) => {
                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                return false;
            }
        }
        pos += chunk_size;
    }
    true
}

/// Process a single object in the bulk put stream.
async fn bulk_put_one_object(
    init: &PutObjectInit,
    stream: &mut Streaming<BulkPutRequest>,
    store: &Arc<BucketStore>,
) -> Result<(String, String, u64), Status> {
    let tmp_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = store
        .bucket_dir()
        .join(format!(".tmp_grpc_{tmp_id:020}"));

    let mut file = std::fs::File::create(&tmp_path)
        .map_err(|e| Status::internal(format!("create tmp: {e}")))?;
    let mut hasher = Md5::new();
    let mut size = 0u64;

    loop {
        let msg = match stream.message().await {
            Ok(Some(m)) => m,
            Ok(None) => break,
            Err(e) => {
                std::fs::remove_file(&tmp_path).ok();
                return Err(Status::internal(e.to_string()));
            }
        };

        match msg.request {
            Some(bulk_put_request::Request::Data(chunk)) => {
                if let Err(e) = file.write_all(&chunk) {
                    std::fs::remove_file(&tmp_path).ok();
                    return Err(Status::internal(e.to_string()));
                }
                hasher.update(&chunk);
                #[allow(clippy::cast_possible_truncation)]
                {
                    size += chunk.len() as u64;
                }
            }
            Some(bulk_put_request::Request::Init(_)) => break,
            None => {}
        }
    }

    file.flush().map_err(|e| {
        std::fs::remove_file(&tmp_path).ok();
        Status::internal(format!("flush tmp: {e}"))
    })?;

    let etag = format!("{:x}", hasher.finalize());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();

    let key = init.key.clone();
    let content_type = if init.content_type.is_empty() {
        None
    } else {
        Some(init.content_type.clone())
    };
    let etag_clone = etag.clone();
    let metadata = init.user_metadata.clone();
    let s = Arc::clone(store);

    let meta = tokio::task::spawn_blocking(move || {
        s.put_object_streamed(&key, &tmp_path, content_type, etag_clone, now, metadata)
    })
    .await
    .map_err(|e| Status::internal(format!("task panicked: {e}")))?
    .map_err(map_io_err)?;

    let content_md5 = meta.content_md5.unwrap_or_default();
    Ok((etag, content_md5, size))
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
        let mut stream = request.into_inner();

        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty stream"))?;
        let Some(proto::put_object_request::Request::Init(init)) = first.request else {
            return Err(Status::invalid_argument("first message must be init"));
        };

        let store = self.bucket(&init.bucket)?;
        let tmp_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = store.bucket_dir().join(format!(".tmp_grpc_{tmp_id:020}"));

        let (etag, size) = match stream_to_tmp(&mut stream, &tmp_path).await {
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
            store.put_object_streamed(&key, &tmp_path, content_type, etag_clone, now, metadata)
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
        let input = request.into_inner();
        let store = self.bucket(&input.bucket)?;

        let key = input.key.clone();
        let s = Arc::clone(&store);
        let meta = tokio::task::spawn_blocking(move || s.get_meta(&key))
            .await
            .map_err(|e| Status::internal(format!("task panicked: {e}")))?
            .map_err(map_io_err)?
            .ok_or_else(|| Status::not_found("object not found"))?;

        let (range_offset, range_len) = if let Some(start) = input.range_start {
            let end = input.range_end.unwrap_or(meta.length);
            if start >= meta.length || end > meta.length || start >= end {
                return Err(Status::out_of_range("invalid range"));
            }
            (meta.offset + start, end - start)
        } else {
            (meta.offset, meta.length)
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
                size: meta.length,
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
                        let header = BulkGetResponse {
                            response: Some(bulk_get_response::Response::ObjectStart(
                                BulkGetObjectStart {
                                    key,
                                    metadata: None,
                                    not_found: true,
                                },
                            )),
                        };
                        if tx.send(Ok(header)).await.is_err() {
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

                let header = BulkGetResponse {
                    response: Some(bulk_get_response::Response::ObjectStart(
                        BulkGetObjectStart {
                            key: key.clone(),
                            metadata: Some(meta_to_proto(&key, &meta)),
                            not_found: false,
                        },
                    )),
                };
                if tx.send(Ok(header)).await.is_err() {
                    return;
                }

                if !stream_object_chunks(&store, &meta, &tx).await {
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
        let mut stream = request.into_inner();
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
        _request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
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
        }))
    }
}
