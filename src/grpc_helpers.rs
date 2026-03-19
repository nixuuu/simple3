use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::grpc::proto::{
    self, BulkGetObjectStart, BulkGetResponse, BulkPutRequest, DeleteError,
    DeleteMarkerProto, DeleteObjectIdentifier, DeletedObjectResult, GetObjectResponse,
    ListObjectVersionsResponse, ListObjectsResponse, ObjectInfo, ObjectMetadata, PutObjectInit,
    PutObjectRequest, StatsResponse, VerifyResponse,
};
use crate::storage::versioning::ListVersionsResult;
use crate::storage::{BucketStore, SegmentStat, VerifyResult};
use crate::types::ObjectMeta;

/// Monotonic counter for unique temp file names.
pub static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Chunk size for streaming downloads (256 KB).
pub const STREAM_CHUNK_SIZE: u64 = 256 * 1024;

#[allow(clippy::needless_pass_by_value)]
pub fn map_io_err(e: io::Error) -> Status {
    match e.kind() {
        io::ErrorKind::NotFound => Status::not_found(e.to_string()),
        io::ErrorKind::AlreadyExists => Status::already_exists(e.to_string()),
        io::ErrorKind::PermissionDenied => Status::permission_denied(e.to_string()),
        io::ErrorKind::InvalidData => Status::invalid_argument(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

pub fn meta_to_proto(key: &str, meta: &ObjectMeta) -> ObjectMetadata {
    ObjectMetadata {
        key: key.to_owned(),
        etag: meta.etag.clone(),
        content_type: meta.content_type.clone().unwrap_or_default(),
        size: meta.data_length(),
        last_modified: meta.last_modified,
        user_metadata: meta.user_metadata.clone(),
        content_md5: meta.content_md5.clone().unwrap_or_default(),
        version_id: meta.version_id.clone(),
        is_delete_marker: if meta.is_delete_marker { Some(true) } else { None },
    }
}

pub fn segments_to_proto(stats: Vec<SegmentStat>) -> Vec<proto::SegmentStat> {
    stats
        .into_iter()
        .map(|s| proto::SegmentStat {
            id: s.id,
            size: s.size,
            dead_bytes: s.dead_bytes,
        })
        .collect()
}

/// Write incoming stream chunks to a temp file, computing MD5 and CRC32C.
/// When `max_size > 0`, rejects uploads exceeding the limit.
pub async fn stream_to_tmp(
    stream: &mut Streaming<PutObjectRequest>,
    tmp_path: &std::path::Path,
    max_size: u64,
) -> Result<(String, u64, u32), Status> {
    let mut file =
        std::fs::File::create(tmp_path).map_err(|e| Status::internal(format!("create tmp: {e}")))?;
    let mut hasher = Md5::new();
    let mut crc: u32 = 0;
    let mut size = 0u64;

    while let Some(msg) = stream.message().await? {
        let Some(proto::put_object_request::Request::Data(chunk)) = msg.request else {
            return Err(Status::invalid_argument(
                "expected data chunk after init message",
            ));
        };
        #[allow(clippy::cast_possible_truncation)]
        {
            size += chunk.len() as u64;
        }
        if max_size > 0 && size > max_size {
            std::fs::remove_file(tmp_path).ok();
            return Err(Status::invalid_argument(format!(
                "EntityTooLarge: object size {size} exceeds limit {max_size}"
            )));
        }
        file.write_all(&chunk)
            .map_err(|e| Status::internal(format!("write tmp: {e}")))?;
        hasher.update(&chunk);
        crc = crc32c::crc32c_append(crc, &chunk);
    }
    file.flush()
        .map_err(|e| Status::internal(format!("flush tmp: {e}")))?;

    metrics::counter!("simple3_bytes_received_total").increment(size);

    let etag = format!("{:x}", hasher.finalize());
    Ok((etag, size, crc))
}

/// Stream object data in chunks via an mpsc channel.
#[allow(clippy::needless_pass_by_value)]
pub fn spawn_download_stream(
    store: Arc<BucketStore>,
    meta: ObjectMeta,
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
                    metrics::counter!("simple3_bytes_sent_total").increment(bytes.len() as u64);
                    let resp = GetObjectResponse {
                        response: Some(proto::get_object_response::Response::Data(bytes)),
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
pub async fn stream_object_chunks(
    store: &Arc<BucketStore>,
    meta: &ObjectMeta,
    tx: &mpsc::Sender<Result<BulkGetResponse, Status>>,
) -> bool {
    let data_len = meta.data_length();
    let mut pos = 0u64;
    while pos < data_len {
        let chunk_size = (data_len - pos).min(STREAM_CHUNK_SIZE);
        let s = Arc::clone(store);
        let seg_id = meta.segment_id;
        let offset = meta.offset + pos;
        let data =
            tokio::task::spawn_blocking(move || s.read_data(seg_id, offset, chunk_size)).await;

        match data {
            Ok(Ok(bytes)) => {
                metrics::counter!("simple3_bytes_sent_total").increment(bytes.len() as u64);
                let resp = BulkGetResponse {
                    response: Some(proto::bulk_get_response::Response::Data(bytes)),
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
pub async fn bulk_put_one_object(
    init: &PutObjectInit,
    stream: &mut Streaming<BulkPutRequest>,
    store: &Arc<BucketStore>,
    max_size: u64,
) -> Result<(String, String, u64), Status> {
    let tmp_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = store
        .bucket_dir()
        .join(format!(".tmp_grpc_{tmp_id:020}"));

    let mut file = std::fs::File::create(&tmp_path)
        .map_err(|e| Status::internal(format!("create tmp: {e}")))?;
    let mut hasher = Md5::new();
    let mut crc: u32 = 0;
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
            Some(proto::bulk_put_request::Request::Data(chunk)) => {
                #[allow(clippy::cast_possible_truncation)]
                {
                    size += chunk.len() as u64;
                }
                if max_size > 0 && size > max_size {
                    std::fs::remove_file(&tmp_path).ok();
                    return Err(Status::invalid_argument(format!(
                        "EntityTooLarge: object size {size} exceeds limit {max_size}"
                    )));
                }
                if let Err(e) = file.write_all(&chunk) {
                    std::fs::remove_file(&tmp_path).ok();
                    return Err(Status::internal(e.to_string()));
                }
                hasher.update(&chunk);
                crc = crc32c::crc32c_append(crc, &chunk);
            }
            Some(proto::bulk_put_request::Request::Init(_)) => break,
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

    metrics::counter!("simple3_bytes_received_total").increment(size);

    let meta = tokio::task::spawn_blocking(move || {
        s.put_object_streamed(&key, &tmp_path, content_type, etag_clone, now, metadata, Some(crc))
    })
    .await
    .map_err(|e| Status::internal(format!("task panicked: {e}")))?
    .map_err(map_io_err)?;

    let content_md5 = meta.content_md5.unwrap_or_default();
    Ok((etag, content_md5, size))
}

/// Build a `BulkGetObjectStart` header for a not-found key.
pub const fn bulk_get_not_found(key: String) -> BulkGetResponse {
    BulkGetResponse {
        response: Some(proto::bulk_get_response::Response::ObjectStart(
            BulkGetObjectStart {
                key,
                metadata: None,
                not_found: true,
            },
        )),
    }
}

/// Build a `BulkGetObjectStart` header with metadata.
pub fn bulk_get_header(key: &str, meta: &ObjectMeta) -> BulkGetResponse {
    BulkGetResponse {
        response: Some(proto::bulk_get_response::Response::ObjectStart(
            BulkGetObjectStart {
                key: key.to_owned(),
                metadata: Some(meta_to_proto(key, meta)),
                not_found: false,
            },
        )),
    }
}

// ================================================================
// Helpers extracted from grpc.rs
// ================================================================

/// Convert an empty string to `None`.
pub(crate) fn non_empty(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

/// Clamp a signed `max_keys` value from the client to a positive `usize`
/// bounded by the server-configured limit.
#[allow(clippy::cast_sign_loss)]
pub(crate) fn clamp_max_keys(raw: i32, server_limit: usize) -> usize {
    (if raw <= 0 { 1000 } else { raw as usize }).min(server_limit)
}

/// Build a `ListObjectsResponse` from storage results.
pub(crate) fn build_list_objects_response(
    entries: Vec<(String, ObjectMeta)>,
    common_prefixes: Vec<String>,
    truncated: bool,
) -> ListObjectsResponse {
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

    ListObjectsResponse {
        objects,
        common_prefixes,
        is_truncated: truncated,
        next_continuation_token: next_token,
    }
}

/// Build a `ListObjectVersionsResponse` from storage results.
pub(crate) fn build_list_versions_response(
    result: ListVersionsResult,
) -> ListObjectVersionsResponse {
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

    ListObjectVersionsResponse {
        versions,
        delete_markers,
        common_prefixes: result.common_prefixes,
        is_truncated: result.is_truncated,
        next_key_marker: result.next_key_marker.unwrap_or_default(),
        next_version_id_marker: result.next_version_id_marker.unwrap_or_default(),
    }
}

/// Execute batch delete on a list of items, returning deleted and error lists.
pub(crate) fn execute_batch_delete(
    store: &Arc<BucketStore>,
    items: Vec<DeleteObjectIdentifier>,
) -> (Vec<DeletedObjectResult>, Vec<DeleteError>) {
    let mut deleted = Vec::new();
    let mut errors = Vec::new();
    for item in items {
        let result = if let Some(vid) = &item.version_id {
            let s = Arc::clone(store);
            let k = item.key.clone();
            let v: String = vid.clone();
            (|| -> io::Result<Option<ObjectMeta>> {
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
                let (vid, dm) = meta.as_ref().map_or((None, None), |m| {
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
}

/// Build a `VerifyResponse` from a storage `VerifyResult`.
pub(crate) fn build_verify_response(result: VerifyResult) -> VerifyResponse {
    let errors = result
        .errors
        .into_iter()
        .map(|e| proto::VerifyError {
            key: e.key,
            kind: format!("{:?}", e.kind),
            detail: e.detail,
        })
        .collect();

    VerifyResponse {
        total_objects: result.total_objects,
        verified_ok: result.verified_ok,
        checksum_errors: result.checksum_errors,
        read_errors: result.read_errors,
        errors,
        crc_errors: result.crc_errors,
    }
}

/// Parameters for `execute_copy_blocking`.
pub(crate) struct CopyParams {
    pub src_key: String,
    pub src_version_id: Option<String>,
    pub dest_key: String,
    pub tmp_path: std::path::PathBuf,
    pub replace_metadata: bool,
    pub req_content_type: Option<String>,
    pub req_metadata: std::collections::HashMap<String, String>,
    pub max_obj_size: u64,
    pub now: u64,
}

/// Execute the blocking copy-object logic: read source, write to tmp, put in dest.
///
/// Returns `(etag, dest_meta, source_version_id, data_length)`.
pub(crate) fn execute_copy_blocking(
    src_store: &Arc<BucketStore>,
    dest_store: &Arc<BucketStore>,
    p: CopyParams,
) -> io::Result<(String, ObjectMeta, Option<String>, u64)> {
    let src_meta = src_store
        .get_object_or_version(&p.src_key, p.src_version_id.as_deref())?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "source object not found"))?;
    if src_meta.is_delete_marker {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "source is a delete marker",
        ));
    }
    if p.max_obj_size > 0 && src_meta.data_length() > p.max_obj_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "EntityTooLarge: source object exceeds max_object_size",
        ));
    }

    let (content_type, user_metadata) = if p.replace_metadata {
        (p.req_content_type, p.req_metadata)
    } else {
        (src_meta.content_type.clone(), src_meta.user_metadata.clone())
    };

    let (etag, crc) = src_store.copy_to_tmp_file(&src_meta, &p.tmp_path)?;
    let data_len = src_meta.data_length();
    match dest_store.put_object_streamed(
        &p.dest_key, &p.tmp_path, content_type, etag.clone(), p.now, user_metadata, Some(crc),
    ) {
        Ok(m) => Ok((etag, m, src_meta.version_id, data_len)),
        Err(e) => {
            std::fs::remove_file(&p.tmp_path).ok();
            Err(e)
        }
    }
}

/// Gather segment and version stats and build a `StatsResponse`.
pub(crate) fn build_stats_response(
    bucket: String,
    store: &BucketStore,
) -> io::Result<StatsResponse> {
    let stats = store.segment_stats()?;
    let (version_count, delete_marker_count, total_versioned_size) = store.version_stats()?;
    let total_size: u64 = stats.iter().map(|s| s.size).sum();
    let total_dead_bytes: u64 = stats.iter().map(|s| s.dead_bytes).sum();

    Ok(StatsResponse {
        bucket,
        segments: segments_to_proto(stats),
        total_size,
        total_dead_bytes,
        version_count,
        delete_marker_count,
        total_versioned_size,
    })
}
