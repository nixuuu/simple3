use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::grpc::proto::{
    self, BulkGetObjectStart, BulkGetResponse, BulkPutRequest, GetObjectResponse,
    ObjectMetadata, PutObjectInit, PutObjectRequest,
};
use crate::storage::{BucketStore, SegmentStat};
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
pub async fn stream_to_tmp(
    stream: &mut Streaming<PutObjectRequest>,
    tmp_path: &std::path::Path,
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
        file.write_all(&chunk)
            .map_err(|e| Status::internal(format!("write tmp: {e}")))?;
        hasher.update(&chunk);
        crc = crc32c::crc32c_append(crc, &chunk);
        #[allow(clippy::cast_possible_truncation)]
        {
            size += chunk.len() as u64;
        }
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
                if let Err(e) = file.write_all(&chunk) {
                    std::fs::remove_file(&tmp_path).ok();
                    return Err(Status::internal(e.to_string()));
                }
                hasher.update(&chunk);
                crc = crc32c::crc32c_append(crc, &chunk);
                #[allow(clippy::cast_possible_truncation)]
                {
                    size += chunk.len() as u64;
                }
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
