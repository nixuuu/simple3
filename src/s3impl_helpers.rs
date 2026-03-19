use std::io;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, UNIX_EPOCH};

use futures::TryStreamExt;
use md5::{Digest, Md5};
use s3s::dto::{
    DeleteMarkerEntry, DeletedObject, ETag, ObjectVersion, StreamingBlob, Timestamp,
};
use s3s::{s3_error, S3Result};

use crate::storage::versioning::VersionEntry;
use crate::storage::BucketStore;
use crate::types::ObjectMeta;

/// Monotonic counter for unique temp file names across concurrent requests.
pub static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Run a blocking closure on the tokio blocking thread pool.
pub async fn blocking<F, T>(f: F) -> Result<T, io::Error>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| io::Error::other(format!("task panicked: {e}")))?
}

/// Stream request body to a temp file, returning `(md5_hex, crc32c)`.
/// When `max_size > 0`, aborts with `EntityTooLarge` if the body exceeds the limit.
pub async fn stream_body_to_tmp(
    body: StreamingBlob,
    tmp_path: &Path,
    max_size: u64,
) -> S3Result<(String, u32)> {
    let file = tokio::fs::File::create(tmp_path).await
        .map_err(|e| { tracing::error!("create tmp file: {e}"); s3_error!(e, InternalError) })?;
    let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);
    let mut hasher = Md5::new();
    let mut crc: u32 = 0;
    let mut total_bytes = 0u64;
    let mut stream = body;

    while let Some(chunk) = stream.try_next().await.map_err(|e| { tracing::error!("read request body: {e}"); s3_error!(InternalError) })? {
        total_bytes += chunk.len() as u64;
        if max_size > 0 && total_bytes > max_size {
            drop(writer);
            tokio::fs::remove_file(tmp_path).await.ok();
            return Err(s3_error!(EntityTooLarge));
        }
        tokio::io::AsyncWriteExt::write_all(&mut writer, &chunk).await
            .map_err(|e| { tracing::error!("write tmp file: {e}"); s3_error!(e, InternalError) })?;
        hasher.update(&chunk);
        crc = crc32c::crc32c_append(crc, &chunk);
    }
    tokio::io::AsyncWriteExt::flush(&mut writer).await
        .map_err(|e| { tracing::error!("flush tmp file: {e}"); s3_error!(e, InternalError) })?;

    metrics::counter!("simple3_bytes_received_total").increment(total_bytes);
    Ok((format!("{:x}", hasher.finalize()), crc))
}

pub fn version_id_string(vid: Option<&str>) -> Option<String> {
    vid.map(str::to_owned)
}

/// Delete a single object with a specific version ID.
pub fn delete_one_versioned(
    store: &BucketStore,
    key: String,
    vid: String,
) -> Result<DeletedObject, s3s::dto::Error> {
    match store.delete_current_version(&key, &vid) {
        Ok(Some(meta)) => Ok(DeletedObject {
            key: Some(key),
            version_id: Some(vid),
            delete_marker: Some(meta.is_delete_marker),
            ..Default::default()
        }),
        Ok(None) => match store.delete_version(&key, &vid) {
            Ok(Some(meta)) => Ok(DeletedObject {
                key: Some(key),
                version_id: Some(vid),
                delete_marker: if meta.is_delete_marker { Some(true) } else { None },
                ..Default::default()
            }),
            Ok(None) => Ok(DeletedObject {
                key: Some(key),
                version_id: Some(vid),
                ..Default::default()
            }),
            Err(e) => Err(s3s::dto::Error {
                code: Some("InternalError".to_owned()),
                key: Some(key),
                message: Some(e.to_string()),
                version_id: Some(vid),
            }),
        },
        Err(e) => Err(s3s::dto::Error {
            code: Some("InternalError".to_owned()),
            key: Some(key),
            message: Some(e.to_string()),
            version_id: Some(vid),
        }),
    }
}

/// Delete a single object without a version ID (hard delete or create delete marker).
pub fn delete_one_unversioned(
    store: &BucketStore,
    key: String,
) -> Result<DeletedObject, s3s::dto::Error> {
    match store.delete_object(&key) {
        Ok(Some(meta)) => Ok(DeletedObject {
            key: Some(key),
            version_id: version_id_string(meta.version_id.as_deref()),
            delete_marker: if meta.is_delete_marker { Some(true) } else { None },
            ..Default::default()
        }),
        Ok(None) => Ok(DeletedObject {
            key: Some(key),
            ..Default::default()
        }),
        Err(e) => Err(s3s::dto::Error {
            code: Some("InternalError".to_owned()),
            key: Some(key),
            message: Some(e.to_string()),
            version_id: None,
        }),
    }
}

/// Build version/delete-marker lists from storage version entries.
pub fn build_version_lists(
    entries: Vec<VersionEntry>,
) -> (Vec<ObjectVersion>, Vec<DeleteMarkerEntry>) {
    let mut versions = Vec::new();
    let mut delete_markers = Vec::new();

    for entry in entries {
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

    (versions, delete_markers)
}

/// Resolve an object by key, optionally looking up a specific version.
/// Uses a single read transaction to avoid TOCTOU between tables.
/// Returns the meta or an `io::Error` with appropriate `ErrorKind`.
pub fn resolve_object_version(
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

/// Execute copy-object blocking operation: resolve source, copy to tmp, put in destination.
#[allow(clippy::too_many_arguments)] // all fields required for atomic copy-put; matches existing pattern in grpc_helpers::CopyParams
pub fn execute_copy_blocking(
    src_store: &BucketStore,
    dest_store: &BucketStore,
    src_key: &str,
    src_version_id: Option<&str>,
    dest_key: &str,
    tmp_path: &std::path::Path,
    replace_metadata: bool,
    req_content_type: Option<String>,
    req_metadata: std::collections::HashMap<String, String>,
    now: u64,
    max_obj_size: u64,
) -> io::Result<(String, ObjectMeta, Option<String>, u64)> {
    let src_meta = resolve_object_version(src_store, src_key, src_version_id)?;

    if max_obj_size > 0 && src_meta.data_length() > max_obj_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "EntityTooLarge: source object exceeds max_object_size",
        ));
    }

    let (content_type, user_metadata) = if replace_metadata {
        (req_content_type, req_metadata)
    } else {
        (src_meta.content_type.clone(), src_meta.user_metadata.clone())
    };

    let (etag_hex, crc) = src_store.copy_to_tmp_file(&src_meta, tmp_path)?;
    let data_len = src_meta.data_length();
    let src_vid = src_meta.version_id;
    match dest_store.put_object_streamed(
        dest_key, tmp_path, content_type, etag_hex.clone(), now, user_metadata, Some(crc),
    ) {
        Ok(m) => Ok((etag_hex, m, src_vid, data_len)),
        Err(e) => {
            std::fs::remove_file(tmp_path).ok();
            Err(e)
        }
    }
}
