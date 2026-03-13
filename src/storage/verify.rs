use std::io;
use std::os::unix::fs::FileExt;

use md5::{Digest, Md5};
use redb::{ReadableDatabase, ReadableTable};
use serde::Serialize;

use crate::types::ObjectMeta;

use super::{BucketStore, COPY_BUF_SIZE, OBJECTS};

#[derive(Debug, Serialize)]
pub struct VerifyResult {
    pub total_objects: u64,
    pub verified_ok: u64,
    pub checksum_errors: u64,
    pub read_errors: u64,
    pub errors: Vec<VerifyError>,
}

#[derive(Debug, Serialize)]
pub struct VerifyError {
    pub key: String,
    pub kind: VerifyErrorKind,
    pub detail: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyErrorKind {
    ChecksumMismatch,
    ReadError,
}

#[allow(clippy::missing_errors_doc)]
impl BucketStore {
    pub fn verify_integrity(&self) -> io::Result<VerifyResult> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;

        let mut result = VerifyResult {
            total_objects: 0,
            verified_ok: 0,
            checksum_errors: 0,
            read_errors: 0,
            errors: Vec::new(),
        };

        let iter = table.iter().map_err(io::Error::other)?;
        for entry in iter {
            let (k, v): (redb::AccessGuard<'_, &str>, redb::AccessGuard<'_, &[u8]>) =
                entry.map_err(io::Error::other)?;
            let key = k.value().to_owned();
            let meta = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
            result.total_objects += 1;

            match self.verify_object(&key, &meta) {
                Ok(()) => result.verified_ok += 1,
                Err(e) => result.errors.push(e),
            }
        }

        result.checksum_errors = result
            .errors
            .iter()
            .filter(|e| matches!(e.kind, VerifyErrorKind::ChecksumMismatch))
            .count() as u64;
        result.read_errors = result
            .errors
            .iter()
            .filter(|e| matches!(e.kind, VerifyErrorKind::ReadError))
            .count() as u64;

        Ok(result)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn verify_object(&self, key: &str, meta: &ObjectMeta) -> Result<(), VerifyError> {
        let seg_arc = self.get_segment_handle(meta.segment_id).map_err(|e| {
            VerifyError {
                key: key.to_owned(),
                kind: VerifyErrorKind::ReadError,
                detail: format!("segment {}: {e}", meta.segment_id),
            }
        })?;

        let file = seg_arc.read().map_err(|_| VerifyError {
            key: key.to_owned(),
            kind: VerifyErrorKind::ReadError,
            detail: format!("segment {}: lock poisoned", meta.segment_id),
        })?;

        let file_size = file.metadata().map_err(|e| VerifyError {
            key: key.to_owned(),
            kind: VerifyErrorKind::ReadError,
            detail: format!("segment {}: {e}", meta.segment_id),
        })?.len();

        if meta.offset + meta.length > file_size {
            return Err(VerifyError {
                key: key.to_owned(),
                kind: VerifyErrorKind::ReadError,
                detail: format!(
                    "segment {}: offset {}+length {} exceeds file size {file_size}",
                    meta.segment_id, meta.offset, meta.length
                ),
            });
        }

        let mut hasher = Md5::new();
        let mut buf = vec![0u8; COPY_BUF_SIZE];
        let mut remaining = meta.length;
        let mut offset = meta.offset;

        while remaining > 0 {
            let chunk = (remaining as usize).min(buf.len());
            file.read_exact_at(&mut buf[..chunk], offset).map_err(|e| VerifyError {
                key: key.to_owned(),
                kind: VerifyErrorKind::ReadError,
                detail: format!("segment {}: read error at offset {offset}: {e}", meta.segment_id),
            })?;
            hasher.update(&buf[..chunk]);
            offset += chunk as u64;
            remaining -= chunk as u64;
        }
        drop(file);

        let computed = format!("{:x}", hasher.finalize());

        // Determine expected MD5: prefer content_md5 (always whole-object MD5),
        // fall back to etag for single-part uploads (where etag IS the MD5).
        let expected = meta
            .content_md5
            .as_deref()
            .or_else(|| {
                // Single-part ETags (no "-") are plain MD5 hex
                if meta.etag.contains('-') { None } else { Some(meta.etag.as_str()) }
            });

        if let Some(expected) = expected
            && computed != expected
        {
            return Err(VerifyError {
                key: key.to_owned(),
                kind: VerifyErrorKind::ChecksumMismatch,
                detail: format!("expected {expected}, computed {computed}"),
            });
        }
        // Legacy multipart objects without content_md5: data was read successfully → pass

        Ok(())
    }
}
