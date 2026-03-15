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
    pub crc_errors: u64,
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
    CrcMismatch,
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
            crc_errors: 0,
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

        for e in &result.errors {
            match e.kind {
                VerifyErrorKind::ChecksumMismatch => result.checksum_errors += 1,
                VerifyErrorKind::CrcMismatch => result.crc_errors += 1,
                VerifyErrorKind::ReadError => result.read_errors += 1,
            }
        }

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

        let data_len = meta.data_length();
        let mut md5_hasher = Md5::new();
        let mut crc: u32 = 0;
        let has_crc = meta.content_crc32c.is_some();
        let mut buf = vec![0u8; COPY_BUF_SIZE];
        let mut remaining = data_len;
        let mut offset = meta.offset;

        while remaining > 0 {
            let chunk = (remaining as usize).min(buf.len());
            file.read_exact_at(&mut buf[..chunk], offset).map_err(|e| VerifyError {
                key: key.to_owned(),
                kind: VerifyErrorKind::ReadError,
                detail: format!("segment {}: read error at offset {offset}: {e}", meta.segment_id),
            })?;
            md5_hasher.update(&buf[..chunk]);
            if has_crc {
                crc = crc32c::crc32c_append(crc, &buf[..chunk]);
            }
            offset += chunk as u64;
            remaining -= chunk as u64;
        }
        // Verify CRC32C: computed from data + on-disk trailer
        if let Some(expected_crc) = meta.content_crc32c {
            if crc != expected_crc {
                drop(file);
                return Err(VerifyError {
                    key: key.to_owned(),
                    kind: VerifyErrorKind::CrcMismatch,
                    detail: format!("expected {expected_crc:#010x}, computed {crc:#010x}"),
                });
            }
            // Read the 4-byte on-disk trailer and compare
            let mut trailer = [0u8; 4];
            file.read_exact_at(&mut trailer, meta.offset + data_len).map_err(|e| VerifyError {
                key: key.to_owned(),
                kind: VerifyErrorKind::ReadError,
                detail: format!("segment {}: CRC trailer read error: {e}", meta.segment_id),
            })?;
            let stored_crc = u32::from_le_bytes(trailer);
            if stored_crc != expected_crc {
                drop(file);
                return Err(VerifyError {
                    key: key.to_owned(),
                    kind: VerifyErrorKind::CrcMismatch,
                    detail: format!(
                        "trailer mismatch: expected {expected_crc:#010x}, stored {stored_crc:#010x}"
                    ),
                });
            }
        }
        drop(file);

        // Verify MD5
        let computed = format!("{:x}", md5_hasher.finalize());

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

        Ok(())
    }
}
