use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};

use crate::types::ObjectMeta;

use super::{BucketStore, COPY_BUF_SIZE};

#[allow(clippy::missing_errors_doc)]
impl BucketStore {
    pub fn create_multipart_upload(&self) -> String {
        static MPU_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = MPU_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{nanos:032x}_{id:08x}")
    }

    fn part_path(&self, upload_id: &str, part_num: i32) -> PathBuf {
        self.bucket_dir
            .join(format!(".mpu_{upload_id}_{part_num:05}"))
    }

    /// Store a part from a temp file that was streamed to disk.
    /// `md5_hex` is the pre-computed MD5 hex string (from streaming).
    /// The part file stores [data][16-byte raw MD5] for assembly.
    pub fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        tmp_path: &Path,
        md5_hex: &str,
    ) -> io::Result<String> {
        let part_path = self.part_path(upload_id, part_number);
        fs::rename(tmp_path, &part_path)?;
        let mut f = OpenOptions::new().append(true).open(&part_path)?;
        let digest = md5_hex_to_bytes(md5_hex)?;
        f.write_all(&digest)?;
        Ok(md5_hex.to_owned())
    }

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::too_many_arguments)] // logically-distinct storage params; bundling into a struct adds indirection with no reuse benefit
    pub fn complete_multipart_upload(
        &self,
        upload_id: &str,
        key: &str,
        parts: &[(i32, String)],
        content_type: Option<String>,
        last_modified: u64,
        user_metadata: HashMap<String, String>,
        max_object_size: u64,
    ) -> io::Result<(ObjectMeta, String)> {
        let mut sorted_parts: Vec<_> = parts.to_vec();
        sorted_parts.sort_by_key(|(num, _)| *num);

        // Pre-calculate total size for rotation check
        let total_expected: u64 = sorted_parts
            .iter()
            .map(|(part_num, _)| -> io::Result<u64> {
                Ok(fs::metadata(self.part_path(upload_id, *part_num))?
                    .len()
                    .saturating_sub(16))
            })
            .sum::<io::Result<u64>>()?;

        if max_object_size > 0 && total_expected > max_object_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "EntityTooLarge: multipart upload total size {total_expected} exceeds limit {max_object_size}"
                ),
            ));
        }

        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;
        if w.size + total_expected + 4 > self.max_segment_size && w.size > 0 {
            self.rotate_segment(&mut w)?;
        }

        let segment_id = w.id;
        let offset = w.file.seek(SeekFrom::End(0))?;

        let write_result = (|| -> io::Result<(u64, String, String, u32)> {
            let (total_data_len, etag, content_md5, crc) =
                self.assemble_parts(&mut w.file, &sorted_parts, upload_id)?;
            w.file.write_all(&crc.to_le_bytes())?;
            w.file.sync_all()?;
            Ok((total_data_len, etag, content_md5, crc))
        })();
        let (total_data_len, etag, content_md5, crc) = match write_result {
            Ok(v) => v,
            Err(e) => {
                w.file.set_len(offset).ok();
                w.size = offset;
                return Err(e);
            }
        };
        let total_len = total_data_len + 4;
        w.size = offset + total_len;

        let mut meta = ObjectMeta {
            segment_id,
            offset,
            length: total_len,
            content_type,
            etag: etag.clone(),
            last_modified,
            user_metadata,
            content_md5: Some(content_md5),
            content_crc32c: Some(crc),
            version_id: None,
            is_delete_marker: false,
        };

        if let Err(e) = self.commit_put(key, &mut meta) {
            w.file.set_len(offset).ok();
            w.size = offset;
            return Err(e);
        }
        drop(w);

        for (part_num, _) in &sorted_parts {
            fs::remove_file(self.part_path(upload_id, *part_num)).ok();
        }

        Ok((meta, etag))
    }

    /// Read part files, write them sequentially, and compute the combined multipart `ETag`,
    /// whole-object `content_md5`, and CRC32C for integrity verification.
    #[allow(clippy::cast_possible_truncation)]
    fn assemble_parts(
        &self,
        writer: &mut File,
        sorted_parts: &[(i32, String)],
        upload_id: &str,
    ) -> io::Result<(u64, String, String, u32)> {
        let mut total_len: u64 = 0;
        let mut part_md5_concat = Vec::new();
        let mut content_hasher = Md5::new();
        let mut crc: u32 = 0;
        let mut buf = vec![0u8; COPY_BUF_SIZE];

        for (part_num, _etag) in sorted_parts {
            let pp = self.part_path(upload_id, *part_num);
            let mut part_file = File::open(&pp).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("part {part_num} not found: {e}"),
                )
            })?;

            let file_len = part_file.metadata()?.len();
            let data_len = file_len.saturating_sub(16);
            let mut remaining = data_len;

            while remaining > 0 {
                let to_read = (remaining as usize).min(buf.len());
                let n = part_file.read(&mut buf[..to_read])?;
                if n == 0 {
                    break;
                }
                writer.write_all(&buf[..n])?;
                content_hasher.update(&buf[..n]);
                crc = crc32c::crc32c_append(crc, &buf[..n]);
                remaining -= n as u64;
                total_len += n as u64;
            }

            let mut md5_buf = [0u8; 16];
            part_file.read_exact(&mut md5_buf)?;
            part_md5_concat.extend_from_slice(&md5_buf);
        }

        let mut final_hasher = Md5::new();
        final_hasher.update(&part_md5_concat);
        let etag = format!("{:x}-{}", final_hasher.finalize(), sorted_parts.len());
        let content_md5 = format!("{:x}", content_hasher.finalize());

        Ok((total_len, etag, content_md5, crc))
    }

    pub fn abort_multipart_upload(&self, upload_id: &str) -> io::Result<()> {
        let prefix = format!(".mpu_{upload_id}_");
        for entry in fs::read_dir(&self.bucket_dir)? {
            let p = entry?.path();
            if p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&prefix))
            {
                fs::remove_file(&p).ok();
            }
        }
        Ok(())
    }
}

/// Parse a 32-char hex MD5 string into 16 raw bytes.
fn md5_hex_to_bytes(hex: &str) -> io::Result<[u8; 16]> {
    let mut out = [0u8; 16];
    if hex.len() != 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected 32-char MD5 hex, got {}", hex.len()),
        ));
    }
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid MD5 hex: {e}"))
        })?;
    }
    Ok(out)
}
