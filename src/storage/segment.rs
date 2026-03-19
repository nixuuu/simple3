use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, RwLock};

use redb::{ReadableDatabase, ReadableTable};

use crate::types::ObjectMeta;

use super::versioning;
use super::{BucketStore, OBJECTS, SEG_COMPACTING, SEG_DEAD};

pub(super) const COPY_BUF_SIZE: usize = 8 * 1024 * 1024; // 8 MB

/// Copy with 8 MB buffer — reduces syscalls for large files.
pub(super) fn copy_large(reader: &mut impl Read, writer: &mut impl Write) -> io::Result<u64> {
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut total = 0u64;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            return Ok(total);
        }
        writer.write_all(&buf[..n])?;
        #[allow(clippy::cast_possible_truncation)]
        {
            total += n as u64;
        }
    }
}

pub(super) fn segment_filename(id: u32) -> String {
    format!("seg_{id:06}.bin")
}

pub struct SegmentStat {
    pub id: u32,
    pub size: u64,
    pub dead_bytes: u64,
}

// === Per-segment locking ===
//
// Locking strategy:
//   writer: Mutex<ActiveWriter>     — serializes appends + rotation to the active segment
//   segments: RwLock<HashMap<u32, Arc<RwLock<File>>>>
//     outer RwLock  — structural changes (add/remove segments during rotation/compaction)
//     inner RwLock  — per-segment: read lock for concurrent pread, write lock for compaction swap
//
// Lock ordering: writer → segments(outer) → segments(inner per-seg)
//
// Reads from different segments never contend. Reads from the same segment are concurrent (shared lock).
// Writes only block other writes (via Mutex), never block reads.
// Compaction write-locks only the segment being compacted; all other segments remain readable.

pub struct ActiveWriter {
    pub(super) id: u32,
    pub(super) file: File,
    pub(super) size: u64,
}

pub(super) fn discover_segments(bucket_dir: &Path) -> io::Result<Vec<u32>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(bucket_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(rest) = name.strip_prefix("seg_")
            && let Some(num_str) = rest.strip_suffix(".bin")
                && let Ok(id) = num_str.parse::<u32>() {
                    ids.push(id);
                }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Remove leftover temp files from interrupted operations.
pub(super) fn cleanup_temp_files(bucket_dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(bucket_dir)? {
        let p = entry?.path();
        if let Some(name) = p.file_name().and_then(|n| n.to_str())
            && (name.starts_with(".tmp_")
                || name.starts_with(".mpu_")
                || name.ends_with(".bin.tmp")
                || name == "data.bin.tmp")
        {
            fs::remove_file(&p).ok();
        }
    }
    Ok(())
}

// === BucketStore segment I/O ===

#[allow(clippy::missing_errors_doc)] // internal segment I/O — error conditions are self-evident from io::Result
impl BucketStore {
    // === Segment management ===

    pub(super) fn rotate_segment(&self, w: &mut ActiveWriter) -> io::Result<()> {
        w.file.sync_all()?;
        let new_id = w.id + 1;
        let path = self.bucket_dir.join(segment_filename(new_id));
        let new_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        // Add read handle for the new segment
        let reader = new_file.try_clone()?;
        let mut map = self
            .segments
            .write()
            .map_err(|_| io::Error::other("segments lock poisoned"))?;
        map.insert(new_id, Arc::new(RwLock::new(reader)));
        drop(map);

        w.file = new_file;
        w.size = 0;
        w.id = new_id;
        Ok(())
    }

    pub(super) fn remove_segment(&self, segment_id: u32) -> io::Result<()> {
        {
            let mut map = self
                .segments
                .write()
                .map_err(|_| io::Error::other("segments lock poisoned"))?;
            map.remove(&segment_id);
        }

        let seg_path = self.bucket_dir.join(segment_filename(segment_id));
        if let Err(e) = fs::remove_file(&seg_path)
            && e.kind() != io::ErrorKind::NotFound
        {
            return Err(e);
        }

        let txn = self.db.begin_write().map_err(io::Error::other)?;
        {
            let mut t = txn.open_table(SEG_DEAD).map_err(io::Error::other)?;
            t.remove(segment_id).map_err(io::Error::other)?;
        }
        {
            let mut t = txn.open_table(SEG_COMPACTING).map_err(io::Error::other)?;
            t.remove(segment_id).map_err(io::Error::other)?;
        }
        txn.commit().map_err(io::Error::other)?;
        Ok(())
    }

    pub(super) fn get_segment_handle(&self, segment_id: u32) -> io::Result<Arc<RwLock<File>>> {
        let seg = Arc::clone(
            self.segments
                .read()
                .map_err(|_| io::Error::other("segments lock poisoned"))?
                .get(&segment_id)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("segment {segment_id} not found"),
                    )
                })?,
        );
        Ok(seg)
    }

    // === Recovery ===

    pub(super) fn truncate_orphans(&self) -> io::Result<()> {
        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;
        let active_id = w.id;

        let mut max_end: u64 = 0;
        {
            let txn = self.db.begin_read().map_err(io::Error::other)?;

            // Scan objects table
            let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            for result in table.iter().map_err(io::Error::other)? {
                let (_k, v) = result.map_err(io::Error::other)?;
                let obj = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                if obj.segment_id == active_id {
                    max_end = max_end.max(obj.offset + obj.length);
                }
            }

            // Scan versions table
            let ver_table = txn
                .open_table(versioning::VERSIONS)
                .map_err(io::Error::other)?;
            for result in ver_table.iter().map_err(io::Error::other)? {
                let (_k, v) = result.map_err(io::Error::other)?;
                let obj = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                if obj.segment_id == active_id && !obj.is_delete_marker {
                    max_end = max_end.max(obj.offset + obj.length);
                }
            }
        }

        let file_size = w.file.seek(SeekFrom::End(0))?;
        if file_size > max_end {
            w.file.set_len(max_end)?;
            w.size = max_end;
        }
        drop(w);
        Ok(())
    }

    // === Per-segment dead bytes tracking ===

    pub(super) fn seg_dead_bytes(&self, segment_id: u32) -> u64 {
        let Ok(txn) = self.db.begin_read() else {
            return 0;
        };
        let Ok(table) = txn.open_table(SEG_DEAD) else {
            return 0;
        };
        table
            .get(segment_id)
            .ok()
            .flatten()
            .map_or(0, |g| g.value())
    }

    pub(super) fn get_seg_compacting(&self, segment_id: u32) -> bool {
        let Ok(txn) = self.db.begin_read() else {
            return false;
        };
        let Ok(table) = txn.open_table(SEG_COMPACTING) else {
            return false;
        };
        table
            .get(segment_id)
            .ok()
            .flatten()
            .is_some_and(|g| g.value() == 1)
    }

    pub(super) fn set_seg_compacting(&self, segment_id: u32, val: bool) -> io::Result<()> {
        let txn = self.db.begin_write().map_err(io::Error::other)?;
        {
            let mut t = txn.open_table(SEG_COMPACTING).map_err(io::Error::other)?;
            t.insert(segment_id, u8::from(val)).map_err(io::Error::other)?;
        }
        txn.commit().map_err(io::Error::other)?;
        Ok(())
    }

    /// Total dead bytes across all segments.
    pub fn dead_bytes(&self) -> u64 {
        let Ok(txn) = self.db.begin_read() else {
            return 0;
        };
        let Ok(table) = txn.open_table(SEG_DEAD) else {
            return 0;
        };
        let Ok(iter) = table.iter() else { return 0 };
        iter.filter_map(Result::ok).map(|(_, v)| v.value()).sum()
    }

    /// Total size across all segments.
    pub fn data_file_size(&self) -> io::Result<u64> {
        let map = self
            .segments
            .read()
            .map_err(|_| io::Error::other("segments lock poisoned"))?;
        map.values().try_fold(0u64, |total, seg| {
            let file = seg
                .read()
                .map_err(|_| io::Error::other("segment lock poisoned"))?;
            Ok(total + file.metadata()?.len())
        })
    }

    /// Per-segment stats for autovacuum.
    pub fn segment_stats(&self) -> io::Result<Vec<SegmentStat>> {
        let map = self
            .segments
            .read()
            .map_err(|_| io::Error::other("segments lock poisoned"))?;
        let mut stats = Vec::with_capacity(map.len());
        for (&id, seg) in &*map {
            let file = seg
                .read()
                .map_err(|_| io::Error::other("segment lock poisoned"))?;
            let size = file.metadata()?.len();
            drop(file);
            stats.push(SegmentStat {
                id,
                size,
                dead_bytes: self.seg_dead_bytes(id),
            });
        }
        drop(map);
        stats.sort_by_key(|s| s.id);
        Ok(stats)
    }

    /// Fsync the active writer segment to disk.
    pub fn sync_active_segment(&self) -> io::Result<()> {
        let w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;
        w.file.sync_all()
    }

    // === Data operations ===

    pub fn read_data(&self, segment_id: u32, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        let len = usize::try_from(length)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut buf = vec![0u8; len];

        let seg_arc = self.get_segment_handle(segment_id)?;
        let file = seg_arc
            .read()
            .map_err(|_| io::Error::other("segment lock poisoned"))?;
        file.read_exact_at(&mut buf, offset)?;
        drop(file);

        Ok(buf)
    }

    pub fn append_data(&self, data: &[u8]) -> io::Result<(u32, u64, u64, u32)> {
        let crc = crc32c::crc32c(data);
        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;
        #[allow(clippy::cast_possible_truncation)] // usize → u64 never truncates on 64-bit
        let data_len = data.len() as u64;
        let total_len = data_len + 4;

        if w.size + total_len > self.max_segment_size && w.size > 0 {
            self.rotate_segment(&mut w)?;
        }

        let segment_id = w.id;
        let offset = w.file.seek(SeekFrom::End(0))?;
        w.file.write_all(data)?;
        w.file.write_all(&crc.to_le_bytes())?;
        w.size = offset + total_len;
        drop(w);
        Ok((segment_id, offset, total_len, crc))
    }

    /// Read object data, validating the CRC32C checksum if present.
    /// Returns only the data portion (without the 4-byte CRC trailer).
    #[allow(clippy::cast_possible_truncation)] // segment sizes bounded well within usize on 64-bit
    pub fn read_object(&self, meta: &ObjectMeta) -> io::Result<Vec<u8>> {
        if let Some(expected_crc) = meta.content_crc32c {
            if meta.length < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "object length is smaller than the CRC trailer",
                ));
            }
            let full = self.read_data(meta.segment_id, meta.offset, meta.length)?;
            let data_len = meta.data_length() as usize;
            let computed = crc32c::crc32c(&full[..data_len]);
            let stored_crc = u32::from_le_bytes([
                full[data_len],
                full[data_len + 1],
                full[data_len + 2],
                full[data_len + 3],
            ]);
            if computed != expected_crc || stored_crc != expected_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "CRC32C mismatch: expected {expected_crc:#010x}, \
                         stored {stored_crc:#010x}, computed {computed:#010x}"
                    ),
                ));
            }
            let mut data = full;
            data.truncate(data_len);
            Ok(data)
        } else {
            self.read_data(meta.segment_id, meta.offset, meta.length)
        }
    }

    /// Copy object data from a source segment to a temp file in 256 KB chunks,
    /// computing MD5 and CRC32C incrementally. Returns `(md5_hex, crc32c)`.
    /// Cleans up the temp file on any error.
    pub fn copy_to_tmp_file(&self, meta: &ObjectMeta, tmp_path: &Path) -> io::Result<(String, u32)> {
        use md5::{Digest, Md5};

        let mut hasher = Md5::new();
        let mut crc: u32 = 0;
        let data_len = meta.data_length();
        let result: io::Result<()> = (|| {
            let file = File::create(tmp_path)?;
            let mut writer = io::BufWriter::with_capacity(1024 * 1024, file);
            let mut pos = 0u64;
            while pos < data_len {
                let chunk_size = (data_len - pos).min(256 * 1024);
                let chunk = self.read_data(meta.segment_id, meta.offset + pos, chunk_size)?;
                writer.write_all(&chunk)?;
                hasher.update(&chunk);
                crc = crc32c::crc32c_append(crc, &chunk);
                pos += chunk_size;
            }
            writer.flush()?;
            Ok(())
        })();
        if let Err(e) = result {
            fs::remove_file(tmp_path).ok();
            return Err(e);
        }
        Ok((format!("{:x}", hasher.finalize()), crc))
    }

    // === Streamed PUT ===

    /// Write tmp file data into the active segment — rename if empty, copy otherwise.
    /// If `content_crc32c` is provided, appends a 4-byte LE CRC after the data.
    pub(super) fn write_to_segment(
        &self,
        w: &mut ActiveWriter,
        tmp_path: &Path,
        mut tmp: File,
        tmp_size: u64,
        content_crc32c: Option<u32>,
    ) -> io::Result<(u64, u64)> {
        let crc_len = if content_crc32c.is_some() { 4u64 } else { 0 };
        let seg_path = self.bucket_dir.join(segment_filename(w.id));
        if w.size == 0 {
            // Empty segment — rename tmp file to become the segment (O(1) vs O(n) copy)
            drop(tmp);
            fs::rename(tmp_path, &seg_path)?;
            w.file = OpenOptions::new().read(true).write(true).open(&seg_path)?;
            if let Some(crc) = content_crc32c {
                w.file.seek(SeekFrom::End(0))?;
                w.file.write_all(&crc.to_le_bytes())?;
            }
            w.file.sync_all()?;
            let total = tmp_size + crc_len;
            w.size = total;
            let reader = w.file.try_clone()?;
            self.segments
                .write()
                .map_err(|_| io::Error::other("segments lock poisoned"))?
                .insert(w.id, Arc::new(RwLock::new(reader)));
            Ok((0, total))
        } else {
            let offset = w.file.seek(SeekFrom::End(0))?;
            let data_length = copy_large(&mut tmp, &mut w.file)?;
            if let Some(crc) = content_crc32c {
                w.file.write_all(&crc.to_le_bytes())?;
            }
            w.file.sync_all()?;
            let total = data_length + crc_len;
            w.size = offset + total;
            Ok((offset, total))
        }
    }
}
