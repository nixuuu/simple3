use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

use crate::types::ObjectMeta;

/// Result of listing objects: (objects, `common_prefixes`, truncated).
pub type ListResult = (Vec<(String, ObjectMeta)>, Vec<String>, bool);

const STORAGE_VERSION_KEY: &[u8] = b"__storage_version__";
const COPY_BUF_SIZE: usize = 8 * 1024 * 1024; // 8 MB
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 4 * 1024 * 1024 * 1024; // 4 GB

/// Copy with 8 MB buffer — reduces syscalls for large files.
fn copy_large(reader: &mut impl Read, writer: &mut impl Write) -> io::Result<u64> {
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

fn segment_filename(id: u32) -> String {
    format!("seg_{id:06}.bin")
}

fn seg_dead_key(id: u32) -> Vec<u8> {
    format!("__seg_dead_{id:06}__").into_bytes()
}

fn seg_compacting_key(id: u32) -> Vec<u8> {
    format!("__compacting_{id:06}__").into_bytes()
}

/// V1 format (without `segment_id`) for migration.
#[derive(Serialize, Deserialize)]
struct ObjectMetaV1 {
    offset: u64,
    length: u64,
    content_type: Option<String>,
    etag: String,
    last_modified: u64,
    user_metadata: HashMap<String, String>,
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

struct ActiveWriter {
    id: u32,
    file: File,
    size: u64,
}

fn discover_segments(bucket_dir: &Path) -> io::Result<Vec<u32>> {
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

/// Migrate v1 (single data.bin) to v2 (segments). Idempotent.
fn migrate_v1_to_v2(
    bucket_dir: &Path,
    objects: &sled::Tree,
    meta: &sled::Tree,
) -> io::Result<()> {
    let data_bin = bucket_dir.join("data.bin");
    if data_bin.exists() {
        fs::rename(&data_bin, bucket_dir.join(segment_filename(0)))?;
    }

    // Re-serialize all objects with segment_id = 0.
    // Uses serialize-back check to skip entries already in v2 format.
    let mut batch = sled::Batch::default();
    let mut has_updates = false;

    for result in objects {
        let (k, v) = result.map_err(io::Error::other)?;
        if let Ok(v1) = bincode::deserialize::<ObjectMetaV1>(&v) {
            let v1_bytes = bincode::serialize(&v1).map_err(io::Error::other)?;
            if v1_bytes == v.as_ref() {
                let v2 = ObjectMeta {
                    segment_id: 0,
                    offset: v1.offset,
                    length: v1.length,
                    content_type: v1.content_type,
                    etag: v1.etag,
                    last_modified: v1.last_modified,
                    user_metadata: v1.user_metadata,
                };
                batch.insert(k, bincode::serialize(&v2).map_err(io::Error::other)?);
                has_updates = true;
            }
        }
    }
    if has_updates {
        objects.apply_batch(batch).map_err(io::Error::other)?;
    }

    // Migrate global dead_bytes → segment 0
    if let Some(v) = meta.get(b"__dead_bytes__").map_err(io::Error::other)? {
        meta.insert(seg_dead_key(0), v.as_ref())
            .map_err(io::Error::other)?;
        meta.remove(b"__dead_bytes__").map_err(io::Error::other)?;
    }

    // Migrate old compaction flag → segment 0
    if meta
        .get(b"__compacting__")
        .map_err(io::Error::other)?
        .is_some_and(|v| !v.is_empty() && v[0] == 1)
    {
        meta.insert(seg_compacting_key(0), &[1u8])
            .map_err(io::Error::other)?;
    }
    meta.remove(b"__compacting__").map_err(io::Error::other)?;

    meta.insert(STORAGE_VERSION_KEY, b"2")
        .map_err(io::Error::other)?;
    Ok(())
}

// === BucketStore ===

pub struct BucketStore {
    #[allow(dead_code)]
    db: sled::Db,
    objects: sled::Tree,
    meta: sled::Tree,
    /// Serializes appends and rotation on the active segment.
    writer: Mutex<ActiveWriter>,
    /// Per-segment read handles.
    /// Outer `RwLock`: structural mutations (add/remove segments).
    /// Inner `RwLock<File>`: read lock for concurrent `pread`, write lock for compaction swap.
    segments: RwLock<HashMap<u32, Arc<RwLock<File>>>>,
    bucket_dir: PathBuf,
    max_segment_size: u64,
}

#[allow(clippy::missing_errors_doc)]
impl BucketStore {
    fn open(bucket_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(bucket_dir)?;

        let db = sled::open(bucket_dir.join("index.db")).map_err(io::Error::other)?;
        let objects = db.open_tree("objects").map_err(io::Error::other)?;
        let meta = db.open_tree("meta").map_err(io::Error::other)?;

        // GC: remove leftover temp files
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

        // Migrate from v1 (single data.bin) to v2 (segments) if needed
        if meta.get(STORAGE_VERSION_KEY).map_err(io::Error::other)?.is_none() {
            migrate_v1_to_v2(bucket_dir, &objects, &meta)?;
        }

        // Discover existing segments
        let segment_ids = discover_segments(bucket_dir)?;
        let active_id = segment_ids.last().copied().unwrap_or(0);

        let active_path = bucket_dir.join(segment_filename(active_id));
        let active_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&active_path)?;
        let active_size = active_file.metadata()?.len();

        // Build per-segment read handles
        let mut seg_map: HashMap<u32, Arc<RwLock<File>>> = HashMap::new();
        seg_map.insert(active_id, Arc::new(RwLock::new(active_file.try_clone()?)));

        for &id in &segment_ids {
            if id != active_id {
                let path = bucket_dir.join(segment_filename(id));
                let file = File::open(&path)?;
                seg_map.insert(id, Arc::new(RwLock::new(file)));
            }
        }

        let store = Self {
            db,
            objects,
            meta,
            writer: Mutex::new(ActiveWriter {
                id: active_id,
                file: active_file,
                size: active_size,
            }),
            segments: RwLock::new(seg_map),
            bucket_dir: bucket_dir.to_path_buf(),
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
        };

        // Recovery: check per-segment compaction flags
        for &id in &segment_ids {
            if store.get_seg_compacting(id) {
                store.recover_segment_compaction(id)?;
            }
        }

        store.truncate_orphans()?;
        Ok(store)
    }

    // === Segment management ===

    fn rotate_segment(&self, w: &mut ActiveWriter) -> io::Result<()> {
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

    fn remove_segment(&self, segment_id: u32) -> io::Result<()> {
        {
            let mut map = self
                .segments
                .write()
                .map_err(|_| io::Error::other("segments lock poisoned"))?;
            map.remove(&segment_id);
        }

        let seg_path = self.bucket_dir.join(segment_filename(segment_id));
        if seg_path.exists() {
            fs::remove_file(&seg_path)?;
        }
        self.meta
            .remove(seg_dead_key(segment_id))
            .map_err(io::Error::other)?;
        self.meta
            .remove(seg_compacting_key(segment_id))
            .map_err(io::Error::other)?;
        Ok(())
    }

    // === Recovery ===

    fn truncate_orphans(&self) -> io::Result<()> {
        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;
        let active_id = w.id;

        let mut max_end: u64 = 0;
        for result in &self.objects {
            let (_k, v) = result.map_err(io::Error::other)?;
            let obj: ObjectMeta = bincode::deserialize(&v).map_err(io::Error::other)?;
            if obj.segment_id == active_id {
                let end = obj.offset + obj.length;
                if end > max_end {
                    max_end = end;
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

    fn collect_live_objects_for_segment(
        &self,
        segment_id: u32,
    ) -> io::Result<Vec<(Vec<u8>, ObjectMeta)>> {
        let mut live = Vec::new();
        for result in &self.objects {
            let (k, v) = result.map_err(io::Error::other)?;
            let obj: ObjectMeta = bincode::deserialize(&v).map_err(io::Error::other)?;
            if obj.segment_id == segment_id {
                live.push((k.to_vec(), obj));
            }
        }
        live.sort_by_key(|(_, m)| m.offset);
        Ok(live)
    }

    fn recover_segment_compaction(&self, segment_id: u32) -> io::Result<()> {
        let live = self.collect_live_objects_for_segment(segment_id)?;
        let compacted_size: u64 = live.iter().map(|(_, obj)| obj.length).sum();
        let seg_path = self.bucket_dir.join(segment_filename(segment_id));
        let current_size = fs::metadata(&seg_path).map(|m| m.len()).unwrap_or(0);
        let original_end = live
            .iter()
            .map(|(_, obj)| obj.offset + obj.length)
            .max()
            .unwrap_or(0);

        // If file matches compacted size (rename happened, sled has old offsets), fix offsets
        if current_size == compacted_size && current_size != original_end && !live.is_empty() {
            let mut batch = sled::Batch::default();
            let mut new_offset: u64 = 0;
            for (key, mut obj) in live {
                obj.offset = new_offset;
                new_offset += obj.length;
                batch.insert(key, bincode::serialize(&obj).map_err(io::Error::other)?);
            }
            self.objects
                .apply_batch(batch)
                .map_err(io::Error::other)?;
        }

        self.set_seg_dead_bytes(segment_id, 0)?;
        self.set_seg_compacting(segment_id, false)?;
        Ok(())
    }

    // === Per-segment dead bytes tracking ===

    fn seg_dead_bytes(&self, segment_id: u32) -> u64 {
        self.meta
            .get(seg_dead_key(segment_id))
            .ok()
            .flatten()
            .and_then(|v| v.as_ref().try_into().ok())
            .map_or(0, u64::from_le_bytes)
    }

    fn set_seg_dead_bytes(&self, segment_id: u32, n: u64) -> io::Result<()> {
        self.meta
            .insert(seg_dead_key(segment_id), &n.to_le_bytes())
            .map_err(io::Error::other)?;
        Ok(())
    }

    fn add_seg_dead_bytes(&self, segment_id: u32, n: u64) -> io::Result<()> {
        let current = self.seg_dead_bytes(segment_id);
        self.set_seg_dead_bytes(segment_id, current + n)
    }

    fn get_seg_compacting(&self, segment_id: u32) -> bool {
        self.meta
            .get(seg_compacting_key(segment_id))
            .ok()
            .flatten()
            .is_some_and(|v| !v.is_empty() && v[0] == 1)
    }

    fn set_seg_compacting(&self, segment_id: u32, val: bool) -> io::Result<()> {
        self.meta
            .insert(seg_compacting_key(segment_id), &[u8::from(val)])
            .map_err(io::Error::other)?;
        Ok(())
    }

    /// Total dead bytes across all segments.
    pub fn dead_bytes(&self) -> u64 {
        self.meta
            .scan_prefix(b"__seg_dead_")
            .filter_map(Result::ok)
            .filter_map(|(_k, v)| v.as_ref().try_into().ok().map(u64::from_le_bytes))
            .sum()
    }

    /// Total size across all segments.
    pub fn data_file_size(&self) -> io::Result<u64> {
        let map = self
            .segments
            .read()
            .map_err(|_| io::Error::other("segments lock poisoned"))?;
        let mut total = 0u64;
        for seg in map.values() {
            let file = seg
                .read()
                .map_err(|_| io::Error::other("segment lock poisoned"))?;
            total += file.metadata()?.len();
        }
        drop(map);
        Ok(total)
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

    // === Data operations ===

    pub fn read_data(&self, segment_id: u32, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        let len = usize::try_from(length)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut buf = vec![0u8; len];

        let seg_arc = {
            let map = self
                .segments
                .read()
                .map_err(|_| io::Error::other("segments lock poisoned"))?;
            Arc::clone(map.get(&segment_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment {segment_id} not found"),
                )
            })?)
        };
        let file = seg_arc
            .read()
            .map_err(|_| io::Error::other("segment lock poisoned"))?;
        file.read_exact_at(&mut buf, offset)?;
        drop(file);

        Ok(buf)
    }

    pub fn append_data(&self, data: &[u8]) -> io::Result<(u32, u64, u64)> {
        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;
        #[allow(clippy::cast_possible_truncation)]
        let data_len = data.len() as u64;

        if w.size + data_len > self.max_segment_size && w.size > 0 {
            self.rotate_segment(&mut w)?;
        }

        let segment_id = w.id;
        let offset = w.file.seek(SeekFrom::End(0))?;
        w.file.write_all(data)?;
        w.size = offset + data_len;
        drop(w);
        Ok((segment_id, offset, data_len))
    }

    pub fn put_meta(&self, key: &str, meta: &ObjectMeta) -> io::Result<()> {
        let v = bincode::serialize(meta).map_err(io::Error::other)?;
        self.objects
            .insert(key.as_bytes(), v)
            .map_err(io::Error::other)?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        match self
            .objects
            .get(key.as_bytes())
            .map_err(io::Error::other)?
        {
            Some(v) => {
                let meta: ObjectMeta =
                    bincode::deserialize(&v).map_err(io::Error::other)?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    pub fn bucket_dir(&self) -> &Path {
        &self.bucket_dir
    }

    // === Streamed PUT ===

    pub fn put_object_streamed(
        &self,
        key: &str,
        tmp_path: &Path,
        content_type: Option<String>,
        etag: String,
        last_modified: u64,
        user_metadata: HashMap<String, String>,
    ) -> io::Result<ObjectMeta> {
        let old_meta = self.get_meta(key)?;

        let mut tmp = File::open(tmp_path)?;
        let tmp_size = tmp.metadata()?.len();
        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;

        if w.size + tmp_size > self.max_segment_size && w.size > 0 {
            self.rotate_segment(&mut w)?;
        }

        let segment_id = w.id;
        let offset = w.file.seek(SeekFrom::End(0))?;
        let length = copy_large(&mut tmp, &mut w.file)?;
        w.file.sync_all()?;
        w.size = offset + length;
        drop(tmp);

        let meta = ObjectMeta {
            segment_id,
            offset,
            length,
            content_type,
            etag,
            last_modified,
            user_metadata,
        };

        if let Err(e) = self.put_meta(key, &meta) {
            w.file.set_len(offset).ok();
            w.size = offset;
            fs::remove_file(tmp_path).ok();
            return Err(e);
        }

        drop(w);
        fs::remove_file(tmp_path).ok();

        if let Some(old) = old_meta {
            self.add_seg_dead_bytes(old.segment_id, old.length)?;
        }

        Ok(meta)
    }

    // === Multipart upload ===

    pub fn create_multipart_upload(&self) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{nanos:032x}")
    }

    fn part_path(&self, upload_id: &str, part_num: i32) -> PathBuf {
        self.bucket_dir
            .join(format!(".mpu_{upload_id}_{part_num:05}"))
    }

    pub fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        data: &[u8],
    ) -> io::Result<String> {
        let digest = Md5::digest(data);
        let mut f = File::create(self.part_path(upload_id, part_number))?;
        f.write_all(data)?;
        f.write_all(&digest)?;
        Ok(format!("{digest:x}"))
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn complete_multipart_upload(
        &self,
        upload_id: &str,
        key: &str,
        parts: &[(i32, String)],
        content_type: Option<String>,
        last_modified: u64,
        user_metadata: HashMap<String, String>,
    ) -> io::Result<(ObjectMeta, String)> {
        let old_meta = self.get_meta(key)?;

        let mut sorted_parts: Vec<_> = parts.to_vec();
        sorted_parts.sort_by_key(|(num, _)| *num);

        // Pre-calculate total size for rotation check
        let mut total_expected: u64 = 0;
        for (part_num, _) in &sorted_parts {
            let pp = self.part_path(upload_id, *part_num);
            total_expected += fs::metadata(&pp)?.len().saturating_sub(16);
        }

        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;
        if w.size + total_expected > self.max_segment_size && w.size > 0 {
            self.rotate_segment(&mut w)?;
        }

        let segment_id = w.id;
        let offset = w.file.seek(SeekFrom::End(0))?;
        let mut total_len: u64 = 0;
        let mut part_md5_concat = Vec::new();
        let mut buf = vec![0u8; COPY_BUF_SIZE];

        for (part_num, _etag) in &sorted_parts {
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
                w.file.write_all(&buf[..n])?;
                remaining -= n as u64;
                total_len += n as u64;
            }

            let mut md5_buf = [0u8; 16];
            part_file.read_exact(&mut md5_buf)?;
            part_md5_concat.extend_from_slice(&md5_buf);
        }
        w.file.sync_all()?;
        w.size = offset + total_len;

        let mut final_hasher = Md5::new();
        final_hasher.update(&part_md5_concat);
        let etag = format!("{:x}-{}", final_hasher.finalize(), sorted_parts.len());

        let meta = ObjectMeta {
            segment_id,
            offset,
            length: total_len,
            content_type,
            etag: etag.clone(),
            last_modified,
            user_metadata,
        };

        if let Err(e) = self.put_meta(key, &meta) {
            w.file.set_len(offset).ok();
            w.size = offset;
            return Err(e);
        }
        drop(w);

        for (part_num, _) in &sorted_parts {
            fs::remove_file(self.part_path(upload_id, *part_num)).ok();
        }

        if let Some(old) = old_meta {
            self.add_seg_dead_bytes(old.segment_id, old.length)?;
        }

        Ok((meta, etag))
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

    // === Delete ===

    pub fn delete_object(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        let k = key.as_bytes();

        let meta = match self.objects.get(k).map_err(io::Error::other)? {
            Some(v) => bincode::deserialize::<ObjectMeta>(&v).map_err(io::Error::other)?,
            None => return Ok(None),
        };

        self.objects.remove(k).map_err(io::Error::other)?;
        self.add_seg_dead_bytes(meta.segment_id, meta.length)?;
        Ok(Some(meta))
    }

    /// Backward compat alias for tests.
    pub fn delete_and_compact(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        self.delete_object(key)
    }

    // === Compaction ===

    /// Compact all segments that have dead bytes.
    pub fn compact(&self) -> io::Result<()> {
        let stats = self.segment_stats()?;
        for stat in stats {
            if stat.dead_bytes > 0 {
                self.compact_segment(stat.id)?;
            }
        }
        Ok(())
    }

    /// Swap a compacted segment file and update sled metadata atomically.
    ///
    /// The per-segment write lock MUST be held across both the file swap and the sled
    /// batch apply. Dropping it between would let readers see the new (compacted) file
    /// with old (pre-compaction) offsets — corrupted reads.
    #[allow(clippy::significant_drop_tightening)]
    fn apply_compaction(
        &self,
        seg: &RwLock<File>,
        tmp_path: &Path,
        seg_path: &Path,
        batch: sled::Batch,
        segment_id: u32,
    ) -> io::Result<()> {
        let mut file = seg
            .write()
            .map_err(|_| io::Error::other("segment lock poisoned"))?;
        fs::rename(tmp_path, seg_path)?;
        *file = File::open(seg_path)?;

        self.objects
            .apply_batch(batch)
            .map_err(io::Error::other)?;
        self.set_seg_dead_bytes(segment_id, 0)?;
        self.set_seg_compacting(segment_id, false)?;
        Ok(())
    }

    /// Compact a single segment. Only locks that segment during the swap phase.
    pub fn compact_segment(&self, segment_id: u32) -> io::Result<()> {
        // If compacting active segment, rotate first so it becomes non-active
        {
            let mut w = self
                .writer
                .lock()
                .map_err(|_| io::Error::other("writer lock poisoned"))?;
            if segment_id == w.id {
                self.rotate_segment(&mut w)?;
            }
        }

        let live = self.collect_live_objects_for_segment(segment_id)?;

        if live.is_empty() {
            return self.remove_segment(segment_id);
        }

        // Get per-segment handle (Arc clone so we can drop the outer map lock)
        let seg_arc = {
            let map = self
                .segments
                .read()
                .map_err(|_| io::Error::other("segments lock poisoned"))?;
            Arc::clone(map.get(&segment_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("segment {segment_id} not found"),
                )
            })?)
        };

        let seg_path = self.bucket_dir.join(segment_filename(segment_id));
        let tmp_path = self
            .bucket_dir
            .join(format!("{}.tmp", segment_filename(segment_id)));
        let mut tmp = File::create(&tmp_path)?;
        let mut new_offset: u64 = 0;

        // Phase 1: Read live data (per-segment read lock — concurrent with other readers)
        let updated = {
            let file = seg_arc
                .read()
                .map_err(|_| io::Error::other("segment lock poisoned"))?;
            let mut buf = vec![0u8; COPY_BUF_SIZE];
            let mut entries = Vec::with_capacity(live.len());
            for (key, mut obj) in live {
                let mut remaining = obj.length;
                let mut read_off = obj.offset;
                obj.offset = new_offset;
                new_offset += obj.length;

                while remaining > 0 {
                    #[allow(clippy::cast_possible_truncation)]
                    let chunk = (remaining as usize).min(buf.len());
                    file.read_exact_at(&mut buf[..chunk], read_off)?;
                    tmp.write_all(&buf[..chunk])?;
                    read_off += chunk as u64;
                    remaining -= chunk as u64;
                }

                entries.push((key, obj));
            }
            drop(file);
            entries
        };

        // Build sled batch outside the read lock (serialization doesn't need segment access)
        let mut batch = sled::Batch::default();
        for (key, obj) in updated {
            batch.insert(key, bincode::serialize(&obj).map_err(io::Error::other)?);
        }

        tmp.flush()?;
        tmp.sync_all()?;
        drop(tmp);

        self.set_seg_compacting(segment_id, true)?;
        self.meta.flush().map_err(io::Error::other)?;

        // Phase 2: Swap file + update sled under per-segment write lock
        self.apply_compaction(&seg_arc, &tmp_path, &seg_path, batch, segment_id)
    }

    // === List objects ===

    pub fn list_objects(
        &self,
        prefix: Option<&str>,
        max_keys: usize,
        continuation_token: Option<&str>,
    ) -> io::Result<(Vec<(String, ObjectMeta)>, bool)> {
        let (objects, _, truncated) =
            self.list_objects_with_delimiter(prefix, None, max_keys, continuation_token)?;
        Ok((objects, truncated))
    }

    pub fn list_objects_with_delimiter(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: usize,
        continuation_token: Option<&str>,
    ) -> io::Result<ListResult> {
        use std::collections::BTreeSet;

        let iter: Box<dyn Iterator<Item = sled::Result<(sled::IVec, sled::IVec)>>> =
            prefix.map_or_else(
                || Box::new(self.objects.iter()) as Box<dyn Iterator<Item = _>>,
                |p| Box::new(self.objects.scan_prefix(p.as_bytes())),
            );

        let mut results = Vec::new();
        let mut common_prefixes: BTreeSet<String> = BTreeSet::new();
        let mut truncated = false;
        let prefix_str = prefix.unwrap_or("");
        let prefix_len = prefix_str.len();

        for result in iter {
            let (k, v) = result.map_err(io::Error::other)?;
            let obj_key = std::str::from_utf8(&k)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .to_string();

            if let Some(token) = continuation_token
                && obj_key.as_str() <= token
            {
                continue;
            }

            if let Some(delim) = delimiter {
                let rest = &obj_key[prefix_len..];
                if let Some(pos) = rest.find(delim) {
                    let mut cp = String::with_capacity(prefix_len + pos + delim.len());
                    cp.push_str(prefix_str);
                    cp.push_str(&rest[..(pos + delim.len())]);
                    if common_prefixes.contains(&cp) {
                        continue;
                    }
                    if results.len() + common_prefixes.len() >= max_keys {
                        truncated = true;
                        break;
                    }
                    common_prefixes.insert(cp);
                    continue;
                }
            }

            if results.len() + common_prefixes.len() >= max_keys {
                truncated = true;
                break;
            }

            let meta: ObjectMeta =
                bincode::deserialize(&v).map_err(io::Error::other)?;
            results.push((obj_key, meta));
        }

        Ok((results, common_prefixes.into_iter().collect(), truncated))
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.objects.is_empty())
    }
}

// === Storage (manages multiple buckets) ===

pub struct Storage {
    data_dir: PathBuf,
    buckets: RwLock<HashMap<String, Arc<BucketStore>>>,
}

#[allow(clippy::missing_errors_doc)]
impl Storage {
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;

        let mut map = HashMap::new();

        if data_dir.exists() {
            for entry in fs::read_dir(data_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let name = entry
                        .file_name()
                        .to_str()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "invalid bucket dir name")
                        })?
                        .to_string();
                    let store = BucketStore::open(&entry.path())?;
                    map.insert(name, Arc::new(store));
                }
            }
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            buckets: RwLock::new(map),
        })
    }

    fn read_buckets(&self) -> io::Result<RwLockReadGuard<'_, HashMap<String, Arc<BucketStore>>>> {
        self.buckets
            .read()
            .map_err(|_| io::Error::other("buckets lock poisoned"))
    }

    fn write_buckets(&self) -> io::Result<RwLockWriteGuard<'_, HashMap<String, Arc<BucketStore>>>> {
        self.buckets
            .write()
            .map_err(|_| io::Error::other("buckets lock poisoned"))
    }

    pub fn create_bucket(&self, name: &str) -> io::Result<bool> {
        let mut map = self.write_buckets()?;
        if map.contains_key(name) {
            return Ok(true);
        }
        let bucket_dir = self.data_dir.join(name);
        let store = BucketStore::open(&bucket_dir)?;
        map.insert(name.to_string(), Arc::new(store));
        drop(map);
        Ok(false)
    }

    pub fn get_bucket(&self, name: &str) -> io::Result<Option<Arc<BucketStore>>> {
        Ok(self.read_buckets()?.get(name).cloned())
    }

    pub fn delete_bucket(&self, name: &str) -> io::Result<bool> {
        let mut map = self.write_buckets()?;
        if map.remove(name).is_none() {
            return Ok(false);
        }
        drop(map);
        let bucket_dir = self.data_dir.join(name);
        fs::remove_dir_all(&bucket_dir)?;
        Ok(true)
    }

    pub fn list_buckets(&self) -> io::Result<Vec<String>> {
        let mut names: Vec<String> = self.read_buckets()?.keys().cloned().collect();
        names.sort();
        Ok(names)
    }
}
