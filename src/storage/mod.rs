mod compaction;
mod migration;
mod multipart;

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use crate::types::ObjectMeta;

/// Result of listing objects: (objects, `common_prefixes`, truncated).
pub type ListResult = (Vec<(String, ObjectMeta)>, Vec<String>, bool);

const OBJECTS: TableDefinition<&str, &[u8]> = TableDefinition::new("objects");
const SEG_DEAD: TableDefinition<u32, u64> = TableDefinition::new("seg_dead");
const SEG_COMPACTING: TableDefinition<u32, u8> = TableDefinition::new("seg_compacting");

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

/// Remove leftover temp files from interrupted operations.
fn cleanup_temp_files(bucket_dir: &Path) -> io::Result<()> {
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

// === BucketStore ===

pub struct BucketStore {
    db: Database,
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
        cleanup_temp_files(bucket_dir)?;
        migration::migrate_sled_to_redb(bucket_dir)?;

        let redb_path = bucket_dir.join("index.redb");
        let db = Database::create(&redb_path).map_err(io::Error::other)?;

        // Ensure tables exist
        {
            let txn = db.begin_write().map_err(io::Error::other)?;
            let _ = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            let _ = txn.open_table(SEG_DEAD).map_err(io::Error::other)?;
            let _ = txn.open_table(SEG_COMPACTING).map_err(io::Error::other)?;
            txn.commit().map_err(io::Error::other)?;
        }

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

    fn get_segment_handle(&self, segment_id: u32) -> io::Result<Arc<RwLock<File>>> {
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

    fn truncate_orphans(&self) -> io::Result<()> {
        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;
        let active_id = w.id;

        let mut max_end: u64 = 0;
        {
            let txn = self.db.begin_read().map_err(io::Error::other)?;
            let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            for result in table.iter().map_err(io::Error::other)? {
                let (_k, v) = result.map_err(io::Error::other)?;
                let obj: ObjectMeta =
                    bincode::deserialize(v.value()).map_err(io::Error::other)?;
                if obj.segment_id == active_id {
                    let end = obj.offset + obj.length;
                    if end > max_end {
                        max_end = end;
                    }
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

    fn seg_dead_bytes(&self, segment_id: u32) -> u64 {
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

    fn get_seg_compacting(&self, segment_id: u32) -> bool {
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

    fn set_seg_compacting(&self, segment_id: u32, val: bool) -> io::Result<()> {
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

        let seg_arc = self.get_segment_handle(segment_id)?;
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
        self.commit_put(key, meta)
    }

    pub fn get_meta(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
        match table.get(key).map_err(io::Error::other)? {
            Some(v) => {
                let meta: ObjectMeta =
                    bincode::deserialize(v.value()).map_err(io::Error::other)?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    /// Atomically insert/update an object and track dead bytes from the previous version.
    ///
    /// Reads the old meta **inside** the write transaction to avoid TOCTOU races
    /// where concurrent overwrites double-count dead bytes.
    fn commit_put(&self, key: &str, meta: &ObjectMeta) -> io::Result<()> {
        let v = bincode::serialize(meta).map_err(io::Error::other)?;
        let txn = self.db.begin_write().map_err(io::Error::other)?;
        let old_meta = {
            let mut table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            let old = table
                .get(key)
                .map_err(io::Error::other)?
                .map(|v| bincode::deserialize::<ObjectMeta>(v.value()))
                .transpose()
                .map_err(io::Error::other)?;
            table
                .insert(key, v.as_slice())
                .map_err(io::Error::other)?;
            old
        };
        if let Some(old) = old_meta {
            let mut dead = txn.open_table(SEG_DEAD).map_err(io::Error::other)?;
            let current = dead
                .get(old.segment_id)
                .map_err(io::Error::other)?
                .map_or(0, |g| g.value());
            dead.insert(old.segment_id, current + old.length)
                .map_err(io::Error::other)?;
        }
        txn.commit().map_err(io::Error::other)?;
        Ok(())
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

        if let Err(e) = self.commit_put(key, &meta) {
            w.file.set_len(offset).ok();
            w.size = offset;
            fs::remove_file(tmp_path).ok();
            return Err(e);
        }

        drop(w);
        fs::remove_file(tmp_path).ok();
        Ok(meta)
    }

    // === Delete ===

    pub fn delete_object(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        let txn = self.db.begin_write().map_err(io::Error::other)?;

        let meta = {
            let mut table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            match table.remove(key).map_err(io::Error::other)? {
                Some(v) => {
                    bincode::deserialize::<ObjectMeta>(v.value()).map_err(io::Error::other)?
                }
                None => return Ok(None),
            }
        };

        {
            let mut dead = txn.open_table(SEG_DEAD).map_err(io::Error::other)?;
            let current = dead
                .get(meta.segment_id)
                .map_err(io::Error::other)?
                .map_or(0, |g| g.value());
            dead.insert(meta.segment_id, current + meta.length)
                .map_err(io::Error::other)?;
        }

        txn.commit().map_err(io::Error::other)?;
        Ok(Some(meta))
    }

    /// Backward compat alias for tests.
    pub fn delete_and_compact(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        self.delete_object(key)
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

        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;

        let prefix_str = prefix.unwrap_or("");
        let prefix_len = prefix_str.len();

        let iter = if prefix_str.is_empty() {
            table.iter().map_err(io::Error::other)?
        } else {
            table.range(prefix_str..).map_err(io::Error::other)?
        };

        let mut results = Vec::new();
        let mut common_prefixes: BTreeSet<String> = BTreeSet::new();
        let mut truncated = false;

        for result in iter {
            let (k, v) = result.map_err(io::Error::other)?;
            let obj_key = k.value();

            // Stop when we've passed the prefix
            if !prefix_str.is_empty() && !obj_key.starts_with(prefix_str) {
                break;
            }

            if let Some(token) = continuation_token
                && obj_key <= token
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
                bincode::deserialize(v.value()).map_err(io::Error::other)?;
            results.push((obj_key.to_owned(), meta));
        }

        Ok((results, common_prefixes.into_iter().collect(), truncated))
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
        table.is_empty().map_err(io::Error::other)
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
