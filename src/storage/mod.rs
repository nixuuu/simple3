mod compaction;
mod multipart;
mod segment;
mod verify;
pub mod versioning;

pub use verify::VerifyResult;

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use crate::types::ObjectMeta;

// Re-export from segment module for sibling modules and public API.
pub use segment::SegmentStat;
use segment::{ActiveWriter, COPY_BUF_SIZE, cleanup_temp_files, discover_segments, segment_filename};

/// Result of listing objects: (objects, `common_prefixes`, truncated).
pub type ListResult = (Vec<(String, ObjectMeta)>, Vec<String>, bool);

const OBJECTS: TableDefinition<&str, &[u8]> = TableDefinition::new("objects");
const SEG_DEAD: TableDefinition<u32, u64> = TableDefinition::new("seg_dead");
const SEG_COMPACTING: TableDefinition<u32, u8> = TableDefinition::new("seg_compacting");

const DEFAULT_MAX_SEGMENT_SIZE: u64 = 4 * 1024 * 1024 * 1024; // 4 GB

// === BucketStore ===

pub struct BucketStore {
    pub(super) db: Database,
    /// Serializes appends and rotation on the active segment.
    pub(super) writer: Mutex<ActiveWriter>,
    /// Per-segment read handles.
    /// Outer `RwLock`: structural mutations (add/remove segments).
    /// Inner `RwLock<File>`: read lock for concurrent `pread`, write lock for compaction swap.
    pub(super) segments: RwLock<HashMap<u32, Arc<RwLock<File>>>>,
    pub(super) bucket_dir: PathBuf,
    pub(super) max_segment_size: u64,
}

#[allow(clippy::missing_errors_doc)]
impl BucketStore {
    fn open(bucket_dir: &Path, max_segment_size: u64) -> io::Result<Self> {
        fs::create_dir_all(bucket_dir)?;
        cleanup_temp_files(bucket_dir)?;

        let redb_path = bucket_dir.join("index.redb");
        let db = Database::create(&redb_path).map_err(io::Error::other)?;

        // Ensure tables exist
        {
            let txn = db.begin_write().map_err(io::Error::other)?;
            let _ = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            let _ = txn.open_table(SEG_DEAD).map_err(io::Error::other)?;
            let _ = txn.open_table(SEG_COMPACTING).map_err(io::Error::other)?;
            let _ = txn
                .open_table(versioning::VERSIONS)
                .map_err(io::Error::other)?;
            let _ = txn
                .open_table(versioning::BUCKET_CONFIG)
                .map_err(io::Error::other)?;
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
            max_segment_size,
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

    pub fn put_meta(&self, key: &str, meta: &mut ObjectMeta) -> io::Result<()> {
        self.commit_put(key, meta)
    }

    pub fn get_meta(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
        match table.get(key).map_err(io::Error::other)? {
            Some(v) => {
                let meta = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    /// Atomically insert/update an object and handle versioning.
    ///
    /// Reads versioning state **inside** the write transaction to avoid TOCTOU races.
    fn commit_put(&self, key: &str, meta: &mut ObjectMeta) -> io::Result<()> {
        self.commit_put_versioned(key, meta)
    }

    pub fn bucket_dir(&self) -> &Path {
        &self.bucket_dir
    }

    #[allow(clippy::too_many_arguments)] // all fields required for atomic put; builder would complicate rollback
    pub fn put_object_streamed(
        &self,
        key: &str,
        tmp_path: &Path,
        content_type: Option<String>,
        etag: String,
        last_modified: u64,
        user_metadata: HashMap<String, String>,
        content_crc32c: Option<u32>,
    ) -> io::Result<ObjectMeta> {
        let tmp = File::open(tmp_path)?;
        let tmp_size = tmp.metadata()?.len();
        let mut w = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("writer lock poisoned"))?;

        let crc_len = if content_crc32c.is_some() { 4u64 } else { 0 };
        if w.size + tmp_size + crc_len > self.max_segment_size && w.size > 0 {
            self.rotate_segment(&mut w)?;
        }

        let segment_id = w.id;
        let (offset, length) =
            self.write_to_segment(&mut w, tmp_path, tmp, tmp_size, content_crc32c)?;

        let content_md5 = Some(etag.clone());
        let mut meta = ObjectMeta {
            segment_id,
            offset,
            length,
            content_type,
            etag,
            last_modified,
            user_metadata,
            content_md5,
            content_crc32c,
            version_id: None,
            is_delete_marker: false,
        };

        if let Err(e) = self.commit_put(key, &mut meta) {
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

    /// Delete an object. Behavior depends on versioning state:
    /// - Absent: hard delete, mark data as dead.
    /// - Enabled: insert delete marker, move current to versions table.
    /// - Suspended: insert delete marker with vid="null".
    ///
    /// Reads versioning state inside the write transaction to avoid TOCTOU races.
    /// Returns the delete marker meta (if versioned) or the removed meta (if not).
    pub fn delete_object(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        self.delete_object_versioned(key)
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

            // Skip delete markers — they must not appear in normal ListObjects
            let meta = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
            if meta.is_delete_marker {
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

            results.push((obj_key.to_owned(), meta));
        }

        Ok((results, common_prefixes.into_iter().collect(), truncated))
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let objects = txn.open_table(OBJECTS).map_err(io::Error::other)?;
        if !objects.is_empty().map_err(io::Error::other)? {
            return Ok(false);
        }
        let versions = txn
            .open_table(versioning::VERSIONS)
            .map_err(io::Error::other)?;
        versions.is_empty().map_err(io::Error::other)
    }
}

// === Storage (manages multiple buckets) ===

pub struct Storage {
    data_dir: PathBuf,
    buckets: RwLock<HashMap<String, Arc<BucketStore>>>,
    max_segment_size: u64,
    compacting: AtomicUsize,
}

#[allow(clippy::missing_errors_doc)]
impl Storage {
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        Self::open_with_segment_size(data_dir, DEFAULT_MAX_SEGMENT_SIZE)
    }

    pub fn open_with_segment_size(data_dir: &Path, max_segment_size: u64) -> io::Result<Self> {
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
                    let store = BucketStore::open(&entry.path(), max_segment_size)?;
                    map.insert(name, Arc::new(store));
                }
            }
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            buckets: RwLock::new(map),
            max_segment_size,
            compacting: AtomicUsize::new(0),
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn begin_compacting(&self) {
        self.compacting.fetch_add(1, Ordering::Relaxed);
    }

    pub fn end_compacting(&self) {
        self.compacting
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
    }

    pub fn is_compacting(&self) -> bool {
        self.compacting.load(Ordering::Relaxed) > 0
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
        let store = BucketStore::open(&bucket_dir, self.max_segment_size)?;
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

    /// Fsync the active segment of every loaded bucket.
    pub fn sync_all(&self) -> io::Result<()> {
        let stores: Vec<(String, Arc<BucketStore>)> = self
            .read_buckets()?
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        let mut first_err = None;
        for (name, store) in &stores {
            if let Err(e) = store.sync_active_segment() {
                tracing::error!("sync_all: {name} — {e}");
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        first_err.map_or(Ok(()), Err)
    }
}
