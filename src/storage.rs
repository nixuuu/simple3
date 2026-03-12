use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};

use crate::types::ObjectMeta;

/// Result of listing objects: (objects, `common_prefixes`, truncated).
pub type ListResult = (Vec<(String, ObjectMeta)>, Vec<String>, bool);

const DEAD_BYTES_KEY: &[u8] = b"__dead_bytes__";
const COMPACTING_KEY: &[u8] = b"__compacting__";

pub struct BucketStore {
    #[allow(dead_code)]
    db: sled::Db,
    objects: sled::Tree,
    meta: sled::Tree,
    data_file: RwLock<File>,
    bucket_dir: PathBuf,
}

impl BucketStore {
    fn open(bucket_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(bucket_dir)?;

        let db = sled::open(bucket_dir.join("index.db"))
            .map_err(io::Error::other)?;
        let objects = db
            .open_tree("objects")
            .map_err(io::Error::other)?;
        let meta = db
            .open_tree("meta")
            .map_err(io::Error::other)?;

        let data_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(bucket_dir.join("data.bin"))?;

        // GC: remove leftover temp files, multipart parts, and compaction temps
        for entry in fs::read_dir(bucket_dir)? {
            let p = entry?.path();
            if let Some(name) = p.file_name().and_then(|n| n.to_str())
                && (name.starts_with(".tmp_")
                    || name.starts_with(".mpu_")
                    || name == "data.bin.tmp")
                {
                    fs::remove_file(&p).ok();
                }
        }

        let store = Self {
            db,
            objects,
            meta,
            data_file: RwLock::new(data_file),
            bucket_dir: bucket_dir.to_path_buf(),
        };

        // Recovery: if compaction was interrupted, rebuild data.bin from sled
        if store.get_meta_flag(COMPACTING_KEY) {
            store.recover_from_interrupted_compaction()?;
        }

        // Truncate orphaned bytes at end of data.bin
        store.truncate_orphans()?;

        Ok(store)
    }

    // === Recovery ===

    fn truncate_orphans(&self) -> io::Result<()> {
        let mut file = self.data_file.write().unwrap();
        let file_size = file.seek(SeekFrom::End(0))?;

        // Find max(offset + length) across all live objects
        let mut max_end: u64 = 0;
        for result in &self.objects {
            let (_k, v) = result.map_err(io::Error::other)?;
            let obj: ObjectMeta = bincode::deserialize(&v)
                .map_err(io::Error::other)?;
            let end = obj.offset + obj.length;
            if end > max_end {
                max_end = end;
            }
        }

        if file_size > max_end {
            file.set_len(max_end)?;
        }
        drop(file);

        Ok(())
    }

    fn recover_from_interrupted_compaction(&self) -> io::Result<()> {
        // Compaction was interrupted after rename but before sled batch.
        // Rebuild: re-read all live objects, rewrite data.bin from scratch.
        // This is the nuclear recovery option — correct but slow.
        let mut live: Vec<(String, ObjectMeta)> = Vec::new();
        for result in &self.objects {
            let (k, v) = result.map_err(io::Error::other)?;
            let key = std::str::from_utf8(&k)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .to_string();
            let obj: ObjectMeta = bincode::deserialize(&v)
                .map_err(io::Error::other)?;
            live.push((key, obj));
        }
        live.sort_by_key(|(_, m)| m.offset);

        // Read all live data using current offsets (may be old or new)
        let file = self.data_file.read().unwrap();
        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        let mut data_chunks: Vec<(String, ObjectMeta, Vec<u8>)> = Vec::new();

        for (key, obj) in &live {
            if obj.offset + obj.length <= file_size {
                let len = usize::try_from(obj.length)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let mut buf = vec![0u8; len];
                if file.read_exact_at(&mut buf, obj.offset).is_ok() {
                    data_chunks.push((key.clone(), obj.clone(), buf));
                }
            }
        }
        drop(file);

        // Rewrite data.bin
        let tmp_path = self.bucket_dir.join("data.bin.recover");
        let mut tmp = File::create(&tmp_path)?;
        let mut batch = sled::Batch::default();
        let mut offset: u64 = 0;

        for (key, mut obj, data) in data_chunks {
            tmp.write_all(&data)?;
            obj.offset = offset;
            offset += obj.length;
            let v = bincode::serialize(&obj)
                .map_err(io::Error::other)?;
            batch.insert(key.as_bytes(), v);
        }
        tmp.flush()?;
        tmp.sync_all()?;
        drop(tmp);

        // Atomic rename
        fs::rename(&tmp_path, self.bucket_dir.join("data.bin"))?;

        // Reopen file handle
        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.bucket_dir.join("data.bin"))?;
        *self.data_file.write().unwrap() = new_file;

        // Apply batch + clear flags
        self.objects
            .apply_batch(batch)
            .map_err(io::Error::other)?;
        self.set_dead_bytes(0)?;
        self.set_meta_flag(COMPACTING_KEY, false)?;

        Ok(())
    }

    // === Dead space tracking ===

    /// Returns the number of dead (reclaimable) bytes in the data file.
    pub fn dead_bytes(&self) -> u64 {
        self.meta
            .get(DEAD_BYTES_KEY)
            .ok()
            .flatten()
            .and_then(|v| v.as_ref().try_into().ok())
            .map_or(0, u64::from_le_bytes)
    }

    /// # Errors
    /// Returns `io::Error` if reading file metadata fails.
    /// # Panics
    /// Panics if the data file `RwLock` is poisoned.
    pub fn data_file_size(&self) -> io::Result<u64> {
        let file = self.data_file.read().unwrap();
        Ok(file.metadata()?.len())
    }

    fn add_dead_bytes(&self, n: u64) -> io::Result<()> {
        let current = self.dead_bytes();
        self.set_dead_bytes(current + n)
    }

    fn set_dead_bytes(&self, n: u64) -> io::Result<()> {
        self.meta
            .insert(DEAD_BYTES_KEY, &n.to_le_bytes())
            .map_err(io::Error::other)?;
        Ok(())
    }

    fn get_meta_flag(&self, key: &[u8]) -> bool {
        self.meta
            .get(key)
            .ok()
            .flatten()
            .is_some_and(|v| !v.is_empty() && v[0] == 1)
    }

    fn set_meta_flag(&self, key: &[u8], val: bool) -> io::Result<()> {
        self.meta
            .insert(key, &[u8::from(val)])
            .map_err(io::Error::other)?;
        Ok(())
    }

    // === Data operations (append-only) ===

    /// # Errors
    /// Returns `io::Error` if data file write fails.
    /// # Panics
    /// Panics if the data file `RwLock` is poisoned.
    pub fn append_data(&self, data: &[u8]) -> io::Result<(u64, u64)> {
        let mut file = self.data_file.write().unwrap();
        let offset = file.seek(SeekFrom::End(0))?;
        file.write_all(data)?;
        drop(file);
        #[allow(clippy::cast_possible_truncation)]
        let len = data.len() as u64;
        Ok((offset, len))
    }

    /// # Errors
    /// Returns `io::Error` if data file read fails.
    /// # Panics
    /// Panics if the data file `RwLock` is poisoned.
    pub fn read_data(&self, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        let len = usize::try_from(length)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut buf = vec![0u8; len];
        self.data_file.read().unwrap().read_exact_at(&mut buf, offset)?;
        Ok(buf)
    }

    /// # Errors
    /// Returns `io::Error` if sled insert fails.
    pub fn put_meta(&self, key: &str, meta: &ObjectMeta) -> io::Result<()> {
        let v = bincode::serialize(meta)
            .map_err(io::Error::other)?;
        self.objects
            .insert(key.as_bytes(), v)
            .map_err(io::Error::other)?;
        Ok(())
    }

    pub fn bucket_dir(&self) -> &Path {
        &self.bucket_dir
    }

    /// Append data from tmp file to data.bin + write metadata.
    /// If key already exists, old data becomes dead space (no inline compaction).
    /// Crash-safe: append-only + sled is individually crash-safe.
    ///
    /// # Errors
    /// Returns `io::Error` on file I/O or sled failure.
    /// # Panics
    /// Panics if the data file `RwLock` is poisoned.
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
        let mut file = self.data_file.write().unwrap();

        let offset = file.seek(SeekFrom::End(0))?;
        let length = io::copy(&mut tmp, &mut *file)?;
        file.flush()?;
        drop(tmp);

        let meta = ObjectMeta {
            offset,
            length,
            content_type,
            etag,
            last_modified,
            user_metadata,
        };

        if let Err(e) = self.put_meta(key, &meta) {
            file.set_len(offset).ok();
            fs::remove_file(tmp_path).ok();
            return Err(e);
        }

        drop(file);
        fs::remove_file(tmp_path).ok();

        // Track dead space from overwritten object
        if let Some(old) = old_meta {
            self.add_dead_bytes(old.length)?;
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

    /// # Errors
    /// Returns `io::Error` if writing the part file fails.
    pub fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        data: &[u8],
    ) -> io::Result<String> {
        let part_path = self
            .bucket_dir
            .join(format!(".mpu_{upload_id}_{part_number:05}"));
        fs::write(&part_path, data)?;

        let mut hasher = Md5::new();
        hasher.update(data);
        let etag = format!("{:x}", hasher.finalize());
        Ok(etag)
    }

    /// # Errors
    /// Returns `io::Error` on file I/O or sled failure.
    /// # Panics
    /// Panics if the data file `RwLock` is poisoned.
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

        let mut file = self.data_file.write().unwrap();
        let offset = file.seek(SeekFrom::End(0))?;
        let mut total_len: u64 = 0;
        let mut part_md5_concat = Vec::new();

        for (part_num, _etag) in &sorted_parts {
            let part_path = self
                .bucket_dir
                .join(format!(".mpu_{upload_id}_{part_num:05}"));
            let mut part_file = File::open(&part_path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("part {part_num} not found: {e}"),
                )
            })?;

            let mut part_hasher = Md5::new();
            let mut buf = vec![0u8; 65536];
            loop {
                let n = part_file.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                file.write_all(&buf[..n])?;
                part_hasher.update(&buf[..n]);
                #[allow(clippy::cast_possible_truncation)]
                { total_len += n as u64; }
            }
            part_md5_concat.extend_from_slice(&part_hasher.finalize());
        }
        file.flush()?;

        let mut final_hasher = Md5::new();
        final_hasher.update(&part_md5_concat);
        let etag = format!("{:x}-{}", final_hasher.finalize(), sorted_parts.len());

        let meta = ObjectMeta {
            offset,
            length: total_len,
            content_type,
            etag: etag.clone(),
            last_modified,
            user_metadata,
        };

        if let Err(e) = self.put_meta(key, &meta) {
            file.set_len(offset).ok();
            return Err(e);
        }
        drop(file);

        // Cleanup part files
        for (part_num, _) in &sorted_parts {
            let part_path = self
                .bucket_dir
                .join(format!(".mpu_{upload_id}_{part_num:05}"));
            fs::remove_file(&part_path).ok();
        }

        // Track dead space from overwritten object
        if let Some(old) = old_meta {
            self.add_dead_bytes(old.length)?;
        }

        Ok((meta, etag))
    }

    /// # Errors
    /// Returns `io::Error` if directory listing fails.
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

    /// # Errors
    /// Returns `io::Error` if sled read or deserialization fails.
    pub fn get_meta(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        match self
            .objects
            .get(key.as_bytes())
            .map_err(io::Error::other)?
        {
            Some(v) => {
                let meta: ObjectMeta = bincode::deserialize(&v)
                    .map_err(io::Error::other)?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    // === Delete (append-only: just remove sled key, data becomes dead space) ===

    /// # Errors
    /// Returns `io::Error` if sled operations fail.
    pub fn delete_object(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        let k = key.as_bytes();

        let meta = match self
            .objects
            .get(k)
            .map_err(io::Error::other)?
        {
            Some(v) => bincode::deserialize::<ObjectMeta>(&v)
                .map_err(io::Error::other)?,
            None => return Ok(None),
        };

        self.objects
            .remove(k)
            .map_err(io::Error::other)?;

        self.add_dead_bytes(meta.length)?;

        Ok(Some(meta))
    }

    /// Backward compat alias for tests.
    /// # Errors
    /// Returns `io::Error` if sled operations fail.
    pub fn delete_and_compact(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        self.delete_object(key)
    }

    // === Deferred full-rewrite compaction ===

    /// # Errors
    /// Returns `io::Error` on file I/O or sled failure.
    /// # Panics
    /// Panics if the data file `RwLock` is poisoned.
    #[allow(clippy::significant_drop_tightening)]
    pub fn compact(&self) -> io::Result<()> {
        // Collect live objects sorted by offset
        let mut live: Vec<(Vec<u8>, ObjectMeta)> = Vec::new();
        for result in &self.objects {
            let (k, v) = result.map_err(io::Error::other)?;
            let obj: ObjectMeta = bincode::deserialize(&v)
                .map_err(io::Error::other)?;
            live.push((k.to_vec(), obj));
        }
        live.sort_by_key(|(_, m)| m.offset);

        if live.is_empty() {
            self.data_file.read().unwrap().set_len(0)?;
            self.set_dead_bytes(0)?;
            return Ok(());
        }

        // Write data.bin.tmp with only live data
        let tmp_path = self.bucket_dir.join("data.bin.tmp");
        let mut tmp = File::create(&tmp_path)?;
        let mut batch = sled::Batch::default();
        let mut new_offset: u64 = 0;

        {
            let file = self.data_file.read().unwrap();
            for (key, mut obj) in live {
                let len = usize::try_from(obj.length)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let mut buf = vec![0u8; len];
                file.read_exact_at(&mut buf, obj.offset)?;
                tmp.write_all(&buf)?;
                obj.offset = new_offset;
                new_offset += obj.length;
                let v = bincode::serialize(&obj)
                    .map_err(io::Error::other)?;
                batch.insert(key, v);
            }
        }

        tmp.flush()?;
        tmp.sync_all()?;
        drop(tmp);

        // Set compaction flag BEFORE rename (crash between rename and sled batch → recovery)
        self.set_meta_flag(COMPACTING_KEY, true)?;
        self.meta
            .flush()
            .map_err(io::Error::other)?;

        // Atomic rename
        fs::rename(&tmp_path, self.bucket_dir.join("data.bin"))?;

        // Reopen file handle
        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.bucket_dir.join("data.bin"))?;
        *self.data_file.write().unwrap() = new_file;

        // Apply new offsets + clear dead bytes + clear flag
        self.objects
            .apply_batch(batch)
            .map_err(io::Error::other)?;
        self.set_dead_bytes(0)?;
        self.set_meta_flag(COMPACTING_KEY, false)?;

        Ok(())
    }

    /// # Errors
    /// Returns `io::Error` if sled iteration or deserialization fails.
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

    /// List objects with optional delimiter support (for directory-like browsing).
    ///
    /// # Errors
    /// Returns `io::Error` if sled iteration or deserialization fails.
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
        let prefix_len = prefix.map_or(0, str::len);

        for result in iter {
            let (k, v) = result.map_err(io::Error::other)?;
            let obj_key = std::str::from_utf8(&k)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .to_string();

            if let Some(token) = continuation_token
                && obj_key.as_str() <= token {
                    continue;
                }

            // Check delimiter: if key has delimiter after prefix, group into common_prefixes
            if let Some(delim) = delimiter {
                let rest = &obj_key[prefix_len..];
                if let Some(pos) = rest.find(delim) {
                    let cp = format!("{}{}", prefix.unwrap_or(""), &rest[..(pos + delim.len())]);
                    if common_prefixes.contains(&cp) {
                        continue; // already counted
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

            let meta: ObjectMeta = bincode::deserialize(&v)
                .map_err(io::Error::other)?;
            results.push((obj_key, meta));
        }

        Ok((results, common_prefixes.into_iter().collect(), truncated))
    }

    /// # Errors
    /// This method currently always succeeds.
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.objects.is_empty())
    }
}

pub struct Storage {
    data_dir: PathBuf,
    buckets: RwLock<HashMap<String, Arc<BucketStore>>>,
}

impl Storage {
    /// # Errors
    /// Returns `io::Error` if directory creation or bucket loading fails.
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

    /// # Errors
    /// Returns `io::Error` if bucket directory creation fails.
    /// # Panics
    /// Panics if the buckets `RwLock` is poisoned.
    pub fn create_bucket(&self, name: &str) -> io::Result<bool> {
        let mut map = self.buckets.write().unwrap();
        if map.contains_key(name) {
            return Ok(true);
        }
        let bucket_dir = self.data_dir.join(name);
        let store = BucketStore::open(&bucket_dir)?;
        map.insert(name.to_string(), Arc::new(store));
        drop(map);
        Ok(false)
    }

    /// # Panics
    /// Panics if the buckets `RwLock` is poisoned.
    pub fn get_bucket(&self, name: &str) -> Option<Arc<BucketStore>> {
        self.buckets.read().unwrap().get(name).cloned()
    }

    /// # Errors
    /// Returns `io::Error` if removing the bucket directory fails.
    /// # Panics
    /// Panics if the buckets `RwLock` is poisoned.
    pub fn delete_bucket(&self, name: &str) -> io::Result<bool> {
        let mut map = self.buckets.write().unwrap();
        if map.remove(name).is_none() {
            return Ok(false);
        }
        drop(map);
        let bucket_dir = self.data_dir.join(name);
        fs::remove_dir_all(&bucket_dir)?;
        Ok(true)
    }

    /// # Panics
    /// Panics if the buckets `RwLock` is poisoned.
    pub fn list_buckets(&self) -> Vec<String> {
        let mut names: Vec<String> = self.buckets.read().unwrap().keys().cloned().collect();
        names.sort();
        names
    }
}
