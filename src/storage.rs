use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::types::ObjectMeta;

pub struct BucketStore {
    #[allow(dead_code)]
    db: sled::Db,
    objects: sled::Tree,
    data_file: RwLock<File>,
}

impl BucketStore {
    fn open(bucket_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(bucket_dir)?;

        let db = sled::open(bucket_dir.join("index.db"))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let objects = db
            .open_tree("objects")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let data_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(bucket_dir.join("data.bin"))?;

        Ok(Self {
            db,
            objects,
            data_file: RwLock::new(data_file),
        })
    }

    pub fn append_data(&self, data: &[u8]) -> io::Result<(u64, u64)> {
        let mut file = self.data_file.write().unwrap();
        let offset = file.seek(SeekFrom::End(0))?;
        file.write_all(data)?;
        Ok((offset, data.len() as u64))
    }

    pub fn read_data(&self, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        let file = self.data_file.read().unwrap();
        let mut buf = vec![0u8; length as usize];
        file.read_exact_at(&mut buf, offset)?;
        Ok(buf)
    }

    pub fn put_meta(&self, key: &str, meta: &ObjectMeta) -> io::Result<()> {
        let v = bincode::serialize(meta)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.objects
            .insert(key.as_bytes(), v)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        match self
            .objects
            .get(key.as_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        {
            Some(v) => {
                let meta: ObjectMeta = bincode::deserialize(&v)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    pub fn delete_and_compact(&self, key: &str) -> io::Result<Option<ObjectMeta>> {
        let k = key.as_bytes();

        let meta = match self
            .objects
            .get(k)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        {
            Some(v) => bincode::deserialize::<ObjectMeta>(&v)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
            None => return Ok(None),
        };

        let del_offset = meta.offset;
        let del_length = meta.length;

        let mut file = self.data_file.write().unwrap();
        let file_size = file.seek(SeekFrom::End(0))?;
        let tail_start = del_offset + del_length;

        if tail_start < file_size {
            let tail_len = (file_size - tail_start) as usize;
            let mut tail = vec![0u8; tail_len];
            file.read_exact_at(&mut tail, tail_start)?;
            file.seek(SeekFrom::Start(del_offset))?;
            file.write_all(&tail)?;
        }

        file.set_len(file_size - del_length)?;

        let mut batch = sled::Batch::default();
        for result in self.objects.iter() {
            let (ik, iv) = result.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            if ik == k {
                continue;
            }
            let mut obj_meta: ObjectMeta = bincode::deserialize(&iv)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            if obj_meta.offset > del_offset {
                obj_meta.offset -= del_length;
                let new_v = bincode::serialize(&obj_meta)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                batch.insert(ik, new_v);
            }
        }
        batch.remove(k);
        self.objects
            .apply_batch(batch)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(Some(meta))
    }

    pub fn list_objects(
        &self,
        prefix: Option<&str>,
        max_keys: usize,
        continuation_token: Option<&str>,
    ) -> io::Result<(Vec<(String, ObjectMeta)>, bool)> {
        let iter: Box<dyn Iterator<Item = sled::Result<(sled::IVec, sled::IVec)>>> =
            match prefix {
                Some(p) => Box::new(self.objects.scan_prefix(p.as_bytes())),
                None => Box::new(self.objects.iter()),
            };

        let mut results = Vec::new();
        let mut truncated = false;

        for result in iter {
            let (k, v) = result.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let obj_key = std::str::from_utf8(&k)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .to_string();

            if let Some(token) = continuation_token {
                if obj_key.as_str() <= token {
                    continue;
                }
            }

            if results.len() >= max_keys {
                truncated = true;
                break;
            }

            let meta: ObjectMeta = bincode::deserialize(&v)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            results.push((obj_key, meta));
        }

        Ok((results, truncated))
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self
            .objects
            .is_empty())
    }
}

pub struct Storage {
    data_dir: PathBuf,
    buckets: RwLock<HashMap<String, Arc<BucketStore>>>,
}

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

    pub fn create_bucket(&self, name: &str) -> io::Result<bool> {
        let mut map = self.buckets.write().unwrap();
        if map.contains_key(name) {
            return Ok(true);
        }
        let bucket_dir = self.data_dir.join(name);
        let store = BucketStore::open(&bucket_dir)?;
        map.insert(name.to_string(), Arc::new(store));
        Ok(false)
    }

    pub fn get_bucket(&self, name: &str) -> Option<Arc<BucketStore>> {
        let map = self.buckets.read().unwrap();
        map.get(name).cloned()
    }

    pub fn delete_bucket(&self, name: &str) -> io::Result<bool> {
        let mut map = self.buckets.write().unwrap();
        if map.remove(name).is_none() {
            return Ok(false);
        }
        let bucket_dir = self.data_dir.join(name);
        fs::remove_dir_all(&bucket_dir)?;
        Ok(true)
    }

    pub fn list_buckets(&self) -> Vec<String> {
        let map = self.buckets.read().unwrap();
        let mut names: Vec<String> = map.keys().cloned().collect();
        names.sort();
        names
    }
}
