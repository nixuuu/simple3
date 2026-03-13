use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

use redb::Database;
use serde::{Deserialize, Serialize};

use crate::types::ObjectMeta;

use super::{OBJECTS, SEG_COMPACTING, SEG_DEAD, segment_filename};

/// V1 format (without `segment_id`) for sled→redb migration.
#[derive(Serialize, Deserialize)]
struct ObjectMetaV1 {
    offset: u64,
    length: u64,
    content_type: Option<String>,
    etag: String,
    last_modified: u64,
    user_metadata: HashMap<String, String>,
}

/// Migrate from sled (v1 or v2) to redb. Idempotent — only runs if
/// `index.db/` exists and `index.redb` does not.
pub(super) fn migrate_sled_to_redb(bucket_dir: &Path) -> io::Result<()> {
    let sled_path = bucket_dir.join("index.db");
    let redb_path = bucket_dir.join("index.redb");

    if !sled_path.exists() || redb_path.exists() {
        return Ok(());
    }

    let sled_db = sled::open(&sled_path).map_err(io::Error::other)?;
    let sled_objects = sled_db.open_tree("objects").map_err(io::Error::other)?;
    let sled_meta = sled_db.open_tree("meta").map_err(io::Error::other)?;

    let is_v1 = sled_meta
        .get(b"__storage_version__")
        .map_err(io::Error::other)?
        .is_none();

    // v1: rename data.bin → seg_000000.bin
    if is_v1 {
        let data_bin = bucket_dir.join("data.bin");
        if data_bin.exists() {
            fs::rename(&data_bin, bucket_dir.join(segment_filename(0)))?;
        }
    }

    let rdb = Database::create(&redb_path).map_err(io::Error::other)?;
    let txn = rdb.begin_write().map_err(io::Error::other)?;

    migrate_objects(&txn, &sled_objects, is_v1)?;
    migrate_dead_bytes(&txn, &sled_meta, is_v1)?;

    // Create SEG_COMPACTING table (migration assumes clean shutdown — no active compactions)
    {
        let _ = txn.open_table(SEG_COMPACTING).map_err(io::Error::other)?;
    }

    txn.commit().map_err(io::Error::other)?;
    drop(rdb);
    drop(sled_objects);
    drop(sled_meta);
    drop(sled_db);

    fs::rename(&sled_path, bucket_dir.join("index.db.bak"))?;
    Ok(())
}

fn migrate_objects(
    txn: &redb::WriteTransaction,
    sled_objects: &sled::Tree,
    is_v1: bool,
) -> io::Result<()> {
    let mut table = txn.open_table(OBJECTS).map_err(io::Error::other)?;

    for result in sled_objects {
        let (k, v) = result.map_err(io::Error::other)?;
        let key_str = std::str::from_utf8(&k)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        if is_v1
            && let Ok(v1) = bincode::deserialize::<ObjectMetaV1>(&v)
        {
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
                    content_md5: None,
                };
                let bytes = bincode::serialize(&v2).map_err(io::Error::other)?;
                table
                    .insert(key_str, bytes.as_slice())
                    .map_err(io::Error::other)?;
                continue;
            }
        }
        // v2 format, unrecognized v1, or non-v1 — copy as-is
        table
            .insert(key_str, v.as_ref())
            .map_err(io::Error::other)?;
    }
    Ok(())
}

fn migrate_dead_bytes(
    txn: &redb::WriteTransaction,
    sled_meta: &sled::Tree,
    is_v1: bool,
) -> io::Result<()> {
    let mut dead_table = txn.open_table(SEG_DEAD).map_err(io::Error::other)?;

    if is_v1 {
        if let Some(v) = sled_meta.get(b"__dead_bytes__").map_err(io::Error::other)?
            && let Ok(bytes) = <[u8; 8]>::try_from(v.as_ref())
        {
            dead_table
                .insert(0u32, u64::from_le_bytes(bytes))
                .map_err(io::Error::other)?;
        }
    } else {
        for result in sled_meta.scan_prefix(b"__seg_dead_") {
            let (k, v) = result.map_err(io::Error::other)?;
            let key_str = std::str::from_utf8(&k).unwrap_or("");
            if let Some(id_str) = key_str
                .strip_prefix("__seg_dead_")
                .and_then(|s| s.strip_suffix("__"))
                && let (Ok(seg_id), Ok(bytes)) =
                    (id_str.parse::<u32>(), <[u8; 8]>::try_from(v.as_ref()))
            {
                dead_table
                    .insert(seg_id, u64::from_le_bytes(bytes))
                    .map_err(io::Error::other)?;
            }
        }
    }
    Ok(())
}
