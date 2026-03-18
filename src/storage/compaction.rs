use std::fs::{self, File};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, RwLock};

use redb::{ReadableDatabase, ReadableTable};

use crate::types::ObjectMeta;

use super::{BucketStore, COPY_BUF_SIZE, OBJECTS, SEG_COMPACTING, SEG_DEAD, segment_filename};
use super::versioning::VERSIONS;

/// Source table for a live object entry during compaction.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TableSource {
    Objects,
    Versions,
}

/// A live object entry with its source table and redb key.
pub(super) struct LiveEntry {
    source: TableSource,
    redb_key: String,
    meta: ObjectMeta,
}

#[allow(clippy::missing_errors_doc)]
impl BucketStore {
    pub(super) fn collect_live_objects_for_segment(
        &self,
        segment_id: u32,
    ) -> io::Result<Vec<LiveEntry>> {
        let mut live = Vec::new();
        let txn = self.db.begin_read().map_err(io::Error::other)?;

        // Scan objects table
        let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
        for result in table.iter().map_err(io::Error::other)? {
            let (k, v) = result.map_err(io::Error::other)?;
            let obj = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
            if obj.segment_id == segment_id && !obj.is_delete_marker {
                live.push(LiveEntry {
                    source: TableSource::Objects,
                    redb_key: k.value().to_owned(),
                    meta: obj,
                });
            }
        }

        // Scan versions table
        let ver_table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
        for result in ver_table.iter().map_err(io::Error::other)? {
            let (k, v) = result.map_err(io::Error::other)?;
            let obj = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
            if obj.segment_id == segment_id && !obj.is_delete_marker {
                live.push(LiveEntry {
                    source: TableSource::Versions,
                    redb_key: k.value().to_owned(),
                    meta: obj,
                });
            }
        }

        live.sort_by_key(|e| e.meta.offset);
        Ok(live)
    }

    pub(super) fn recover_segment_compaction(&self, segment_id: u32) -> io::Result<()> {
        let live = self.collect_live_objects_for_segment(segment_id)?;
        let compacted_size: u64 = live.iter().map(|e| e.meta.length).sum();
        let seg_path = self.bucket_dir.join(segment_filename(segment_id));
        let current_size = fs::metadata(&seg_path).map(|m| m.len()).unwrap_or(0);
        let original_end = live
            .iter()
            .map(|e| e.meta.offset + e.meta.length)
            .max()
            .unwrap_or(0);

        let txn = self.db.begin_write().map_err(io::Error::other)?;

        // If file matches compacted size (rename happened, redb has old offsets), fix offsets
        if current_size == compacted_size && current_size != original_end && !live.is_empty() {
            let mut obj_table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            let mut ver_table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
            let mut new_offset: u64 = 0;
            for entry in live {
                let mut obj = entry.meta;
                obj.offset = new_offset;
                new_offset += obj.length;
                let bytes = bincode::serialize(&obj).map_err(io::Error::other)?;
                match entry.source {
                    TableSource::Objects => {
                        obj_table
                            .insert(entry.redb_key.as_str(), bytes.as_slice())
                            .map_err(io::Error::other)?;
                    }
                    TableSource::Versions => {
                        ver_table
                            .insert(entry.redb_key.as_str(), bytes.as_slice())
                            .map_err(io::Error::other)?;
                    }
                }
            }
        }

        {
            let mut t = txn.open_table(SEG_DEAD).map_err(io::Error::other)?;
            t.insert(segment_id, 0u64).map_err(io::Error::other)?;
        }
        {
            let mut t = txn.open_table(SEG_COMPACTING).map_err(io::Error::other)?;
            t.insert(segment_id, 0u8).map_err(io::Error::other)?;
        }

        txn.commit().map_err(io::Error::other)?;
        Ok(())
    }

    /// Swap a compacted segment file and update redb metadata atomically.
    ///
    /// The per-segment write lock MUST be held across both the file swap and the redb
    /// transaction commit. Dropping it between would let readers see the new (compacted)
    /// file with old (pre-compaction) offsets — corrupted reads.
    #[allow(clippy::significant_drop_tightening)]
    fn apply_compaction(
        &self,
        seg: &RwLock<File>,
        tmp_path: &Path,
        seg_path: &Path,
        updates: &[LiveEntry],
        segment_id: u32,
    ) -> io::Result<()> {
        let mut file = seg
            .write()
            .map_err(|_| io::Error::other("segment lock poisoned"))?;
        fs::rename(tmp_path, seg_path)?;
        *file = File::open(seg_path)?;

        let txn = self.db.begin_write().map_err(io::Error::other)?;
        let mut stale_bytes: u64 = 0;
        {
            let mut obj_table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            let mut ver_table = txn.open_table(VERSIONS).map_err(io::Error::other)?;

            for entry in updates {
                let table = match entry.source {
                    TableSource::Objects => &mut obj_table,
                    TableSource::Versions => &mut ver_table,
                };
                stale_bytes +=
                    update_compacted_entry(table, entry, segment_id)?;
            }
        }
        {
            let mut t = txn.open_table(SEG_DEAD).map_err(io::Error::other)?;
            t.insert(segment_id, stale_bytes)
                .map_err(io::Error::other)?;
        }
        {
            let mut t = txn.open_table(SEG_COMPACTING).map_err(io::Error::other)?;
            t.insert(segment_id, 0u8).map_err(io::Error::other)?;
        }
        txn.commit().map_err(io::Error::other)?;
        Ok(())
    }

    /// Compact a single segment. Only locks that segment during the swap phase.
    pub fn compact_segment(&self, segment_id: u32) -> io::Result<()> {
        let start = std::time::Instant::now();
        let bucket_name = self
            .bucket_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_owned();

        let result = self.compact_segment_inner(segment_id);

        let status = if result.is_ok() { "ok" } else { "error" };
        metrics::histogram!(
            "simple3_compaction_duration_seconds",
            "bucket" => bucket_name.clone(),
            "status" => status,
        )
        .record(start.elapsed().as_secs_f64());
        metrics::counter!(
            "simple3_compaction_runs_total",
            "bucket" => bucket_name,
            "status" => status,
        )
        .increment(1);

        result
    }

    fn compact_segment_inner(&self, segment_id: u32) -> io::Result<()> {
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

        let seg_arc = self.get_segment_handle(segment_id)?;
        let seg_path = self.bucket_dir.join(segment_filename(segment_id));
        let tmp_path = self
            .bucket_dir
            .join(format!("{}.tmp", segment_filename(segment_id)));

        let updated = copy_live_objects(&seg_arc, &live, &tmp_path)?;

        self.set_seg_compacting(segment_id, true)?;

        // Phase 2: Swap file + update redb under per-segment write lock
        self.apply_compaction(&seg_arc, &tmp_path, &seg_path, &updated, segment_id)
    }

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
}

/// Copy live objects from a segment to a temp file with contiguous offsets.
fn copy_live_objects(
    seg_arc: &Arc<RwLock<File>>,
    live: &[LiveEntry],
    tmp_path: &Path,
) -> io::Result<Vec<LiveEntry>> {
    let mut tmp = File::create(tmp_path)?;
    let mut new_offset: u64 = 0;

    let file = seg_arc
        .read()
        .map_err(|_| io::Error::other("segment lock poisoned"))?;
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut entries = Vec::with_capacity(live.len());

    for entry in live {
        let mut remaining = entry.meta.length;
        let mut read_off = entry.meta.offset;
        let mut updated = entry.meta.clone();
        updated.offset = new_offset;
        new_offset += entry.meta.length;

        while remaining > 0 {
            #[allow(clippy::cast_possible_truncation)]
            let chunk = (remaining as usize).min(buf.len());
            file.read_exact_at(&mut buf[..chunk], read_off)?;
            tmp.write_all(&buf[..chunk])?;
            read_off += chunk as u64;
            remaining -= chunk as u64;
        }

        entries.push(LiveEntry {
            source: entry.source,
            redb_key: entry.redb_key.clone(),
            meta: updated,
        });
    }
    drop(file);

    tmp.flush()?;
    tmp.sync_all()?;
    drop(tmp);

    Ok(entries)
}

/// Update a single compacted entry in its table. Returns stale bytes if the
/// entry was concurrently deleted/moved, 0 if successfully updated.
fn update_compacted_entry(
    table: &mut redb::Table<&str, &[u8]>,
    entry: &LiveEntry,
    segment_id: u32,
) -> io::Result<u64> {
    let still_here = table
        .get(entry.redb_key.as_str())
        .map_err(io::Error::other)?
        .map(|v| ObjectMeta::from_bytes(v.value()))
        .transpose()
        .map_err(io::Error::other)?
        .is_some_and(|cur| cur.segment_id == segment_id);

    if still_here {
        let v = bincode::serialize(&entry.meta).map_err(io::Error::other)?;
        table
            .insert(entry.redb_key.as_str(), v.as_slice())
            .map_err(io::Error::other)?;
        Ok(0)
    } else {
        Ok(entry.meta.length)
    }
}
