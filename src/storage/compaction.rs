use std::fs::{self, File};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, RwLock};

use redb::{ReadableDatabase, ReadableTable};

use crate::types::ObjectMeta;

use super::{BucketStore, COPY_BUF_SIZE, OBJECTS, SEG_COMPACTING, SEG_DEAD, segment_filename};

#[allow(clippy::missing_errors_doc)]
impl BucketStore {
    pub(super) fn collect_live_objects_for_segment(
        &self,
        segment_id: u32,
    ) -> io::Result<Vec<(String, ObjectMeta)>> {
        let mut live = Vec::new();
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
        for result in table.iter().map_err(io::Error::other)? {
            let (k, v) = result.map_err(io::Error::other)?;
            let obj: ObjectMeta =
                bincode::deserialize(v.value()).map_err(io::Error::other)?;
            if obj.segment_id == segment_id {
                live.push((k.value().to_owned(), obj));
            }
        }
        live.sort_by_key(|(_, m)| m.offset);
        Ok(live)
    }

    pub(super) fn recover_segment_compaction(&self, segment_id: u32) -> io::Result<()> {
        let live = self.collect_live_objects_for_segment(segment_id)?;
        let compacted_size: u64 = live.iter().map(|(_, obj)| obj.length).sum();
        let seg_path = self.bucket_dir.join(segment_filename(segment_id));
        let current_size = fs::metadata(&seg_path).map(|m| m.len()).unwrap_or(0);
        let original_end = live
            .iter()
            .map(|(_, obj)| obj.offset + obj.length)
            .max()
            .unwrap_or(0);

        let txn = self.db.begin_write().map_err(io::Error::other)?;

        // If file matches compacted size (rename happened, redb has old offsets), fix offsets
        if current_size == compacted_size && current_size != original_end && !live.is_empty() {
            let mut table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            let mut new_offset: u64 = 0;
            for (key, mut obj) in live {
                obj.offset = new_offset;
                new_offset += obj.length;
                let bytes = bincode::serialize(&obj).map_err(io::Error::other)?;
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(io::Error::other)?;
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
        updates: &[(String, ObjectMeta)],
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
            let mut table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            for (key, obj) in updates {
                // Check current state — object may have been deleted or moved
                // between collect_live_objects and this write transaction.
                let still_here = table
                    .get(key.as_str())
                    .map_err(io::Error::other)?
                    .map(|v| bincode::deserialize::<ObjectMeta>(v.value()))
                    .transpose()
                    .map_err(io::Error::other)?
                    .is_some_and(|cur| cur.segment_id == segment_id);

                if still_here {
                    let v = bincode::serialize(obj).map_err(io::Error::other)?;
                    table
                        .insert(key.as_str(), v.as_slice())
                        .map_err(io::Error::other)?;
                } else {
                    stale_bytes += obj.length;
                }
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
    live: &[(String, ObjectMeta)],
    tmp_path: &Path,
) -> io::Result<Vec<(String, ObjectMeta)>> {
    let mut tmp = File::create(tmp_path)?;
    let mut new_offset: u64 = 0;

    let file = seg_arc
        .read()
        .map_err(|_| io::Error::other("segment lock poisoned"))?;
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut entries = Vec::with_capacity(live.len());

    for (key, obj) in live {
        let mut remaining = obj.length;
        let mut read_off = obj.offset;
        let mut updated = obj.clone();
        updated.offset = new_offset;
        new_offset += obj.length;

        while remaining > 0 {
            #[allow(clippy::cast_possible_truncation)]
            let chunk = (remaining as usize).min(buf.len());
            file.read_exact_at(&mut buf[..chunk], read_off)?;
            tmp.write_all(&buf[..chunk])?;
            read_off += chunk as u64;
            remaining -= chunk as u64;
        }

        entries.push((key.clone(), updated));
    }
    drop(file);

    tmp.flush()?;
    tmp.sync_all()?;
    drop(tmp);

    Ok(entries)
}
