use std::fmt::Write;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::types::ObjectMeta;

use super::BucketStore;

pub const VERSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("versions");
pub const BUCKET_CONFIG: TableDefinition<&str, &[u8]> = TableDefinition::new("bucket_config");

/// Monotonic counter to guarantee unique version IDs even within the same nanosecond.
static VERSION_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Generate a globally unique, time-ordered version ID.
///
/// Format: 16-char zero-padded hex nanoseconds + 4-char zero-padded hex counter = 20 chars.
/// Lexicographic ordering matches chronological ordering.
#[allow(clippy::cast_possible_truncation)] // nanos won't exceed u64 until year 2554
pub fn generate_version_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    // Relaxed is fine: fetch_add returns a unique value per call regardless of ordering.
    // Counter wraps at u32::MAX — practically impossible within a single nanosecond
    // since the writer Mutex serializes all writes.
    let seq = VERSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut s = String::with_capacity(20);
    write!(s, "{nanos:016x}{seq:04x}").expect("write to String never fails");
    s
}

/// Null version ID for objects in Suspended versioning mode.
pub const NULL_VERSION_ID: &str = "null";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersioningState {
    Enabled,
    Suspended,
}

/// Entry in a version listing (current + historical versions).
pub struct VersionEntry {
    pub key: String,
    pub version_id: String,
    pub meta: ObjectMeta,
    pub is_latest: bool,
}

/// Result of listing object versions.
pub struct ListVersionsResult {
    pub entries: Vec<VersionEntry>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

/// Build the composite key for the versions table: `"object_key\0version_id"`.
pub fn version_key(key: &str, version_id: &str) -> String {
    format!("{key}\0{version_id}")
}

/// Split a composite versions-table key into `(object_key, version_id)`.
fn split_version_key(composite: &str) -> Option<(&str, &str)> {
    composite.split_once('\0')
}

#[allow(clippy::missing_errors_doc)]
impl BucketStore {
    // === Versioning configuration ===

    pub fn get_versioning_state(&self) -> io::Result<Option<VersioningState>> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(BUCKET_CONFIG).map_err(io::Error::other)?;
        match table.get("versioning").map_err(io::Error::other)? {
            Some(v) => {
                let state: VersioningState =
                    serde_json::from_slice(v.value()).map_err(io::Error::other)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    pub fn set_versioning_state(&self, state: VersioningState) -> io::Result<()> {
        let bytes = serde_json::to_vec(&state).map_err(io::Error::other)?;
        let txn = self.db.begin_write().map_err(io::Error::other)?;
        {
            let mut table = txn.open_table(BUCKET_CONFIG).map_err(io::Error::other)?;
            table
                .insert("versioning", bytes.as_slice())
                .map_err(io::Error::other)?;
        }
        txn.commit().map_err(io::Error::other)?;
        Ok(())
    }

    /// Read versioning state inside an existing write transaction (TOCTOU-safe).
    fn read_versioning_in_txn(
        txn: &redb::WriteTransaction,
    ) -> io::Result<Option<VersioningState>> {
        let table = txn.open_table(BUCKET_CONFIG).map_err(io::Error::other)?;
        match table.get("versioning").map_err(io::Error::other)? {
            Some(v) => {
                let state: VersioningState =
                    serde_json::from_slice(v.value()).map_err(io::Error::other)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Atomically insert/update an object with versioning handled inside a
    /// single write transaction (no TOCTOU race on versioning state).
    pub(super) fn commit_put_versioned(
        &self,
        key: &str,
        meta: &mut ObjectMeta,
    ) -> io::Result<()> {
        let txn = self.db.begin_write().map_err(io::Error::other)?;
        let vs = Self::read_versioning_in_txn(&txn)?;

        // Assign version_id based on versioning state
        match vs {
            Some(VersioningState::Enabled) => {
                meta.version_id = Some(generate_version_id());
            }
            Some(VersioningState::Suspended) => {
                meta.version_id = Some(NULL_VERSION_ID.to_owned());
            }
            None => {
                meta.version_id = None;
            }
        }

        let v = bincode::serialize(&*meta).map_err(io::Error::other)?;

        let old_meta = {
            let mut table = txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
            let old = table
                .get(key)
                .map_err(io::Error::other)?
                .map(|v| ObjectMeta::from_bytes(v.value()))
                .transpose()
                .map_err(io::Error::other)?;
            table
                .insert(key, v.as_slice())
                .map_err(io::Error::other)?;
            old
        };

        if let Some(old) = old_meta {
            Self::handle_old_version_in_txn(&txn, key, &old, vs)?;
        }

        txn.commit().map_err(io::Error::other)?;
        Ok(())
    }

    /// Handle the old version during a put/delete: move to versions table or
    /// mark as dead, depending on versioning state. Called inside a write txn.
    fn handle_old_version_in_txn(
        txn: &redb::WriteTransaction,
        key: &str,
        old: &ObjectMeta,
        vs: Option<VersioningState>,
    ) -> io::Result<()> {
        match vs {
            Some(VersioningState::Enabled) => {
                // Move old to versions table (data stays live)
                let old_vid = old.version_id.as_deref()
                    .unwrap_or(NULL_VERSION_ID);
                let old_bytes = bincode::serialize(old).map_err(io::Error::other)?;
                let composite = version_key(key, old_vid);
                let mut ver_table =
                    txn.open_table(VERSIONS).map_err(io::Error::other)?;
                ver_table
                    .insert(composite.as_str(), old_bytes.as_slice())
                    .map_err(io::Error::other)?;
            }
            Some(VersioningState::Suspended) => {
                let old_vid = old.version_id.as_deref()
                    .unwrap_or(NULL_VERSION_ID);
                if old_vid == NULL_VERSION_ID {
                    // Overwriting null version — old data becomes dead
                    if !old.is_delete_marker && old.length > 0 {
                        let mut dead =
                            txn.open_table(super::SEG_DEAD).map_err(io::Error::other)?;
                        let current = dead
                            .get(old.segment_id)
                            .map_err(io::Error::other)?
                            .map_or(0, |g| g.value());
                        dead.insert(old.segment_id, current + old.length)
                            .map_err(io::Error::other)?;
                    }
                } else {
                    // Named version — move to versions table (data stays live)
                    let old_bytes =
                        bincode::serialize(old).map_err(io::Error::other)?;
                    let composite = version_key(key, old_vid);
                    let mut ver_table =
                        txn.open_table(VERSIONS).map_err(io::Error::other)?;
                    ver_table
                        .insert(composite.as_str(), old_bytes.as_slice())
                        .map_err(io::Error::other)?;
                }
            }
            None => {
                // No versioning — old data becomes dead
                let mut dead = txn.open_table(super::SEG_DEAD).map_err(io::Error::other)?;
                let current = dead
                    .get(old.segment_id)
                    .map_err(io::Error::other)?
                    .map_or(0, |g| g.value());
                dead.insert(old.segment_id, current + old.length)
                    .map_err(io::Error::other)?;
            }
        }
        Ok(())
    }

    /// Delete an object with versioning handled inside a single write
    /// transaction (no TOCTOU race on versioning state).
    pub(super) fn delete_object_versioned(
        &self,
        key: &str,
    ) -> io::Result<Option<ObjectMeta>> {
        let txn = self.db.begin_write().map_err(io::Error::other)?;
        let vs = Self::read_versioning_in_txn(&txn)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let result = match vs {
            None => {
                // No versioning — hard delete (original behavior)
                let meta = {
                    let mut table =
                        txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
                    match table.remove(key).map_err(io::Error::other)? {
                        Some(v) => {
                            ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?
                        }
                        // Safe early return: no modifications made, transaction rolled back on drop
                        None => return Ok(None),
                    }
                };
                {
                    let mut dead =
                        txn.open_table(super::SEG_DEAD).map_err(io::Error::other)?;
                    let current = dead
                        .get(meta.segment_id)
                        .map_err(io::Error::other)?
                        .map_or(0, |g| g.value());
                    dead.insert(meta.segment_id, current + meta.length)
                        .map_err(io::Error::other)?;
                }
                Some(meta)
            }
            Some(state) => {
                // Versioned — insert delete marker
                let marker_vid = match state {
                    VersioningState::Enabled => generate_version_id(),
                    VersioningState::Suspended => NULL_VERSION_ID.to_owned(),
                };
                let marker = ObjectMeta::delete_marker(Some(marker_vid), now);
                let marker_bytes =
                    bincode::serialize(&marker).map_err(io::Error::other)?;

                // Read current object
                let old_meta = {
                    let table =
                        txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
                    table
                        .get(key)
                        .map_err(io::Error::other)?
                        .map(|v| ObjectMeta::from_bytes(v.value()))
                        .transpose()
                        .map_err(io::Error::other)?
                };

                // Move current to versions table if it exists
                if let Some(ref old) = old_meta {
                    Self::handle_old_version_in_txn(
                        &txn, key, old, Some(state),
                    )?;
                }

                // Insert delete marker as current
                {
                    let mut table =
                        txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
                    table
                        .insert(key, marker_bytes.as_slice())
                        .map_err(io::Error::other)?;
                }

                Some(marker)
            }
        };

        txn.commit().map_err(io::Error::other)?;
        Ok(result)
    }

    // === Version read/write ===

    /// Retrieve a specific version of an object from the versions table.
    pub fn get_version(&self, key: &str, vid: &str) -> io::Result<Option<ObjectMeta>> {
        let composite = version_key(key, vid);
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
        match table.get(composite.as_str()).map_err(io::Error::other)? {
            Some(v) => {
                let meta = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    /// Look up an object by key, optionally for a specific version, in a single
    /// read transaction (avoids TOCTOU between versions and objects table).
    /// Returns the meta if found, None otherwise. Does NOT check `is_delete_marker`.
    pub fn get_object_or_version(
        &self,
        key: &str,
        version_id: Option<&str>,
    ) -> io::Result<Option<ObjectMeta>> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        if let Some(vid) = version_id {
            // Check versions table first
            let ver_table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
            let composite = version_key(key, vid);
            if let Some(v) = ver_table.get(composite.as_str()).map_err(io::Error::other)? {
                return Ok(Some(
                    ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?,
                ));
            }
            // Fall back to current object if version_id matches
            let obj_table = txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
            if let Some(v) = obj_table.get(key).map_err(io::Error::other)? {
                let meta = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                if meta.version_id.as_deref() == Some(vid) {
                    return Ok(Some(meta));
                }
            }
            Ok(None)
        } else {
            let obj_table = txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
            match obj_table.get(key).map_err(io::Error::other)? {
                Some(v) => Ok(Some(
                    ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?,
                )),
                None => Ok(None),
            }
        }
    }

    /// Permanently delete a specific version. Returns the deleted meta if found.
    /// Increments dead bytes for the version's segment data (unless delete marker).
    pub fn delete_version(&self, key: &str, vid: &str) -> io::Result<Option<ObjectMeta>> {
        let composite = version_key(key, vid);
        let txn = self.db.begin_write().map_err(io::Error::other)?;

        let meta = {
            let mut table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
            match table.remove(composite.as_str()).map_err(io::Error::other)? {
                Some(v) => ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?,
                None => return Ok(None),
            }
        };

        // Delete markers have no segment data (segment_id=0, length=0).
        if !meta.is_delete_marker && meta.length > 0 {
            let mut dead = txn.open_table(super::SEG_DEAD).map_err(io::Error::other)?;
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

    /// Delete a specific version that is the current object in the objects table.
    /// Used when DELETE with versionId targets the current version.
    /// Returns the deleted meta. Promotes the latest non-delete-marker version if available.
    pub fn delete_current_version(
        &self,
        key: &str,
        vid: &str,
    ) -> io::Result<Option<ObjectMeta>> {
        let txn = self.db.begin_write().map_err(io::Error::other)?;

        let meta = {
            let table = txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
            match table.get(key).map_err(io::Error::other)? {
                Some(v) => {
                    let m = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                    if m.version_id.as_deref() != Some(vid) {
                        return Ok(None);
                    }
                    m
                }
                None => return Ok(None),
            }
        };

        // Remove from objects table
        {
            let mut table = txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
            table.remove(key).map_err(io::Error::other)?;
        }

        // Track dead bytes for non-delete-marker data
        if !meta.is_delete_marker && meta.length > 0 {
            let mut dead = txn.open_table(super::SEG_DEAD).map_err(io::Error::other)?;
            let current = dead
                .get(meta.segment_id)
                .map_err(io::Error::other)?
                .map_or(0, |g| g.value());
            dead.insert(meta.segment_id, current + meta.length)
                .map_err(io::Error::other)?;
        }

        // If this was a delete marker, promote the latest version (undelete)
        if meta.is_delete_marker {
            Self::promote_latest_version_in_txn(&txn, key)?;
        }

        txn.commit().map_err(io::Error::other)?;
        Ok(Some(meta))
    }

    /// Within an active write transaction, promote the latest non-delete-marker
    /// version from the versions table to the objects table.
    fn promote_latest_version_in_txn(
        txn: &redb::WriteTransaction,
        key: &str,
    ) -> io::Result<()> {
        let prefix_start = format!("{key}\0");
        let prefix_end = format!("{key}\x01"); // \x01 > \0, so this bounds all versions for key

        // Collect versions for this key. redb stores entries in ascending key order
        // and version IDs are lexicographically time-ordered, so the last entry is the latest.
        let mut versions: Vec<(String, ObjectMeta)> = Vec::new();
        {
            let table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
            let range = table
                .range(prefix_start.as_str()..prefix_end.as_str())
                .map_err(io::Error::other)?;
            for entry in range {
                let (k, v) = entry.map_err(io::Error::other)?;
                let composite = k.value().to_owned();
                let m = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                versions.push((composite, m));
            }
        }

        // Iterate in reverse (latest first) — no sort needed, redb ascending order is sufficient
        let promoted = versions
            .iter()
            .rev()
            .find(|(_, m)| !m.is_delete_marker);

        if let Some((composite_key, meta)) = promoted {
            // Move to objects table
            let bytes = bincode::serialize(meta).map_err(io::Error::other)?;
            {
                let mut obj_table =
                    txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
                obj_table
                    .insert(key, bytes.as_slice())
                    .map_err(io::Error::other)?;
            }
            // Remove from versions table
            {
                let mut ver_table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
                ver_table
                    .remove(composite_key.as_str())
                    .map_err(io::Error::other)?;
            }
        }

        Ok(())
    }

    /// Check if the versions table has any entries (used for bucket emptiness check).
    pub fn has_versions(&self) -> io::Result<bool> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
        let empty = table.is_empty().map_err(io::Error::other)?;
        Ok(!empty)
    }

    // === List object versions ===

    #[allow(clippy::too_many_lines)] // aggregates two tables + sort + paginate + delimiter in one pass
    pub fn list_object_versions(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: usize,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
    ) -> io::Result<ListVersionsResult> {
        use std::collections::BTreeSet;

        let txn = self.db.begin_read().map_err(io::Error::other)?;

        // Collect all version entries: current objects + versions table
        let mut all_entries: Vec<(String, String, ObjectMeta)> = Vec::new(); // (key, vid, meta)

        let prefix_str = prefix.unwrap_or("");

        // Use marker-based range start to skip entries before the pagination point.
        // For OBJECTS table, start at key_marker (or prefix) to skip early keys.
        let range_start = match (key_marker, prefix) {
            (Some(km), Some(pfx)) => {
                if km >= pfx { km } else { pfx }
            }
            (Some(km), None) => km,
            (_, Some(pfx)) => pfx,
            _ => "",
        };

        // 1. Current objects from OBJECTS table
        {
            let table = txn.open_table(super::OBJECTS).map_err(io::Error::other)?;
            let iter = if range_start.is_empty() {
                table.iter().map_err(io::Error::other)?
            } else {
                table.range(range_start..).map_err(io::Error::other)?
            };
            for result in iter {
                let (k, v) = result.map_err(io::Error::other)?;
                let key = k.value().to_owned();

                if !prefix_str.is_empty() && !key.starts_with(prefix_str) {
                    break;
                }

                let meta = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                let vid = meta.version_id.clone().unwrap_or_else(|| NULL_VERSION_ID.to_owned());
                all_entries.push((key, vid, meta));
            }
        }

        // 2. Historical versions from VERSIONS table
        //    Version keys are "objkey\0vid"; start range at marker key to skip early entries.
        {
            let table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
            let iter = if range_start.is_empty() {
                table.iter().map_err(io::Error::other)?
            } else {
                table.range(range_start..).map_err(io::Error::other)?
            };
            for result in iter {
                let (k, v) = result.map_err(io::Error::other)?;
                let composite = k.value();
                let Some((key, vid)) = split_version_key(composite) else {
                    continue;
                };

                if !prefix_str.is_empty() && !key.starts_with(prefix_str) {
                    break;
                }

                let meta = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                all_entries.push((key.to_owned(), vid.to_owned(), meta));
            }
        }

        // Sort: key ascending, then version_id descending (latest first)
        all_entries.sort_by(|a, b| {
            let key_cmp = a.0.cmp(&b.0);
            if key_cmp != std::cmp::Ordering::Equal {
                return key_cmp;
            }
            b.1.cmp(&a.1) // descending version_id
        });

        // Apply key_marker / version_id_marker for pagination
        let start_idx = key_marker.map_or(0, |km| {
            let vim = version_id_marker.unwrap_or("");
            all_entries
                .iter()
                .position(|(k, v, _)| (k.as_str(), v.as_str()) > (km, vim))
                .unwrap_or(all_entries.len())
        });

        // Handle delimiter for common prefixes
        let mut common_prefixes = BTreeSet::new();
        let mut entries = Vec::new();
        let mut count = 0;
        let mut is_truncated = false;
        let mut next_key_marker = None;
        let mut next_version_id_marker = None;

        for (key, vid, meta) in all_entries.into_iter().skip(start_idx) {
            if count >= max_keys {
                is_truncated = true;
                // Markers point to the LAST returned entry (AWS S3 style)
                if let Some(last) = entries.last() {
                    let last: &VersionEntry = last;
                    next_key_marker = Some(last.key.clone());
                    next_version_id_marker = Some(last.version_id.clone());
                }
                break;
            }

            // Delimiter handling
            if let (Some(delim), Some(pfx)) = (delimiter, prefix) {
                let after_prefix = &key[pfx.len()..];
                if let Some(pos) = after_prefix.find(delim) {
                    let cp = format!("{pfx}{}", &after_prefix[..(pos + delim.len())]);
                    common_prefixes.insert(cp);
                    count += 1;
                    continue;
                }
            } else if let Some(delim) = delimiter && let Some(pos) = key.find(delim) {
                let cp = key[..(pos + delim.len())].to_owned();
                common_prefixes.insert(cp);
                count += 1;
                continue;
            }

            // First entry per key is latest (entries sorted by key asc, vid desc)
            let is_latest = entries
                .last()
                .is_none_or(|e: &VersionEntry| e.key != key);

            entries.push(VersionEntry {
                key,
                version_id: vid,
                meta,
                is_latest,
            });
            count += 1;
        }

        Ok(ListVersionsResult {
            entries,
            common_prefixes: common_prefixes.into_iter().collect(),
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    /// Count versions and delete markers in the versions table (for stats).
    pub fn version_stats(&self) -> io::Result<(u64, u64, u64)> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(VERSIONS).map_err(io::Error::other)?;
        let mut version_count: u64 = 0;
        let mut delete_marker_count: u64 = 0;
        let mut total_versioned_size: u64 = 0;
        for result in table.iter().map_err(io::Error::other)? {
            let (_, v) = result.map_err(io::Error::other)?;
            let meta = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
            if meta.is_delete_marker {
                delete_marker_count += 1;
            } else {
                version_count += 1;
                total_versioned_size += meta.length;
            }
        }
        Ok((version_count, delete_marker_count, total_versioned_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_version_id_format() {
        let vid = generate_version_id();
        assert_eq!(vid.len(), 20);
        // Should be valid hex
        assert!(vid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_generate_version_id_unique() {
        let a = generate_version_id();
        let b = generate_version_id();
        assert_ne!(a, b);
    }

    #[test]
    fn test_generate_version_id_ordering() {
        let a = generate_version_id();
        let b = generate_version_id();
        // b should be >= a (time-ordered + counter)
        assert!(b >= a);
    }

    #[test]
    fn test_version_key_roundtrip() {
        let composite = version_key("my/object.txt", "0000019706a3f80000a1");
        let (key, vid) = split_version_key(&composite).unwrap();
        assert_eq!(key, "my/object.txt");
        assert_eq!(vid, "0000019706a3f80000a1");
    }

    #[test]
    fn test_version_key_ordering() {
        // Same key, different versions — should sort by version
        let a = version_key("key", "0000000000000001");
        let b = version_key("key", "0000000000000002");
        assert!(a < b);

        // Different keys — should sort by key first
        let c = version_key("aaa", "9999999999999999");
        let d = version_key("bbb", "0000000000000000");
        assert!(c < d);
    }
}
