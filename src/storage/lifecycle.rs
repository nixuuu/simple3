use std::io;

use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};

use crate::types::ObjectMeta;

use super::versioning::BUCKET_CONFIG;
use super::{BucketStore, OBJECTS, Storage};

/// One day in seconds.
const ONE_DAY: u64 = 86_400;

/// Per-bucket lifecycle configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecycleConfig {
    /// Days after `last_modified` before the object becomes eligible for deletion.
    /// 0 means objects are expired immediately on the next sweep — useful for tests.
    pub expiration_days: u32,
}

/// Aggregate result of a single lifecycle sweep over a bucket.
#[derive(Debug, Default, Clone, Serialize)]
pub struct LifecycleStats {
    pub bucket: String,
    pub scanned: u64,
    pub deleted: u64,
}

impl BucketStore {
    pub fn get_lifecycle(&self) -> io::Result<Option<LifecycleConfig>> {
        let txn = self.db.begin_read().map_err(io::Error::other)?;
        let table = txn.open_table(BUCKET_CONFIG).map_err(io::Error::other)?;
        match table.get("lifecycle").map_err(io::Error::other)? {
            Some(v) => {
                let cfg: LifecycleConfig =
                    serde_json::from_slice(v.value()).map_err(io::Error::other)?;
                Ok(Some(cfg))
            }
            None => Ok(None),
        }
    }

    pub fn set_lifecycle(&self, cfg: &LifecycleConfig) -> io::Result<()> {
        let bytes = serde_json::to_vec(cfg).map_err(io::Error::other)?;
        let txn = self.db.begin_write().map_err(io::Error::other)?;
        {
            let mut table = txn.open_table(BUCKET_CONFIG).map_err(io::Error::other)?;
            table
                .insert("lifecycle", bytes.as_slice())
                .map_err(io::Error::other)?;
        }
        txn.commit().map_err(io::Error::other)?;
        Ok(())
    }

    pub fn delete_lifecycle(&self) -> io::Result<bool> {
        let txn = self.db.begin_write().map_err(io::Error::other)?;
        let removed = {
            let mut table = txn.open_table(BUCKET_CONFIG).map_err(io::Error::other)?;
            table
                .remove("lifecycle")
                .map_err(io::Error::other)?
                .is_some()
        };
        txn.commit().map_err(io::Error::other)?;
        Ok(removed)
    }

    /// Apply the bucket's lifecycle rule, deleting all expired live objects.
    ///
    /// "Expired" means `last_modified + expiration_days * 86400 <= now`. Delete markers
    /// and historical versions are not touched here — only currently-listed objects.
    /// Returns the scan and delete counters; `(0, 0)` when no rule is configured.
    pub fn apply_lifecycle(&self, now: u64) -> io::Result<LifecycleStats> {
        let Some(cfg) = self.get_lifecycle()? else {
            return Ok(LifecycleStats::default());
        };

        let threshold = now.saturating_sub(u64::from(cfg.expiration_days).saturating_mul(ONE_DAY));

        // Scan together with last_modified, then delete only if the per-key
        // snapshot still matches the (key, last_modified) we observed. A
        // concurrent overwrite bumps last_modified and we leave it alone.
        let candidates: Vec<(String, u64)> = {
            let txn = self.db.begin_read().map_err(io::Error::other)?;
            let table = txn.open_table(OBJECTS).map_err(io::Error::other)?;
            let mut acc = Vec::new();
            for entry in table.iter().map_err(io::Error::other)? {
                let (k, v) = entry.map_err(io::Error::other)?;
                let meta = ObjectMeta::from_bytes(v.value()).map_err(io::Error::other)?;
                if meta.is_delete_marker {
                    continue;
                }
                if meta.last_modified <= threshold {
                    acc.push((k.value().to_owned(), meta.last_modified));
                }
            }
            acc
        };

        let mut stats = LifecycleStats {
            scanned: candidates.len() as u64,
            ..Default::default()
        };
        for (key, expected_last_modified) in candidates {
            // Re-check inside a fresh read before deleting so a concurrent
            // overwrite that bumped last_modified is preserved.
            let current = self.get_meta(&key)?;
            let should_delete = current.as_ref().is_some_and(|m| {
                !m.is_delete_marker
                    && m.last_modified == expected_last_modified
                    && m.last_modified <= threshold
            });
            if !should_delete {
                continue;
            }
            // delete_object honors versioning — produces a delete marker when enabled
            // and increments dead bytes for the unversioned case.
            self.delete_object(&key)?;
            stats.deleted += 1;
        }
        Ok(stats)
    }
}

impl Storage {
    /// Run lifecycle sweep over every bucket and return per-bucket stats.
    /// Buckets without a configured rule are skipped silently.
    pub fn apply_lifecycle_all(&self, now: u64) -> io::Result<Vec<LifecycleStats>> {
        let names = self.list_buckets()?;
        let mut results = Vec::new();
        for name in names {
            let Some(store) = self.get_bucket(&name)? else {
                continue;
            };
            if store.get_lifecycle()?.is_none() {
                continue;
            }
            let mut stats = store.apply_lifecycle(now)?;
            stats.bucket = name;
            results.push(stats);
        }
        Ok(results)
    }
}
