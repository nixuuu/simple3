use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use simple3::storage::Storage;

/// Drop-guard that decrements the active HTTP connections gauge on drop.
pub struct ConnectionGuard;

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        metrics::gauge!("simple3_connections_active").decrement(1.0);
    }
}

/// Update per-bucket storage gauges and zero out gauges for deleted buckets.
pub fn update_storage_gauges(storage: &Storage, prev_buckets: &mut HashSet<String>) {
    let Ok(buckets) = storage.list_buckets() else {
        return;
    };
    let current: HashSet<String> = buckets.iter().cloned().collect();

    // Zero out gauges for deleted buckets
    for removed in prev_buckets.difference(&current) {
        let b: &str = removed;
        metrics::gauge!("simple3_segments_total", "bucket" => b.to_owned()).set(0.0);
        metrics::gauge!("simple3_dead_bytes", "bucket" => b.to_owned()).set(0.0);
        metrics::gauge!("simple3_dead_space_ratio", "bucket" => b.to_owned()).set(0.0);
    }

    for name in &buckets {
        let Some(store) = storage.get_bucket(name).ok().flatten() else {
            continue;
        };
        let Ok(stats) = store.segment_stats() else {
            continue;
        };
        let seg_count = stats.len();
        let total_dead: u64 = stats.iter().map(|s| s.dead_bytes).sum();
        let total_size: u64 = stats.iter().map(|s| s.size).sum();

        #[allow(clippy::cast_precision_loss)] // u64 -> f64: gauge values; sub-ULP loss irrelevant for monitoring
        let ratio = if total_size > 0 {
            total_dead as f64 / total_size as f64
        } else {
            0.0
        };

        let bucket = name.clone();
        #[allow(clippy::cast_precision_loss)] // u64/usize -> f64: gauge values; sub-ULP loss irrelevant
        {
            metrics::gauge!("simple3_segments_total", "bucket" => bucket.clone())
                .set(seg_count as f64);
            metrics::gauge!("simple3_dead_bytes", "bucket" => bucket.clone())
                .set(total_dead as f64);
        }
        metrics::gauge!("simple3_dead_space_ratio", "bucket" => bucket).set(ratio);
    }

    *prev_buckets = current;
}

/// Spawn a background task that updates storage gauges every 15 seconds.
pub fn spawn_metrics_updater(
    storage: &Arc<Storage>,
    shutdown_rx: &watch::Receiver<bool>,
) -> JoinHandle<()> {
    let storage = Arc::clone(storage);
    let mut rx = shutdown_rx.clone();
    tokio::spawn(async move {
        let mut prev_buckets = HashSet::new();
        loop {
            {
                let s = Arc::clone(&storage);
                let mut pb = std::mem::take(&mut prev_buckets);
                let result = tokio::task::spawn_blocking(move || {
                    update_storage_gauges(&s, &mut pb);
                    pb
                })
                .await;
                if let Ok(pb) = result {
                    prev_buckets = pb;
                }
            }
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(15)) => {}
                _ = rx.changed() => break,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_storage_gauges_sets_values() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        storage.create_bucket("metrics-test").unwrap();

        let mut prev = HashSet::new();
        update_storage_gauges(&storage, &mut prev);

        assert_eq!(prev.len(), 1);
        assert!(prev.contains("metrics-test"));
    }

    #[test]
    fn test_update_storage_gauges_cleans_deleted_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        storage.create_bucket("alive").unwrap();
        storage.create_bucket("doomed").unwrap();

        let mut prev = HashSet::new();
        update_storage_gauges(&storage, &mut prev);
        assert_eq!(prev.len(), 2);

        // Delete one bucket
        storage.delete_bucket("doomed").unwrap();
        update_storage_gauges(&storage, &mut prev);

        // prev_buckets should now only contain "alive"
        assert_eq!(prev.len(), 1);
        assert!(prev.contains("alive"));
        assert!(!prev.contains("doomed"));
    }

    #[test]
    fn test_prometheus_render_contains_metrics() {
        // Build a recorder + handle without installing globally
        let recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        // Install so metrics macros work (may fail if another test already installed)
        let _ = metrics::set_global_recorder(recorder);

        // Record a metric via the facade
        metrics::histogram!(
            "simple3_request_duration_seconds",
            "method" => "S3",
            "operation" => "PutObject",
        )
        .record(0.042);

        metrics::counter!("simple3_bytes_received_total").increment(1024);

        let output = handle.render();
        assert!(
            output.contains("simple3_request_duration_seconds"),
            "render output should contain histogram: {output}"
        );
        assert!(
            output.contains("simple3_bytes_received_total"),
            "render output should contain counter: {output}"
        );
    }
}
