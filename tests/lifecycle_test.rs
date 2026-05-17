//! Integration tests for per-bucket lifecycle policies (#16).

use std::time::{SystemTime, UNIX_EPOCH};

use simple3::storage::{LifecycleConfig, Storage};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[test]
fn lifecycle_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_bucket("b1").unwrap();
    let store = storage.get_bucket("b1").unwrap().unwrap();

    assert!(store.get_lifecycle().unwrap().is_none());

    let cfg = LifecycleConfig { expiration_days: 7 };
    store.set_lifecycle(&cfg).unwrap();
    let read = store.get_lifecycle().unwrap().unwrap();
    assert_eq!(read.expiration_days, 7);

    assert!(store.delete_lifecycle().unwrap());
    assert!(store.get_lifecycle().unwrap().is_none());
    assert!(!store.delete_lifecycle().unwrap());
}

#[test]
fn lifecycle_zero_day_expires_all_live_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_bucket("b1").unwrap();
    let store = storage.get_bucket("b1").unwrap().unwrap();

    // Put three objects directly via storage.
    for key in ["a", "b", "c"] {
        let tmp_file = tmp.path().join(format!(".tmp_{key}"));
        std::fs::write(&tmp_file, b"hello").unwrap();
        store
            .put_object_streamed(
                key,
                &tmp_file,
                Some("text/plain".into()),
                "etag".into(),
                now() - 10,
                std::collections::HashMap::new(),
                None,
            )
            .unwrap();
    }
    assert_eq!(store.list_objects(None, 100, None).unwrap().0.len(), 3);

    store
        .set_lifecycle(&LifecycleConfig { expiration_days: 0 })
        .unwrap();
    let stats = store.apply_lifecycle(now()).unwrap();
    assert_eq!(stats.scanned, 3);
    assert_eq!(stats.deleted, 3);
    assert_eq!(store.list_objects(None, 100, None).unwrap().0.len(), 0);
}

#[test]
fn lifecycle_threshold_preserves_recent_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_bucket("b1").unwrap();
    let store = storage.get_bucket("b1").unwrap().unwrap();

    let n = now();
    // Old object (10 days ago) and fresh object (1 hour ago).
    let pairs = [("old", n - 10 * 86_400), ("fresh", n - 3_600)];
    for (key, last_modified) in pairs {
        let tmp_file = tmp.path().join(format!(".tmp_{key}"));
        std::fs::write(&tmp_file, b"x").unwrap();
        store
            .put_object_streamed(
                key,
                &tmp_file,
                None,
                "etag".into(),
                last_modified,
                std::collections::HashMap::new(),
                None,
            )
            .unwrap();
    }

    store
        .set_lifecycle(&LifecycleConfig { expiration_days: 7 })
        .unwrap();
    let stats = store.apply_lifecycle(n).unwrap();
    assert_eq!(stats.deleted, 1);

    let remaining: Vec<_> = store
        .list_objects(None, 100, None)
        .unwrap()
        .0
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(remaining, vec!["fresh".to_owned()]);
}

#[test]
fn lifecycle_unconfigured_bucket_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_bucket("b1").unwrap();
    let store = storage.get_bucket("b1").unwrap().unwrap();

    let tmp_file = tmp.path().join(".tmp_x");
    std::fs::write(&tmp_file, b"x").unwrap();
    store
        .put_object_streamed(
            "k",
            &tmp_file,
            None,
            "etag".into(),
            now() - 10_000_000,
            std::collections::HashMap::new(),
            None,
        )
        .unwrap();

    let stats = store.apply_lifecycle(now()).unwrap();
    assert_eq!(stats.scanned, 0);
    assert_eq!(stats.deleted, 0);
    assert_eq!(store.list_objects(None, 100, None).unwrap().0.len(), 1);
}

#[test]
fn lifecycle_apply_all_skips_unconfigured() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_bucket("with_rule").unwrap();
    storage.create_bucket("without_rule").unwrap();

    let with_store = storage.get_bucket("with_rule").unwrap().unwrap();
    with_store
        .set_lifecycle(&LifecycleConfig { expiration_days: 0 })
        .unwrap();
    let tmp_file = tmp.path().join(".tmp_z");
    std::fs::write(&tmp_file, b"x").unwrap();
    with_store
        .put_object_streamed(
            "k",
            &tmp_file,
            None,
            "etag".into(),
            now() - 60,
            std::collections::HashMap::new(),
            None,
        )
        .unwrap();

    let results = storage.apply_lifecycle_all(now()).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].bucket, "with_rule");
    assert_eq!(results[0].deleted, 1);
}

#[tokio::test]
async fn lifecycle_compaction_reclaims_space_for_expired_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_bucket("b1").unwrap();
    let store = storage.get_bucket("b1").unwrap().unwrap();

    let payload = vec![0u8; 64 * 1024];
    for i in 0..4 {
        let tmp_file = tmp.path().join(format!(".tmp_{i}"));
        std::fs::write(&tmp_file, &payload).unwrap();
        store
            .put_object_streamed(
                &format!("k{i}"),
                &tmp_file,
                None,
                "etag".into(),
                now() - 86_400,
                std::collections::HashMap::new(),
                None,
            )
            .unwrap();
    }
    let pre = store
        .segment_stats()
        .unwrap()
        .into_iter()
        .map(|s| s.dead_bytes)
        .sum::<u64>();
    assert_eq!(pre, 0);

    store
        .set_lifecycle(&LifecycleConfig { expiration_days: 0 })
        .unwrap();
    store.apply_lifecycle(now()).unwrap();

    let post = store
        .segment_stats()
        .unwrap()
        .into_iter()
        .map(|s| s.dead_bytes)
        .sum::<u64>();
    assert!(post > pre, "dead bytes should grow after lifecycle delete");
}

#[tokio::test]
async fn lifecycle_versioned_bucket_creates_delete_markers() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::open(tmp.path()).unwrap();
    storage.create_bucket("b1").unwrap();
    let store = storage.get_bucket("b1").unwrap().unwrap();
    store
        .set_versioning_state(simple3::storage::versioning::VersioningState::Enabled)
        .unwrap();

    let tmp_file = tmp.path().join(".tmp_v");
    std::fs::write(&tmp_file, b"data").unwrap();
    store
        .put_object_streamed(
            "k",
            &tmp_file,
            None,
            "etag".into(),
            now() - 86_400,
            std::collections::HashMap::new(),
            None,
        )
        .unwrap();

    store
        .set_lifecycle(&LifecycleConfig { expiration_days: 0 })
        .unwrap();
    let stats = store.apply_lifecycle(now()).unwrap();
    assert_eq!(stats.deleted, 1);

    // ListObjects must not return the deleted-marker entry.
    assert_eq!(store.list_objects(None, 100, None).unwrap().0.len(), 0);
    // ListObjectVersions must surface both the original version and the new marker.
    let versions = store
        .list_object_versions(None, None, 100, None, None)
        .unwrap();
    assert_eq!(versions.entries.len(), 2);
    assert!(versions.entries.iter().any(|e| e.meta.is_delete_marker));
}

