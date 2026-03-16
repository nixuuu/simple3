use std::collections::HashMap;
use std::fs;

use md5::{Digest, Md5};
use simple3::storage::Storage;
use simple3::storage::versioning::VersioningState;

fn write_obj(
    bucket: &simple3::storage::BucketStore,
    key: &str,
    data: &[u8],
) -> simple3::types::ObjectMeta {
    let tmp = bucket.bucket_dir().join(format!(".tmp_test_{key}"));
    fs::write(&tmp, data).unwrap();
    let etag = format!("{:x}", Md5::digest(data));
    let crc = crc32c::crc32c(data);
    bucket
        .put_object_streamed(
            key,
            &tmp,
            Some("text/plain".into()),
            etag,
            1000,
            HashMap::new(),
            Some(crc),
        )
        .unwrap()
}

// === Versioning state ===

#[test]
fn test_versioning_state_default_absent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    assert!(bucket.get_versioning_state().unwrap().is_none());
}

#[test]
fn test_set_versioning_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    bucket.set_versioning_state(VersioningState::Enabled).unwrap();
    assert_eq!(bucket.get_versioning_state().unwrap(), Some(VersioningState::Enabled));
}

#[test]
fn test_set_versioning_suspended() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    bucket.set_versioning_state(VersioningState::Suspended).unwrap();
    assert_eq!(bucket.get_versioning_state().unwrap(), Some(VersioningState::Suspended));
}

// === PUT with versioning enabled ===

#[test]
fn test_put_versioned_generates_version_id() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    let meta = write_obj(&bucket, "key1", b"version1");
    assert!(meta.version_id.is_some());
    assert_eq!(meta.version_id.as_ref().unwrap().len(), 20);
}

#[test]
fn test_overwrite_moves_old_to_versions() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    let meta1 = write_obj(&bucket, "key1", b"version1");
    let vid1 = meta1.version_id.clone().unwrap();

    let meta2 = write_obj(&bucket, "key1", b"version2");
    let vid2 = meta2.version_id.clone().unwrap();

    // Current should be version2
    let current = bucket.get_meta("key1").unwrap().unwrap();
    assert_eq!(current.version_id.as_deref(), Some(vid2.as_str()));

    // Old version should be in versions table
    let old = bucket.get_version("key1", &vid1).unwrap().unwrap();
    assert_eq!(old.version_id.as_deref(), Some(vid1.as_str()));
}

// === GET with versionId ===

#[test]
fn test_get_specific_version() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    let meta1 = write_obj(&bucket, "key1", b"version1");
    let vid1 = meta1.version_id.clone().unwrap();

    write_obj(&bucket, "key1", b"version2");

    // Get specific old version
    let old = bucket.get_version("key1", &vid1).unwrap().unwrap();
    let data = bucket.read_object(&old).unwrap();
    assert_eq!(data, b"version1");
}

// === DELETE with versioning ===

#[test]
fn test_delete_creates_marker() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    let meta1 = write_obj(&bucket, "key1", b"data");
    let vid1 = meta1.version_id.clone().unwrap();

    // Delete without versionId → creates delete marker
    let result = bucket.delete_object("key1").unwrap().unwrap();
    assert!(result.is_delete_marker);
    assert!(result.version_id.is_some());

    // Current is now a delete marker
    let current = bucket.get_meta("key1").unwrap().unwrap();
    assert!(current.is_delete_marker);

    // Old version still accessible
    let old = bucket.get_version("key1", &vid1).unwrap();
    assert!(old.is_some());
}

#[test]
fn test_delete_version_permanently() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    let meta1 = write_obj(&bucket, "key1", b"version1");
    let vid1 = meta1.version_id.clone().unwrap();

    write_obj(&bucket, "key1", b"version2");

    // Permanently delete old version
    let deleted = bucket.delete_version("key1", &vid1).unwrap();
    assert!(deleted.is_some());

    // Old version is gone
    assert!(bucket.get_version("key1", &vid1).unwrap().is_none());
}

// === Undelete (delete a delete marker) ===

#[test]
fn test_undelete_by_deleting_marker() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    let _meta1 = write_obj(&bucket, "key1", b"original data");

    // Delete → creates delete marker
    let marker = bucket.delete_object("key1").unwrap().unwrap();
    assert!(marker.is_delete_marker);
    let marker_vid = marker.version_id.unwrap();

    // Current is a delete marker
    let current = bucket.get_meta("key1").unwrap().unwrap();
    assert!(current.is_delete_marker);

    // Delete the delete marker → undelete
    let removed = bucket.delete_current_version("key1", &marker_vid).unwrap();
    assert!(removed.is_some());

    // Current should now be the original data
    let restored = bucket.get_meta("key1").unwrap().unwrap();
    assert!(!restored.is_delete_marker);
    let data = bucket.read_object(&restored).unwrap();
    assert_eq!(data, b"original data");
}

// === Suspended mode ===

#[test]
fn test_suspended_mode_null_version() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Suspended).unwrap();

    let meta = write_obj(&bucket, "key1", b"data");
    assert_eq!(meta.version_id.as_deref(), Some("null"));
}

#[test]
fn test_suspended_overwrite_null_increments_dead() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Suspended).unwrap();

    write_obj(&bucket, "key1", b"data1");
    write_obj(&bucket, "key1", b"data2");

    // In suspended mode, overwriting null replaces it — dead bytes should increase
    let stats = bucket.segment_stats().unwrap();
    let dead: u64 = stats.iter().map(|s| s.dead_bytes).sum();
    assert!(dead > 0, "dead bytes should increase after null overwrite");
}

#[test]
fn test_suspended_preserves_named_versions() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Enable versioning, write object → gets named version_id
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();
    let meta1 = write_obj(&bucket, "key1", b"v1");
    let vid1 = meta1.version_id.clone().unwrap();
    assert_ne!(vid1, "null");

    // Suspend versioning, overwrite → named version moved to versions table
    bucket.set_versioning_state(VersioningState::Suspended).unwrap();
    let meta2 = write_obj(&bucket, "key1", b"v2");
    assert_eq!(meta2.version_id.as_deref(), Some("null"));

    // Named version still accessible
    let old = bucket.get_version("key1", &vid1).unwrap();
    assert!(old.is_some());
}

// === ListObjectVersions ===

#[test]
fn test_list_object_versions() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    write_obj(&bucket, "key1", b"v1");
    write_obj(&bucket, "key1", b"v2");
    write_obj(&bucket, "key2", b"v1");

    let result = bucket
        .list_object_versions(None, None, 1000, None, None)
        .unwrap();

    // Should have 3 entries: key1(current), key1(old version), key2(current)
    assert_eq!(result.entries.len(), 3);

    // Sorted: key1 entries first (latest first), then key2
    assert_eq!(result.entries[0].key, "key1");
    assert!(result.entries[0].is_latest);
    assert_eq!(result.entries[1].key, "key1");
    assert!(!result.entries[1].is_latest);
    assert_eq!(result.entries[2].key, "key2");
    assert!(result.entries[2].is_latest);
}

#[test]
fn test_list_versions_with_delete_markers() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    write_obj(&bucket, "key1", b"v1");
    bucket.delete_object("key1").unwrap();

    let result = bucket
        .list_object_versions(None, None, 1000, None, None)
        .unwrap();

    // Should have 2 entries: delete marker (current) + old version
    assert_eq!(result.entries.len(), 2);
    assert!(result.entries[0].meta.is_delete_marker);
    assert!(result.entries[0].is_latest);
    assert!(!result.entries[1].meta.is_delete_marker);
}

#[test]
fn test_list_versions_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    write_obj(&bucket, "a", b"v1");
    write_obj(&bucket, "b", b"v1");
    write_obj(&bucket, "c", b"v1");

    // Page 1: max_keys=2
    let page1 = bucket
        .list_object_versions(None, None, 2, None, None)
        .unwrap();
    assert_eq!(page1.entries.len(), 2);
    assert!(page1.is_truncated);
    assert!(page1.next_key_marker.is_some());

    // Page 2
    let page2 = bucket
        .list_object_versions(
            None,
            None,
            2,
            page1.next_key_marker.as_deref(),
            page1.next_version_id_marker.as_deref(),
        )
        .unwrap();
    assert_eq!(page2.entries.len(), 1);
    assert!(!page2.is_truncated);
}

// === Bucket emptiness ===

#[test]
fn test_is_empty_with_versions() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    write_obj(&bucket, "key1", b"v1");
    write_obj(&bucket, "key1", b"v2");

    // Delete current → creates marker, old version in versions table
    bucket.delete_object("key1").unwrap();

    // Bucket is NOT empty — has delete marker + old version
    assert!(!bucket.is_empty().unwrap());
}

// === Non-versioned behavior unchanged ===

#[test]
fn test_no_versioning_no_version_id() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let meta = write_obj(&bucket, "key1", b"data");
    assert!(meta.version_id.is_none());
}

#[test]
fn test_no_versioning_overwrite_increments_dead() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    write_obj(&bucket, "key1", b"data1");
    write_obj(&bucket, "key1", b"data2");

    let stats = bucket.segment_stats().unwrap();
    let dead: u64 = stats.iter().map(|s| s.dead_bytes).sum();
    assert!(dead > 0, "dead bytes should increase after overwrite");
}

// === Version stats ===

#[test]
fn test_version_stats() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    write_obj(&bucket, "key1", b"v1");
    write_obj(&bucket, "key1", b"v2");
    bucket.delete_object("key1").unwrap();

    let (ver_count, dm_count, _size) = bucket.version_stats().unwrap();
    // versions table: v1 (old version) + v2 (moved when delete marker created)
    // delete markers: the current object in objects table is a delete marker,
    //   but version_stats only counts versions table entries
    assert_eq!(ver_count, 2);
    // The delete marker was placed in the objects table, not versions table,
    // so delete_marker_count from versions should be 0
    assert_eq!(dm_count, 0);
}

// === Enable versioning on existing objects ===

#[test]
fn test_enable_versioning_on_existing_objects() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Put object before versioning
    let meta_pre = write_obj(&bucket, "key1", b"pre-version");
    assert!(meta_pre.version_id.is_none());

    // Enable versioning
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    // Overwrite → old (pre-versioning) object moves to versions with vid="null"
    let meta_new = write_obj(&bucket, "key1", b"versioned");
    assert!(meta_new.version_id.is_some());

    // Old version accessible with vid="null"
    let old = bucket.get_version("key1", "null").unwrap();
    assert!(old.is_some());
}

// === Compaction with versioned objects ===

#[test]
fn test_compaction_preserves_versioned_data() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    // Write v1, then v2 (v1 moves to versions table)
    let meta1 = write_obj(&bucket, "key1", b"version1-data");
    let vid1 = meta1.version_id.clone().unwrap();
    write_obj(&bucket, "key1", b"version2-data");

    // Write an unversioned-deleted object to create dead bytes
    write_obj(&bucket, "garbage", b"to-be-deleted");
    // Temporarily disable versioning so delete actually frees space
    bucket.set_versioning_state(VersioningState::Suspended).unwrap();
    bucket.delete_object("garbage").unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    // Compact
    bucket.compact().unwrap();

    // Both versions should still be readable
    let current = bucket.get_meta("key1").unwrap().unwrap();
    let data2 = bucket.read_object(&current).unwrap();
    assert_eq!(data2, b"version2-data");

    let old = bucket.get_version("key1", &vid1).unwrap().unwrap();
    let data1 = bucket.read_object(&old).unwrap();
    assert_eq!(data1, b"version1-data");
}

// === Version-specific delete targets current vs historical ===

#[test]
fn test_delete_version_current_vs_historical() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    let meta1 = write_obj(&bucket, "key1", b"v1");
    let vid1 = meta1.version_id.clone().unwrap();
    let meta2 = write_obj(&bucket, "key1", b"v2");
    let vid2 = meta2.version_id.clone().unwrap();

    // Delete the current version (v2) by version_id
    let deleted = bucket.delete_current_version("key1", &vid2).unwrap();
    assert!(deleted.is_some());

    // key1 should be gone from objects table (v1 is in versions table but
    // delete_current_version doesn't auto-promote unless it's a delete marker)
    let current = bucket.get_meta("key1").unwrap();
    assert!(current.is_none());

    // v1 should still be in versions table
    let v1 = bucket.get_version("key1", &vid1).unwrap();
    assert!(v1.is_some());

    // Delete historical version (v1) from versions table
    let deleted_v1 = bucket.delete_version("key1", &vid1).unwrap();
    assert!(deleted_v1.is_some());

    // Both versions gone
    assert!(bucket.get_version("key1", &vid1).unwrap().is_none());
}

// === Zero-length objects with versioning ===

#[test]
fn test_zero_length_object_versioned() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    // Write a zero-length object
    let meta = write_obj(&bucket, "empty", b"");
    assert!(meta.version_id.is_some());
    assert!(!meta.is_delete_marker);
    assert_eq!(meta.data_length(), 0);

    // Overwrite with non-empty data
    let meta2 = write_obj(&bucket, "empty", b"now-has-data");
    assert!(meta2.version_id.is_some());
    assert_ne!(meta2.version_id, meta.version_id);

    // Old zero-length version should be accessible
    let old = bucket
        .get_version("empty", meta.version_id.as_ref().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(old.data_length(), 0);
    assert!(!old.is_delete_marker);

    // Verify: list versions shows both
    let result = bucket
        .list_object_versions(None, None, 100, None, None)
        .unwrap();
    let empty_versions: Vec<_> = result
        .entries
        .iter()
        .filter(|e| e.key == "empty")
        .collect();
    assert_eq!(empty_versions.len(), 2);
}

// === Verify integrity covers versions ===

#[test]
fn test_verify_includes_versions() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();
    bucket.set_versioning_state(VersioningState::Enabled).unwrap();

    write_obj(&bucket, "key1", b"v1");
    write_obj(&bucket, "key1", b"v2");

    let result = bucket.verify_integrity().unwrap();
    // Should verify both current (v2) and old version (v1)
    assert_eq!(result.total_objects, 2);
    assert_eq!(result.verified_ok, 2);
    assert!(result.errors.is_empty());
}
