use simple3::storage::Storage;
use simple3::types::ObjectMeta;
use std::collections::HashMap;

fn make_meta(offset: u64, length: u64, etag: &str) -> ObjectMeta {
    ObjectMeta {
        offset,
        length,
        content_type: Some("text/plain".into()),
        etag: etag.into(),
        last_modified: 1000,
        user_metadata: HashMap::new(),
    }
}

// === Bucket operations ===

#[test]
fn test_create_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();

    let existed = storage.create_bucket("mybucket").unwrap();
    assert!(!existed);

    let buckets = storage.list_buckets();
    assert_eq!(buckets, vec!["mybucket"]);
}

#[test]
fn test_create_bucket_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();

    assert!(!storage.create_bucket("dup").unwrap());
    assert!(storage.create_bucket("dup").unwrap());
}

#[test]
fn test_delete_bucket_empty() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();

    storage.create_bucket("gone").unwrap();
    assert!(storage.delete_bucket("gone").unwrap());
    assert!(storage.list_buckets().is_empty());
    assert!(!dir.path().join("gone").exists());
}

#[test]
fn test_delete_bucket_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();

    assert!(!storage.delete_bucket("nope").unwrap());
}

#[test]
fn test_get_bucket_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();

    assert!(storage.get_bucket("nope").is_none());
}

// === Object operations ===

#[test]
fn test_put_get_object() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    let data = b"hello world";
    let (offset, length) = bucket.append_data(data).unwrap();

    let meta = make_meta(offset, length, "abc123");
    bucket.put_meta("key1", &meta).unwrap();

    let read_back = bucket.read_data(offset, length).unwrap();
    assert_eq!(read_back, data);

    let got = bucket.get_meta("key1").unwrap().unwrap();
    assert_eq!(got.offset, offset);
    assert_eq!(got.length, length);
    assert_eq!(got.etag, "abc123");
}

#[test]
fn test_put_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    let (off1, len1) = bucket.append_data(b"old data").unwrap();
    bucket.put_meta("key", &make_meta(off1, len1, "old")).unwrap();

    let (off2, len2) = bucket.append_data(b"new data!!!").unwrap();
    bucket.put_meta("key", &make_meta(off2, len2, "new")).unwrap();

    let meta = bucket.get_meta("key").unwrap().unwrap();
    assert_eq!(meta.etag, "new");
    assert_eq!(meta.length, 11);

    let data = bucket.read_data(meta.offset, meta.length).unwrap();
    assert_eq!(data, b"new data!!!");
}

#[test]
fn test_get_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    assert!(bucket.get_meta("nope").unwrap().is_none());
}

#[test]
fn test_head_object_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    let data = b"some content";
    let (offset, length) = bucket.append_data(data).unwrap();
    let meta = ObjectMeta {
        offset,
        length,
        content_type: Some("application/json".into()),
        etag: "etag123".into(),
        last_modified: 42,
        user_metadata: [("x-custom".into(), "val".into())].into(),
    };
    bucket.put_meta("obj", &meta).unwrap();

    let got = bucket.get_meta("obj").unwrap().unwrap();
    assert_eq!(got.content_type.as_deref(), Some("application/json"));
    assert_eq!(got.length, 12);
    assert_eq!(got.last_modified, 42);
    assert_eq!(got.user_metadata.get("x-custom").unwrap(), "val");
}

#[test]
fn test_delete_object() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    let (off, len) = bucket.append_data(b"deleteme").unwrap();
    bucket.put_meta("k", &make_meta(off, len, "e")).unwrap();

    let removed = bucket.delete_and_compact("k").unwrap();
    assert!(removed.is_some());
    assert!(bucket.get_meta("k").unwrap().is_none());
}

#[test]
fn test_delete_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    assert!(bucket.delete_and_compact("nope").unwrap().is_none());
}

// === Compaction ===

#[test]
fn test_compaction_shrinks_file() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    let (off_a, len_a) = bucket.append_data(b"AAAA").unwrap();
    bucket.put_meta("a", &make_meta(off_a, len_a, "ea")).unwrap();

    let (off_b, len_b) = bucket.append_data(b"BBBBBB").unwrap();
    bucket.put_meta("b", &make_meta(off_b, len_b, "eb")).unwrap();

    // data.bin = 10 bytes (4 + 6)
    let data_path = dir.path().join("b").join("data.bin");
    assert_eq!(std::fs::metadata(&data_path).unwrap().len(), 10);

    bucket.delete_and_compact("a").unwrap();

    // data.bin = 6 bytes (only B remains)
    assert_eq!(std::fs::metadata(&data_path).unwrap().len(), 6);
}

#[test]
fn test_compaction_preserves_other_objects() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    let (off_a, len_a) = bucket.append_data(b"AAA").unwrap();
    bucket.put_meta("a", &make_meta(off_a, len_a, "ea")).unwrap();

    let (off_b, len_b) = bucket.append_data(b"BBB").unwrap();
    bucket.put_meta("b", &make_meta(off_b, len_b, "eb")).unwrap();

    let (off_c, len_c) = bucket.append_data(b"CCC").unwrap();
    bucket.put_meta("c", &make_meta(off_c, len_c, "ec")).unwrap();

    // Delete middle object
    bucket.delete_and_compact("b").unwrap();

    // A and C still readable with correct data
    let meta_a = bucket.get_meta("a").unwrap().unwrap();
    let data_a = bucket.read_data(meta_a.offset, meta_a.length).unwrap();
    assert_eq!(data_a, b"AAA");

    let meta_c = bucket.get_meta("c").unwrap().unwrap();
    let data_c = bucket.read_data(meta_c.offset, meta_c.length).unwrap();
    assert_eq!(data_c, b"CCC");

    // B is gone
    assert!(bucket.get_meta("b").unwrap().is_none());

    // File shrunk
    let data_path = dir.path().join("b").join("data.bin");
    assert_eq!(std::fs::metadata(&data_path).unwrap().len(), 6);
}

// === List objects ===

#[test]
fn test_list_objects_empty() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    let (items, truncated) = bucket.list_objects(None, 1000, None).unwrap();
    assert!(items.is_empty());
    assert!(!truncated);
}

#[test]
fn test_list_objects_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    for key in ["photos/a.jpg", "photos/b.jpg", "docs/x.txt", "docs/y.txt"] {
        let (off, len) = bucket.append_data(b"data").unwrap();
        bucket.put_meta(key, &make_meta(off, len, "e")).unwrap();
    }

    let (items, _) = bucket.list_objects(Some("photos/"), 1000, None).unwrap();
    assert_eq!(items.len(), 2);
    assert!(items.iter().all(|(k, _)| k.starts_with("photos/")));

    let (items, _) = bucket.list_objects(Some("docs/"), 1000, None).unwrap();
    assert_eq!(items.len(), 2);
}

#[test]
fn test_list_objects_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap();

    for i in 0..5 {
        let key = format!("key{i:02}");
        let (off, len) = bucket.append_data(b"x").unwrap();
        bucket.put_meta(&key, &make_meta(off, len, "e")).unwrap();
    }

    // Page 1
    let (page1, trunc1) = bucket.list_objects(None, 2, None).unwrap();
    assert_eq!(page1.len(), 2);
    assert!(trunc1);

    // Page 2 using continuation
    let token = &page1.last().unwrap().0;
    let (page2, trunc2) = bucket.list_objects(None, 2, Some(token)).unwrap();
    assert_eq!(page2.len(), 2);
    assert!(trunc2);

    // Page 3
    let token2 = &page2.last().unwrap().0;
    let (page3, trunc3) = bucket.list_objects(None, 2, Some(token2)).unwrap();
    assert_eq!(page3.len(), 1);
    assert!(!trunc3);
}

// === Persistence ===

#[test]
fn test_reopen_storage() {
    let dir = tempfile::tempdir().unwrap();

    {
        let storage = Storage::open(dir.path()).unwrap();
        storage.create_bucket("persist").unwrap();
        let bucket = storage.get_bucket("persist").unwrap();
        let (off, len) = bucket.append_data(b"survive restart").unwrap();
        bucket.put_meta("key1", &make_meta(off, len, "etag")).unwrap();
    }

    // Reopen
    let storage = Storage::open(dir.path()).unwrap();
    let buckets = storage.list_buckets();
    assert_eq!(buckets, vec!["persist"]);

    let bucket = storage.get_bucket("persist").unwrap();
    let meta = bucket.get_meta("key1").unwrap().unwrap();
    let data = bucket.read_data(meta.offset, meta.length).unwrap();
    assert_eq!(data, b"survive restart");
}
