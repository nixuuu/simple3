use md5::{Digest, Md5};
use simple3::storage::Storage;
use simple3::types::ObjectMeta;
use std::collections::HashMap;
use std::io::Write;

fn make_meta(segment_id: u32, offset: u64, length: u64, etag: &str) -> ObjectMeta {
    ObjectMeta {
        segment_id,
        offset,
        length,
        content_type: Some("text/plain".into()),
        etag: etag.into(),
        last_modified: 1000,
        user_metadata: HashMap::new(),
        content_md5: None,
    }
}

// === Bucket operations ===

#[test]
fn test_create_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();

    let existed = storage.create_bucket("mybucket").unwrap();
    assert!(!existed);

    let buckets = storage.list_buckets().unwrap();
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
    assert!(storage.list_buckets().unwrap().is_empty());
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

    assert!(storage.get_bucket("nope").unwrap().is_none());
}

// === Object operations ===

#[test]
fn test_put_get_object() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let data = b"hello world";
    let (seg_id, offset, length) = bucket.append_data(data).unwrap();

    let meta = make_meta(seg_id, offset, length, "abc123");
    bucket.put_meta("key1", &meta).unwrap();

    let read_back = bucket.read_data(seg_id, offset, length).unwrap();
    assert_eq!(read_back, data);

    let got = bucket.get_meta("key1").unwrap().unwrap();
    assert_eq!(got.segment_id, seg_id);
    assert_eq!(got.offset, offset);
    assert_eq!(got.length, length);
    assert_eq!(got.etag, "abc123");
}

#[test]
fn test_put_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg1, off1, len1) = bucket.append_data(b"old data").unwrap();
    bucket
        .put_meta("key", &make_meta(seg1, off1, len1, "old"))
        .unwrap();

    let (seg2, off2, len2) = bucket.append_data(b"new data!!!").unwrap();
    bucket
        .put_meta("key", &make_meta(seg2, off2, len2, "new"))
        .unwrap();

    let meta = bucket.get_meta("key").unwrap().unwrap();
    assert_eq!(meta.etag, "new");
    assert_eq!(meta.length, 11);

    let data = bucket.read_data(meta.segment_id, meta.offset, meta.length).unwrap();
    assert_eq!(data, b"new data!!!");
}

#[test]
fn test_get_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    assert!(bucket.get_meta("nope").unwrap().is_none());
}

#[test]
fn test_head_object_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let data = b"some content";
    let (seg_id, offset, length) = bucket.append_data(data).unwrap();
    let meta = ObjectMeta {
        segment_id: seg_id,
        offset,
        length,
        content_type: Some("application/json".into()),
        etag: "etag123".into(),
        last_modified: 42,
        user_metadata: [("x-custom".into(), "val".into())].into(),
        content_md5: None,
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
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg, off, len) = bucket.append_data(b"deleteme").unwrap();
    bucket
        .put_meta("k", &make_meta(seg, off, len, "e"))
        .unwrap();

    let removed = bucket.delete_object("k").unwrap();
    assert!(removed.is_some());
    assert!(bucket.get_meta("k").unwrap().is_none());
}

#[test]
fn test_delete_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    assert!(bucket.delete_object("nope").unwrap().is_none());
}

// === Compaction ===

#[test]
fn test_compaction_shrinks_file() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg, off_a, len_a) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &make_meta(seg, off_a, len_a, "ea"))
        .unwrap();

    let (seg, off_b, len_b) = bucket.append_data(b"BBBBBB").unwrap();
    bucket
        .put_meta("b", &make_meta(seg, off_b, len_b, "eb"))
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 10);

    // Delete leaves dead space, then compact reclaims it
    bucket.delete_object("a").unwrap();
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 10);

    bucket.compact().unwrap();
    // After compact: segment 0 rewritten with only "b" (6 bytes)
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 6);
}

#[test]
fn test_compaction_preserves_other_objects() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg, off_a, len_a) = bucket.append_data(b"AAA").unwrap();
    bucket
        .put_meta("a", &make_meta(seg, off_a, len_a, "ea"))
        .unwrap();

    let (seg, off_b, len_b) = bucket.append_data(b"BBB").unwrap();
    bucket
        .put_meta("b", &make_meta(seg, off_b, len_b, "eb"))
        .unwrap();

    let (seg, off_c, len_c) = bucket.append_data(b"CCC").unwrap();
    bucket
        .put_meta("c", &make_meta(seg, off_c, len_c, "ec"))
        .unwrap();

    bucket.delete_object("b").unwrap();
    bucket.compact().unwrap();

    let meta_a = bucket.get_meta("a").unwrap().unwrap();
    let data_a = bucket
        .read_data(meta_a.segment_id, meta_a.offset, meta_a.length)
        .unwrap();
    assert_eq!(data_a, b"AAA");

    let meta_c = bucket.get_meta("c").unwrap().unwrap();
    let data_c = bucket
        .read_data(meta_c.segment_id, meta_c.offset, meta_c.length)
        .unwrap();
    assert_eq!(data_c, b"CCC");

    assert!(bucket.get_meta("b").unwrap().is_none());

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 6);
}

// === List objects ===

#[test]
fn test_list_objects_empty() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (items, truncated) = bucket.list_objects(None, 1000, None).unwrap();
    assert!(items.is_empty());
    assert!(!truncated);
}

#[test]
fn test_list_objects_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    for key in ["photos/a.jpg", "photos/b.jpg", "docs/x.txt", "docs/y.txt"] {
        let (seg, off, len) = bucket.append_data(b"data").unwrap();
        bucket.put_meta(key, &make_meta(seg, off, len, "e")).unwrap();
    }

    let (items, _) = bucket.list_objects(Some("photos/"), 1000, None).unwrap();
    assert_eq!(items.len(), 2);
    assert!(items.iter().all(|(k, _)| k.starts_with("photos/")));

    let (items, _) = bucket.list_objects(Some("docs/"), 1000, None).unwrap();
    assert_eq!(items.len(), 2);
}

#[test]
fn test_list_objects_delimiter() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    for key in [
        "photos/vacation/a.jpg",
        "photos/vacation/b.jpg",
        "photos/work/c.jpg",
        "photos/d.jpg",
        "docs/x.txt",
        "root.txt",
    ] {
        let (seg, off, len) = bucket.append_data(b"data").unwrap();
        bucket.put_meta(key, &make_meta(seg, off, len, "e")).unwrap();
    }

    // Root level with delimiter
    let (objects, prefixes, trunc) = bucket
        .list_objects_with_delimiter(None, Some("/"), 1000, None)
        .unwrap();
    assert!(!trunc);
    assert_eq!(objects.len(), 1); // "root.txt"
    assert_eq!(objects[0].0, "root.txt");
    assert_eq!(prefixes, vec!["docs/", "photos/"]);

    // "photos/" prefix with delimiter
    let (objects, prefixes, _) = bucket
        .list_objects_with_delimiter(Some("photos/"), Some("/"), 1000, None)
        .unwrap();
    assert_eq!(objects.len(), 1); // "photos/d.jpg"
    assert_eq!(objects[0].0, "photos/d.jpg");
    assert_eq!(prefixes, vec!["photos/vacation/", "photos/work/"]);

    // No delimiter — flat list
    let (objects, prefixes, _) = bucket
        .list_objects_with_delimiter(Some("photos/"), None, 1000, None)
        .unwrap();
    assert_eq!(objects.len(), 4);
    assert!(prefixes.is_empty());
}

#[test]
fn test_list_objects_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    for i in 0..5 {
        let key = format!("key{i:02}");
        let (seg, off, len) = bucket.append_data(b"x").unwrap();
        bucket
            .put_meta(&key, &make_meta(seg, off, len, "e"))
            .unwrap();
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
        let bucket = storage.get_bucket("persist").unwrap().unwrap();
        let (seg, off, len) = bucket.append_data(b"survive restart").unwrap();
        bucket
            .put_meta("key1", &make_meta(seg, off, len, "etag"))
            .unwrap();
    }

    // Reopen
    let storage = Storage::open(dir.path()).unwrap();
    let buckets = storage.list_buckets().unwrap();
    assert_eq!(buckets, vec!["persist"]);

    let bucket = storage.get_bucket("persist").unwrap().unwrap();
    let meta = bucket.get_meta("key1").unwrap().unwrap();
    let data = bucket
        .read_data(meta.segment_id, meta.offset, meta.length)
        .unwrap();
    assert_eq!(data, b"survive restart");
}

// === Temp file cleanup (GC) ===

#[test]
fn test_tmp_cleanup_on_startup() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    drop(storage);

    // Plant orphan temp files
    let bucket_dir = dir.path().join("b");
    std::fs::write(bucket_dir.join(".tmp_000001"), b"orphan1").unwrap();
    std::fs::write(bucket_dir.join(".tmp_000002"), b"orphan2").unwrap();
    assert!(bucket_dir.join(".tmp_000001").exists());

    // Reopen — should clean up .tmp_*
    let _storage = Storage::open(dir.path()).unwrap();
    assert!(!bucket_dir.join(".tmp_000001").exists());
    assert!(!bucket_dir.join(".tmp_000002").exists());
    // segment file untouched
    assert!(bucket_dir.join("seg_000000.bin").exists());
}

// === Streamed put ===

#[test]
fn test_put_object_streamed() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Write data to temp file
    let tmp_path = bucket.bucket_dir().join(".tmp_test");
    let mut tmp = std::fs::File::create(&tmp_path).unwrap();
    tmp.write_all(b"streamed data here").unwrap();
    tmp.flush().unwrap();
    drop(tmp);

    let meta = bucket
        .put_object_streamed(
            "skey",
            &tmp_path,
            Some("text/plain".into()),
            "etag_s".into(),
            999,
            HashMap::new(),
        )
        .unwrap();

    assert_eq!(meta.length, 18);
    assert_eq!(meta.etag, "etag_s");

    // Temp file removed
    assert!(!tmp_path.exists());

    // Data readable
    let data = bucket
        .read_data(meta.segment_id, meta.offset, meta.length)
        .unwrap();
    assert_eq!(data, b"streamed data here");

    // Meta stored
    let got = bucket.get_meta("skey").unwrap().unwrap();
    assert_eq!(got.content_type.as_deref(), Some("text/plain"));
}

// === Multipart upload ===

#[test]
fn test_multipart_upload_basic() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let upload_id = bucket.create_multipart_upload();
    assert!(!upload_id.is_empty());

    // Upload 3 parts
    let etag1 = bucket.upload_part(&upload_id, 1, b"AAAA").unwrap();
    let etag2 = bucket.upload_part(&upload_id, 2, b"BBBB").unwrap();
    let etag3 = bucket.upload_part(&upload_id, 3, b"CCCC").unwrap();

    // Complete
    let parts = vec![(1, etag1), (2, etag2), (3, etag3)];
    let (meta, etag) = bucket
        .complete_multipart_upload(&upload_id, "multipart_obj", &parts, None, 500, HashMap::new())
        .unwrap();

    // ETag has -N suffix
    assert!(etag.ends_with("-3"), "etag should end with -3: {etag}");
    assert_eq!(meta.length, 12); // 4+4+4

    // Data is concatenation of parts in order
    let data = bucket
        .read_data(meta.segment_id, meta.offset, meta.length)
        .unwrap();
    assert_eq!(data, b"AAAABBBBCCCC");

    // Part files cleaned up
    let bucket_dir = dir.path().join("b");
    let mpu_files: Vec<_> = std::fs::read_dir(&bucket_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(".mpu_"))
        })
        .collect();
    assert!(mpu_files.is_empty());
}

#[test]
fn test_multipart_upload_out_of_order() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let upload_id = bucket.create_multipart_upload();

    // Upload parts out of order
    let etag3 = bucket.upload_part(&upload_id, 3, b"CCC").unwrap();
    let etag1 = bucket.upload_part(&upload_id, 1, b"AAA").unwrap();
    let etag2 = bucket.upload_part(&upload_id, 2, b"BBB").unwrap();

    let parts = vec![(3, etag3), (1, etag1), (2, etag2)];
    let (meta, _) = bucket
        .complete_multipart_upload(&upload_id, "ooo", &parts, None, 0, HashMap::new())
        .unwrap();

    // Should be sorted by part number
    let data = bucket
        .read_data(meta.segment_id, meta.offset, meta.length)
        .unwrap();
    assert_eq!(data, b"AAABBBCCC");
}

#[test]
fn test_abort_multipart_upload() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let upload_id = bucket.create_multipart_upload();
    bucket.upload_part(&upload_id, 1, b"data").unwrap();
    bucket.upload_part(&upload_id, 2, b"more").unwrap();

    // Part files exist
    let bucket_dir = dir.path().join("b");
    assert!(bucket_dir
        .join(format!(".mpu_{upload_id}_00001"))
        .exists());

    // Abort
    bucket.abort_multipart_upload(&upload_id).unwrap();

    // Part files gone
    assert!(!bucket_dir
        .join(format!(".mpu_{upload_id}_00001"))
        .exists());
    assert!(!bucket_dir
        .join(format!(".mpu_{upload_id}_00002"))
        .exists());

    // No object created
    assert!(bucket.get_meta("anything").unwrap().is_none());
}

#[test]
fn test_mpu_cleanup_on_startup() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    drop(storage);

    // Plant orphan multipart files
    let bucket_dir = dir.path().join("b");
    std::fs::write(bucket_dir.join(".mpu_abc123_00001"), b"orphan").unwrap();
    std::fs::write(bucket_dir.join(".mpu_abc123_00002"), b"orphan").unwrap();

    // Reopen
    let _storage = Storage::open(dir.path()).unwrap();
    assert!(!bucket_dir.join(".mpu_abc123_00001").exists());
    assert!(!bucket_dir.join(".mpu_abc123_00002").exists());
}

// === Overwrite compaction ===

#[test]
fn test_overwrite_compacts_old_data() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Upload via streamed put
    let tmp1 = bucket.bucket_dir().join(".tmp_ow1");
    std::fs::write(&tmp1, b"AAAAAAAAAA").unwrap(); // 10 bytes
    bucket
        .put_object_streamed("key", &tmp1, None, "e1".into(), 0, HashMap::new())
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 10);

    // Overwrite same key with smaller data (append-only: file grows)
    let tmp2 = bucket.bucket_dir().join(".tmp_ow2");
    std::fs::write(&tmp2, b"BBB").unwrap(); // 3 bytes
    let meta = bucket
        .put_object_streamed("key", &tmp2, None, "e2".into(), 0, HashMap::new())
        .unwrap();

    // Append-only: seg = 10 + 3 = 13, dead_bytes = 10
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 13);
    assert_eq!(bucket.dead_bytes(), 10);
    assert_eq!(meta.length, 3);

    // Data readable correctly
    let data = bucket
        .read_data(meta.segment_id, meta.offset, meta.length)
        .unwrap();
    assert_eq!(data, b"BBB");

    // Compact reclaims dead space
    bucket.compact().unwrap();
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 3);
    assert_eq!(bucket.dead_bytes(), 0);
}

#[test]
fn test_multipart_overwrite_compacts() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // First upload: 10 bytes
    let tmp = bucket.bucket_dir().join(".tmp_mow");
    std::fs::write(&tmp, b"XXXXXXXXXX").unwrap();
    bucket
        .put_object_streamed("key", &tmp, None, "e1".into(), 0, HashMap::new())
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 10);

    // Overwrite via multipart: 6 bytes total (append-only: file grows)
    let uid = bucket.create_multipart_upload();
    let e1 = bucket.upload_part(&uid, 1, b"AAA").unwrap();
    let e2 = bucket.upload_part(&uid, 2, b"BBB").unwrap();
    let (meta, _) = bucket
        .complete_multipart_upload(&uid, "key", &[(1, e1), (2, e2)], None, 0, HashMap::new())
        .unwrap();

    // Append-only: 10 + 6 = 16, dead_bytes = 10
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 16);
    assert_eq!(bucket.dead_bytes(), 10);
    assert_eq!(meta.length, 6);

    let data = bucket
        .read_data(meta.segment_id, meta.offset, meta.length)
        .unwrap();
    assert_eq!(data, b"AAABBB");

    // Compact reclaims dead space
    bucket.compact().unwrap();
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 6);
    assert_eq!(bucket.dead_bytes(), 0);
}

// === Append-only & crash recovery tests ===

#[test]
fn test_dead_bytes_tracking() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    assert_eq!(bucket.dead_bytes(), 0);

    // Put two objects
    let (seg, off_a, len_a) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &make_meta(seg, off_a, len_a, "ea"))
        .unwrap();
    let (seg, off_b, len_b) = bucket.append_data(b"BBBBBB").unwrap();
    bucket
        .put_meta("b", &make_meta(seg, off_b, len_b, "eb"))
        .unwrap();

    assert_eq!(bucket.dead_bytes(), 0);

    // Delete "a" (4 bytes become dead)
    bucket.delete_object("a").unwrap();
    assert_eq!(bucket.dead_bytes(), 4);

    // Overwrite "b" (6 bytes become dead)
    let tmp = bucket.bucket_dir().join(".tmp_db");
    std::fs::write(&tmp, b"CC").unwrap();
    bucket
        .put_object_streamed("b", &tmp, None, "e2".into(), 0, HashMap::new())
        .unwrap();
    assert_eq!(bucket.dead_bytes(), 10); // 4 + 6
}

#[test]
fn test_compact_full_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Put 3 objects: A(4) + B(6) + C(3) = 13 bytes
    let (seg, off_a, len_a) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &make_meta(seg, off_a, len_a, "ea"))
        .unwrap();
    let (seg, off_b, len_b) = bucket.append_data(b"BBBBBB").unwrap();
    bucket
        .put_meta("b", &make_meta(seg, off_b, len_b, "eb"))
        .unwrap();
    let (seg, off_c, len_c) = bucket.append_data(b"CCC").unwrap();
    bucket
        .put_meta("c", &make_meta(seg, off_c, len_c, "ec"))
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 13);

    // Delete B
    bucket.delete_object("b").unwrap();
    assert_eq!(bucket.dead_bytes(), 6);

    // Compact
    bucket.compact().unwrap();
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 7); // A(4) + C(3)
    assert_eq!(bucket.dead_bytes(), 0);

    // Verify data intact
    let meta_a = bucket.get_meta("a").unwrap().unwrap();
    assert_eq!(
        bucket
            .read_data(meta_a.segment_id, meta_a.offset, meta_a.length)
            .unwrap(),
        b"AAAA"
    );
    let meta_c = bucket.get_meta("c").unwrap().unwrap();
    assert_eq!(
        bucket
            .read_data(meta_c.segment_id, meta_c.offset, meta_c.length)
            .unwrap(),
        b"CCC"
    );
    assert!(bucket.get_meta("b").unwrap().is_none());
}

#[test]
fn test_recovery_truncates_orphans() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg, off, len) = bucket.append_data(b"HELLO").unwrap();
    bucket
        .put_meta("key", &make_meta(seg, off, len, "e1"))
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 5);

    // Simulate orphaned bytes (crashed append without metadata update)
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .unwrap();
        f.write_all(b"ORPHAN_JUNK").unwrap();
    }
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 16); // 5 + 11

    drop(bucket);
    drop(storage);

    // Reopen — recovery should truncate orphans
    let storage2 = Storage::open(dir.path()).unwrap();
    let bucket2 = storage2.get_bucket("b").unwrap().unwrap();

    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 5);
    let meta = bucket2.get_meta("key").unwrap().unwrap();
    assert_eq!(
        bucket2
            .read_data(meta.segment_id, meta.offset, meta.length)
            .unwrap(),
        b"HELLO"
    );
}

#[test]
fn test_recovery_after_interrupted_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Put objects
    let (seg, off_a, len_a) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &make_meta(seg, off_a, len_a, "ea"))
        .unwrap();
    let (seg, off_b, len_b) = bucket.append_data(b"BBBB").unwrap();
    bucket
        .put_meta("b", &make_meta(seg, off_b, len_b, "eb"))
        .unwrap();

    // Simulate interrupted compaction: leave temp file
    let bucket_dir = dir.path().join("b");
    std::fs::write(bucket_dir.join("seg_000000.bin.tmp"), b"garbage").unwrap();

    drop(bucket);
    drop(storage);

    // Reopen — should clean up .bin.tmp and remain consistent
    let storage2 = Storage::open(dir.path()).unwrap();
    let bucket2 = storage2.get_bucket("b").unwrap().unwrap();

    assert!(!bucket_dir.join("seg_000000.bin.tmp").exists());

    let meta_a = bucket2.get_meta("a").unwrap().unwrap();
    assert_eq!(
        bucket2
            .read_data(meta_a.segment_id, meta_a.offset, meta_a.length)
            .unwrap(),
        b"AAAA"
    );
    let meta_b = bucket2.get_meta("b").unwrap().unwrap();
    assert_eq!(
        bucket2
            .read_data(meta_b.segment_id, meta_b.offset, meta_b.length)
            .unwrap(),
        b"BBBB"
    );
}

// === Segment stats ===

#[test]
fn test_segment_stats() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg, off, len) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &make_meta(seg, off, len, "ea"))
        .unwrap();
    let (seg, off, len) = bucket.append_data(b"BBBB").unwrap();
    bucket
        .put_meta("b", &make_meta(seg, off, len, "eb"))
        .unwrap();

    bucket.delete_object("a").unwrap();

    let stats = bucket.segment_stats().unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].id, 0);
    assert_eq!(stats[0].size, 8); // 4 + 4
    assert_eq!(stats[0].dead_bytes, 4);
}

// === Verify integrity ===

#[test]
fn test_verify_integrity_ok() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Put objects with correct content_md5
    let data = b"hello world";
    let md5 = format!("{:x}", Md5::digest(data));
    let (seg, off, len) = bucket.append_data(data).unwrap();
    let meta = ObjectMeta {
        segment_id: seg,
        offset: off,
        length: len,
        content_type: None,
        etag: md5.clone(),
        last_modified: 0,
        user_metadata: HashMap::new(),
        content_md5: Some(md5),
    };
    bucket.put_meta("key1", &meta).unwrap();

    let result = bucket.verify_integrity().unwrap();
    assert_eq!(result.total_objects, 1);
    assert_eq!(result.verified_ok, 1);
    assert!(result.errors.is_empty());
}

#[test]
fn test_verify_integrity_checksum_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let data = b"hello world";
    let (seg, off, len) = bucket.append_data(data).unwrap();
    let meta = ObjectMeta {
        segment_id: seg,
        offset: off,
        length: len,
        content_type: None,
        etag: "wrong_md5_value".into(),
        last_modified: 0,
        user_metadata: HashMap::new(),
        content_md5: Some("wrong_md5_value".into()),
    };
    bucket.put_meta("key1", &meta).unwrap();

    let result = bucket.verify_integrity().unwrap();
    assert_eq!(result.total_objects, 1);
    assert_eq!(result.verified_ok, 0);
    assert_eq!(result.checksum_errors, 1);
}

#[test]
fn test_verify_streamed_put() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Use put_object_streamed which sets content_md5 automatically
    let tmp_path = dir.path().join("tmp_upload");
    let data = b"test data for verification";
    std::fs::write(&tmp_path, data).unwrap();
    let etag = format!("{:x}", Md5::digest(data));

    bucket
        .put_object_streamed("obj1", &tmp_path, None, etag, 0, HashMap::new())
        .unwrap();

    let result = bucket.verify_integrity().unwrap();
    assert_eq!(result.total_objects, 1);
    assert_eq!(result.verified_ok, 1);
    assert!(result.errors.is_empty());
}

#[test]
fn test_verify_multipart_upload() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let upload_id = bucket.create_multipart_upload();
    let etag1 = bucket.upload_part(&upload_id, 1, b"part one ").unwrap();
    let etag2 = bucket.upload_part(&upload_id, 2, b"part two").unwrap();
    let parts = vec![(1, etag1), (2, etag2)];
    bucket
        .complete_multipart_upload(&upload_id, "mpu_obj", &parts, None, 0, HashMap::new())
        .unwrap();

    // Verify should pass — content_md5 was set during assembly
    let result = bucket.verify_integrity().unwrap();
    assert_eq!(result.total_objects, 1);
    assert_eq!(result.verified_ok, 1);
    assert!(result.errors.is_empty());

    // Also verify the content_md5 matches MD5 of the full assembled data
    let meta = bucket.get_meta("mpu_obj").unwrap().unwrap();
    let full_data = bucket
        .read_data(meta.segment_id, meta.offset, meta.length)
        .unwrap();
    assert_eq!(full_data, b"part one part two");
    let expected_md5 = format!("{:x}", Md5::digest(&full_data));
    assert_eq!(meta.content_md5.as_deref(), Some(expected_md5.as_str()));
}
