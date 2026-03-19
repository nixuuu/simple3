mod common;
use common::write_part;

use md5::{Digest, Md5};
use simple3::storage::Storage;
use simple3::types::ObjectMeta;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

fn make_meta(segment_id: u32, offset: u64, length: u64, etag: &str, crc: u32) -> ObjectMeta {
    ObjectMeta {
        segment_id,
        offset,
        length,
        content_type: Some("text/plain".into()),
        etag: etag.into(),
        last_modified: 1000,
        user_metadata: HashMap::new(),
        content_md5: None,
        content_crc32c: Some(crc),
        version_id: None,
        is_delete_marker: false,
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
    let (seg_id, offset, length, crc) = bucket.append_data(data).unwrap();

    let mut meta = make_meta(seg_id, offset, length, "abc123", crc);
    bucket.put_meta("key1", &mut meta).unwrap();

    let read_back = bucket.read_object(&meta).unwrap();
    assert_eq!(read_back, data);

    let got = bucket.get_meta("key1").unwrap().unwrap();
    assert_eq!(got.segment_id, seg_id);
    assert_eq!(got.offset, offset);
    assert_eq!(got.data_length(), data.len() as u64);
    assert_eq!(got.etag, "abc123");
}

#[test]
fn test_put_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg1, off1, len1, crc1) = bucket.append_data(b"old data").unwrap();
    bucket
        .put_meta("key", &mut make_meta(seg1, off1, len1, "old", crc1))
        .unwrap();

    let (seg2, off2, len2, crc2) = bucket.append_data(b"new data!!!").unwrap();
    bucket
        .put_meta("key", &mut make_meta(seg2, off2, len2, "new", crc2))
        .unwrap();

    let meta = bucket.get_meta("key").unwrap().unwrap();
    assert_eq!(meta.etag, "new");
    assert_eq!(meta.data_length(), 11);

    let data = bucket.read_object(&meta).unwrap();
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
    let (seg_id, offset, length, crc) = bucket.append_data(data).unwrap();
    let mut meta = ObjectMeta {
        segment_id: seg_id,
        offset,
        length,
        content_type: Some("application/json".into()),
        etag: "etag123".into(),
        last_modified: 42,
        user_metadata: [("x-custom".into(), "val".into())].into(),
        content_md5: None,
        content_crc32c: Some(crc),
        version_id: None,
        is_delete_marker: false,
    };
    bucket.put_meta("obj", &mut meta).unwrap();

    let got = bucket.get_meta("obj").unwrap().unwrap();
    assert_eq!(got.content_type.as_deref(), Some("application/json"));
    assert_eq!(got.data_length(), 12);
    assert_eq!(got.last_modified, 42);
    assert_eq!(got.user_metadata.get("x-custom").unwrap(), "val");
}

#[test]
fn test_delete_object() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg, off, len, crc) = bucket.append_data(b"deleteme").unwrap();
    bucket
        .put_meta("k", &mut make_meta(seg, off, len, "e", crc))
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

    let (seg, off_a, len_a, crc_a) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &mut make_meta(seg, off_a, len_a, "ea", crc_a))
        .unwrap();

    let (seg, off_b, len_b, crc_b) = bucket.append_data(b"BBBBBB").unwrap();
    bucket
        .put_meta("b", &mut make_meta(seg, off_b, len_b, "eb", crc_b))
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    // AAAA+crc(4) + BBBBBB+crc(4) = 4+4+6+4 = 18
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 18);

    // Delete leaves dead space, then compact reclaims it
    bucket.delete_object("a").unwrap();
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 18);

    bucket.compact().unwrap();
    // After compact: segment 0 rewritten with only "b" (6+4 = 10 bytes)
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 10);
}

#[test]
fn test_compaction_preserves_other_objects() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg, off_a, len_a, crc_a) = bucket.append_data(b"AAA").unwrap();
    bucket
        .put_meta("a", &mut make_meta(seg, off_a, len_a, "ea", crc_a))
        .unwrap();

    let (seg, off_b, len_b, crc_b) = bucket.append_data(b"BBB").unwrap();
    bucket
        .put_meta("b", &mut make_meta(seg, off_b, len_b, "eb", crc_b))
        .unwrap();

    let (seg, off_c, len_c, crc_c) = bucket.append_data(b"CCC").unwrap();
    bucket
        .put_meta("c", &mut make_meta(seg, off_c, len_c, "ec", crc_c))
        .unwrap();

    bucket.delete_object("b").unwrap();
    bucket.compact().unwrap();

    let meta_a = bucket.get_meta("a").unwrap().unwrap();
    let data_a = bucket.read_object(&meta_a).unwrap();
    assert_eq!(data_a, b"AAA");

    let meta_c = bucket.get_meta("c").unwrap().unwrap();
    let data_c = bucket.read_object(&meta_c).unwrap();
    assert_eq!(data_c, b"CCC");

    assert!(bucket.get_meta("b").unwrap().is_none());

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    // AAA+crc(4) + CCC+crc(4) = 14
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 14);
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
        let (seg, off, len, crc) = bucket.append_data(b"data").unwrap();
        bucket.put_meta(key, &mut make_meta(seg, off, len, "e", crc)).unwrap();
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
        let (seg, off, len, crc) = bucket.append_data(b"data").unwrap();
        bucket.put_meta(key, &mut make_meta(seg, off, len, "e", crc)).unwrap();
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
        let (seg, off, len, crc) = bucket.append_data(b"x").unwrap();
        bucket
            .put_meta(&key, &mut make_meta(seg, off, len, "e", crc))
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
        let (seg, off, len, crc) = bucket.append_data(b"survive restart").unwrap();
        bucket
            .put_meta("key1", &mut make_meta(seg, off, len, "etag", crc))
            .unwrap();
    }

    // Reopen
    let storage = Storage::open(dir.path()).unwrap();
    let buckets = storage.list_buckets().unwrap();
    assert_eq!(buckets, vec!["persist"]);

    let bucket = storage.get_bucket("persist").unwrap().unwrap();
    let meta = bucket.get_meta("key1").unwrap().unwrap();
    let data = bucket.read_object(&meta).unwrap();
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

    let data_bytes = b"streamed data here";
    let crc = crc32c::crc32c(data_bytes);
    let meta = bucket
        .put_object_streamed(
            "skey",
            &tmp_path,
            Some("text/plain".into()),
            "etag_s".into(),
            999,
            HashMap::new(),
            Some(crc),
        )
        .unwrap();

    assert_eq!(meta.data_length(), 18);
    assert_eq!(meta.etag, "etag_s");
    assert!(meta.content_crc32c.is_some());
    assert_eq!(meta.length, meta.data_length() + 4);

    // Temp file removed
    assert!(!tmp_path.exists());

    // Data readable
    let data = bucket.read_object(&meta).unwrap();
    assert_eq!(data, data_bytes);

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
    let etag1 = write_part(&bucket, &upload_id, 1, b"AAAA");
    let etag2 = write_part(&bucket, &upload_id, 2, b"BBBB");
    let etag3 = write_part(&bucket, &upload_id, 3, b"CCCC");

    // Complete
    let parts = vec![(1, etag1), (2, etag2), (3, etag3)];
    let (meta, etag) = bucket
        .complete_multipart_upload(&upload_id, "multipart_obj", &parts, None, 500, HashMap::new(), 0)
        .unwrap();

    // ETag has -N suffix
    assert!(etag.ends_with("-3"), "etag should end with -3: {etag}");
    assert_eq!(meta.data_length(), 12); // 4+4+4

    // Data is concatenation of parts in order
    let data = bucket.read_object(&meta).unwrap();
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
    let etag3 = write_part(&bucket, &upload_id, 3, b"CCC");
    let etag1 = write_part(&bucket, &upload_id, 1, b"AAA");
    let etag2 = write_part(&bucket, &upload_id, 2, b"BBB");

    let parts = vec![(3, etag3), (1, etag1), (2, etag2)];
    let (meta, _) = bucket
        .complete_multipart_upload(&upload_id, "ooo", &parts, None, 0, HashMap::new(), 0)
        .unwrap();

    // Should be sorted by part number
    let data = bucket.read_object(&meta).unwrap();
    assert_eq!(data, b"AAABBBCCC");
}

#[test]
fn test_abort_multipart_upload() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let upload_id = bucket.create_multipart_upload();
    write_part(&bucket, &upload_id, 1, b"data");
    write_part(&bucket, &upload_id, 2, b"more");

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
    let data1 = b"AAAAAAAAAA";
    std::fs::write(&tmp1, data1).unwrap(); // 10 bytes
    let crc1 = crc32c::crc32c(data1);
    bucket
        .put_object_streamed("key", &tmp1, None, "e1".into(), 0, HashMap::new(), Some(crc1))
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    // 10 data + 4 crc = 14
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 14);

    // Overwrite same key with smaller data (append-only: file grows)
    let tmp2 = bucket.bucket_dir().join(".tmp_ow2");
    let data2 = b"BBB";
    std::fs::write(&tmp2, data2).unwrap(); // 3 bytes
    let crc2 = crc32c::crc32c(data2);
    let meta = bucket
        .put_object_streamed("key", &tmp2, None, "e2".into(), 0, HashMap::new(), Some(crc2))
        .unwrap();

    // Append-only: seg = 14 + 3+4 = 21, dead_bytes = 14
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 21);
    assert_eq!(bucket.dead_bytes(), 14);
    assert_eq!(meta.data_length(), 3);

    // Data readable correctly
    let data = bucket.read_object(&meta).unwrap();
    assert_eq!(data, b"BBB");

    // Compact reclaims dead space
    bucket.compact().unwrap();
    // 3 data + 4 crc = 7
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 7);
    assert_eq!(bucket.dead_bytes(), 0);
}

#[test]
fn test_multipart_overwrite_compacts() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // First upload: 10 bytes data
    let tmp = bucket.bucket_dir().join(".tmp_mow");
    let d1 = b"XXXXXXXXXX";
    std::fs::write(&tmp, d1).unwrap();
    let crc1 = crc32c::crc32c(d1);
    bucket
        .put_object_streamed("key", &tmp, None, "e1".into(), 0, HashMap::new(), Some(crc1))
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    // 10 data + 4 crc = 14
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 14);

    // Overwrite via multipart: 6 bytes data total (append-only: file grows)
    let uid = bucket.create_multipart_upload();
    let e1 = write_part(&bucket, &uid, 1, b"AAA");
    let e2 = write_part(&bucket, &uid, 2, b"BBB");
    let (meta, _) = bucket
        .complete_multipart_upload(&uid, "key", &[(1, e1), (2, e2)], None, 0, HashMap::new(), 0)
        .unwrap();

    // Append-only: 14 + 6+4 = 24, dead_bytes = 14
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 24);
    assert_eq!(bucket.dead_bytes(), 14);
    assert_eq!(meta.data_length(), 6);

    let data = bucket.read_object(&meta).unwrap();
    assert_eq!(data, b"AAABBB");

    // Compact reclaims dead space
    bucket.compact().unwrap();
    // 6 data + 4 crc = 10
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 10);
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
    let (seg, off_a, len_a, crc_a) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &mut make_meta(seg, off_a, len_a, "ea", crc_a))
        .unwrap();
    let (seg, off_b, len_b, crc_b) = bucket.append_data(b"BBBBBB").unwrap();
    bucket
        .put_meta("b", &mut make_meta(seg, off_b, len_b, "eb", crc_b))
        .unwrap();

    assert_eq!(bucket.dead_bytes(), 0);

    // Delete "a" (4 data + 4 crc = 8 bytes become dead)
    bucket.delete_object("a").unwrap();
    assert_eq!(bucket.dead_bytes(), 8);

    // Overwrite "b" (6 data + 4 crc = 10 bytes become dead)
    let tmp = bucket.bucket_dir().join(".tmp_db");
    let dd = b"CC";
    std::fs::write(&tmp, dd).unwrap();
    let crc_cc = crc32c::crc32c(dd);
    bucket
        .put_object_streamed("b", &tmp, None, "e2".into(), 0, HashMap::new(), Some(crc_cc))
        .unwrap();
    assert_eq!(bucket.dead_bytes(), 18); // 8 + 10
}

#[test]
fn test_compact_full_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Put 3 objects: A(4) + B(6) + C(3) = 13 bytes
    let (seg, off_a, len_a, crc_a) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &mut make_meta(seg, off_a, len_a, "ea", crc_a))
        .unwrap();
    let (seg, off_b, len_b, crc_b) = bucket.append_data(b"BBBBBB").unwrap();
    bucket
        .put_meta("b", &mut make_meta(seg, off_b, len_b, "eb", crc_b))
        .unwrap();
    let (seg, off_c, len_c, crc_c) = bucket.append_data(b"CCC").unwrap();
    bucket
        .put_meta("c", &mut make_meta(seg, off_c, len_c, "ec", crc_c))
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    // (4+4) + (6+4) + (3+4) = 25
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 25);

    // Delete B
    bucket.delete_object("b").unwrap();
    assert_eq!(bucket.dead_bytes(), 10); // 6 data + 4 crc

    // Compact
    bucket.compact().unwrap();
    // A(4+4) + C(3+4) = 15
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 15);
    assert_eq!(bucket.dead_bytes(), 0);

    // Verify data intact
    let meta_a = bucket.get_meta("a").unwrap().unwrap();
    assert_eq!(bucket.read_object(&meta_a).unwrap(), b"AAAA");
    let meta_c = bucket.get_meta("c").unwrap().unwrap();
    assert_eq!(bucket.read_object(&meta_c).unwrap(), b"CCC");
    assert!(bucket.get_meta("b").unwrap().is_none());
}

#[test]
fn test_recovery_truncates_orphans() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg, off, len, crc) = bucket.append_data(b"HELLO").unwrap();
    bucket
        .put_meta("key", &mut make_meta(seg, off, len, "e1", crc))
        .unwrap();

    let seg_path = dir.path().join("b").join("seg_000000.bin");
    // 5 data + 4 crc = 9
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 9);

    // Simulate orphaned bytes (crashed append without metadata update)
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .unwrap();
        f.write_all(b"ORPHAN_JUNK").unwrap();
    }
    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 20); // 9 + 11

    drop(bucket);
    drop(storage);

    // Reopen — recovery should truncate orphans
    let storage2 = Storage::open(dir.path()).unwrap();
    let bucket2 = storage2.get_bucket("b").unwrap().unwrap();

    assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), 9);
    let meta = bucket2.get_meta("key").unwrap().unwrap();
    assert_eq!(bucket2.read_object(&meta).unwrap(), b"HELLO");
}

#[test]
fn test_recovery_after_interrupted_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // Put objects
    let (seg, off_a, len_a, crc_a) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &mut make_meta(seg, off_a, len_a, "ea", crc_a))
        .unwrap();
    let (seg, off_b, len_b, crc_b) = bucket.append_data(b"BBBB").unwrap();
    bucket
        .put_meta("b", &mut make_meta(seg, off_b, len_b, "eb", crc_b))
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
    assert_eq!(bucket2.read_object(&meta_a).unwrap(), b"AAAA");
    let meta_b = bucket2.get_meta("b").unwrap().unwrap();
    assert_eq!(bucket2.read_object(&meta_b).unwrap(), b"BBBB");
}

// === Segment stats ===

#[test]
fn test_segment_stats() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let (seg, off, len, crc) = bucket.append_data(b"AAAA").unwrap();
    bucket
        .put_meta("a", &mut make_meta(seg, off, len, "ea", crc))
        .unwrap();
    let (seg, off, len, crc) = bucket.append_data(b"BBBB").unwrap();
    bucket
        .put_meta("b", &mut make_meta(seg, off, len, "eb", crc))
        .unwrap();

    bucket.delete_object("a").unwrap();

    let stats = bucket.segment_stats().unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].id, 0);
    // (4+4) + (4+4) = 16
    assert_eq!(stats[0].size, 16);
    // 4 data + 4 crc = 8
    assert_eq!(stats[0].dead_bytes, 8);
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
    let (seg, off, len, crc) = bucket.append_data(data).unwrap();
    let mut meta = ObjectMeta {
        segment_id: seg,
        offset: off,
        length: len,
        content_type: None,
        etag: md5.clone(),
        last_modified: 0,
        user_metadata: HashMap::new(),
        content_md5: Some(md5),
        content_crc32c: Some(crc),
        version_id: None,
        is_delete_marker: false,
    };
    bucket.put_meta("key1", &mut meta).unwrap();

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
    let (seg, off, len, crc) = bucket.append_data(data).unwrap();
    let mut meta = ObjectMeta {
        segment_id: seg,
        offset: off,
        length: len,
        content_type: None,
        etag: "wrong_md5_value".into(),
        last_modified: 0,
        user_metadata: HashMap::new(),
        content_md5: Some("wrong_md5_value".into()),
        content_crc32c: Some(crc),
        version_id: None,
        is_delete_marker: false,
    };
    bucket.put_meta("key1", &mut meta).unwrap();

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

    let crc = crc32c::crc32c(data);
    bucket
        .put_object_streamed("obj1", &tmp_path, None, etag, 0, HashMap::new(), Some(crc))
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
    let etag1 = write_part(&bucket, &upload_id, 1, b"part one ");
    let etag2 = write_part(&bucket, &upload_id, 2, b"part two");
    let parts = vec![(1, etag1), (2, etag2)];
    bucket
        .complete_multipart_upload(&upload_id, "mpu_obj", &parts, None, 0, HashMap::new(), 0)
        .unwrap();

    // Verify should pass — content_md5 was set during assembly
    let result = bucket.verify_integrity().unwrap();
    assert_eq!(result.total_objects, 1);
    assert_eq!(result.verified_ok, 1);
    assert!(result.errors.is_empty());

    // Verify CRC metadata persisted on multipart path
    let meta = bucket.get_meta("mpu_obj").unwrap().unwrap();
    assert!(meta.content_crc32c.is_some());
    assert_eq!(meta.length, meta.data_length() + 4);

    // Verify the content_md5 matches MD5 of the full assembled data
    let full_data = bucket.read_object(&meta).unwrap();
    assert_eq!(full_data, b"part one part two");
    let expected_md5 = format!("{:x}", Md5::digest(&full_data));
    assert_eq!(meta.content_md5.as_deref(), Some(expected_md5.as_str()));
}

// === CRC validation ===

#[test]
fn test_read_object_crc_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let data = b"hello world";
    let (seg, off, len, crc) = bucket.append_data(data).unwrap();
    let mut meta = make_meta(seg, off, len, "etag", crc);
    bucket.put_meta("key", &mut meta).unwrap();

    // Valid read succeeds
    let result = bucket.read_object(&meta).unwrap();
    assert_eq!(result, data);

    // Corrupt one byte in the data portion of the segment file
    let seg_path = dir.path().join("b").join("seg_000000.bin");
    {
        let mut f = OpenOptions::new().write(true).open(&seg_path).unwrap();
        f.seek(SeekFrom::Start(off)).unwrap();
        f.write_all(&[0xFF]).unwrap();
        f.sync_all().unwrap();
    }

    // read_object should detect the CRC mismatch
    let err = bucket.read_object(&meta).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("CRC32C mismatch"));
}

// === Backup / restore ===

fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let target = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_recursive(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

#[test]
fn test_backup_restore_cold_copy() {
    // --- Set up original storage with objects ---
    let orig_dir = tempfile::tempdir().unwrap();

    // Create auth store so _auth.redb exists
    let (_auth, _bootstrap) = simple3::auth::AuthStore::open(orig_dir.path()).unwrap();
    drop(_auth);

    let storage = Storage::open(orig_dir.path()).unwrap();
    storage.create_bucket("docs").unwrap();
    let bucket = storage.get_bucket("docs").unwrap().unwrap();

    // 1) Simple object via append_data + put_meta
    let data1 = b"first object";
    let md5_1 = format!("{:x}", Md5::digest(data1));
    let (seg, off, len, crc) = bucket.append_data(data1).unwrap();
    let mut meta1 = ObjectMeta {
        segment_id: seg,
        offset: off,
        length: len,
        content_type: Some("text/plain".into()),
        etag: md5_1.clone(),
        last_modified: 100,
        user_metadata: HashMap::new(),
        content_md5: Some(md5_1),
        content_crc32c: Some(crc),
        version_id: None,
        is_delete_marker: false,
    };
    bucket.put_meta("simple.txt", &mut meta1).unwrap();

    // 2) Streamed put
    let data2 = b"streamed backup content";
    let tmp_path = bucket.bucket_dir().join(".tmp_backup_test");
    std::fs::write(&tmp_path, data2).unwrap();
    let etag2 = format!("{:x}", Md5::digest(data2));
    let crc2 = crc32c::crc32c(data2);
    bucket
        .put_object_streamed(
            "streamed.txt",
            &tmp_path,
            Some("application/octet-stream".into()),
            etag2,
            200,
            HashMap::new(),
            Some(crc2),
        )
        .unwrap();

    // 3) Multipart upload
    let upload_id = bucket.create_multipart_upload();
    let etag_p1 = write_part(&bucket, &upload_id, 1, b"alpha-");
    let etag_p2 = write_part(&bucket, &upload_id, 2, b"beta");
    let parts = vec![(1, etag_p1), (2, etag_p2)];
    bucket
        .complete_multipart_upload(&upload_id, "multi.bin", &parts, None, 300, HashMap::new(), 0)
        .unwrap();

    // Drop storage to simulate server stop
    drop(bucket);
    drop(storage);

    // --- Cold copy to backup dir ---
    let backup_dir = tempfile::tempdir().unwrap();
    copy_dir_recursive(orig_dir.path(), backup_dir.path());

    // --- Reopen from backup ---
    let restored = Storage::open(backup_dir.path()).unwrap();
    let rb = restored.get_bucket("docs").unwrap().unwrap();

    // Verify simple object
    let m1 = rb.get_meta("simple.txt").unwrap().unwrap();
    assert_eq!(rb.read_object(&m1).unwrap(), data1);

    // Verify streamed object
    let m2 = rb.get_meta("streamed.txt").unwrap().unwrap();
    assert_eq!(rb.read_object(&m2).unwrap(), data2);

    // Verify multipart object
    let m3 = rb.get_meta("multi.bin").unwrap().unwrap();
    assert_eq!(rb.read_object(&m3).unwrap(), b"alpha-beta");

    // Full integrity check
    let result = rb.verify_integrity().unwrap();
    assert_eq!(result.total_objects, 3);
    assert_eq!(result.verified_ok, 3);
    assert!(result.errors.is_empty());

    // Auth store opens cleanly from backup
    let (auth_restored, bootstrap) = simple3::auth::AuthStore::open(backup_dir.path()).unwrap();
    assert!(matches!(
        bootstrap,
        simple3::auth::types::BootstrapResult::Existing
    ));
    drop(auth_restored);
}
