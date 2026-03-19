use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use md5::{Digest, Md5};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use simple3::storage::{BucketStore, Storage};
use simple3::types::ObjectMeta;

static TEST_TMP: AtomicU64 = AtomicU64::new(0);

fn write_part(bucket: &BucketStore, upload_id: &str, part_number: i32, data: &[u8]) -> String {
    let id = TEST_TMP.fetch_add(1, Ordering::Relaxed);
    let tmp = bucket.bucket_dir().join(format!(".tmp_test_{id}"));
    fs::write(&tmp, data).unwrap();
    let md5_hex = format!("{:x}", Md5::digest(data));
    bucket.upload_part(upload_id, part_number, &tmp, &md5_hex).unwrap()
}

const OBJECTS: TableDefinition<&str, &[u8]> = TableDefinition::new("objects");
const SEG_COMPACTING: TableDefinition<u32, u8> = TableDefinition::new("seg_compacting");

fn seg_filename(id: u32) -> String {
    format!("seg_{id:06}.bin")
}

fn open_redb(bucket_dir: &Path) -> Database {
    Database::create(bucket_dir.join("index.redb")).unwrap()
}

fn write_obj(
    bucket: &simple3::storage::BucketStore,
    key: &str,
    data: &[u8],
) -> ObjectMeta {
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

fn append_orphan(bucket_dir: &Path, seg_id: u32, junk: &[u8]) {
    let seg = bucket_dir.join(seg_filename(seg_id));
    let mut f = fs::OpenOptions::new().append(true).open(&seg).unwrap();
    f.write_all(junk).unwrap();
}

fn set_compacting_flag(bucket_dir: &Path, seg_id: u32) {
    let db = open_redb(bucket_dir);
    let txn = db.begin_write().unwrap();
    {
        let mut t = txn.open_table(SEG_COMPACTING).unwrap();
        t.insert(seg_id, 1u8).unwrap();
    }
    txn.commit().unwrap();
    drop(db);
}

fn read_all_meta(bucket_dir: &Path) -> Vec<(String, ObjectMeta)> {
    let db = open_redb(bucket_dir);
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(OBJECTS).unwrap();
    let mut out = Vec::new();
    for r in table.iter().unwrap() {
        let (k, v): (redb::AccessGuard<&str>, redb::AccessGuard<&[u8]>) = r.unwrap();
        let meta = ObjectMeta::from_bytes(v.value()).unwrap();
        out.push((k.value().to_owned(), meta));
    }
    drop(table);
    drop(txn);
    drop(db);
    out
}

// === Test 1: committed objects survive a crashed PUT ===

#[test]
fn test_crash_during_put_committed_survive() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    write_obj(&bucket, "obj1", b"AAAA");
    write_obj(&bucket, "obj2", b"BBBBBB");
    write_obj(&bucket, "obj3", b"CCC");

    let bd = bucket.bucket_dir().to_path_buf();
    drop(bucket);
    drop(storage);

    // Simulate: data appended but metadata not committed + leftover tmp file
    append_orphan(&bd, 0, b"ORPHAN_DATA_FROM_CRASHED_PUT");
    fs::write(bd.join(".tmp_abcdef"), b"temp upload").unwrap();

    let storage2 = Storage::open(dir.path()).unwrap();
    let b2 = storage2.get_bucket("b").unwrap().unwrap();

    // Committed objects intact
    let m1 = b2.get_meta("obj1").unwrap().unwrap();
    assert_eq!(b2.read_object(&m1).unwrap(), b"AAAA");
    let m2 = b2.get_meta("obj2").unwrap().unwrap();
    assert_eq!(b2.read_object(&m2).unwrap(), b"BBBBBB");
    let m3 = b2.get_meta("obj3").unwrap().unwrap();
    assert_eq!(b2.read_object(&m3).unwrap(), b"CCC");

    // Orphans truncated, tmp cleaned
    let seg_size = fs::metadata(bd.join("seg_000000.bin")).unwrap().len();
    // (4+4) + (6+4) + (3+4) = 25
    assert_eq!(seg_size, 25);
    assert!(!bd.join(".tmp_abcdef").exists());

    // Integrity check
    let v = b2.verify_integrity().unwrap();
    assert_eq!(v.total_objects, 3);
    assert_eq!(v.verified_ok, 3);
    assert!(v.errors.is_empty());

    // Can still write after recovery
    write_obj(&b2, "obj4", b"POST_RECOVERY");
    let m4 = b2.get_meta("obj4").unwrap().unwrap();
    assert_eq!(b2.read_object(&m4).unwrap(), b"POST_RECOVERY");
}

// === Test 2: crash during PUT with multiple segments ===

#[test]
fn test_crash_during_put_multi_segment() {
    let dir = tempfile::tempdir().unwrap();
    // Segment size 30 to ensure "a" (15+4=19) fits, "b" (10+4=14) triggers rotation
    let storage = Storage::open_with_segment_size(dir.path(), 30).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    write_obj(&bucket, "a", b"AAAAAAAAAAAAAAA"); // 15 data + 4 crc = 19
    let m_b = write_obj(&bucket, "b", b"BBBBBBBBBB");       // 10 data + 4 crc = 14, triggers rotation

    let bd = bucket.bucket_dir().to_path_buf();
    let active_seg = m_b.segment_id;
    drop(bucket);
    drop(storage);

    append_orphan(&bd, active_seg, b"ORPHAN_MULTI_SEG");

    let storage2 = Storage::open_with_segment_size(dir.path(), 30).unwrap();
    let b2 = storage2.get_bucket("b").unwrap().unwrap();

    let ma = b2.get_meta("a").unwrap().unwrap();
    assert_eq!(b2.read_object(&ma).unwrap(), b"AAAAAAAAAAAAAAA");
    let mb = b2.get_meta("b").unwrap().unwrap();
    assert_eq!(b2.read_object(&mb).unwrap(), b"BBBBBBBBBB");

    let active_size = fs::metadata(bd.join(seg_filename(active_seg))).unwrap().len();
    assert_eq!(active_size, 14); // 10 data + 4 crc

    let v = b2.verify_integrity().unwrap();
    assert_eq!(v.verified_ok, 2);
    assert!(v.errors.is_empty());
}

// === Test 3: crash during multipart before metadata commit ===

#[test]
fn test_crash_during_multipart_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    write_obj(&bucket, "existing", b"PRESERVED");

    // Start multipart but don't complete
    let upload_id = bucket.create_multipart_upload();
    write_part(&bucket, &upload_id, 1, b"part-one-");
    write_part(&bucket, &upload_id, 2, b"part-two");

    let bd = bucket.bucket_dir().to_path_buf();
    drop(bucket);
    drop(storage);

    // Simulate: partially assembled data written to segment + mpu files remain
    append_orphan(&bd, 0, b"part-one-part-two");

    let storage2 = Storage::open(dir.path()).unwrap();
    let b2 = storage2.get_bucket("b").unwrap().unwrap();

    // Committed object survives
    let m = b2.get_meta("existing").unwrap().unwrap();
    assert_eq!(b2.read_object(&m).unwrap(), b"PRESERVED");

    // No multipart object committed
    assert!(b2.get_meta("multipart_key").unwrap().is_none());

    // mpu files cleaned
    let mpu_count = fs::read_dir(&bd)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(".mpu_"))
        })
        .count();
    assert_eq!(mpu_count, 0);

    let v = b2.verify_integrity().unwrap();
    assert_eq!(v.verified_ok, 1);
    assert!(v.errors.is_empty());
}

// === Test 4: crash after multipart commit but before part cleanup ===

#[test]
fn test_crash_during_multipart_after_commit() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    let upload_id = bucket.create_multipart_upload();
    let e1 = write_part(&bucket, &upload_id, 1, b"part-one-");
    let e2 = write_part(&bucket, &upload_id, 2, b"part-two");
    let parts = vec![(1, e1), (2, e2)];
    bucket
        .complete_multipart_upload(&upload_id, "mpu_obj", &parts, None, 1000, HashMap::new(), 0)
        .unwrap();

    let bd = bucket.bucket_dir().to_path_buf();
    drop(bucket);
    drop(storage);

    // Simulate: crash before part file cleanup — re-create part files
    fs::write(bd.join(format!(".mpu_{upload_id}_00001")), b"leftover1").unwrap();
    fs::write(bd.join(format!(".mpu_{upload_id}_00002")), b"leftover2").unwrap();

    let storage2 = Storage::open(dir.path()).unwrap();
    let b2 = storage2.get_bucket("b").unwrap().unwrap();

    let m = b2.get_meta("mpu_obj").unwrap().unwrap();
    assert_eq!(
        b2.read_object(&m).unwrap(),
        b"part-one-part-two"
    );

    // Part files cleaned
    let mpu_count = fs::read_dir(&bd)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(".mpu_"))
        })
        .count();
    assert_eq!(mpu_count, 0);

    let v = b2.verify_integrity().unwrap();
    assert_eq!(v.verified_ok, 1);
    assert!(v.errors.is_empty());
}

// === Test 5: crash during compaction AFTER rename (critical path) ===

#[test]
fn test_crash_compaction_after_rename() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    write_obj(&bucket, "a", b"AAAA");   // 4+4=8 bytes @ offset 0
    write_obj(&bucket, "b", b"BBBBBB"); // 6+4=10 bytes @ offset 8
    write_obj(&bucket, "c", b"CCC");    // 3+4=7 bytes @ offset 18

    bucket.delete_object("b").unwrap();

    let bd = bucket.bucket_dir().to_path_buf();
    drop(bucket);
    drop(storage);

    // Read original meta to know old offsets
    let metas = read_all_meta(&bd);
    let meta_a = metas.iter().find(|(k, _)| k == "a").unwrap().1.clone();
    let meta_c = metas.iter().find(|(k, _)| k == "c").unwrap().1.clone();
    assert_eq!(meta_a.offset, 0);
    assert_eq!(meta_a.length, 8); // 4 data + 4 crc
    assert_eq!(meta_c.offset, 18);
    assert_eq!(meta_c.length, 7); // 3 data + 4 crc

    // Build compacted segment: A(8) + C(7) = 15 bytes contiguous
    let seg_path = bd.join("seg_000000.bin");
    let original = fs::read(&seg_path).unwrap();
    let mut compacted = Vec::new();
    compacted.extend_from_slice(&original[0..8]);   // A data+crc
    compacted.extend_from_slice(&original[18..25]);  // C data+crc
    assert_eq!(compacted.len(), 15);
    fs::write(&seg_path, &compacted).unwrap();

    // Set compacting flag (simulates: flag set, rename done, metadata commit didn't happen)
    set_compacting_flag(&bd, 0);

    // Reopen — recover_segment_compaction should fix offsets
    let storage2 = Storage::open(dir.path()).unwrap();
    let b2 = storage2.get_bucket("b").unwrap().unwrap();

    // Offsets should be remapped: A@0, C@8
    let ma = b2.get_meta("a").unwrap().unwrap();
    assert_eq!(ma.offset, 0);
    assert_eq!(ma.length, 8);
    assert_eq!(b2.read_object(&ma).unwrap(), b"AAAA");

    let mc = b2.get_meta("c").unwrap().unwrap();
    assert_eq!(mc.offset, 8);
    assert_eq!(mc.length, 7);
    assert_eq!(b2.read_object(&mc).unwrap(), b"CCC");

    assert!(b2.get_meta("b").unwrap().is_none());

    let v = b2.verify_integrity().unwrap();
    assert_eq!(v.verified_ok, 2);
    assert!(v.errors.is_empty());

    // Can write more objects after recovery
    write_obj(&b2, "d", b"POST_COMPACT_RECOVERY");
    let md = b2.get_meta("d").unwrap().unwrap();
    assert_eq!(
        b2.read_object(&md).unwrap(),
        b"POST_COMPACT_RECOVERY"
    );
}

// === Test 6: crash during compaction BEFORE rename ===

#[test]
fn test_crash_compaction_before_rename() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    write_obj(&bucket, "a", b"AAAA");
    write_obj(&bucket, "b", b"BBBBBB");
    write_obj(&bucket, "c", b"CCC");

    bucket.delete_object("b").unwrap();

    let bd = bucket.bucket_dir().to_path_buf();
    drop(bucket);
    drop(storage);

    // Create .bin.tmp (compaction copy was done but rename didn't happen)
    let seg_path = bd.join("seg_000000.bin");
    let original = fs::read(&seg_path).unwrap();
    let mut compacted = Vec::new();
    compacted.extend_from_slice(&original[0..8]);   // A data+crc
    compacted.extend_from_slice(&original[18..25]);  // C data+crc
    fs::write(bd.join("seg_000000.bin.tmp"), &compacted).unwrap();

    // Set flag (but original file unchanged — 25 bytes)
    set_compacting_flag(&bd, 0);

    let storage2 = Storage::open(dir.path()).unwrap();
    let b2 = storage2.get_bucket("b").unwrap().unwrap();

    // .bin.tmp removed
    assert!(!bd.join("seg_000000.bin.tmp").exists());

    // Original offsets preserved (file_size=25, compacted_size=15, no remap)
    let ma = b2.get_meta("a").unwrap().unwrap();
    assert_eq!(ma.offset, 0);
    assert_eq!(b2.read_object(&ma).unwrap(), b"AAAA");
    let mc = b2.get_meta("c").unwrap().unwrap();
    assert_eq!(mc.offset, 18);
    assert_eq!(b2.read_object(&mc).unwrap(), b"CCC");

    let v = b2.verify_integrity().unwrap();
    assert_eq!(v.verified_ok, 2);
    assert!(v.errors.is_empty());
}

// === Test 7: compaction recovery + orphan truncation on different segments ===

#[test]
fn test_crash_compaction_with_orphan_on_active() {
    let dir = tempfile::tempdir().unwrap();
    // 30 bytes per segment to trigger rotation
    let storage = Storage::open_with_segment_size(dir.path(), 30).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    // seg 0: 15+4=19 bytes
    write_obj(&bucket, "a", b"AAAAAAAAAAAAAAA");
    // seg 1 (rotation at 30): 10+4=14, 19+14=33>30
    let m_b = write_obj(&bucket, "b", b"BBBBBBBBBB");
    let active_seg = m_b.segment_id;
    assert!(active_seg > 0, "expected rotation to seg 1+");

    // Delete a from seg 0 to create dead bytes
    bucket.delete_object("a").unwrap();

    // Write another object to active segment
    write_obj(&bucket, "c", b"CCCCC"); // 5+4=9 bytes on active seg

    let bd = bucket.bucket_dir().to_path_buf();
    drop(bucket);
    drop(storage);

    // Simulate compaction of seg 0 (after rename) — seg 0 is now empty after delete
    // Actually a was the only object on seg 0 and it's deleted, so compacted = 0 bytes.
    // Let's just set the flag and write an empty file for seg 0.
    fs::write(bd.join("seg_000000.bin"), b"").unwrap();
    set_compacting_flag(&bd, 0);

    // Simulate orphan on active segment
    append_orphan(&bd, active_seg, b"ORPHAN_JUNK_ON_ACTIVE");

    let storage2 = Storage::open_with_segment_size(dir.path(), 20).unwrap();
    let b2 = storage2.get_bucket("b").unwrap().unwrap();

    // b and c survive
    let mb = b2.get_meta("b").unwrap().unwrap();
    assert_eq!(b2.read_object(&mb).unwrap(), b"BBBBBBBBBB");
    let mc = b2.get_meta("c").unwrap().unwrap();
    assert_eq!(b2.read_object(&mc).unwrap(), b"CCCCC");

    // a was deleted
    assert!(b2.get_meta("a").unwrap().is_none());

    // Active segment orphan truncated
    let active_size = fs::metadata(bd.join(seg_filename(active_seg))).unwrap().len();
    // (10+4) + (5+4) = 23
    assert_eq!(active_size, 23);

    let v = b2.verify_integrity().unwrap();
    assert_eq!(v.verified_ok, 2);
    assert!(v.errors.is_empty());
}

// === Test 8: multiple crash artifacts simultaneously ===

#[test]
fn test_combined_crash_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    write_obj(&bucket, "x", b"XXXXX");
    write_obj(&bucket, "y", b"YYYYYYY");

    let bd = bucket.bucket_dir().to_path_buf();
    drop(bucket);
    drop(storage);

    // Scatter crash artifacts
    append_orphan(&bd, 0, b"ORPHAN");
    fs::write(bd.join(".tmp_upload1"), b"garbage").unwrap();
    fs::write(bd.join(".tmp_upload2"), b"garbage2").unwrap();
    fs::write(bd.join(".mpu_fakeid_00001"), b"part").unwrap();
    fs::write(bd.join(".mpu_fakeid_00002"), b"part2").unwrap();
    fs::write(bd.join("seg_000000.bin.tmp"), b"compact_wip").unwrap();

    let storage2 = Storage::open(dir.path()).unwrap();
    let b2 = storage2.get_bucket("b").unwrap().unwrap();

    // All artifacts cleaned
    assert!(!bd.join(".tmp_upload1").exists());
    assert!(!bd.join(".tmp_upload2").exists());
    assert!(!bd.join(".mpu_fakeid_00001").exists());
    assert!(!bd.join(".mpu_fakeid_00002").exists());
    assert!(!bd.join("seg_000000.bin.tmp").exists());

    // Segment truncated
    let seg_size = fs::metadata(bd.join("seg_000000.bin")).unwrap().len();
    // (5+4) + (7+4) = 20
    assert_eq!(seg_size, 20);

    // Objects intact
    let mx = b2.get_meta("x").unwrap().unwrap();
    assert_eq!(b2.read_object(&mx).unwrap(), b"XXXXX");
    let my = b2.get_meta("y").unwrap().unwrap();
    assert_eq!(b2.read_object(&my).unwrap(), b"YYYYYYY");

    let v = b2.verify_integrity().unwrap();
    assert_eq!(v.verified_ok, 2);
    assert!(v.errors.is_empty());
}

// === Test 9: recovery then compaction still works ===

#[test]
fn test_recovery_then_compaction_works() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("b").unwrap();
    let bucket = storage.get_bucket("b").unwrap().unwrap();

    write_obj(&bucket, "a", b"AAAA");
    write_obj(&bucket, "b", b"BBBBBB");
    write_obj(&bucket, "c", b"CCC");

    bucket.delete_object("b").unwrap();
    assert_eq!(bucket.dead_bytes(), 10); // 6 data + 4 crc

    let bd = bucket.bucket_dir().to_path_buf();
    drop(bucket);
    drop(storage);

    append_orphan(&bd, 0, b"JUNK");

    let storage2 = Storage::open(dir.path()).unwrap();
    let b2 = storage2.get_bucket("b").unwrap().unwrap();

    // Recovery truncated orphans
    let seg_size = fs::metadata(bd.join("seg_000000.bin")).unwrap().len();
    // (4+4) + (6+4) + (3+4) = 25
    assert_eq!(seg_size, 25);

    // Compaction should still work
    b2.compact().unwrap();

    let seg_size = fs::metadata(bd.join("seg_000000.bin")).unwrap().len();
    // (4+4) + (3+4) = 15
    assert_eq!(seg_size, 15);
    assert_eq!(b2.dead_bytes(), 0);

    let ma = b2.get_meta("a").unwrap().unwrap();
    assert_eq!(b2.read_object(&ma).unwrap(), b"AAAA");
    let mc = b2.get_meta("c").unwrap().unwrap();
    assert_eq!(b2.read_object(&mc).unwrap(), b"CCC");

    let v = b2.verify_integrity().unwrap();
    assert_eq!(v.verified_ok, 2);
    assert!(v.errors.is_empty());
}
