//! Chaos / fault injection tests (#17). These tests stress concurrent writes,
//! compaction, and partial-write recovery. Disk-full and kill-loop scenarios
//! live in `chaos/` as shell scripts because they need OS-level fixtures
//! (tmpfs, signals) that don't compose cleanly into `cargo test`.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use md5::{Digest, Md5};
use simple3::storage::{BucketStore, Storage};

fn put(bucket: &BucketStore, key: &str, data: &[u8]) {
    let tmp = bucket
        .bucket_dir()
        .join(format!(".tmp_chaos_{key}_{}", uuid::Uuid::new_v4()));
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
        .unwrap();
}

#[test]
fn chaos_eight_writers_plus_compaction_keep_data_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).unwrap());
    storage.create_bucket("chaos").unwrap();
    let bucket = storage.get_bucket("chaos").unwrap().unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_secs(2);

    // 8 writer threads, each rewriting the same set of keys to maximize
    // dead-byte growth, plus a compaction thread sweeping continuously.
    let mut handles = Vec::new();
    for tid in 0..8 {
        let b = Arc::clone(&bucket);
        let s = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            let mut n = 0u64;
            while !s.load(std::sync::atomic::Ordering::Relaxed) && Instant::now() < deadline {
                let key = format!("k-{}", n % 20);
                let payload = vec![tid as u8; 8 * 1024];
                put(&b, &key, &payload);
                n += 1;
            }
            n
        }));
    }

    let b = Arc::clone(&bucket);
    let s = Arc::clone(&stop);
    let compactor = std::thread::spawn(move || {
        let mut runs = 0u64;
        while !s.load(std::sync::atomic::Ordering::Relaxed) && Instant::now() < deadline {
            let stats = b.segment_stats().unwrap();
            for seg in stats {
                if seg.dead_bytes > 0 {
                    // Best-effort: a put might rotate the segment under us.
                    let _ = b.compact_segment(seg.id);
                }
            }
            std::thread::sleep(Duration::from_millis(50));
            runs += 1;
        }
        runs
    });

    let writes: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let compactions = compactor.join().unwrap();

    assert!(writes > 0, "writers must perform at least one PUT");
    println!("chaos: {writes} writes, {compactions} compactions");

    // Integrity: every surviving object must read back, hash must match.
    let result = bucket.verify_integrity().unwrap();
    assert!(
        result.errors.is_empty(),
        "verify reported errors after concurrent stress: {:?}",
        result.errors
    );
}

#[test]
fn chaos_partial_write_to_segment_tail_recovers_via_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket("chaos").unwrap();
    let bucket = storage.get_bucket("chaos").unwrap().unwrap();

    // Establish a known-good baseline.
    for i in 0..16 {
        put(&bucket, &format!("k-{i:02}"), &vec![i as u8; 4096]);
    }
    let baseline = bucket.list_objects(None, 1000, None).unwrap().0;
    drop(bucket);
    drop(storage);

    // Append junk to the latest segment to simulate a torn write that landed
    // on disk but had no metadata commit.
    let seg_path = dir.path().join("chaos").join("seg_000000.bin");
    {
        let mut f = OpenOptions::new().append(true).open(&seg_path).unwrap();
        f.write_all(&vec![0xAA; 12345]).unwrap();
    }
    let pre_recovery_size = fs::metadata(&seg_path).unwrap().len();

    // Re-open: the recovery path should truncate the orphan tail.
    let storage = Storage::open(dir.path()).unwrap();
    let bucket = storage.get_bucket("chaos").unwrap().unwrap();
    let post_recovery_size = fs::metadata(&seg_path).unwrap().len();
    assert!(
        post_recovery_size < pre_recovery_size,
        "recovery must shrink the segment ({pre_recovery_size} -> {post_recovery_size})"
    );

    let recovered = bucket.list_objects(None, 1000, None).unwrap().0;
    assert_eq!(
        recovered.len(),
        baseline.len(),
        "all committed objects must survive recovery"
    );
    let result = bucket.verify_integrity().unwrap();
    assert!(result.errors.is_empty(), "verify after recovery: {:?}", result.errors);
}
