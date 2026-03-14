use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use simple3::storage::Storage;
use simple3::types::ObjectMeta;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

fn make_meta(segment_id: u32, offset: u64, length: u64) -> ObjectMeta {
    ObjectMeta {
        segment_id,
        offset,
        length,
        content_type: Some("application/octet-stream".into()),
        etag: "bench".into(),
        last_modified: 0,
        user_metadata: HashMap::new(),
        content_md5: None,
    }
}

fn setup_bucket(name: &str) -> (tempfile::TempDir, Storage) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket(name).unwrap();
    (dir, storage)
}

/// Pre-fill bucket with N objects of given size, return keys
fn prefill(storage: &Storage, bucket: &str, count: usize, obj_size: usize) -> Vec<String> {
    let b = storage.get_bucket(bucket).unwrap().unwrap();
    let data = vec![0x42u8; obj_size];
    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let key = format!("key{i:06}");
        let (seg, off, len) = b.append_data(&data).unwrap();
        b.put_meta(&key, &make_meta(seg, off, len)).unwrap();
        keys.push(key);
    }
    keys
}

// === Put benchmarks (shared bucket, unique keys via atomic counter) ===

fn bench_put(c: &mut Criterion) {
    let data_1kb = vec![0x42u8; 1024];
    let data_1mb = vec![0x42u8; 1024 * 1024];

    c.bench_function("put_1kb", |b| {
        let (_dir, storage) = setup_bucket("b");
        let bucket = storage.get_bucket("b").unwrap().unwrap();
        let counter = AtomicU64::new(0);
        b.iter(|| {
            let i = counter.fetch_add(1, Ordering::Relaxed);
            let key = format!("k{i}");
            let (seg, off, len) = bucket.append_data(&data_1kb).unwrap();
            bucket.put_meta(&key, &make_meta(seg, off, len)).unwrap();
        });
    });

    c.bench_function("put_1mb", |b| {
        let (_dir, storage) = setup_bucket("b");
        let bucket = storage.get_bucket("b").unwrap().unwrap();
        let counter = AtomicU64::new(0);
        b.iter(|| {
            let i = counter.fetch_add(1, Ordering::Relaxed);
            let key = format!("k{i}");
            let (seg, off, len) = bucket.append_data(&data_1mb).unwrap();
            bucket.put_meta(&key, &make_meta(seg, off, len)).unwrap();
        });
    });
}

// === Get benchmarks (random key access across many objects) ===

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_random");

    for &(label, obj_size, count) in &[
        ("1kb_among_100", 1024, 100),
        ("1kb_among_1000", 1024, 1000),
        ("1mb_among_100", 1024 * 1024, 100),
    ] {
        group.bench_function(label, |b| {
            let (_dir, storage) = setup_bucket("b");
            let keys = prefill(&storage, "b", count, obj_size);
            let bucket = storage.get_bucket("b").unwrap().unwrap();
            let mut idx = 0usize;
            b.iter(|| {
                let key = &keys[idx % keys.len()];
                idx = idx.wrapping_add(7919); // prime stride for pseudo-random
                let meta = bucket.get_meta(key).unwrap().unwrap();
                bucket.read_data(meta.segment_id, meta.offset, meta.length).unwrap();
            });
        });
    }

    group.finish();
}

// === Delete+compact scaling ===

fn bench_compact_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("compact_scaling");
    group.sample_size(30);

    for count in [100, 1000, 5000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_batched(
                || {
                    let (dir, storage) = setup_bucket("b");
                    let keys = prefill(&storage, "b", count, 64);
                    let mid_key = keys[count / 2].clone();
                    (dir, storage, mid_key)
                },
                |(_dir, storage, mid_key)| {
                    let bucket = storage.get_bucket("b").unwrap().unwrap();
                    bucket.delete_object(&mid_key).unwrap();
                    bucket.compact().unwrap();
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

// === List scaling ===

fn bench_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_scaling");

    for count in [100, 1000, 5000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            let (_dir, storage) = setup_bucket("b");
            prefill(&storage, "b", count, 1);
            let bucket = storage.get_bucket("b").unwrap().unwrap();
            b.iter(|| {
                bucket.list_objects(None, count, None).unwrap();
            });
        });
    }

    group.finish();
}

// === Concurrent read/write ===

fn bench_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent");
    group.sample_size(20);

    // Concurrent reads (4 threads)
    group.bench_function("4_thread_get_1kb", |b| {
        let (_dir, storage) = setup_bucket("b");
        prefill(&storage, "b", 100, 1024);
        let bucket = storage.get_bucket("b").unwrap().unwrap();

        b.iter(|| {
            std::thread::scope(|s| {
                for t in 0..4u64 {
                    let bucket = Arc::clone(&bucket);
                    s.spawn(move || {
                        for i in 0..25 {
                            let key = format!("key{:06}", (t * 25 + i) % 100);
                            let meta = bucket.get_meta(&key).unwrap().unwrap();
                            bucket.read_data(meta.segment_id, meta.offset, meta.length).unwrap();
                        }
                    });
                }
            });
        });
    });

    // Concurrent writes (4 threads)
    group.bench_function("4_thread_put_1kb", |b| {
        let data = vec![0x42u8; 1024];

        b.iter_batched(
            || setup_bucket("b"),
            |(_dir, storage)| {
                let bucket = storage.get_bucket("b").unwrap().unwrap();
                std::thread::scope(|s| {
                    for t in 0..4u64 {
                        let bucket = Arc::clone(&bucket);
                        let data = &data;
                        s.spawn(move || {
                            for i in 0..25 {
                                let key = format!("t{t}k{i}");
                                let (seg, off, len) = bucket.append_data(data).unwrap();
                                bucket.put_meta(&key, &make_meta(seg, off, len)).unwrap();
                            }
                        });
                    }
                });
            },
            criterion::BatchSize::LargeInput,
        );
    });

    // Mixed: 2 readers + 2 writers
    group.bench_function("mixed_2r_2w", |b| {
        let data = vec![0x42u8; 1024];

        b.iter_batched(
            || {
                let (dir, storage) = setup_bucket("b");
                prefill(&storage, "b", 50, 1024);
                (dir, storage)
            },
            |(_dir, storage)| {
                let bucket = storage.get_bucket("b").unwrap().unwrap();
                std::thread::scope(|s| {
                    // 2 readers
                    for t in 0..2u64 {
                        let bucket = Arc::clone(&bucket);
                        s.spawn(move || {
                            for i in 0..25 {
                                let key = format!("key{:06}", (t * 25 + i) % 50);
                                let meta = bucket.get_meta(&key).unwrap().unwrap();
                                bucket.read_data(meta.segment_id, meta.offset, meta.length).unwrap();
                            }
                        });
                    }
                    // 2 writers
                    for t in 0..2u64 {
                        let bucket = Arc::clone(&bucket);
                        let data = &data;
                        s.spawn(move || {
                            for i in 0..25 {
                                let key = format!("new_t{t}k{i}");
                                let (seg, off, len) = bucket.append_data(data).unwrap();
                                bucket.put_meta(&key, &make_meta(seg, off, len)).unwrap();
                            }
                        });
                    }
                });
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_put,
    bench_get,
    bench_compact_scaling,
    bench_list,
    bench_concurrent
);
criterion_main!(benches);
