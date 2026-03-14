use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use simple3::storage::Storage;
use simple3::types::ObjectMeta;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const KB: usize = 1024;
const MB: usize = 1024 * KB;

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

/// Pre-fill bucket with N objects of given size, return keys.
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

/// Pre-fill and delete a fraction of objects (for compaction benchmarks).
fn prefill_and_delete(
    storage: &Storage,
    bucket: &str,
    count: usize,
    obj_size: usize,
    delete_fraction: f64,
) {
    let keys = prefill(storage, bucket, count, obj_size);
    let b = storage.get_bucket(bucket).unwrap().unwrap();
    let delete_count = (count as f64 * delete_fraction) as usize;
    let step = if delete_count > 0 {
        count / delete_count
    } else {
        count + 1
    };
    for (i, key) in keys.iter().enumerate() {
        if i % step == 0 {
            b.delete_object(key).unwrap();
        }
    }
}

// === Put benchmarks (parameterized over sizes, with throughput) ===

fn bench_put(c: &mut Criterion) {
    let sizes: &[(&str, usize, usize)] = &[
        ("1kb", KB, 100),
        ("10kb", 10 * KB, 100),
        ("1mb", MB, 30),
        ("10mb", 10 * MB, 10),
        ("100mb", 100 * MB, 10),
    ];

    let mut group = c.benchmark_group("put");

    for &(label, size, sample) in sizes {
        group.sample_size(sample);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            let data = vec![0x42u8; size];
            let (_dir, storage) = setup_bucket("b");
            let bucket = storage.get_bucket("b").unwrap().unwrap();
            let counter = AtomicU64::new(0);
            b.iter(|| {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("k{i}");
                let (seg, off, len) = bucket.append_data(&data).unwrap();
                bucket.put_meta(&key, &make_meta(seg, off, len)).unwrap();
            });
        });
    }

    group.finish();
}

// === Get benchmarks (parameterized, with throughput) ===

fn bench_get(c: &mut Criterion) {
    let cases: &[(&str, usize, usize, usize)] = &[
        ("1kb_among_1k", KB, 1_000, 100),
        ("1kb_among_10k", KB, 10_000, 50),
        ("10kb_among_1k", 10 * KB, 1_000, 100),
        ("10kb_among_10k", 10 * KB, 10_000, 30),
        ("1mb_among_100", MB, 100, 30),
        ("1mb_among_1k", MB, 1_000, 10),
        ("10mb_among_100", 10 * MB, 100, 10),
    ];

    let mut group = c.benchmark_group("get");

    for &(label, obj_size, count, sample) in cases {
        group.sample_size(sample);
        group.throughput(Throughput::Bytes(obj_size as u64));
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            let (_dir, storage) = setup_bucket("b");
            let keys = prefill(&storage, "b", count, obj_size);
            let bucket = storage.get_bucket("b").unwrap().unwrap();
            let mut idx = 0usize;
            b.iter(|| {
                let key = &keys[idx % keys.len()];
                idx = idx.wrapping_add(7919);
                let meta = bucket.get_meta(key).unwrap().unwrap();
                bucket
                    .read_data(meta.segment_id, meta.offset, meta.length)
                    .unwrap();
            });
        });
    }

    group.finish();
}

// === List scaling ===

fn bench_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("list");

    let cases: &[(usize, usize)] = &[(1_000, 100), (10_000, 30), (50_000, 10)];

    for &(count, sample) in cases {
        group.sample_size(sample);
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

// === Delete scaling ===

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");

    let cases: &[(&str, usize, usize, usize)] = &[
        ("among_1k", 1_000, KB, 30),
        ("among_10k", 10_000, KB, 10),
    ];

    for &(label, count, obj_size, sample) in cases {
        group.sample_size(sample);
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter_batched(
                || {
                    let (dir, storage) = setup_bucket("b");
                    let keys = prefill(&storage, "b", count, obj_size);
                    let target = keys[count / 2].clone();
                    (dir, storage, target)
                },
                |(_dir, storage, target)| {
                    let bucket = storage.get_bucket("b").unwrap().unwrap();
                    bucket.delete_object(&target).unwrap();
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

// === Overwrite (update) performance ===

fn bench_overwrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("overwrite");

    let cases: &[(&str, usize, usize, usize)] = &[
        ("1k_objects_1kb", 1_000, KB, 100),
        ("10k_objects_1kb", 10_000, KB, 30),
    ];

    for &(label, count, obj_size, sample) in cases {
        group.sample_size(sample);
        group.throughput(Throughput::Bytes(obj_size as u64));
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            let (_dir, storage) = setup_bucket("b");
            let keys = prefill(&storage, "b", count, obj_size);
            let bucket = storage.get_bucket("b").unwrap().unwrap();
            let data = vec![0x55u8; obj_size];
            let mut idx = 0usize;
            b.iter(|| {
                let key = &keys[idx % keys.len()];
                idx = idx.wrapping_add(7919);
                let (seg, off, len) = bucket.append_data(&data).unwrap();
                bucket.put_meta(key, &make_meta(seg, off, len)).unwrap();
            });
        });
    }

    group.finish();
}

// === Compact scaling (delete 10% then compact) ===

fn bench_compact(c: &mut Criterion) {
    let mut group = c.benchmark_group("compact");
    group.sample_size(10);

    let cases: &[(&str, usize, usize)] = &[
        ("1k_x_1kb", 1_000, KB),
        ("10k_x_1kb", 10_000, KB),
        ("50k_x_64b", 50_000, 64),
        ("1k_x_1mb", 1_000, MB),
    ];

    for &(label, count, obj_size) in cases {
        let live_bytes = (count as f64 * 0.9) as u64 * obj_size as u64;
        group.throughput(Throughput::Bytes(live_bytes));
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter_batched(
                || {
                    let (dir, storage) = setup_bucket("b");
                    prefill_and_delete(&storage, "b", count, obj_size, 0.1);
                    (dir, storage)
                },
                |(_dir, storage)| {
                    let bucket = storage.get_bucket("b").unwrap().unwrap();
                    bucket.compact().unwrap();
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

// === Concurrent benchmarks ===

fn bench_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent");
    group.sample_size(10);

    // 8 threads reading 1KB objects from 10K store
    {
        let threads: u64 = 8;
        let ops: u64 = 100;
        group.throughput(Throughput::Bytes(threads * ops * KB as u64));
        group.bench_function("8t_get_1kb", |b| {
            b.iter_batched(
                || {
                    let (dir, storage) = setup_bucket("b");
                    prefill(&storage, "b", 10_000, KB);
                    let bucket = storage.get_bucket("b").unwrap().unwrap();
                    (dir, storage, bucket)
                },
                |(_, _, bucket)| {
                    std::thread::scope(|s| {
                        for t in 0..threads {
                            let bucket = Arc::clone(&bucket);
                            s.spawn(move || {
                                for i in 0..ops {
                                    let key = format!("key{:06}", (t * ops + i) % 10_000);
                                    let meta = bucket.get_meta(&key).unwrap().unwrap();
                                    bucket
                                        .read_data(meta.segment_id, meta.offset, meta.length)
                                        .unwrap();
                                }
                            });
                        }
                    });
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    // 8 threads reading 1MB objects from 200 store
    {
        let threads: u64 = 8;
        let ops: u64 = 10;
        group.throughput(Throughput::Bytes(threads * ops * MB as u64));
        group.bench_function("8t_get_1mb", |b| {
            b.iter_batched(
                || {
                    let (dir, storage) = setup_bucket("b");
                    prefill(&storage, "b", 200, MB);
                    let bucket = storage.get_bucket("b").unwrap().unwrap();
                    (dir, storage, bucket)
                },
                |(_, _, bucket)| {
                    std::thread::scope(|s| {
                        for t in 0..threads {
                            let bucket = Arc::clone(&bucket);
                            s.spawn(move || {
                                for i in 0..ops {
                                    let key = format!("key{:06}", (t * ops + i) % 200);
                                    let meta = bucket.get_meta(&key).unwrap().unwrap();
                                    bucket
                                        .read_data(meta.segment_id, meta.offset, meta.length)
                                        .unwrap();
                                }
                            });
                        }
                    });
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    // 8 threads writing 1KB objects
    {
        let threads: u64 = 8;
        let ops: u64 = 100;
        group.throughput(Throughput::Bytes(threads * ops * KB as u64));
        group.bench_function("8t_put_1kb", |b| {
            let data = vec![0x42u8; KB];
            b.iter_batched(
                || setup_bucket("b"),
                |(_dir, storage)| {
                    let bucket = storage.get_bucket("b").unwrap().unwrap();
                    std::thread::scope(|s| {
                        for t in 0..threads {
                            let bucket = Arc::clone(&bucket);
                            let data = &data;
                            s.spawn(move || {
                                for i in 0..ops {
                                    let key = format!("t{t}k{i}");
                                    let (seg, off, len) = bucket.append_data(data).unwrap();
                                    bucket
                                        .put_meta(&key, &make_meta(seg, off, len))
                                        .unwrap();
                                }
                            });
                        }
                    });
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    // 8 threads writing 10KB objects
    {
        let threads: u64 = 8;
        let ops: u64 = 100;
        group.throughput(Throughput::Bytes(threads * ops * 10 * KB as u64));
        group.bench_function("8t_put_10kb", |b| {
            let data = vec![0x42u8; 10 * KB];
            b.iter_batched(
                || setup_bucket("b"),
                |(_dir, storage)| {
                    let bucket = storage.get_bucket("b").unwrap().unwrap();
                    std::thread::scope(|s| {
                        for t in 0..threads {
                            let bucket = Arc::clone(&bucket);
                            let data = &data;
                            s.spawn(move || {
                                for i in 0..ops {
                                    let key = format!("t{t}k{i}");
                                    let (seg, off, len) = bucket.append_data(data).unwrap();
                                    bucket
                                        .put_meta(&key, &make_meta(seg, off, len))
                                        .unwrap();
                                }
                            });
                        }
                    });
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    // Mixed: 4 readers + 4 writers, 1KB
    {
        let r_threads: u64 = 4;
        let w_threads: u64 = 4;
        let ops: u64 = 50;
        group.throughput(Throughput::Bytes(
            (r_threads + w_threads) * ops * KB as u64,
        ));
        group.bench_function("mixed_4r_4w_1kb", |b| {
            let data = vec![0x42u8; KB];
            b.iter_batched(
                || {
                    let (dir, storage) = setup_bucket("b");
                    prefill(&storage, "b", 5_000, KB);
                    (dir, storage)
                },
                |(_dir, storage)| {
                    let bucket = storage.get_bucket("b").unwrap().unwrap();
                    std::thread::scope(|s| {
                        for t in 0..r_threads {
                            let bucket = Arc::clone(&bucket);
                            s.spawn(move || {
                                for i in 0..ops {
                                    let key = format!("key{:06}", (t * ops + i) % 5_000);
                                    let meta = bucket.get_meta(&key).unwrap().unwrap();
                                    bucket
                                        .read_data(meta.segment_id, meta.offset, meta.length)
                                        .unwrap();
                                }
                            });
                        }
                        for t in 0..w_threads {
                            let bucket = Arc::clone(&bucket);
                            let data = &data;
                            s.spawn(move || {
                                for i in 0..ops {
                                    let key = format!("new_t{t}k{i}");
                                    let (seg, off, len) = bucket.append_data(data).unwrap();
                                    bucket
                                        .put_meta(&key, &make_meta(seg, off, len))
                                        .unwrap();
                                }
                            });
                        }
                    });
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    // Mixed: 4 readers + 4 writers, 10KB
    {
        let r_threads: u64 = 4;
        let w_threads: u64 = 4;
        let ops: u64 = 50;
        group.throughput(Throughput::Bytes(
            (r_threads + w_threads) * ops * 10 * KB as u64,
        ));
        group.bench_function("mixed_4r_4w_10kb", |b| {
            let data = vec![0x42u8; 10 * KB];
            b.iter_batched(
                || {
                    let (dir, storage) = setup_bucket("b");
                    prefill(&storage, "b", 1_000, 10 * KB);
                    (dir, storage)
                },
                |(_dir, storage)| {
                    let bucket = storage.get_bucket("b").unwrap().unwrap();
                    std::thread::scope(|s| {
                        for t in 0..r_threads {
                            let bucket = Arc::clone(&bucket);
                            s.spawn(move || {
                                for i in 0..ops {
                                    let key = format!("key{:06}", (t * ops + i) % 1_000);
                                    let meta = bucket.get_meta(&key).unwrap().unwrap();
                                    bucket
                                        .read_data(meta.segment_id, meta.offset, meta.length)
                                        .unwrap();
                                }
                            });
                        }
                        for t in 0..w_threads {
                            let bucket = Arc::clone(&bucket);
                            let data = &data;
                            s.spawn(move || {
                                for i in 0..ops {
                                    let key = format!("new_t{t}k{i}");
                                    let (seg, off, len) = bucket.append_data(data).unwrap();
                                    bucket
                                        .put_meta(&key, &make_meta(seg, off, len))
                                        .unwrap();
                                }
                            });
                        }
                    });
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_put,
    bench_get,
    bench_list,
    bench_delete,
    bench_overwrite,
    bench_compact,
    bench_concurrent,
);
criterion_main!(benches);
