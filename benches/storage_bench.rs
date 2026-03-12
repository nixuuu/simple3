use criterion::{criterion_group, criterion_main, Criterion};
use simple3::storage::Storage;
use simple3::types::ObjectMeta;
use std::collections::HashMap;

fn make_meta(offset: u64, length: u64) -> ObjectMeta {
    ObjectMeta {
        offset,
        length,
        content_type: Some("application/octet-stream".into()),
        etag: "bench".into(),
        last_modified: 0,
        user_metadata: HashMap::new(),
    }
}

fn setup_bucket(prefix: &str) -> (tempfile::TempDir, Storage) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.create_bucket(prefix).unwrap();
    (dir, storage)
}

fn bench_put(c: &mut Criterion) {
    let data_1kb = vec![0x42u8; 1024];
    let data_1mb = vec![0x42u8; 1024 * 1024];

    c.bench_function("put_1kb", |b| {
        let (_dir, storage) = setup_bucket("b");
        let bucket = storage.get_bucket("b").unwrap();
        let mut i = 0u64;
        b.iter(|| {
            let (off, len) = bucket.append_data(&data_1kb).unwrap();
            bucket
                .put_meta(&format!("k{i}"), &make_meta(off, len))
                .unwrap();
            i += 1;
        });
    });

    c.bench_function("put_1mb", |b| {
        let (_dir, storage) = setup_bucket("b");
        let bucket = storage.get_bucket("b").unwrap();
        let mut i = 0u64;
        b.iter(|| {
            let (off, len) = bucket.append_data(&data_1mb).unwrap();
            bucket
                .put_meta(&format!("k{i}"), &make_meta(off, len))
                .unwrap();
            i += 1;
        });
    });
}

fn bench_get(c: &mut Criterion) {
    let data_1kb = vec![0x42u8; 1024];
    let data_1mb = vec![0x42u8; 1024 * 1024];

    c.bench_function("get_1kb", |b| {
        let (_dir, storage) = setup_bucket("b");
        let bucket = storage.get_bucket("b").unwrap();
        let (off, len) = bucket.append_data(&data_1kb).unwrap();
        bucket.put_meta("obj", &make_meta(off, len)).unwrap();
        b.iter(|| {
            let meta = bucket.get_meta("obj").unwrap().unwrap();
            bucket.read_data(meta.offset, meta.length).unwrap();
        });
    });

    c.bench_function("get_1mb", |b| {
        let (_dir, storage) = setup_bucket("b");
        let bucket = storage.get_bucket("b").unwrap();
        let (off, len) = bucket.append_data(&data_1mb).unwrap();
        bucket.put_meta("obj", &make_meta(off, len)).unwrap();
        b.iter(|| {
            let meta = bucket.get_meta("obj").unwrap().unwrap();
            bucket.read_data(meta.offset, meta.length).unwrap();
        });
    });
}

fn bench_delete_with_compact(c: &mut Criterion) {
    c.bench_function("delete_compact_100_objects", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (_dir, storage) = setup_bucket("b");
                let bucket = storage.get_bucket("b").unwrap();
                // Pre-fill 100 objects
                for i in 0..100 {
                    let data = format!("data-{i:04}");
                    let (off, len) = bucket.append_data(data.as_bytes()).unwrap();
                    bucket
                        .put_meta(&format!("key{i:04}"), &make_meta(off, len))
                        .unwrap();
                }
                // Bench: delete from middle
                let start = std::time::Instant::now();
                bucket.delete_and_compact("key0050").unwrap();
                total += start.elapsed();
            }
            total
        });
    });
}

fn bench_list(c: &mut Criterion) {
    c.bench_function("list_100_objects", |b| {
        let (_dir, storage) = setup_bucket("b");
        let bucket = storage.get_bucket("b").unwrap();
        for i in 0..100 {
            let (off, len) = bucket.append_data(b"x").unwrap();
            bucket
                .put_meta(&format!("k{i:04}"), &make_meta(off, len))
                .unwrap();
        }
        b.iter(|| {
            bucket.list_objects(None, 1000, None).unwrap();
        });
    });

    c.bench_function("list_1000_objects", |b| {
        let (_dir, storage) = setup_bucket("b");
        let bucket = storage.get_bucket("b").unwrap();
        for i in 0..1000 {
            let (off, len) = bucket.append_data(b"x").unwrap();
            bucket
                .put_meta(&format!("k{i:04}"), &make_meta(off, len))
                .unwrap();
        }
        b.iter(|| {
            bucket.list_objects(None, 1000, None).unwrap();
        });
    });
}

criterion_group!(benches, bench_put, bench_get, bench_delete_with_compact, bench_list);
criterion_main!(benches);
