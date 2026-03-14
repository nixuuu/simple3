use std::path::Path;

use simple3::storage::Storage;

pub fn run(data_dir: &Path, bucket: Option<String>) -> anyhow::Result<()> {
    let storage = Storage::open(data_dir)?;
    let buckets = bucket.map_or_else(|| storage.list_buckets(), |name| Ok(vec![name]))?;
    for name in &buckets {
        let store = storage
            .get_bucket(name)?
            .ok_or_else(|| anyhow::anyhow!("bucket '{name}' not found"))?;
        let dead = store.dead_bytes();
        if dead == 0 {
            println!("{name}: no dead bytes, skipping");
            continue;
        }
        println!("{name}: compacting ({dead} dead bytes)...");
        store.compact()?;
        println!("{name}: done");
    }
    Ok(())
}
