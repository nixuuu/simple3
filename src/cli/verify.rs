use std::path::Path;

use simple3::storage::Storage;

pub fn run(data_dir: &Path, bucket: Option<String>) -> anyhow::Result<()> {
    let storage = Storage::open(data_dir)?;
    let buckets = bucket.map_or_else(|| storage.list_buckets(), |name| Ok(vec![name]))?;
    let mut all_ok = true;
    for name in &buckets {
        let store = storage
            .get_bucket(name)?
            .ok_or_else(|| anyhow::anyhow!("bucket '{name}' not found"))?;
        println!("{name}: verifying...");
        let result = store.verify_integrity()?;
        if result.errors.is_empty() {
            println!("{name}: OK ({} objects verified)", result.verified_ok);
        } else {
            all_ok = false;
            for err in &result.errors {
                println!("  FAIL {}: {}", err.key, err.detail);
            }
            println!(
                "{name}: {} ok, {} checksum errors, {} read errors",
                result.verified_ok, result.checksum_errors, result.read_errors
            );
        }
    }
    if !all_ok {
        std::process::exit(1);
    }
    Ok(())
}
