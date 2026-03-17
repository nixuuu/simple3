use std::collections::HashMap;

/// Known Mint test failures mapped to their tracking GitHub issue URL.
/// Tests in this map are expected to fail and won't cause CI failure.
///
/// Populate after first run, e.g.:
/// ```ignore
/// map.insert("awscli::make_bucket", "https://github.com/nixuuu/simple3/issues/XX");
/// ```
pub fn mint_known_failures() -> HashMap<&'static str, &'static str> {
    HashMap::new()
}

/// Known Ceph s3-tests failures mapped to their tracking GitHub issue URL.
pub fn ceph_known_failures() -> HashMap<&'static str, &'static str> {
    HashMap::new()
}
