/// Runtime-resolved request limits.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Maximum single-object size in bytes (0 = unlimited).
    pub max_object_size: u64,
    /// Maximum keys returned per `ListObjects` / `ListObjectVersions` page.
    pub max_list_keys: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_object_size: 5 * 1024 * 1024 * 1024, // 5 GB
            max_list_keys: 1000,
        }
    }
}
