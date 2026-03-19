use simple3::limits::Limits;

/// Resolved configuration for the `serve` command.
pub struct ServeConfig {
    pub host: String,
    pub port: u16,
    pub grpc_port: u16,
    pub autovacuum_interval: u64,
    pub autovacuum_threshold: f64,
    pub max_segment_size_mb: u64,
    pub scrub_interval: u64,
    pub shutdown_timeout: u64,
    pub min_disk_free_mb: u64,
    pub rate_limit_rps: u32,
    pub limits: Limits,
}
