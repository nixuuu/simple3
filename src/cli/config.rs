use std::path::Path;

use serde::Deserialize;

#[derive(Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    // Parsed from TOML but not yet wired into server startup (reserved for auth.enabled toggle)
    #[serde(default)]
    #[allow(dead_code)]
    pub auth: AuthConfig,
}

#[derive(Deserialize, Default)]
pub struct ServerConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub grpc_port: Option<u16>,
}

#[derive(Deserialize, Default)]
pub struct StorageConfig {
    pub max_segment_size_mb: Option<u64>,
    pub autovacuum_interval: Option<u64>,
    pub autovacuum_threshold: Option<f64>,
}

// Parsed from TOML but not yet wired into server startup
#[derive(Deserialize, Default)]
#[allow(dead_code)]
pub struct AuthConfig {
    pub enabled: Option<bool>,
}

pub fn load_config(config_path: Option<&Path>, data_dir: &Path) -> Config {
    // Try explicit config path first, then default location in data dir
    let path = config_path
        .map(|p| p.to_owned())
        .unwrap_or_else(|| data_dir.join("simple3.toml"));

    match std::fs::read_to_string(&path) {
        Ok(content) => match toml::from_str(&content) {
            Ok(config) => {
                tracing::info!("loaded config from {}", path.display());
                config
            }
            Err(e) => {
                tracing::warn!("failed to parse {}: {e}", path.display());
                Config::default()
            }
        },
        Err(_) => Config::default(),
    }
}
