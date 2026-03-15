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
    pub shutdown_timeout: Option<u64>,
}

#[derive(Deserialize, Default)]
pub struct StorageConfig {
    pub max_segment_size_mb: Option<u64>,
    pub autovacuum_interval: Option<u64>,
    pub autovacuum_threshold: Option<f64>,
    pub scrub_interval: Option<u64>,
}

// Parsed from TOML but not yet wired into server startup
#[derive(Deserialize, Default)]
#[allow(dead_code)]
pub struct AuthConfig {
    pub enabled: Option<bool>,
}

pub fn load_config(config_path: Option<&Path>, data_dir: &Path) -> anyhow::Result<Config> {
    let explicit = config_path.is_some();
    let path = config_path.map_or_else(|| data_dir.join("simple3.toml"), ToOwned::to_owned);

    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if !explicit && e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Config::default());
        }
        Err(e) if explicit => {
            anyhow::bail!("failed to read config {}: {e}", path.display());
        }
        Err(e) => {
            tracing::warn!("failed to read config {}: {e}", path.display());
            return Ok(Config::default());
        }
    };
    match toml::from_str(&content) {
        Ok(config) => {
            tracing::info!("loaded config from {}", path.display());
            Ok(config)
        }
        Err(e) => {
            if explicit {
                anyhow::bail!("failed to parse {}: {e}", path.display());
            }
            tracing::warn!("failed to parse {}: {e}", path.display());
            Ok(Config::default())
        }
    }
}
