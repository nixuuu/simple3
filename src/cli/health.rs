use std::path::Path;

use super::config;

pub async fn run(data_dir: &Path, config_path: Option<&Path>) -> anyhow::Result<()> {
    let cfg = config::load_config(config_path, data_dir)?;
    let host = cfg.server.host.as_deref().unwrap_or("localhost");
    let port = cfg.server.port.unwrap_or(8080);
    let url = format!("http://{host}:{port}/health");

    let resp = reqwest::Client::new().get(&url).send().await?;
    let status = resp.status();
    let text = resp.text().await?;

    if status.is_success() {
        let body: serde_json::Value = serde_json::from_str(&text)?;
        println!("{}", serde_json::to_string_pretty(&body)?);
        Ok(())
    } else {
        anyhow::bail!("health check failed: {status} {text}");
    }
}
