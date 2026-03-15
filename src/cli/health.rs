use std::path::Path;
use std::time::Duration;

use super::config;

pub async fn run(data_dir: &Path, config_path: Option<&Path>) -> anyhow::Result<()> {
    let cfg = config::load_config(config_path, data_dir)?;
    let raw_host = cfg.server.host.as_deref().unwrap_or("127.0.0.1");
    let host = match raw_host {
        "0.0.0.0" => "127.0.0.1",
        "::" => "[::1]",
        h => h,
    };
    let port = cfg.server.port.unwrap_or(8080);
    let url = format!("http://{host}:{port}/health");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let resp = client.get(&url).send().await?;
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
