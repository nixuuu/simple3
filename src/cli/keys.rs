use clap::Subcommand;

use super::client::ClientArgs;
use super::util::format_epoch;

#[derive(Subcommand)]
pub enum KeysCommand {
    /// Create a new access key
    Create {
        /// Description for the key
        #[arg(long, default_value = "")]
        description: String,
        /// Create an admin key (bypasses all policy checks)
        #[arg(long)]
        admin: bool,
    },
    /// List all access keys
    List,
    /// Show details for an access key
    Show {
        /// The access key ID
        access_key_id: String,
    },
    /// Delete an access key
    Delete {
        /// The access key ID
        access_key_id: String,
    },
    /// Enable an access key
    Enable {
        /// The access key ID
        access_key_id: String,
    },
    /// Disable an access key
    Disable {
        /// The access key ID
        access_key_id: String,
    },
}

pub async fn run(args: ClientArgs, cmd: KeysCommand) -> anyhow::Result<()> {
    let client = AdminClient::new(&args);

    match cmd {
        KeysCommand::Create { description, admin } => {
            let body = serde_json::json!({"description": description, "admin": admin});
            let resp: serde_json::Value = client.post("/_/keys", &body).await?;
            println!("Access Key ID: {}", resp["access_key_id"].as_str().unwrap_or(""));
            println!("Secret Key:    {}", resp["secret_key"].as_str().unwrap_or(""));
            if admin {
                println!("Admin:         yes");
            }
        }
        KeysCommand::List => {
            let resp: serde_json::Value = client.get("/_/keys").await?;
            let keys = resp["keys"].as_array();
            if keys.is_none() || keys.is_some_and(Vec::is_empty) {
                println!("no access keys");
                return Ok(());
            }
            println!(
                "{:<22} {:<20} {:<7} {:<8} DESCRIPTION",
                "ACCESS_KEY_ID", "CREATED", "ADMIN", "ENABLED"
            );
            for k in keys.unwrap_or(&vec![]) {
                println!(
                    "{:<22} {:<20} {:<7} {:<8} {}",
                    k["access_key_id"].as_str().unwrap_or(""),
                    format_epoch(k["created_at"].as_u64().unwrap_or(0)),
                    if k["is_admin"].as_bool().unwrap_or(false) { "yes" } else { "no" },
                    if k["enabled"].as_bool().unwrap_or(false) { "yes" } else { "no" },
                    k["description"].as_str().unwrap_or("")
                );
            }
        }
        KeysCommand::Show { access_key_id } => {
            let resp: serde_json::Value =
                client.get(&format!("/_/keys/{access_key_id}")).await?;
            println!("Access Key ID: {}", resp["access_key_id"].as_str().unwrap_or(""));
            println!("Created:       {}", format_epoch(resp["created_at"].as_u64().unwrap_or(0)));
            println!(
                "Admin:         {}",
                if resp["is_admin"].as_bool().unwrap_or(false) { "yes" } else { "no" }
            );
            println!(
                "Enabled:       {}",
                if resp["enabled"].as_bool().unwrap_or(false) { "yes" } else { "no" }
            );
            println!("Description:   {}", resp["description"].as_str().unwrap_or(""));
            let policies = resp["policies"].as_array();
            if policies.is_none() || policies.is_some_and(Vec::is_empty) {
                println!("Policies:      (none)");
            } else {
                println!("Policies:");
                for name in policies.unwrap_or(&vec![]) {
                    println!("  - {}", name.as_str().unwrap_or(""));
                }
            }
        }
        KeysCommand::Delete { access_key_id } => {
            client.delete(&format!("/_/keys/{access_key_id}")).await?;
            println!("deleted key: {access_key_id}");
        }
        KeysCommand::Enable { access_key_id } => {
            client
                .post_empty(&format!("/_/keys/{access_key_id}/enable"))
                .await?;
            println!("enabled key: {access_key_id}");
        }
        KeysCommand::Disable { access_key_id } => {
            client
                .post_empty(&format!("/_/keys/{access_key_id}/disable"))
                .await?;
            println!("disabled key: {access_key_id}");
        }
    }
    Ok(())
}

pub struct AdminClient {
    http: reqwest::Client,
    base_url: String,
    auth_header: String,
}

impl AdminClient {
    pub fn new(args: &ClientArgs) -> Self {
        let base_url = args
            .endpoint_url
            .clone()
            .unwrap_or_else(|| "http://localhost:8080".into());
        let ak = args.access_key.as_deref().unwrap_or("test");
        let sk = args.secret_key.as_deref().unwrap_or("test");
        Self {
            http: reqwest::Client::new(),
            base_url,
            auth_header: format!("Bearer {ak}:{sk}"),
        }
    }

    pub async fn get(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}{path}", self.base_url))
            .header("authorization", &self.auth_header)
            .send()
            .await?;
        self.handle_response(resp).await
    }

    pub async fn post(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}{path}", self.base_url))
            .header("authorization", &self.auth_header)
            .header("content-type", "application/json")
            .body(serde_json::to_string(body)?)
            .send()
            .await?;
        self.handle_response(resp).await
    }

    pub async fn post_empty(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}{path}", self.base_url))
            .header("authorization", &self.auth_header)
            .send()
            .await?;
        self.handle_response(resp).await
    }

    pub async fn delete(&self, path: &str) -> anyhow::Result<serde_json::Value> {
        let resp = self
            .http
            .delete(format!("{}{path}", self.base_url))
            .header("authorization", &self.auth_header)
            .send()
            .await?;
        self.handle_response(resp).await
    }

    async fn handle_response(
        &self,
        resp: reqwest::Response,
    ) -> anyhow::Result<serde_json::Value> {
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            let msg = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["error"].as_str().map(String::from))
                .unwrap_or(body);
            anyhow::bail!("{msg}");
        }
        Ok(serde_json::from_str(&body).unwrap_or_else(|_| serde_json::json!({})))
    }
}
