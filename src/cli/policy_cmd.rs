use clap::Subcommand;

use super::client::ClientArgs;
use super::keys::AdminClient;
use super::util::format_epoch;

#[derive(Subcommand)]
pub enum PolicyCommand {
    /// Create a policy from a JSON document
    Create {
        /// Policy name
        name: String,
        /// Path to JSON policy document
        #[arg(long)]
        document: String,
    },
    /// List all policies
    List,
    /// Show a policy document
    Show {
        /// Policy name
        name: String,
    },
    /// Delete a policy
    Delete {
        /// Policy name
        name: String,
    },
    /// Attach a policy to an access key
    Attach {
        /// Policy name
        policy_name: String,
        /// Access key ID
        access_key_id: String,
    },
    /// Detach a policy from an access key
    Detach {
        /// Policy name
        policy_name: String,
        /// Access key ID
        access_key_id: String,
    },
}

pub async fn run(args: ClientArgs, cmd: PolicyCommand) -> anyhow::Result<()> {
    let client = AdminClient::new(&args);

    match cmd {
        PolicyCommand::Create { name, document } => {
            let content = std::fs::read_to_string(&document)?;
            let doc: serde_json::Value = serde_json::from_str(&content)?;
            let body = serde_json::json!({"name": name, "document": doc});
            client.post("/_/policies", &body).await?;
            println!("created policy: {name}");
        }
        PolicyCommand::List => {
            let resp: serde_json::Value = client.get("/_/policies").await?;
            let policies = resp["policies"].as_array();
            if policies.is_none() || policies.is_some_and(|p| p.is_empty()) {
                println!("no policies");
                return Ok(());
            }
            println!("{:<30} {:<12} {}", "NAME", "STATEMENTS", "CREATED");
            for p in policies.unwrap_or(&vec![]) {
                println!(
                    "{:<30} {:<12} {}",
                    p["name"].as_str().unwrap_or(""),
                    p["statement_count"].as_u64().unwrap_or(0),
                    format_epoch(p["created_at"].as_u64().unwrap_or(0))
                );
            }
        }
        PolicyCommand::Show { name } => {
            let resp: serde_json::Value =
                client.get(&format!("/_/policies/{name}")).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
        PolicyCommand::Delete { name } => {
            client.delete(&format!("/_/policies/{name}")).await?;
            println!("deleted policy: {name}");
        }
        PolicyCommand::Attach {
            policy_name,
            access_key_id,
        } => {
            client
                .post_empty(&format!("/_/policies/{policy_name}/attach/{access_key_id}"))
                .await?;
            println!("attached {policy_name} to {access_key_id}");
        }
        PolicyCommand::Detach {
            policy_name,
            access_key_id,
        } => {
            client
                .delete(&format!("/_/policies/{policy_name}/attach/{access_key_id}"))
                .await?;
            println!("detached {policy_name} from {access_key_id}");
        }
    }
    Ok(())
}
