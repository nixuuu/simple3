use serde::{Deserialize, Serialize};

use super::policy::PolicyDocument;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KeyRecord {
    pub access_key_id: String,
    pub secret_key: String,
    pub created_at: u64,
    pub description: String,
    pub is_admin: bool,
    pub enabled: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PolicyRecord {
    pub name: String,
    pub document: PolicyDocument,
    pub created_at: u64,
}

/// Result of opening the auth store, indicating whether a root key was bootstrapped.
pub enum BootstrapResult {
    Existing,
    NewRootKey {
        access_key_id: String,
        secret_key: String,
    },
}
