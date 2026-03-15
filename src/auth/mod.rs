pub mod grpc_auth;
pub mod policy;
pub mod s3_access;
pub mod s3_auth;
pub mod types;

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::RngExt;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use types::{BootstrapResult, KeyRecord, PolicyRecord};

const AUTH_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("auth_keys");
const AUTH_POLICIES: TableDefinition<&str, &[u8]> = TableDefinition::new("auth_policies");
const AUTH_KEY_POLICIES: TableDefinition<&str, u8> = TableDefinition::new("auth_key_policies");

const ACCESS_KEY_LEN: usize = 20;
const SECRET_KEY_LEN: usize = 40;
const ALPHANUMERIC: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn io_err(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

pub struct AuthStore {
    db: Database,
}

fn generate_random_string(len: usize) -> String {
    let mut rng = rand::rng();
    (0..len)
        .map(|_| {
            let idx = rng.random_range(0..ALPHANUMERIC.len());
            ALPHANUMERIC[idx] as char
        })
        .collect()
}

fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

impl AuthStore {
    /// Open or create the auth database. Returns `BootstrapResult` indicating
    /// whether a new root key was created.
    pub fn open(data_dir: &Path) -> std::io::Result<(Self, BootstrapResult)> {
        let db_path = data_dir.join("_auth.redb");
        let db = Database::create(&db_path)
            .map_err(|e| std::io::Error::other(format!("open auth db: {e}")))?;

        // Ensure tables exist
        let txn = db
            .begin_write()
            .map_err(|e| std::io::Error::other(format!("auth db write: {e}")))?;
        {
            txn.open_table(AUTH_KEYS)
                .map_err(|e| std::io::Error::other(format!("open auth_keys: {e}")))?;
            txn.open_table(AUTH_POLICIES)
                .map_err(|e| std::io::Error::other(format!("open auth_policies: {e}")))?;
            txn.open_table(AUTH_KEY_POLICIES)
                .map_err(|e| std::io::Error::other(format!("open auth_key_policies: {e}")))?;
        }
        txn.commit()
            .map_err(|e| std::io::Error::other(format!("auth db commit: {e}")))?;

        let store = Self { db };
        let bootstrap = store.bootstrap_if_empty()?;
        Ok((store, bootstrap))
    }

    fn bootstrap_if_empty(&self) -> std::io::Result<BootstrapResult> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| std::io::Error::other(format!("auth read: {e}")))?;
        let table = txn
            .open_table(AUTH_KEYS)
            .map_err(|e| std::io::Error::other(format!("open auth_keys: {e}")))?;
        let count = table
            .len()
            .map_err(|e| std::io::Error::other(format!("count keys: {e}")))?;
        drop(table);
        drop(txn);

        if count > 0 {
            return Ok(BootstrapResult::Existing);
        }

        let access_key_id = format!("AK{}", generate_random_string(ACCESS_KEY_LEN - 2));
        let secret_key = format!("SK{}", generate_random_string(SECRET_KEY_LEN - 2));
        let record = KeyRecord {
            access_key_id: access_key_id.clone(),
            secret_key: secret_key.clone(),
            created_at: now_epoch(),
            description: "root admin (auto-generated)".to_owned(),
            is_admin: true,
            enabled: true,
        };
        self.insert_key(&record)?;

        Ok(BootstrapResult::NewRootKey {
            access_key_id,
            secret_key,
        })
    }

    // === Key operations ===

    pub fn create_key(&self, description: &str, is_admin: bool) -> std::io::Result<KeyRecord> {
        let record = KeyRecord {
            access_key_id: format!("AK{}", generate_random_string(ACCESS_KEY_LEN - 2)),
            secret_key: format!("SK{}", generate_random_string(SECRET_KEY_LEN - 2)),
            created_at: now_epoch(),
            description: description.to_owned(),
            is_admin,
            enabled: true,
        };
        self.insert_key(&record)?;
        Ok(record)
    }

    fn insert_key(&self, record: &KeyRecord) -> std::io::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| std::io::Error::other(format!("write txn: {e}")))?;
        {
            let mut table = txn
                .open_table(AUTH_KEYS)
                .map_err(|e| std::io::Error::other(format!("open table: {e}")))?;
            let bytes = serde_json::to_vec(record)
                .map_err(|e| std::io::Error::other(format!("serialize: {e}")))?;
            table
                .insert(record.access_key_id.as_str(), bytes.as_slice())
                .map_err(|e| std::io::Error::other(format!("insert key: {e}")))?;
        }
        txn.commit()
            .map_err(|e| std::io::Error::other(format!("commit: {e}")))?;
        Ok(())
    }

    pub fn get_key(&self, access_key_id: &str) -> std::io::Result<Option<KeyRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| std::io::Error::other(format!("read txn: {e}")))?;
        let table = txn
            .open_table(AUTH_KEYS)
            .map_err(|e| std::io::Error::other(format!("open table: {e}")))?;
        let entry = table
            .get(access_key_id)
            .map_err(|e| std::io::Error::other(format!("get key: {e}")))?;
        match entry {
            Some(v) => {
                let record: KeyRecord = serde_json::from_slice(v.value())
                    .map_err(|e| std::io::Error::other(format!("deserialize: {e}")))?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    pub fn list_keys(&self) -> std::io::Result<Vec<KeyRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| std::io::Error::other(format!("read txn: {e}")))?;
        let table = txn
            .open_table(AUTH_KEYS)
            .map_err(|e| std::io::Error::other(format!("open table: {e}")))?;
        let mut keys = Vec::new();
        for result in table.iter().map_err(io_err)? {
            let (_, v) = result.map_err(io_err)?;
            let record: KeyRecord =
                serde_json::from_slice(v.value()).map_err(io_err)?;
            keys.push(record);
        }
        Ok(keys)
    }

    pub fn delete_key(&self, access_key_id: &str) -> std::io::Result<()> {
        self.check_not_last_admin(access_key_id)?;

        let prefix = format!("{access_key_id}\0");
        let to_remove = collect_kp_composites(&self.db, |c| c.starts_with(&prefix))?;

        let txn = self
            .db
            .begin_write()
            .map_err(|e| std::io::Error::other(format!("write txn: {e}")))?;
        {
            let mut keys_table = txn.open_table(AUTH_KEYS).map_err(io_err)?;
            keys_table.remove(access_key_id).map_err(io_err)?;

            let mut kp_table = txn.open_table(AUTH_KEY_POLICIES).map_err(io_err)?;
            for key in &to_remove {
                kp_table.remove(key.as_str()).map_err(io_err)?;
            }
        }
        txn.commit().map_err(io_err)?;
        Ok(())
    }

    pub fn set_key_enabled(&self, access_key_id: &str, enabled: bool) -> std::io::Result<()> {
        if !enabled {
            self.check_not_last_admin(access_key_id)?;
        }
        let mut record = self
            .get_key(access_key_id)?
            .ok_or_else(|| std::io::Error::other("key not found"))?;
        record.enabled = enabled;
        self.insert_key(&record)
    }

    fn check_not_last_admin(&self, access_key_id: &str) -> std::io::Result<()> {
        let keys = self.list_keys()?;
        let admin_count = keys
            .iter()
            .filter(|k| k.is_admin && k.enabled && k.access_key_id != access_key_id)
            .count();
        if admin_count == 0 {
            let key = keys.iter().find(|k| k.access_key_id == access_key_id);
            if key.is_some_and(|k| k.is_admin) {
                return Err(std::io::Error::other(
                    "cannot remove or disable the last admin key",
                ));
            }
        }
        Ok(())
    }

    // === Policy operations ===

    pub fn create_policy(
        &self,
        name: &str,
        document: policy::PolicyDocument,
    ) -> std::io::Result<PolicyRecord> {
        let record = PolicyRecord {
            name: name.to_owned(),
            document,
            created_at: now_epoch(),
        };
        let txn = self
            .db
            .begin_write()
            .map_err(|e| std::io::Error::other(format!("write txn: {e}")))?;
        {
            let mut table = txn
                .open_table(AUTH_POLICIES)
                .map_err(|e| std::io::Error::other(format!("open table: {e}")))?;
            let bytes = serde_json::to_vec(&record)
                .map_err(|e| std::io::Error::other(format!("serialize: {e}")))?;
            table
                .insert(name, bytes.as_slice())
                .map_err(|e| std::io::Error::other(format!("insert policy: {e}")))?;
        }
        txn.commit()
            .map_err(|e| std::io::Error::other(format!("commit: {e}")))?;
        Ok(record)
    }

    pub fn get_policy(&self, name: &str) -> std::io::Result<Option<PolicyRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| std::io::Error::other(format!("read txn: {e}")))?;
        let table = txn
            .open_table(AUTH_POLICIES)
            .map_err(|e| std::io::Error::other(format!("open table: {e}")))?;
        let entry = table
            .get(name)
            .map_err(|e| std::io::Error::other(format!("get policy: {e}")))?;
        match entry {
            Some(v) => {
                let record: PolicyRecord = serde_json::from_slice(v.value())
                    .map_err(|e| std::io::Error::other(format!("deserialize: {e}")))?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    pub fn list_policies(&self) -> std::io::Result<Vec<PolicyRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| std::io::Error::other(format!("read txn: {e}")))?;
        let table = txn
            .open_table(AUTH_POLICIES)
            .map_err(|e| std::io::Error::other(format!("open table: {e}")))?;
        let mut policies = Vec::new();
        for result in table.iter().map_err(io_err)? {
            let (_, v) = result.map_err(io_err)?;
            let record: PolicyRecord =
                serde_json::from_slice(v.value()).map_err(io_err)?;
            policies.push(record);
        }
        Ok(policies)
    }

    pub fn delete_policy(&self, name: &str) -> std::io::Result<()> {
        let suffix = format!("\0{name}");
        let to_remove = collect_kp_composites(&self.db, |c| c.ends_with(&suffix))?;

        let txn = self
            .db
            .begin_write()
            .map_err(|e| std::io::Error::other(format!("write txn: {e}")))?;
        {
            let mut pol_table = txn.open_table(AUTH_POLICIES).map_err(io_err)?;
            pol_table.remove(name).map_err(io_err)?;

            let mut kp_table = txn.open_table(AUTH_KEY_POLICIES).map_err(io_err)?;
            for key in &to_remove {
                kp_table.remove(key.as_str()).map_err(io_err)?;
            }
        }
        txn.commit().map_err(io_err)?;
        Ok(())
    }

    // === Key-Policy attachments ===

    pub fn attach_policy(&self, access_key_id: &str, policy_name: &str) -> std::io::Result<()> {
        // Verify both exist
        self.get_key(access_key_id)?
            .ok_or_else(|| std::io::Error::other("key not found"))?;
        self.get_policy(policy_name)?
            .ok_or_else(|| std::io::Error::other("policy not found"))?;

        let composite = format!("{access_key_id}\0{policy_name}");
        let txn = self
            .db
            .begin_write()
            .map_err(|e| std::io::Error::other(format!("write txn: {e}")))?;
        {
            let mut table = txn
                .open_table(AUTH_KEY_POLICIES)
                .map_err(|e| std::io::Error::other(format!("open table: {e}")))?;
            table
                .insert(composite.as_str(), 1u8)
                .map_err(|e| std::io::Error::other(format!("insert attachment: {e}")))?;
        }
        txn.commit()
            .map_err(|e| std::io::Error::other(format!("commit: {e}")))?;
        Ok(())
    }

    pub fn detach_policy(&self, access_key_id: &str, policy_name: &str) -> std::io::Result<()> {
        let composite = format!("{access_key_id}\0{policy_name}");
        let txn = self
            .db
            .begin_write()
            .map_err(|e| std::io::Error::other(format!("write txn: {e}")))?;
        {
            let mut table = txn
                .open_table(AUTH_KEY_POLICIES)
                .map_err(|e| std::io::Error::other(format!("open table: {e}")))?;
            table
                .remove(composite.as_str())
                .map_err(|e| std::io::Error::other(format!("remove attachment: {e}")))?;
        }
        txn.commit()
            .map_err(|e| std::io::Error::other(format!("commit: {e}")))?;
        Ok(())
    }

    pub fn get_policies_for_key(
        &self,
        access_key_id: &str,
    ) -> std::io::Result<Vec<policy::PolicyDocument>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| std::io::Error::other(format!("read txn: {e}")))?;
        let pol_table = txn
            .open_table(AUTH_POLICIES)
            .map_err(|e| std::io::Error::other(format!("open table: {e}")))?;

        let names = self.get_policy_names_for_key(access_key_id)?;
        let mut docs = Vec::new();
        for name in &names {
            if let Some(pv) = pol_table.get(name.as_str()).map_err(io_err)? {
                let record: PolicyRecord =
                    serde_json::from_slice(pv.value()).map_err(io_err)?;
                docs.push(record.document);
            }
        }

        Ok(docs)
    }

    pub fn get_policy_names_for_key(
        &self,
        access_key_id: &str,
    ) -> std::io::Result<Vec<String>> {
        let prefix = format!("{access_key_id}\0");
        let composites = collect_kp_composites(&self.db, |c| c.starts_with(&prefix))?;
        Ok(composites
            .into_iter()
            .map(|c| c[prefix.len()..].to_owned())
            .collect())
    }
}

/// Read all composite keys from `AUTH_KEY_POLICIES` matching a filter.
fn collect_kp_composites(
    db: &Database,
    filter: impl Fn(&str) -> bool,
) -> std::io::Result<Vec<String>> {
    let txn = db.begin_read().map_err(io_err)?;
    let table = txn.open_table(AUTH_KEY_POLICIES).map_err(io_err)?;
    let mut result = Vec::new();
    for entry in table.iter().map_err(io_err)? {
        let (k, _) = entry.map_err(io_err)?;
        let composite = k.value().to_owned();
        if filter(&composite) {
            result.push(composite);
        }
    }
    Ok(result)
}
