use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ObjectMeta {
    pub segment_id: u32,
    pub offset: u64,
    pub length: u64,
    pub content_type: Option<String>,
    pub etag: String,
    pub last_modified: u64,
    pub user_metadata: HashMap<String, String>,
}
