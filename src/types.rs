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
    pub content_md5: Option<String>,
}

/// Layout before `content_md5` was added — used only for backward-compatible deserialization.
#[derive(Deserialize)]
struct ObjectMetaV2 {
    segment_id: u32,
    offset: u64,
    length: u64,
    content_type: Option<String>,
    etag: String,
    last_modified: u64,
    user_metadata: HashMap<String, String>,
}

#[allow(clippy::missing_errors_doc)]
impl ObjectMeta {
    pub fn from_bytes(data: &[u8]) -> Result<Self, Box<bincode::ErrorKind>> {
        bincode::deserialize::<Self>(data).or_else(|_| {
            let v2: ObjectMetaV2 = bincode::deserialize(data)?;
            Ok(Self {
                segment_id: v2.segment_id,
                offset: v2.offset,
                length: v2.length,
                content_type: v2.content_type,
                etag: v2.etag,
                last_modified: v2.last_modified,
                user_metadata: v2.user_metadata,
                content_md5: None,
            })
        })
    }
}
