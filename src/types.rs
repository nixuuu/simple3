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
    pub content_crc32c: Option<u32>,
    pub version_id: Option<String>,
    #[serde(default)]
    pub is_delete_marker: bool,
}

impl ObjectMeta {
    /// Returns the actual data length (excluding the 4-byte CRC32C trailer if present).
    /// Delete markers have no data.
    pub const fn data_length(&self) -> u64 {
        if self.is_delete_marker {
            return 0;
        }
        if self.content_crc32c.is_some() {
            self.length.saturating_sub(4)
        } else {
            self.length
        }
    }

    /// Create a delete marker with the given `version_id` and timestamp.
    pub fn delete_marker(version_id: Option<String>, last_modified: u64) -> Self {
        Self {
            segment_id: 0,
            offset: 0,
            length: 0,
            content_type: None,
            etag: String::new(),
            last_modified,
            user_metadata: HashMap::new(),
            content_md5: None,
            content_crc32c: None,
            version_id,
            is_delete_marker: true,
        }
    }
}

/// Layout before `version_id`/`is_delete_marker` were added (has CRC but no versioning).
#[derive(Deserialize)]
struct ObjectMetaV3 {
    segment_id: u32,
    offset: u64,
    length: u64,
    content_type: Option<String>,
    etag: String,
    last_modified: u64,
    user_metadata: HashMap<String, String>,
    content_md5: Option<String>,
    content_crc32c: Option<u32>,
}

/// Layout before `content_crc32c` was added (has `content_md5` but no CRC).
#[derive(Deserialize)]
struct ObjectMetaV2 {
    segment_id: u32,
    offset: u64,
    length: u64,
    content_type: Option<String>,
    etag: String,
    last_modified: u64,
    user_metadata: HashMap<String, String>,
    content_md5: Option<String>,
}

/// Layout before `content_md5` was added (no `content_md5`, no CRC).
#[derive(Deserialize)]
struct ObjectMetaV1 {
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
            bincode::deserialize::<ObjectMetaV3>(data)
                .map(|v3| Self {
                    segment_id: v3.segment_id,
                    offset: v3.offset,
                    length: v3.length,
                    content_type: v3.content_type,
                    etag: v3.etag,
                    last_modified: v3.last_modified,
                    user_metadata: v3.user_metadata,
                    content_md5: v3.content_md5,
                    content_crc32c: v3.content_crc32c,
                    version_id: None,
                    is_delete_marker: false,
                })
                .or_else(|_| {
                    bincode::deserialize::<ObjectMetaV2>(data).map(|v2| Self {
                        segment_id: v2.segment_id,
                        offset: v2.offset,
                        length: v2.length,
                        content_type: v2.content_type,
                        etag: v2.etag,
                        last_modified: v2.last_modified,
                        user_metadata: v2.user_metadata,
                        content_md5: v2.content_md5,
                        content_crc32c: None,
                        version_id: None,
                        is_delete_marker: false,
                    })
                })
                .or_else(|_| {
                    let v1: ObjectMetaV1 = bincode::deserialize(data)?;
                    Ok(Self {
                        segment_id: v1.segment_id,
                        offset: v1.offset,
                        length: v1.length,
                        content_type: v1.content_type,
                        etag: v1.etag,
                        last_modified: v1.last_modified,
                        user_metadata: v1.user_metadata,
                        content_md5: None,
                        content_crc32c: None,
                        version_id: None,
                        is_delete_marker: false,
                    })
                })
        })
    }
}
