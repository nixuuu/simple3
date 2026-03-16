use std::borrow::Cow;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PolicyDocument {
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Statement")]
    pub statements: Vec<Statement>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Statement {
    #[serde(rename = "Sid", default, skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Action")]
    pub action: OneOrMany,
    #[serde(rename = "Resource")]
    pub resource: OneOrMany,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

/// Accepts both a single string and an array of strings in JSON.
/// Uses custom serde for JSON compatibility (single string or array).
#[derive(Clone, Debug)]
pub struct OneOrMany(pub Vec<String>);

impl serde::Serialize for OneOrMany {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() && self.0.len() == 1 {
            self.0[0].serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> serde::Deserialize<'de> for OneOrMany {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            // JSON: accept both "string" and ["array"]
            let value = serde_json::Value::deserialize(deserializer)?;
            match value {
                serde_json::Value::String(s) => Ok(Self(vec![s])),
                serde_json::Value::Array(arr) => {
                    let strings: Result<Vec<String>, _> = arr
                        .into_iter()
                        .map(|v| {
                            v.as_str()
                                .map(String::from)
                                .ok_or_else(|| serde::de::Error::custom("expected string in array"))
                        })
                        .collect();
                    Ok(Self(strings?))
                }
                _ => Err(serde::de::Error::custom("expected string or array")),
            }
        } else {
            // bincode: always Vec<String>
            Vec::<String>::deserialize(deserializer).map(Self)
        }
    }
}

impl OneOrMany {
    pub fn values(&self) -> &[String] {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
}

/// Evaluate whether an action on a resource is allowed by the given policies.
///
/// Follows AWS IAM logic: explicit deny wins, then explicit allow, then implicit deny.
pub fn evaluate(policies: &[PolicyDocument], action: &str, resource: &str) -> Decision {
    let mut allowed = false;

    for policy in policies {
        for stmt in &policy.statements {
            let action_match = stmt.action.values().iter().any(|p| pattern_matches(p, action));
            let resource_match = stmt
                .resource
                .values()
                .iter()
                .any(|p| pattern_matches(p, resource));

            if action_match && resource_match {
                if stmt.effect == Effect::Deny {
                    return Decision::Deny;
                }
                allowed = true;
            }
        }
    }

    if allowed {
        Decision::Allow
    } else {
        Decision::Deny
    }
}

/// Simple glob matching where `*` matches any sequence of characters.
/// Comparison is case-insensitive.
fn pattern_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    let value = value.to_ascii_lowercase();
    glob_match(&pattern, &value)
}

fn glob_match(pattern: &str, value: &str) -> bool {
    let Some(first_star) = pattern.find('*') else {
        return pattern == value;
    };

    // Check prefix before first *
    let prefix = &pattern[..first_star];
    if !value.starts_with(prefix) {
        return false;
    }
    let mut pos = prefix.len();

    // Check suffix after last *
    let last_star = pattern.rfind('*').unwrap_or(first_star);
    let suffix = &pattern[last_star + 1..];
    if !suffix.is_empty() && !value[pos..].ends_with(suffix) {
        return false;
    }

    // Check middle segments between first and last *
    if first_star >= last_star {
        return true;
    }
    for segment in pattern[first_star + 1..last_star].split('*') {
        if segment.is_empty() {
            continue;
        }
        match value[pos..].find(segment) {
            Some(idx) => pos += idx + segment.len(),
            None => return false,
        }
    }

    true
}

/// Map s3s operation names to IAM-style action strings.
pub fn map_op_to_action(op_name: &str) -> Cow<'static, str> {
    match op_name {
        "GetObject" | "HeadObject" => Cow::Borrowed("s3:GetObject"),
        "GetObjectVersion" => Cow::Borrowed("s3:GetObjectVersion"),
        "PutObject" | "CreateMultipartUpload" | "UploadPart" | "CompleteMultipartUpload"
        | "AbortMultipartUpload" => Cow::Borrowed("s3:PutObject"),
        "DeleteObject" | "DeleteObjects" => Cow::Borrowed("s3:DeleteObject"),
        "DeleteObjectVersion" => Cow::Borrowed("s3:DeleteObjectVersion"),
        "ListObjectsV2" | "ListObjects" => Cow::Borrowed("s3:ListBucket"),
        "ListObjectVersions" => Cow::Borrowed("s3:ListBucketVersions"),
        "PutBucketVersioning" => Cow::Borrowed("s3:PutBucketVersioning"),
        "GetBucketVersioning" => Cow::Borrowed("s3:GetBucketVersioning"),
        "CreateBucket" => Cow::Borrowed("s3:CreateBucket"),
        "DeleteBucket" => Cow::Borrowed("s3:DeleteBucket"),
        "ListBuckets" => Cow::Borrowed("s3:ListAllMyBuckets"),
        _ => Cow::Owned(format!("s3:{op_name}")),
    }
}

/// Build resource ARN from bucket and optional key.
pub fn build_resource_arn(bucket: Option<&str>, key: Option<&str>) -> String {
    match (bucket, key) {
        (Some(b), Some(k)) => format!("arn:s3:::{b}/{k}"),
        (Some(b), None) => format!("arn:s3:::{b}"),
        _ => "arn:s3:::*".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_policy(effect: Effect, actions: &[&str], resources: &[&str]) -> PolicyDocument {
        PolicyDocument {
            version: "2024-01-01".to_owned(),
            statements: vec![Statement {
                sid: None,
                effect,
                action: OneOrMany(actions.iter().map(|s| (*s).to_owned()).collect()),
                resource: OneOrMany(resources.iter().map(|s| (*s).to_owned()).collect()),
            }],
        }
    }

    #[test]
    fn test_wildcard_allows_all() {
        let p = make_policy(Effect::Allow, &["*"], &["*"]);
        assert_eq!(evaluate(&[p], "s3:GetObject", "arn:s3:::bucket/key"), Decision::Allow);
    }

    #[test]
    fn test_implicit_deny() {
        let p = make_policy(Effect::Allow, &["s3:GetObject"], &["arn:s3:::bucket/*"]);
        assert_eq!(
            evaluate(&[p], "s3:PutObject", "arn:s3:::bucket/key"),
            Decision::Deny
        );
    }

    #[test]
    fn test_explicit_deny_overrides_allow() {
        let allow = make_policy(Effect::Allow, &["s3:*"], &["arn:s3:::*"]);
        let deny = make_policy(Effect::Deny, &["s3:DeleteObject"], &["arn:s3:::*"]);
        assert_eq!(
            evaluate(&[allow, deny], "s3:DeleteObject", "arn:s3:::bucket/key"),
            Decision::Deny
        );
    }

    #[test]
    fn test_action_prefix_match() {
        let p = make_policy(Effect::Allow, &["s3:*"], &["arn:s3:::*"]);
        assert_eq!(evaluate(&[p], "s3:PutObject", "arn:s3:::b/k"), Decision::Allow);
    }

    #[test]
    fn test_resource_prefix_match() {
        let p = make_policy(Effect::Allow, &["s3:GetObject"], &["arn:s3:::bucket/photos/*"]);
        assert_eq!(
            evaluate(&[p.clone()], "s3:GetObject", "arn:s3:::bucket/photos/cat.jpg"),
            Decision::Allow
        );
        assert_eq!(
            evaluate(&[p], "s3:GetObject", "arn:s3:::bucket/docs/readme.txt"),
            Decision::Deny
        );
    }

    #[test]
    fn test_no_policies_denies() {
        assert_eq!(evaluate(&[], "s3:GetObject", "arn:s3:::b/k"), Decision::Deny);
    }

    #[test]
    fn test_case_insensitive() {
        let p = make_policy(Effect::Allow, &["S3:GetObject"], &["ARN:S3:::Bucket/*"]);
        assert_eq!(
            evaluate(&[p], "s3:getobject", "arn:s3:::bucket/key"),
            Decision::Allow
        );
    }

    #[test]
    fn test_map_op_to_action() {
        assert_eq!(map_op_to_action("GetObject"), "s3:GetObject");
        assert_eq!(map_op_to_action("HeadObject"), "s3:GetObject");
        assert_eq!(map_op_to_action("PutObject"), "s3:PutObject");
        assert_eq!(map_op_to_action("CreateMultipartUpload"), "s3:PutObject");
        assert_eq!(map_op_to_action("DeleteObject"), "s3:DeleteObject");
        assert_eq!(map_op_to_action("ListObjectsV2"), "s3:ListBucket");
        assert_eq!(map_op_to_action("ListBuckets"), "s3:ListAllMyBuckets");
        assert_eq!(map_op_to_action("CreateBucket"), "s3:CreateBucket");
        assert_eq!(map_op_to_action("UnknownOp"), "s3:UnknownOp");
    }

    #[test]
    fn test_build_resource_arn() {
        assert_eq!(build_resource_arn(Some("b"), Some("k")), "arn:s3:::b/k");
        assert_eq!(build_resource_arn(Some("b"), None), "arn:s3:::b");
        assert_eq!(build_resource_arn(None, None), "arn:s3:::*");
    }

    #[test]
    fn test_policy_json_roundtrip() {
        let json = r#"{
            "Version": "2024-01-01",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": ["arn:s3:::bucket/*"]
                }
            ]
        }"#;
        let doc: PolicyDocument = serde_json::from_str(json).unwrap();
        assert_eq!(doc.statements.len(), 1);
        assert_eq!(doc.statements[0].action.values(), &["s3:GetObject"]);
        assert_eq!(doc.statements[0].resource.values(), &["arn:s3:::bucket/*"]);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("s3:*", "s3:GetObject"));
        assert!(glob_match("arn:s3:::bucket/*", "arn:s3:::bucket/key"));
        assert!(glob_match("arn:s3:::bucket/photos/*", "arn:s3:::bucket/photos/cat.jpg"));
        assert!(!glob_match("arn:s3:::bucket/photos/*", "arn:s3:::bucket/docs/file"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "other"));
        assert!(glob_match("*match", "endmatch"));
        assert!(!glob_match("*match", "matchno"));
    }
}
