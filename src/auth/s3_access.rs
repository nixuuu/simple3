use std::sync::Arc;

use s3s::access::S3AccessContext;
use s3s::path::S3Path;
use s3s::s3_error;

use super::policy::{build_resource_arn, evaluate, map_op_to_action, Decision};
use super::s3_auth::AuthProvider;
use super::AuthStore;

#[async_trait::async_trait]
impl s3s::access::S3Access for AuthProvider {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> s3s::S3Result<()> {
        let creds = cx
            .credentials()
            .ok_or_else(|| s3_error!(AccessDenied, "authentication required"))?;

        let store = Arc::clone(&self.store);
        let access_key = creds.access_key.clone();
        let op_name = cx.s3_op().name().to_owned();
        let (bucket, key) = extract_path(cx.s3_path());

        tokio::task::spawn_blocking(move || {
            check_access(&store, &access_key, &op_name, bucket.as_deref(), key.as_deref())
        })
        .await
        .map_err(|e| {
            tracing::error!("access check panicked: {e}");
            s3_error!(InternalError)
        })?
    }
}

fn extract_path(path: &S3Path) -> (Option<String>, Option<String>) {
    match path {
        S3Path::Root => (None, None),
        S3Path::Bucket { bucket } => (Some(bucket.to_string()), None),
        S3Path::Object { bucket, key } => (Some(bucket.to_string()), Some(key.to_string())),
    }
}

fn check_access(
    store: &AuthStore,
    access_key: &str,
    op_name: &str,
    bucket: Option<&str>,
    key: Option<&str>,
) -> s3s::S3Result<()> {
    let key_record = store
        .get_key(access_key)
        .map_err(|_| s3_error!(InternalError))?
        .ok_or_else(|| s3_error!(AccessDenied))?;

    if key_record.is_admin {
        return Ok(());
    }

    let iam_action = map_op_to_action(op_name);
    let resource_arn = build_resource_arn(bucket, key);

    let policies = store
        .get_policies_for_key(access_key)
        .map_err(|_| s3_error!(InternalError))?;

    match evaluate(&policies, &iam_action, &resource_arn) {
        Decision::Allow => Ok(()),
        Decision::Deny => Err(s3_error!(AccessDenied)),
    }
}
