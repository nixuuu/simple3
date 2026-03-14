use tonic::Status;

use super::policy::{evaluate, Decision};
use super::AuthStore;

pub struct GrpcCredentials {
    pub access_key: String,
    pub secret_key: String,
}

/// Extract credentials from gRPC request metadata.
pub fn extract_credentials<T>(request: &tonic::Request<T>) -> Result<GrpcCredentials, Status> {
    extract_credentials_from_metadata(request.metadata())
}

/// Extract credentials from a metadata map directly.
pub fn extract_credentials_from_metadata(
    meta: &tonic::metadata::MetadataMap,
) -> Result<GrpcCredentials, Status> {
    let access_key = meta
        .get("x-access-key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::unauthenticated("missing x-access-key metadata"))?
        .to_owned();
    let secret_key = meta
        .get("x-secret-key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::unauthenticated("missing x-secret-key metadata"))?
        .to_owned();
    Ok(GrpcCredentials {
        access_key,
        secret_key,
    })
}

/// Validate credentials and check IAM policy for a gRPC operation.
pub fn check_grpc_access(
    store: &AuthStore,
    creds: &GrpcCredentials,
    action: &str,
    resource: &str,
) -> Result<(), Status> {
    let key_record = store
        .get_key(&creds.access_key)
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::unauthenticated("invalid access key"))?;

    if !key_record.enabled {
        return Err(Status::unauthenticated("key is disabled"));
    }
    if key_record.secret_key != creds.secret_key {
        return Err(Status::unauthenticated("invalid secret key"));
    }
    if key_record.is_admin {
        return Ok(());
    }

    let policies = store
        .get_policies_for_key(&creds.access_key)
        .map_err(|e| Status::internal(e.to_string()))?;

    match evaluate(&policies, action, resource) {
        Decision::Allow => Ok(()),
        Decision::Deny => Err(Status::permission_denied("access denied by policy")),
    }
}
