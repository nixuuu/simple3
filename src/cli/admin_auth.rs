use std::sync::Arc;

use http_body_util::BodyExt;
use serde::Deserialize;

use simple3::auth::AuthStore;

use super::serve::json_response;

/// Check admin credentials from `Authorization: Bearer <access_key>:<secret_key>`.
#[allow(clippy::result_large_err)]
fn check_admin_auth(
    req: &hyper::Request<hyper::body::Incoming>,
    store: &AuthStore,
) -> Result<(), s3s::HttpResponse> {
    let header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| json_response(401, &serde_json::json!({"error": "missing credentials"})))?;

    let (ak, sk) = header
        .split_once(':')
        .ok_or_else(|| json_response(401, &serde_json::json!({"error": "invalid auth format"})))?;

    let key = store
        .get_key(ak)
        .map_err(|e| json_response(500, &serde_json::json!({"error": e.to_string()})))?
        .ok_or_else(|| json_response(401, &serde_json::json!({"error": "invalid credentials"})))?;

    if !key.enabled || key.secret_key != sk {
        return Err(json_response(
            401,
            &serde_json::json!({"error": "invalid credentials"}),
        ));
    }
    if !key.is_admin {
        return Err(json_response(
            403,
            &serde_json::json!({"error": "admin access required"}),
        ));
    }
    Ok(())
}

pub async fn handle_admin_auth(
    req: hyper::Request<hyper::body::Incoming>,
    path: &str,
    method: &hyper::Method,
    store: Arc<AuthStore>,
) -> s3s::HttpResponse {
    if let Err(resp) = check_admin_auth(&req, &store) {
        return resp;
    }

    if path.starts_with("/_/keys") {
        handle_key_endpoint(req, path, method, store).await
    } else if path.starts_with("/_/policies") {
        handle_policy_endpoint(req, path, method, store).await
    } else {
        json_response(404, &serde_json::json!({"error": "not found"}))
    }
}

async fn handle_key_endpoint(
    req: hyper::Request<hyper::body::Incoming>,
    path: &str,
    method: &hyper::Method,
    store: Arc<AuthStore>,
) -> s3s::HttpResponse {
    if *method == hyper::Method::GET && path == "/_/keys" {
        return spawn(move || list_keys(&store)).await;
    }
    if *method == hyper::Method::POST && path == "/_/keys" {
        let body = read_body(req).await;
        return spawn(move || create_key(&store, &body)).await;
    }
    if *method == hyper::Method::GET && path.starts_with("/_/keys/") {
        let id = path["/_/keys/".len()..].trim_end_matches('/').to_owned();
        return spawn(move || show_key(&store, &id)).await;
    }
    if *method == hyper::Method::DELETE && path.starts_with("/_/keys/") {
        let id = path["/_/keys/".len()..].trim_end_matches('/').to_owned();
        return spawn(move || delete_key(&store, &id)).await;
    }
    if *method == hyper::Method::POST && path.ends_with("/enable") {
        let id = path["/_/keys/".len()..path.len() - "/enable".len()].to_owned();
        return spawn(move || enable_key(&store, &id, true)).await;
    }
    if *method == hyper::Method::POST && path.ends_with("/disable") {
        let id = path["/_/keys/".len()..path.len() - "/disable".len()].to_owned();
        return spawn(move || enable_key(&store, &id, false)).await;
    }
    json_response(404, &serde_json::json!({"error": "not found"}))
}

async fn handle_policy_endpoint(
    req: hyper::Request<hyper::body::Incoming>,
    path: &str,
    method: &hyper::Method,
    store: Arc<AuthStore>,
) -> s3s::HttpResponse {
    if *method == hyper::Method::GET && path == "/_/policies" {
        return spawn(move || list_policies(&store)).await;
    }
    if *method == hyper::Method::POST && path == "/_/policies" {
        let body = read_body(req).await;
        return spawn(move || create_policy(&store, &body)).await;
    }
    if let Some((policy_name, key_id)) = parse_attach_path(path) {
        if *method == hyper::Method::POST {
            return spawn(move || attach_policy(&store, &policy_name, &key_id)).await;
        }
        if *method == hyper::Method::DELETE {
            return spawn(move || detach_policy(&store, &policy_name, &key_id)).await;
        }
    }
    if *method == hyper::Method::GET && path.starts_with("/_/policies/") {
        let name = path["/_/policies/".len()..].trim_end_matches('/').to_owned();
        return spawn(move || show_policy(&store, &name)).await;
    }
    if *method == hyper::Method::DELETE && path.starts_with("/_/policies/") {
        let name = path["/_/policies/".len()..].trim_end_matches('/').to_owned();
        return spawn(move || delete_policy(&store, &name)).await;
    }
    json_response(404, &serde_json::json!({"error": "not found"}))
}

async fn spawn(f: impl FnOnce() -> s3s::HttpResponse + Send + 'static) -> s3s::HttpResponse {
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| json_response(500, &serde_json::json!({"error": e.to_string()})))
}

fn parse_attach_path(path: &str) -> Option<(String, String)> {
    // /_/policies/{name}/attach/{key_id}
    let rest = path.strip_prefix("/_/policies/")?;
    let (policy_name, key_id) = rest.split_once("/attach/")?;
    Some((
        policy_name.to_owned(),
        key_id.trim_end_matches('/').to_owned(),
    ))
}

async fn read_body(req: hyper::Request<hyper::body::Incoming>) -> String {
    let body = req.into_body();
    let bytes = body
        .collect()
        .await
        .map(http_body_util::Collected::to_bytes)
        .unwrap_or_default();
    String::from_utf8_lossy(&bytes).into_owned()
}

// === Key handlers ===

fn list_keys(store: &AuthStore) -> s3s::HttpResponse {
    match store.list_keys() {
        Ok(keys) => {
            let items: Vec<serde_json::Value> = keys
                .iter()
                .map(|k| {
                    serde_json::json!({
                        "access_key_id": k.access_key_id,
                        "created_at": k.created_at,
                        "description": k.description,
                        "is_admin": k.is_admin,
                        "enabled": k.enabled,
                    })
                })
                .collect();
            json_response(200, &serde_json::json!({"keys": items}))
        }
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}

#[derive(Deserialize)]
struct CreateKeyBody {
    #[serde(default)]
    description: String,
    #[serde(default)]
    admin: bool,
}

fn create_key(store: &AuthStore, body: &str) -> s3s::HttpResponse {
    let parsed: CreateKeyBody = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return json_response(400, &serde_json::json!({"error": e.to_string()})),
    };
    match store.create_key(&parsed.description, parsed.admin) {
        Ok(key) => json_response(
            201,
            &serde_json::json!({
                "access_key_id": key.access_key_id,
                "secret_key": key.secret_key,
                "is_admin": key.is_admin,
            }),
        ),
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}

fn show_key(store: &AuthStore, id: &str) -> s3s::HttpResponse {
    let key = match store.get_key(id) {
        Ok(Some(k)) => k,
        Ok(None) => return json_response(404, &serde_json::json!({"error": "key not found"})),
        Err(e) => return json_response(500, &serde_json::json!({"error": e.to_string()})),
    };
    let policies = store.get_policy_names_for_key(id).unwrap_or_default();
    json_response(
        200,
        &serde_json::json!({
            "access_key_id": key.access_key_id,
            "created_at": key.created_at,
            "description": key.description,
            "is_admin": key.is_admin,
            "enabled": key.enabled,
            "policies": policies,
        }),
    )
}

fn delete_key(store: &AuthStore, id: &str) -> s3s::HttpResponse {
    match store.delete_key(id) {
        Ok(()) => json_response(200, &serde_json::json!({"deleted": id})),
        Err(e) => json_response(400, &serde_json::json!({"error": e.to_string()})),
    }
}

fn enable_key(store: &AuthStore, id: &str, enabled: bool) -> s3s::HttpResponse {
    match store.set_key_enabled(id, enabled) {
        Ok(()) => json_response(200, &serde_json::json!({"access_key_id": id, "enabled": enabled})),
        Err(e) => json_response(400, &serde_json::json!({"error": e.to_string()})),
    }
}

// === Policy handlers ===

fn list_policies(store: &AuthStore) -> s3s::HttpResponse {
    match store.list_policies() {
        Ok(policies) => {
            let items: Vec<serde_json::Value> = policies
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "name": p.name,
                        "created_at": p.created_at,
                        "statement_count": p.document.statements.len(),
                    })
                })
                .collect();
            json_response(200, &serde_json::json!({"policies": items}))
        }
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}

#[derive(Deserialize)]
struct CreatePolicyBody {
    name: String,
    document: simple3::auth::policy::PolicyDocument,
}

fn create_policy(store: &AuthStore, body: &str) -> s3s::HttpResponse {
    let parsed: CreatePolicyBody = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return json_response(400, &serde_json::json!({"error": e.to_string()})),
    };
    match store.create_policy(&parsed.name, parsed.document) {
        Ok(_) => json_response(201, &serde_json::json!({"created": parsed.name})),
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}

fn show_policy(store: &AuthStore, name: &str) -> s3s::HttpResponse {
    match store.get_policy(name) {
        Ok(Some(p)) => json_response(200, &p.document),
        Ok(None) => json_response(404, &serde_json::json!({"error": "policy not found"})),
        Err(e) => json_response(500, &serde_json::json!({"error": e.to_string()})),
    }
}

fn delete_policy(store: &AuthStore, name: &str) -> s3s::HttpResponse {
    match store.delete_policy(name) {
        Ok(()) => json_response(200, &serde_json::json!({"deleted": name})),
        Err(e) => json_response(400, &serde_json::json!({"error": e.to_string()})),
    }
}

fn attach_policy(store: &AuthStore, policy_name: &str, key_id: &str) -> s3s::HttpResponse {
    match store.attach_policy(key_id, policy_name) {
        Ok(()) => json_response(
            200,
            &serde_json::json!({"attached": policy_name, "to": key_id}),
        ),
        Err(e) => json_response(400, &serde_json::json!({"error": e.to_string()})),
    }
}

fn detach_policy(store: &AuthStore, policy_name: &str, key_id: &str) -> s3s::HttpResponse {
    match store.detach_policy(key_id, policy_name) {
        Ok(()) => json_response(
            200,
            &serde_json::json!({"detached": policy_name, "from": key_id}),
        ),
        Err(e) => json_response(400, &serde_json::json!({"error": e.to_string()})),
    }
}
