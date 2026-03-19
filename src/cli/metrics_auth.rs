use base64::Engine;

/// Validate HTTP Basic Auth credentials from the request.
/// Returns `true` if the `Authorization: Basic ...` header is present and matches.
pub fn check(
    req: &hyper::Request<hyper::body::Incoming>,
    expected_user: &str,
    expected_pass: &str,
) -> bool {
    req.headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|value| {
            // RFC 7235/9110: auth scheme names are case-insensitive
            let mut parts = value.splitn(2, ' ');
            match (parts.next(), parts.next()) {
                (Some(scheme), Some(encoded)) if scheme.eq_ignore_ascii_case("Basic") => {
                    Some(encoded)
                }
                _ => None,
            }
        })
        .and_then(|encoded| {
            base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .ok()
        })
        .and_then(|b| String::from_utf8(b).ok())
        .is_some_and(|creds| {
            let Some((user, pass)) = creds.split_once(':') else {
                return false;
            };
            user == expected_user && pass == expected_pass
        })
}

pub fn unauthorized_response() -> s3s::HttpResponse {
    hyper::Response::builder()
        .status(401)
        .header("www-authenticate", "Basic realm=\"metrics\"")
        .header("content-type", "application/json")
        .body(s3s::Body::from(r#"{"error":"unauthorized"}"#.to_owned()))
        .expect("static 401 response")
}
