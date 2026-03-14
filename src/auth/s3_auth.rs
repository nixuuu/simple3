use std::sync::Arc;

use s3s::auth::SecretKey;
use s3s::s3_error;

use super::AuthStore;

#[derive(Clone)]
pub struct AuthProvider {
    pub(crate) store: Arc<AuthStore>,
}

impl AuthProvider {
    pub const fn new(store: Arc<AuthStore>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl s3s::auth::S3Auth for AuthProvider {
    async fn get_secret_key(&self, access_key: &str) -> s3s::S3Result<SecretKey> {
        let store = Arc::clone(&self.store);
        let ak = access_key.to_owned();

        let key_record = tokio::task::spawn_blocking(move || store.get_key(&ak))
            .await
            .map_err(|e| {
                tracing::error!("auth task panicked: {e}");
                s3_error!(InternalError)
            })?
            .map_err(|e| {
                tracing::error!("auth db error: {e}");
                s3_error!(InternalError)
            })?;

        match key_record {
            Some(kr) if kr.enabled => Ok(SecretKey::from(kr.secret_key)),
            Some(_) => Err(s3_error!(InvalidAccessKeyId, "key is disabled")),
            None => Err(s3_error!(InvalidAccessKeyId)),
        }
    }
}
