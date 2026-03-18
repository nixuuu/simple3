use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tower::{Layer, Service};
use tracing::Instrument;

/// Generate a new request ID and insert it into the response headers.
///
/// Returns the UUID string for use in tracing spans.
pub fn generate_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Insert `x-request-id` into response headers.
///
/// UUID v4 strings are always valid ASCII header values, so `parse()` is infallible here.
pub fn set_request_id_header<B>(resp: &mut hyper::Response<B>, request_id: &str) {
    if let Ok(val) = request_id.parse() {
        resp.headers_mut().insert("x-request-id", val);
    }
}

// === Tower Layer for gRPC ===

#[derive(Clone)]
pub struct RequestIdLayer;

impl<S> Layer<S> for RequestIdLayer {
    type Service = RequestIdService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestIdService { inner }
    }
}

#[derive(Clone)]
pub struct RequestIdService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<hyper::Request<ReqBody>> for RequestIdService<S>
where
    S: Service<hyper::Request<ReqBody>, Response = hyper::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<ReqBody>) -> Self::Future {
        let request_id = generate_request_id();
        let path = req.uri().path().to_owned();
        let span = tracing::info_span!(
            "grpc_request",
            request_id = %request_id,
            path = %path,
        );
        // Clone-and-swap: `self.inner` was marked ready by `poll_ready`, so we
        // move it into the future and replace it with a fresh clone. This follows
        // the tower `Service` contract — the ready instance is the one that gets called.
        let mut inner = self.inner.clone();
        std::mem::swap(&mut self.inner, &mut inner);
        Box::pin(
            async move {
                let mut resp = inner.call(req).await?;
                set_request_id_header(&mut resp, &request_id);
                Ok(resp)
            }
            .instrument(span),
        )
    }
}
