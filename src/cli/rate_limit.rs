use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::{Context, Poll};

use governor::clock::DefaultClock;
use governor::state::keyed::DashMapStateStore;
use governor::{Quota, RateLimiter};

pub type IpRateLimiter = RateLimiter<IpAddr, DashMapStateStore<IpAddr>, DefaultClock>;

/// Build a per-IP token-bucket rate limiter. Returns `None` when `rps == 0` (disabled).
///
/// Uses `DashMapStateStore` which grows one entry per unique IP and never evicts.
/// TODO: add periodic cleanup or bounded LRU store for long-running servers
/// exposed to many unique IPs.
pub fn build_rate_limiter(rps: u32) -> Option<Arc<IpRateLimiter>> {
    let rps = NonZeroU32::new(rps)?;
    let quota = Quota::per_second(rps);
    Some(Arc::new(RateLimiter::dashmap(quota)))
}

/// Extract peer IP from request extensions.
/// Checks for direct `IpAddr` (set by HTTP `PeerIpService`) first,
/// then falls back to tonic's `TcpConnectInfo` (set automatically for gRPC).
fn extract_peer_ip<B>(req: &hyper::Request<B>) -> IpAddr {
    if let Some(ip) = req.extensions().get::<IpAddr>().copied() {
        return ip;
    }
    if let Some(info) = req
        .extensions()
        .get::<tonic::transport::server::TcpConnectInfo>()
        && let Some(addr) = info.remote_addr()
    {
        return addr.ip();
    }
    IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
}

// --- Tower layer: per-IP rate limiting ---

/// Tower layer that applies per-IP rate limiting.
#[derive(Clone)]
pub struct RateLimitLayer {
    limiter: Arc<IpRateLimiter>,
}

impl RateLimitLayer {
    pub const fn new(limiter: Arc<IpRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl<S> tower::Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: Arc::clone(&self.limiter),
        }
    }
}

/// Tower service that checks rate limits before forwarding requests.
#[derive(Clone)]
pub struct RateLimitService<S> {
    inner: S,
    limiter: Arc<IpRateLimiter>,
}

impl<S, ReqBody, ResBody> tower::Service<hyper::Request<ReqBody>> for RateLimitService<S>
where
    S: tower::Service<hyper::Request<ReqBody>, Response = hyper::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::Either<
        S::Future,
        std::future::Ready<Result<Self::Response, Self::Error>>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<ReqBody>) -> Self::Future {
        let peer_ip = extract_peer_ip(&req);

        if self.limiter.check_key(&peer_ip).is_err() {
            metrics::counter!("simple3_rate_limited_total", "protocol" => "grpc").increment(1);
            let resp = hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .header("content-type", "application/grpc")
                .header("grpc-status", "8") // RESOURCE_EXHAUSTED
                .header("grpc-message", "rate%20limit%20exceeded")
                .body(ResBody::default())
                .expect("static response"); // safe: all headers are static literals
            return futures::future::Either::Right(std::future::ready(Ok(resp)));
        }

        futures::future::Either::Left(self.inner.call(req))
    }
}
