use std::sync::Arc;
use std::time::Duration;

use tokio::task::{JoinHandle, JoinSet};

use simple3::storage::Storage;

pub(super) async fn await_bg_tasks(tasks: &mut [JoinHandle<()>], timeout_secs: u64) {
    let timeout = Duration::from_secs(timeout_secs);
    if tokio::time::timeout(timeout, async {
        for handle in tasks.iter_mut() {
            let _ = handle.await;
        }
    })
    .await
    .is_err()
    {
        tracing::warn!("background tasks did not finish within timeout, aborting");
        for handle in tasks.iter() {
            handle.abort();
        }
    }
}

pub(super) async fn drain_connections(connections: &mut JoinSet<()>, timeout_secs: u64) {
    tracing::info!(
        "draining {} in-flight connection(s) (timeout {}s)",
        connections.len(),
        timeout_secs
    );
    let timeout = Duration::from_secs(timeout_secs);
    if tokio::time::timeout(timeout, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tracing::warn!(
            "shutdown timeout reached, dropping {} connection(s)",
            connections.len()
        );
        connections.shutdown().await;
    }
}

/// Validate metrics auth configuration; returns credential pair if enabled.
pub(super) fn validate_metrics_auth(
    user: Option<String>,
    password: Option<String>,
) -> anyhow::Result<Option<Arc<(String, String)>>> {
    match (user, password) {
        (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => {
            tracing::info!("metrics endpoint auth enabled");
            Ok(Some(Arc::new((u, p))))
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("metrics auth requires non-empty --metrics-user and --metrics-password");
        }
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!(
                "metrics auth partially configured: both --metrics-user and --metrics-password must be set"
            );
        }
        _ => Ok(None),
    }
}

/// Gracefully drain connections and background tasks, then sync storage.
pub(super) async fn graceful_shutdown(
    connections: &mut JoinSet<()>,
    bg_tasks: &mut [JoinHandle<()>],
    storage: Arc<Storage>,
    timeout_secs: u64,
) -> anyhow::Result<()> {
    tokio::join!(
        drain_connections(connections, timeout_secs),
        await_bg_tasks(bg_tasks, timeout_secs),
    );

    match tokio::task::spawn_blocking(move || storage.sync_all()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::error!("failed to sync storage on shutdown: {e}");
            return Err(e.into());
        }
        Err(e) => {
            tracing::error!("sync_all task panicked: {e}");
            return Err(e.into());
        }
    }
    tracing::info!("shutdown complete");
    Ok(())
}
