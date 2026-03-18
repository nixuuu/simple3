#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::run().await
}
