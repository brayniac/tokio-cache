#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static _rjem_malloc_conf: &[u8] = b"thp:always,metadata_thp:always\0";

use async_s3fifo::{Cache as S3fifo, CacheBuilder, ItemGuard};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_cache::config::{print_default_config, parse_hugepage_size_s3fifo, Config};
use tokio_cache::metrics;
use tokio_cache::{Cache, CacheError, CacheGuard};

/// Wrapper for async-s3fifo to implement our Cache trait
struct S3fifoWrapper(S3fifo);

/// Guard wrapper that borrows from s3fifo without copying
struct S3fifoGuard<'a>(ItemGuard<'a>);

impl AsRef<[u8]> for S3fifoGuard<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0.value()
    }
}

impl CacheGuard for S3fifoGuard<'_> {}

impl Cache for S3fifoWrapper {
    type Guard<'a> = S3fifoGuard<'a>;

    async fn get(&self, key: &[u8]) -> Option<Self::Guard<'_>> {
        self.0.get(key).await.map(S3fifoGuard)
    }

    async fn set(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        // S3-FIFO requires a TTL, default to 5 minutes if not specified
        let ttl = ttl.unwrap_or(Duration::from_secs(300));
        self.0
            .set(key, value, ttl)
            .await
            .map_err(|_| CacheError::OutOfMemory)
    }

    async fn delete(&self, key: &[u8]) -> bool {
        // S3-FIFO delete is sync
        self.0.delete(key)
    }

    fn flush(&self) {
        // S3-FIFO doesn't have a flush method, so this is a no-op
    }
}

#[derive(Parser, Debug)]
#[command(name = "s3fifo-server")]
#[command(version)]
#[command(about = "A Redis-compatible caching server using S3-FIFO")]
struct Args {
    /// Path to the TOML configuration file
    config: Option<PathBuf>,

    /// Override number of worker threads
    #[arg(short = 't', long = "threads")]
    threads: Option<usize>,

    /// Print the default configuration and exit
    #[arg(long)]
    print_config: bool,
}

/// Set CPU affinity for the current thread (Linux only)
#[cfg(target_os = "linux")]
fn set_cpu_affinity(cpu_id: usize) -> Result<(), String> {
    use std::mem;

    unsafe {
        let mut cpu_set: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpu_set);
        libc::CPU_SET(cpu_id, &mut cpu_set);

        let result = libc::sched_setaffinity(
            0,
            mem::size_of::<libc::cpu_set_t>(),
            &cpu_set,
        );

        if result == 0 {
            Ok(())
        } else {
            Err(format!("sched_setaffinity failed with error code {}", result))
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn set_cpu_affinity(_cpu_id: usize) -> Result<(), String> {
    Err("CPU affinity setting is only supported on Linux".to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.print_config {
        print_default_config("s3fifo-server");
        return Ok(());
    }

    let mut config = if let Some(ref config_path) = args.config {
        Config::load(config_path)?
    } else {
        Config::default_config()
    };

    if let Some(threads) = args.threads {
        config.server.threads = Some(threads);
    }

    let thread_count = config.threads();
    let cpu_ids = config.cpu_affinity();

    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.worker_threads(thread_count);

    if let Some(ref cpus) = cpu_ids {
        let cpus_clone = cpus.clone();
        let thread_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        runtime_builder.on_thread_start(move || {
            let idx = thread_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            if idx < cpus_clone.len() {
                let cpu_id = cpus_clone[idx];
                let _ = set_cpu_affinity(cpu_id);
            }
        });
    }

    let runtime = runtime_builder.enable_all().build()?;

    runtime.block_on(async_main(config))
}

async fn async_main(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    // Spawn metrics server
    let metrics_addr = config.server.metrics_listen.clone();
    tokio::spawn(async move {
        let _ = metrics::serve(&metrics_addr).await;
    });

    let hugepage_size = parse_hugepage_size_s3fifo(&config.cache.hugepage_size)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let cache = CacheBuilder::new()
        .ram_size(config.cache.heap_size)
        .ram_segment_size(config.cache.segment_size)
        .hashtable_power(config.cache.hashtable_power)
        .hugepage_size(hugepage_size)
        .build()?;

    let wrapper = S3fifoWrapper(cache);

    tokio_cache::server::run(config, wrapper).await
}
