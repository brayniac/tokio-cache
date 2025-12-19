use std::time::Duration;

/// Error type for cache operations
#[derive(Debug)]
pub enum CacheError {
    /// Cache is out of memory
    OutOfMemory,
    /// Hashtable insert failed
    HashTableFull,
    /// Other error
    Other(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::OutOfMemory => write!(f, "out of memory"),
            CacheError::HashTableFull => write!(f, "hashtable full"),
            CacheError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for CacheError {}

/// Trait for cache value guards that provide access to the cached value.
/// The guard keeps the value alive while it's being read.
pub trait CacheGuard: AsRef<[u8]> + Send {
    fn value(&self) -> &[u8] {
        self.as_ref()
    }
}

/// Trait for cache implementations.
///
/// This trait is designed to be object-safe while still allowing for
/// monomorphization when used with concrete types. All methods are async
/// to accommodate both sync and async cache backends.
pub trait Cache: Send + Sync + 'static {
    /// The guard type returned by get operations
    type Guard<'a>: CacheGuard
    where
        Self: 'a;

    /// Get a value from the cache.
    ///
    /// Returns `Some(guard)` if the key exists, `None` otherwise.
    /// The guard provides access to the value and keeps it alive.
    fn get(&self, key: &[u8]) -> impl std::future::Future<Output = Option<Self::Guard<'_>>> + Send;

    /// Set a key-value pair in the cache.
    ///
    /// `ttl` specifies the time-to-live for the entry. If `None`, the entry
    /// will not expire (or use the cache's default TTL behavior).
    fn set(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<(), CacheError>> + Send;

    /// Delete a key from the cache.
    ///
    /// Returns `true` if the key was present and deleted, `false` otherwise.
    fn delete(&self, key: &[u8]) -> impl std::future::Future<Output = bool> + Send;

    /// Flush all entries from the cache.
    fn flush(&self);
}
