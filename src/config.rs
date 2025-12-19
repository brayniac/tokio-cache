use serde::Deserialize;

/// Server configuration loaded from TOML file
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Server configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Cache configuration
    #[serde(default)]
    pub cache: CacheConfig,
}

/// Server configuration
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Address to listen on for Redis protocol (default: "0.0.0.0:6379")
    #[serde(default = "default_listen")]
    pub listen: String,

    /// Address to listen on for Memcached protocol (default: "0.0.0.0:11211")
    /// Set to empty string to disable memcached protocol.
    #[serde(default = "default_memcached_listen")]
    pub memcached_listen: String,

    /// Address for metrics endpoint (default: "0.0.0.0:9090")
    #[serde(default = "default_metrics_listen")]
    pub metrics_listen: String,

    /// Number of worker threads (default: derived from cpu_affinity or number of CPUs)
    pub threads: Option<usize>,

    /// CPU cores to pin worker threads to, Linux-style (e.g., "0-3,6-8")
    /// If not specified, no CPU pinning is performed.
    /// Thread count is automatically derived from the number of CPUs specified.
    pub cpu_affinity: Option<String>,

    /// TCP listen backlog size (default: 4096)
    #[serde(default = "default_backlog")]
    pub backlog: u32,

    /// Socket receive buffer size (default: 64KB)
    #[serde(default = "default_socket_buffer_size", deserialize_with = "deserialize_size")]
    pub recv_buffer_size: usize,

    /// Socket send buffer size (default: 64KB)
    #[serde(default = "default_socket_buffer_size", deserialize_with = "deserialize_size")]
    pub send_buffer_size: usize,

    /// Application read buffer size (default: 64KB)
    #[serde(default = "default_app_buffer_size", deserialize_with = "deserialize_size")]
    pub read_buffer_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            memcached_listen: default_memcached_listen(),
            metrics_listen: default_metrics_listen(),
            threads: None,
            cpu_affinity: None,
            backlog: default_backlog(),
            recv_buffer_size: default_socket_buffer_size(),
            send_buffer_size: default_socket_buffer_size(),
            read_buffer_size: default_app_buffer_size(),
        }
    }
}

fn default_listen() -> String {
    "0.0.0.0:6379".to_string()
}

fn default_memcached_listen() -> String {
    "0.0.0.0:11211".to_string()
}

fn default_metrics_listen() -> String {
    "0.0.0.0:9090".to_string()
}

fn default_backlog() -> u32 {
    4096
}

fn default_socket_buffer_size() -> usize {
    64 * 1024 // 64KB
}

fn default_app_buffer_size() -> usize {
    64 * 1024 // 64KB
}

/// Cache configuration
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Total heap size for cache
    /// Supports suffixes: K/KB, M/MB, G/GB (e.g., "4GB", "512MB")
    #[serde(default = "default_heap_size", deserialize_with = "deserialize_size")]
    pub heap_size: usize,

    /// Segment size for cache
    /// Supports suffixes: K/KB, M/MB (e.g., "1MB", "512KB")
    #[serde(default = "default_segment_size", deserialize_with = "deserialize_size")]
    pub segment_size: usize,

    /// Hashtable power (2^power buckets)
    /// Each bucket holds multiple items, so:
    /// - power 20 = 1M buckets
    /// - power 24 = 16M buckets
    /// - power 26 = 64M buckets
    #[serde(default = "default_hashtable_power")]
    pub hashtable_power: u8,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            heap_size: default_heap_size(),
            segment_size: default_segment_size(),
            hashtable_power: default_hashtable_power(),
        }
    }
}

fn default_heap_size() -> usize {
    4 * 1024 * 1024 * 1024 // 4GB
}

fn default_segment_size() -> usize {
    1024 * 1024 // 1MB
}

fn default_hashtable_power() -> u8 {
    26
}

/// Deserialize a size string like "64MB" or "4GB" into bytes
fn deserialize_size<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SizeValue {
        Number(usize),
        String(String),
    }

    match SizeValue::deserialize(deserializer)? {
        SizeValue::Number(n) => Ok(n),
        SizeValue::String(s) => parse_size(&s).map_err(D::Error::custom),
    }
}

/// Parse a size string like "64MB", "4GB", "1TB" into bytes
pub fn parse_size(s: &str) -> Result<usize, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty size string".to_string());
    }

    let (num_str, suffix) = match s.find(|c: char| c.is_alphabetic()) {
        Some(idx) => (&s[..idx], s[idx..].to_uppercase()),
        None => (s, String::new()),
    };

    let num: usize = num_str
        .trim()
        .parse()
        .map_err(|_| format!("invalid number: {}", num_str))?;

    let multiplier: usize = match suffix.as_str() {
        "" => 1,
        "B" => 1,
        "K" | "KB" | "KIB" => 1024,
        "M" | "MB" | "MIB" => 1024 * 1024,
        "G" | "GB" | "GIB" => 1024 * 1024 * 1024,
        "T" | "TB" | "TIB" => 1024 * 1024 * 1024 * 1024,
        _ => return Err(format!("unknown size suffix: {}", suffix)),
    };

    num.checked_mul(multiplier)
        .ok_or_else(|| "size overflow".to_string())
}

impl Config {
    /// Load configuration from a TOML file
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Create a default configuration
    pub fn default_config() -> Self {
        Self {
            server: ServerConfig::default(),
            cache: CacheConfig::default(),
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.cache.heap_size < self.cache.segment_size {
            return Err(format!(
                "heap_size ({}) must be at least segment_size ({})",
                self.cache.heap_size, self.cache.segment_size
            )
            .into());
        }

        if self.cache.hashtable_power > 30 {
            return Err("hashtable_power must be <= 30".into());
        }

        if let Some(ref affinity) = self.server.cpu_affinity {
            parse_cpu_list(affinity).map_err(|e| format!("Invalid cpu_affinity: {}", e))?;
        }

        Ok(())
    }

    /// Get the number of threads to use.
    /// Priority: explicit threads config > cpu_affinity count > num_cpus
    pub fn threads(&self) -> usize {
        if let Some(threads) = self.server.threads {
            return threads;
        }
        if let Some(ref affinity) = self.server.cpu_affinity
            && let Ok(cpus) = parse_cpu_list(affinity)
        {
            return cpus.len();
        }
        num_cpus::get()
    }

    /// Get the parsed CPU affinity list
    pub fn cpu_affinity(&self) -> Option<Vec<usize>> {
        self.server
            .cpu_affinity
            .as_ref()
            .and_then(|s| parse_cpu_list(s).ok())
    }
}

/// Parse a Linux-style CPU list string into a vector of CPU IDs.
/// Supports formats like:
/// - "0-7" - range from 0 to 7
/// - "0-7:2" - range with stride (0,2,4,6)
/// - "1,3,8" - individual CPUs
/// - "0-3,5,7-9" - mixed ranges and individuals
/// - "0-15:2,1-15:2" - even and odd CPUs
pub fn parse_cpu_list(cpu_list: &str) -> Result<Vec<usize>, String> {
    let mut cpus = Vec::new();

    for part in cpu_list.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Check for stride notation (e.g., "0-15:2")
        if let Some((range_part, stride_str)) = part.split_once(':') {
            let stride: usize = stride_str
                .trim()
                .parse()
                .map_err(|_| format!("Invalid stride: {}", stride_str))?;

            if stride == 0 {
                return Err("Stride cannot be zero".to_string());
            }

            if let Some((start_str, end_str)) = range_part.split_once('-') {
                let start: usize = start_str
                    .trim()
                    .parse()
                    .map_err(|_| format!("Invalid start of range: {}", start_str))?;
                let end: usize = end_str
                    .trim()
                    .parse()
                    .map_err(|_| format!("Invalid end of range: {}", end_str))?;

                if start > end {
                    return Err(format!("Invalid range: start ({}) > end ({})", start, end));
                }

                let mut cpu = start;
                while cpu <= end {
                    cpus.push(cpu);
                    cpu += stride;
                }
            } else {
                return Err(format!("Stride requires a range (e.g., 0-15:2), got: {}", part));
            }
        } else if let Some((start_str, end_str)) = part.split_once('-') {
            let start: usize = start_str
                .trim()
                .parse()
                .map_err(|_| format!("Invalid start of range: {}", start_str))?;
            let end: usize = end_str
                .trim()
                .parse()
                .map_err(|_| format!("Invalid end of range: {}", end_str))?;

            if start > end {
                return Err(format!("Invalid range: start ({}) > end ({})", start, end));
            }

            for cpu in start..=end {
                cpus.push(cpu);
            }
        } else {
            let cpu: usize = part
                .parse()
                .map_err(|_| format!("Invalid CPU number: {}", part))?;
            cpus.push(cpu);
        }
    }

    if cpus.is_empty() {
        return Err("CPU list cannot be empty".to_string());
    }

    // Remove duplicates and sort
    cpus.sort_unstable();
    cpus.dedup();

    Ok(cpus)
}

/// Print the default configuration in TOML format
pub fn print_default_config(cache_type: &str) {
    let config = format!(
        r#"# {cache_type} configuration
#
# This file demonstrates all available configuration options.
# Copy and modify as needed for your deployment.

[server]
# Address and port to listen on for Redis protocol
listen = "0.0.0.0:6379"

# Address and port to listen on for Memcached protocol
# Set to empty string to disable memcached protocol
memcached_listen = "0.0.0.0:11211"

# Address for Prometheus metrics endpoint
metrics_listen = "0.0.0.0:9090"

# Number of worker threads (default: derived from cpu_affinity, or number of CPUs)
# threads = 8

# CPU cores to pin worker threads to, Linux-style (e.g., "0-3" or "0,2,4,6")
# Thread count is automatically derived from the number of CPUs specified.
# cpu_affinity = "0-7"

# TCP listen backlog size
backlog = 4096

# Socket buffer sizes
recv_buffer_size = "64KB"
send_buffer_size = "64KB"

# Application read buffer size
read_buffer_size = "64KB"

[cache]
# Total heap size for cache
# Supports: B, KB/K, MB/M, GB/G, TB/T suffixes
heap_size = "4GB"

# Segment size - smaller segments allow finer-grained eviction
# but increase metadata overhead. 1MB is a good default.
segment_size = "1MB"

# Hashtable power determines the number of buckets: 2^power
#
# Sizing guide:
#   power 20 =  1M buckets
#   power 22 =  4M buckets
#   power 24 = 16M buckets
#   power 26 = 64M buckets
#
hashtable_power = 26
"#
    );
    print!("{}", config);
}

/// Format a size in bytes as a human-readable string
pub fn format_size(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;
    const TB: usize = 1024 * GB;

    if bytes >= TB {
        format!("{}TB", bytes / TB)
    } else if bytes >= GB {
        format!("{}GB", bytes / GB)
    } else if bytes >= MB {
        format!("{}MB", bytes / MB)
    } else if bytes >= KB {
        format!("{}KB", bytes / KB)
    } else {
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("64MB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_size("4GB").unwrap(), 4 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1 GB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_case_insensitive() {
        assert_eq!(parse_size("1mb").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1Mb").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1MB").unwrap(), 1024 * 1024);
    }

    #[test]
    fn test_default_config() {
        let config = Config::default_config();
        assert_eq!(config.server.listen, "0.0.0.0:6379");
        assert_eq!(config.cache.heap_size, 4 * 1024 * 1024 * 1024);
        assert_eq!(config.cache.segment_size, 1024 * 1024);
        assert_eq!(config.cache.hashtable_power, 26);
    }

    #[test]
    fn test_parse_minimal_toml() {
        let toml = r#"
            [cache]
            heap_size = "1GB"
        "#;

        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.cache.heap_size, 1024 * 1024 * 1024);
        assert_eq!(config.cache.segment_size, 1024 * 1024);
        assert_eq!(config.server.listen, "0.0.0.0:6379");
    }

    #[test]
    fn test_parse_full_toml() {
        let toml = r#"
            [server]
            listen = "127.0.0.1:6380"
            threads = 4
            cpu_affinity = "0-3"
            backlog = 2048
            recv_buffer_size = "128KB"
            send_buffer_size = "128KB"
            read_buffer_size = "32KB"

            [cache]
            heap_size = "4GB"
            segment_size = "1MB"
            hashtable_power = 24
        "#;

        let config: Config = toml::from_str(toml).unwrap();

        assert_eq!(config.server.listen, "127.0.0.1:6380");
        assert_eq!(config.server.threads, Some(4));
        assert_eq!(config.server.cpu_affinity, Some("0-3".to_string()));
        assert_eq!(config.cpu_affinity(), Some(vec![0, 1, 2, 3]));
        assert_eq!(config.server.backlog, 2048);
        assert_eq!(config.server.recv_buffer_size, 128 * 1024);
        assert_eq!(config.server.send_buffer_size, 128 * 1024);
        assert_eq!(config.server.read_buffer_size, 32 * 1024);

        assert_eq!(config.cache.heap_size, 4 * 1024 * 1024 * 1024);
        assert_eq!(config.cache.segment_size, 1024 * 1024);
        assert_eq!(config.cache.hashtable_power, 24);
    }

    #[test]
    fn test_parse_cpu_list() {
        assert_eq!(parse_cpu_list("0-7").unwrap(), vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(parse_cpu_list("2-7").unwrap(), vec![2, 3, 4, 5, 6, 7]);
        assert_eq!(parse_cpu_list("1,3,8").unwrap(), vec![1, 3, 8]);
        assert_eq!(parse_cpu_list("0-3,5,7-9").unwrap(), vec![0, 1, 2, 3, 5, 7, 8, 9]);
        assert_eq!(parse_cpu_list("0,2-4,6,8-10").unwrap(), vec![0, 2, 3, 4, 6, 8, 9, 10]);
        // Duplicates removed
        assert_eq!(parse_cpu_list("1,3,1,3").unwrap(), vec![1, 3]);
        assert_eq!(parse_cpu_list("0-3,2-5").unwrap(), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_parse_cpu_list_stride() {
        // Stride notation: 0-15:2 means every 2nd CPU from 0 to 15
        assert_eq!(parse_cpu_list("0-15:2").unwrap(), vec![0, 2, 4, 6, 8, 10, 12, 14]);
        assert_eq!(parse_cpu_list("1-15:2").unwrap(), vec![1, 3, 5, 7, 9, 11, 13, 15]);
        assert_eq!(parse_cpu_list("0-7:3").unwrap(), vec![0, 3, 6]);
        // Combined with regular notation
        assert_eq!(parse_cpu_list("0-7:2,1-7:2").unwrap(), vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_parse_cpu_list_errors() {
        assert!(parse_cpu_list("").is_err());
        assert!(parse_cpu_list("abc").is_err());
        assert!(parse_cpu_list("5-1").is_err()); // start > end
        assert!(parse_cpu_list("0-7:0").is_err()); // stride of 0
        assert!(parse_cpu_list("5:2").is_err()); // stride without range
    }

    #[test]
    fn test_threads_from_cpu_affinity() {
        let toml = r#"
            [server]
            cpu_affinity = "0-3,6-7"
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        // Should derive 6 threads from cpu_affinity (0,1,2,3,6,7)
        assert_eq!(config.threads(), 6);
        assert_eq!(config.cpu_affinity(), Some(vec![0, 1, 2, 3, 6, 7]));
    }
}
