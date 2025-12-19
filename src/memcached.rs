use crate::cache::Cache;
use crate::metrics::{DELETES, FLUSHES, GETS, HITS, MISSES, SETS, SET_ERRORS};
use bytes::BytesMut;

/// Memcached ASCII protocol command
pub enum Command<'a> {
    Get { key: &'a [u8] },
    Set { key: &'a [u8], flags: u32, exptime: u32, data: &'a [u8] },
    Delete { key: &'a [u8] },
    FlushAll,
    Version,
    Quit,
}

/// Parse error for memcached protocol
pub enum ParseError {
    /// Need more data to complete parsing
    Incomplete,
    /// Invalid protocol
    Invalid(&'static str),
}

impl<'a> Command<'a> {
    /// Parse a memcached command from a buffer.
    /// Returns the command and number of bytes consumed, or an error.
    pub fn parse(buffer: &'a [u8]) -> Result<(Self, usize), ParseError> {
        // Find the end of the command line
        let Some(line_end) = find_crlf(buffer) else {
            return Err(ParseError::Incomplete);
        };

        let line = &buffer[..line_end];
        let mut parts = line.split(|&b| b == b' ');

        let cmd = parts.next().ok_or(ParseError::Invalid("empty command"))?;

        match cmd {
            b"get" | b"GET" => {
                let key = parts.next().ok_or(ParseError::Invalid("get requires key"))?;
                if key.is_empty() {
                    return Err(ParseError::Invalid("empty key"));
                }
                Ok((Command::Get { key }, line_end + 2))
            }
            b"set" | b"SET" => {
                let key = parts.next().ok_or(ParseError::Invalid("set requires key"))?;
                let flags_str = parts.next().ok_or(ParseError::Invalid("set requires flags"))?;
                let exptime_str = parts.next().ok_or(ParseError::Invalid("set requires exptime"))?;
                let bytes_str = parts.next().ok_or(ParseError::Invalid("set requires bytes"))?;

                let flags: u32 = parse_u32(flags_str)
                    .ok_or(ParseError::Invalid("invalid flags"))?;
                let exptime: u32 = parse_u32(exptime_str)
                    .ok_or(ParseError::Invalid("invalid exptime"))?;
                let data_len: usize = parse_usize(bytes_str)
                    .ok_or(ParseError::Invalid("invalid bytes"))?;

                // Data block follows the command line: <data>\r\n
                let data_start = line_end + 2;
                let data_end = data_start + data_len;
                let total_len = data_end + 2; // include trailing \r\n

                if buffer.len() < total_len {
                    return Err(ParseError::Incomplete);
                }

                // Verify trailing \r\n
                if buffer[data_end] != b'\r' || buffer[data_end + 1] != b'\n' {
                    return Err(ParseError::Invalid("missing data terminator"));
                }

                let data = &buffer[data_start..data_end];
                Ok((Command::Set { key, flags, exptime, data }, total_len))
            }
            b"delete" | b"DELETE" => {
                let key = parts.next().ok_or(ParseError::Invalid("delete requires key"))?;
                if key.is_empty() {
                    return Err(ParseError::Invalid("empty key"));
                }
                Ok((Command::Delete { key }, line_end + 2))
            }
            b"flush_all" | b"FLUSH_ALL" => {
                Ok((Command::FlushAll, line_end + 2))
            }
            b"version" | b"VERSION" => {
                Ok((Command::Version, line_end + 2))
            }
            b"quit" | b"QUIT" => {
                Ok((Command::Quit, line_end + 2))
            }
            _ => Err(ParseError::Invalid("unknown command")),
        }
    }

    /// Execute the command against the cache, writing the response to the buffer.
    /// Returns true if the connection should be closed (quit command).
    pub async fn execute<C: Cache>(&self, cache: &C, write_buf: &mut BytesMut) -> bool {
        match self {
            Command::Get { key } => {
                GETS.increment();
                match cache.get(key).await {
                    Some(guard) => {
                        HITS.increment();
                        let value = guard.as_ref();
                        // VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n
                        write_buf.extend_from_slice(b"VALUE ");
                        write_buf.extend_from_slice(key);
                        write_buf.extend_from_slice(b" 0 ");
                        let mut len_buf = itoa::Buffer::new();
                        write_buf.extend_from_slice(len_buf.format(value.len()).as_bytes());
                        write_buf.extend_from_slice(b"\r\n");
                        write_buf.extend_from_slice(value);
                        write_buf.extend_from_slice(b"\r\nEND\r\n");
                    }
                    None => {
                        MISSES.increment();
                        write_buf.extend_from_slice(b"END\r\n");
                    }
                }
                false
            }
            Command::Set { key, flags: _, exptime, data } => {
                SETS.increment();
                // Convert exptime to duration
                // exptime=0 means no expiration, use 1 year as practical maximum
                let ttl = if *exptime == 0 {
                    None
                } else {
                    Some(std::time::Duration::from_secs(*exptime as u64))
                };

                // Retry SET operations under pressure
                for attempt in 0..10 {
                    match cache.set(key, data, ttl).await {
                        Ok(()) => {
                            write_buf.extend_from_slice(b"STORED\r\n");
                            return false;
                        }
                        Err(_) => {
                            if attempt < 9 {
                                let delay = std::time::Duration::from_micros(100 << attempt);
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                }
                SET_ERRORS.increment();
                write_buf.extend_from_slice(b"SERVER_ERROR out of memory\r\n");
                false
            }
            Command::Delete { key } => {
                DELETES.increment();
                if cache.delete(key).await {
                    write_buf.extend_from_slice(b"DELETED\r\n");
                } else {
                    write_buf.extend_from_slice(b"NOT_FOUND\r\n");
                }
                false
            }
            Command::FlushAll => {
                FLUSHES.increment();
                cache.flush();
                write_buf.extend_from_slice(b"OK\r\n");
                false
            }
            Command::Version => {
                write_buf.extend_from_slice(b"VERSION tokio-cache\r\n");
                false
            }
            Command::Quit => {
                // Signal to close connection
                true
            }
        }
    }
}

/// Find \r\n in buffer, return position of \r
fn find_crlf(buffer: &[u8]) -> Option<usize> {
    memchr::memchr(b'\r', buffer).and_then(|pos| {
        if pos + 1 < buffer.len() && buffer[pos + 1] == b'\n' {
            Some(pos)
        } else {
            None
        }
    })
}

fn parse_u32(bytes: &[u8]) -> Option<u32> {
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse().ok()
}

fn parse_usize(bytes: &[u8]) -> Option<usize> {
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse().ok()
}
