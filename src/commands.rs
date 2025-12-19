use crate::cache::Cache;
use crate::metrics::{DELETES, FLUSHES, GETS, HITS, MISSES, SETS, SET_ERRORS};
use bytes::Buf;
use std::io::Cursor;

pub enum Command<'a> {
    Get { key: &'a [u8] },
    Set { key: &'a [u8], value: &'a [u8], ttl: Option<u64> },
    Del { key: &'a [u8] },
    Ping,
    Config { subcommand: &'a [u8], key: Option<&'a [u8]> },
    FlushDb,
    FlushAll,
}

pub enum ParseError {
    Incomplete,
    Invalid(String),
}

impl<'a> Command<'a> {
    /// Parse a command directly from a byte buffer without intermediate allocations
    pub fn parse_from_buffer(buffer: &'a [u8]) -> Result<(Self, usize), ParseError> {
        let mut cursor = Cursor::new(buffer);

        // Read array header
        if cursor.remaining() < 1 {
            return Err(ParseError::Incomplete);
        }
        if cursor.get_u8() != b'*' {
            return Err(ParseError::Invalid("Expected array".to_string()));
        }

        // Read array length
        let count = Self::read_integer(&mut cursor)?;
        if count < 1 {
            return Err(ParseError::Invalid("Array must have at least 1 element".to_string()));
        }

        // Read command name
        let cmd_name = Self::read_bulk_string(&mut cursor)?;
        let cmd_str = std::str::from_utf8(cmd_name)
            .map_err(|_| ParseError::Invalid("Invalid UTF-8 in command".to_string()))?;

        // Case-insensitive comparison without allocation
        let command = match () {
            _ if cmd_str.eq_ignore_ascii_case("ping") => Command::Ping,
            _ if cmd_str.eq_ignore_ascii_case("get") => {
                if count != 2 {
                    return Err(ParseError::Invalid("GET requires exactly 1 argument".to_string()));
                }
                let key = Self::read_bulk_string(&mut cursor)?;
                Command::Get { key }
            }
            _ if cmd_str.eq_ignore_ascii_case("set") => {
                if count < 3 {
                    return Err(ParseError::Invalid("SET requires at least 2 arguments".to_string()));
                }
                let key = Self::read_bulk_string(&mut cursor)?;
                let value = Self::read_bulk_string(&mut cursor)?;

                let mut ttl = None;
                let mut remaining_args = count - 3;
                while remaining_args > 0 {
                    let option = Self::read_bulk_string(&mut cursor)?;
                    let option_str = std::str::from_utf8(option)
                        .map_err(|_| ParseError::Invalid("Invalid UTF-8 in option".to_string()))?;

                    if option_str.eq_ignore_ascii_case("ex") {
                        if remaining_args < 2 {
                            return Err(ParseError::Invalid("EX requires a value".to_string()));
                        }
                        let ttl_bytes = Self::read_bulk_string(&mut cursor)?;
                        let ttl_str = std::str::from_utf8(ttl_bytes)
                            .map_err(|_| ParseError::Invalid("Invalid UTF-8 in TTL".to_string()))?;
                        let ttl_secs = ttl_str.parse::<u64>()
                            .map_err(|_| ParseError::Invalid("Invalid TTL value".to_string()))?;
                        ttl = Some(ttl_secs);
                        remaining_args -= 2;
                    } else {
                        return Err(ParseError::Invalid("Unknown option".to_string()));
                    }
                }

                Command::Set { key, value, ttl }
            }
            _ if cmd_str.eq_ignore_ascii_case("del") => {
                if count != 2 {
                    return Err(ParseError::Invalid("DEL requires exactly 1 argument".to_string()));
                }
                let key = Self::read_bulk_string(&mut cursor)?;
                Command::Del { key }
            }
            _ if cmd_str.eq_ignore_ascii_case("config") => {
                if count < 2 {
                    return Err(ParseError::Invalid("CONFIG requires at least 1 argument".to_string()));
                }
                let subcommand = Self::read_bulk_string(&mut cursor)?;
                let key = if count >= 3 {
                    Some(Self::read_bulk_string(&mut cursor)?)
                } else {
                    None
                };
                Command::Config { subcommand, key }
            }
            _ if cmd_str.eq_ignore_ascii_case("flushdb") => Command::FlushDb,
            _ if cmd_str.eq_ignore_ascii_case("flushall") => Command::FlushAll,
            _ => return Err(ParseError::Invalid("Unknown command".to_string())),
        };

        let consumed = cursor.position() as usize;
        Ok((command, consumed))
    }

    fn read_integer(cursor: &mut Cursor<&[u8]>) -> Result<usize, ParseError> {
        let line = Self::read_line(cursor)?;

        let mut result = 0usize;
        for &byte in line {
            if !byte.is_ascii_digit() {
                return Err(ParseError::Invalid("Invalid integer".to_string()));
            }
            result = result * 10 + (byte - b'0') as usize;
        }
        Ok(result)
    }

    fn read_bulk_string<'b>(cursor: &mut Cursor<&'b [u8]>) -> Result<&'b [u8], ParseError> {
        if cursor.remaining() < 1 {
            return Err(ParseError::Incomplete);
        }

        if cursor.get_u8() != b'$' {
            return Err(ParseError::Invalid("Expected bulk string".to_string()));
        }

        let len = Self::read_integer(cursor)?;

        if cursor.remaining() < len + 2 {
            return Err(ParseError::Incomplete);
        }

        let start = cursor.position() as usize;
        let data = &cursor.get_ref()[start..start + len];
        cursor.set_position((start + len) as u64);

        // Verify CRLF
        if cursor.remaining() < 2 {
            return Err(ParseError::Incomplete);
        }
        if cursor.get_u8() != b'\r' || cursor.get_u8() != b'\n' {
            return Err(ParseError::Invalid("Expected CRLF after bulk string".to_string()));
        }

        Ok(data)
    }

    fn read_line<'b>(cursor: &mut Cursor<&'b [u8]>) -> Result<&'b [u8], ParseError> {
        let start = cursor.position() as usize;
        let slice = &cursor.get_ref()[start..];

        if let Some(pos) = memchr::memchr(b'\r', slice)
            && pos + 1 < slice.len()
            && slice[pos + 1] == b'\n'
        {
            let end = start + pos;
            let line = &cursor.get_ref()[start..end];
            cursor.set_position((end + 2) as u64);
            return Ok(line);
        }

        Err(ParseError::Incomplete)
    }

    /// Execute the command against the given cache
    pub async fn execute<C: Cache>(&self, cache: &C, write_buffer: &mut bytes::BytesMut) {
        match self {
            Command::Ping => {
                write_buffer.extend_from_slice(b"+PONG\r\n");
            }
            Command::Get { key } => {
                GETS.increment();
                match cache.get(key).await {
                    Some(guard) => {
                        HITS.increment();
                        let value = guard.as_ref();
                        let mut len_buf = itoa::Buffer::new();
                        let len_str = len_buf.format(value.len());
                        write_buffer.extend_from_slice(b"$");
                        write_buffer.extend_from_slice(len_str.as_bytes());
                        write_buffer.extend_from_slice(b"\r\n");
                        write_buffer.extend_from_slice(value);
                        write_buffer.extend_from_slice(b"\r\n");
                    }
                    None => {
                        MISSES.increment();
                        write_buffer.extend_from_slice(b"$-1\r\n");
                    }
                }
            }
            Command::Set { key, value, ttl } => {
                SETS.increment();
                let ttl_duration = ttl.map(std::time::Duration::from_secs);

                // Retry SET operations - cache may need to evict under pressure
                for attempt in 0..10 {
                    match cache.set(key, value, ttl_duration).await {
                        Ok(()) => {
                            write_buffer.extend_from_slice(b"+OK\r\n");
                            return;
                        }
                        Err(_) => {
                            if attempt < 9 {
                                // Exponential backoff
                                let delay = std::time::Duration::from_micros(100 << attempt);
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                }
                SET_ERRORS.increment();
                write_buffer.extend_from_slice(b"-ERR cache full\r\n");
            }
            Command::Del { key } => {
                DELETES.increment();
                let deleted = cache.delete(key).await;
                write_buffer.extend_from_slice(b":");
                write_buffer.extend_from_slice(if deleted { b"1" } else { b"0" });
                write_buffer.extend_from_slice(b"\r\n");
            }
            Command::Config { subcommand, key: _ } => {
                if subcommand.eq_ignore_ascii_case(b"get") {
                    write_buffer.extend_from_slice(b"*0\r\n");
                } else if subcommand.eq_ignore_ascii_case(b"set")
                    || subcommand.eq_ignore_ascii_case(b"resetstat")
                {
                    write_buffer.extend_from_slice(b"+OK\r\n");
                } else {
                    write_buffer.extend_from_slice(b"-ERR Unknown CONFIG subcommand\r\n");
                }
            }
            Command::FlushDb | Command::FlushAll => {
                FLUSHES.increment();
                cache.flush();
                write_buffer.extend_from_slice(b"+OK\r\n");
            }
        }
    }
}
