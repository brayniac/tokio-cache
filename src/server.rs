use crate::cache::Cache;
use crate::commands::{Command as RedisCommand, ParseError as RedisParseError};
use crate::config::Config;
use crate::memcached::{Command as MemcachedCommand, ParseError as MemcachedParseError};
use crate::metrics::{CONNECTIONS_ACCEPTED, CONNECTIONS_ACTIVE, PROTOCOL_ERRORS};
use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::Interest;
use tokio::net::{TcpListener, TcpStream};

/// Run the cache server with the given configuration and cache implementation.
///
/// This function is generic over the cache type to allow for different cache
/// backends while ensuring static dispatch (monomorphization).
pub async fn run<C: Cache>(config: Config, cache: C) -> Result<(), Box<dyn std::error::Error>> {
    let cache = Arc::new(cache);

    // Create Redis listener
    let redis_listener = create_listener(&config.server.listen, &config)?;

    // Create Memcached listener if enabled
    let memcached_listener = if !config.server.memcached_listen.is_empty() {
        Some(create_listener(&config.server.memcached_listen, &config)?)
    } else {
        None
    };

    // Spawn Memcached accept loop if enabled
    if let Some(listener) = memcached_listener {
        let cache = cache.clone();
        let read_buffer_size = config.server.read_buffer_size;
        let recv_buffer_size = config.server.recv_buffer_size;
        let send_buffer_size = config.server.send_buffer_size;

        tokio::spawn(async move {
            loop {
                if let Ok((socket, _addr)) = listener.accept().await {
                    let cache = cache.clone();

                    CONNECTIONS_ACCEPTED.increment();
                    CONNECTIONS_ACTIVE.increment();

                    tokio::spawn(handle_memcached_client(
                        socket,
                        cache,
                        read_buffer_size,
                        recv_buffer_size,
                        send_buffer_size,
                    ));
                }
            }
        });
    }

    // Redis accept loop (main loop)
    loop {
        let (socket, _addr) = redis_listener.accept().await?;
        let cache = cache.clone();

        CONNECTIONS_ACCEPTED.increment();
        CONNECTIONS_ACTIVE.increment();

        tokio::spawn(handle_redis_client(
            socket,
            cache,
            config.server.read_buffer_size,
            config.server.recv_buffer_size,
            config.server.send_buffer_size,
        ));
    }
}

fn create_listener(addr: &str, config: &Config) -> Result<TcpListener, Box<dyn std::error::Error>> {
    use std::net::{SocketAddr, TcpListener as StdListener};

    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;

    socket.bind(&addr.parse::<SocketAddr>()?.into())?;
    socket.listen(config.server.backlog as i32)?;

    Ok(TcpListener::from_std(StdListener::from(socket))?)
}

// =============================================================================
// Redis Protocol Handler
// =============================================================================

async fn handle_redis_client<C: Cache>(
    socket: TcpStream,
    cache: Arc<C>,
    buffer_size: usize,
    recv_buffer_size: usize,
    send_buffer_size: usize,
) {
    let _ = handle_redis_client_inner(socket, cache, buffer_size, recv_buffer_size, send_buffer_size).await;
    CONNECTIONS_ACTIVE.decrement();
}

async fn handle_redis_client_inner<C: Cache>(
    socket: TcpStream,
    cache: Arc<C>,
    buffer_size: usize,
    recv_buffer_size: usize,
    send_buffer_size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    socket.set_nodelay(true)?;

    let sock_ref = socket2::SockRef::from(&socket);
    let _ = sock_ref.set_recv_buffer_size(recv_buffer_size);
    let _ = sock_ref.set_send_buffer_size(send_buffer_size);

    let mut buffer = BytesMut::with_capacity(buffer_size);
    let mut write_buf = BytesMut::with_capacity(4096);

    loop {
        // Try to read without waiting first
        match socket.try_read_buf(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(_n) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available right now - wait for more
                socket.ready(Interest::READABLE).await?;
                continue;
            }
            Err(e) => return Err(e.into()),
        }

        // Process all complete commands, batching responses
        write_buf.clear();
        loop {
            if buffer.is_empty() {
                break;
            }

            match RedisCommand::parse_from_buffer(&buffer) {
                Ok((cmd, consumed)) => {
                    cmd.execute(&*cache, &mut write_buf).await;
                    buffer.advance(consumed);
                }
                Err(RedisParseError::Incomplete) => break,
                Err(RedisParseError::Invalid(msg)) => {
                    PROTOCOL_ERRORS.increment();
                    if msg.contains("Expected array") {
                        write_buf.extend_from_slice(b"-ERR Protocol error: expected Redis RESP protocol\r\n");
                    } else {
                        write_buf.extend_from_slice(b"-ERR ");
                        write_buf.extend_from_slice(msg.as_bytes());
                        write_buf.extend_from_slice(b"\r\n");
                    }
                    buffer.clear();
                    break;
                }
            }
        }

        // Write all responses in one syscall
        if !write_buf.is_empty() {
            write_all(&socket, &write_buf).await?;
        }
    }
}

// =============================================================================
// Memcached Protocol Handler
// =============================================================================

async fn handle_memcached_client<C: Cache>(
    socket: TcpStream,
    cache: Arc<C>,
    buffer_size: usize,
    recv_buffer_size: usize,
    send_buffer_size: usize,
) {
    let _ = handle_memcached_client_inner(socket, cache, buffer_size, recv_buffer_size, send_buffer_size).await;
    CONNECTIONS_ACTIVE.decrement();
}

async fn handle_memcached_client_inner<C: Cache>(
    socket: TcpStream,
    cache: Arc<C>,
    buffer_size: usize,
    recv_buffer_size: usize,
    send_buffer_size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    socket.set_nodelay(true)?;

    let sock_ref = socket2::SockRef::from(&socket);
    let _ = sock_ref.set_recv_buffer_size(recv_buffer_size);
    let _ = sock_ref.set_send_buffer_size(send_buffer_size);

    let mut buffer = BytesMut::with_capacity(buffer_size);
    let mut write_buf = BytesMut::with_capacity(4096);

    loop {
        // Try to read without waiting first
        match socket.try_read_buf(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(_n) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available right now - wait for more
                socket.ready(Interest::READABLE).await?;
                continue;
            }
            Err(e) => return Err(e.into()),
        }

        // Process all complete commands, batching responses
        write_buf.clear();
        let mut should_quit = false;

        loop {
            if buffer.is_empty() {
                break;
            }

            match MemcachedCommand::parse(&buffer) {
                Ok((cmd, consumed)) => {
                    should_quit = cmd.execute(&*cache, &mut write_buf).await;
                    buffer.advance(consumed);
                    if should_quit {
                        break;
                    }
                }
                Err(MemcachedParseError::Incomplete) => break,
                Err(MemcachedParseError::Invalid(msg)) => {
                    PROTOCOL_ERRORS.increment();
                    write_buf.extend_from_slice(b"ERROR ");
                    write_buf.extend_from_slice(msg.as_bytes());
                    write_buf.extend_from_slice(b"\r\n");
                    buffer.clear();
                    break;
                }
            }
        }

        // Write all responses in one syscall
        if !write_buf.is_empty() {
            write_all(&socket, &write_buf).await?;
        }

        if should_quit {
            return Ok(());
        }
    }
}

// =============================================================================
// Shared Utilities
// =============================================================================

async fn write_all(socket: &TcpStream, data: &[u8]) -> Result<(), std::io::Error> {
    let mut written = 0;
    while written < data.len() {
        match socket.try_write(&data[written..]) {
            Ok(n) => written += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                socket.ready(Interest::WRITABLE).await?;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
