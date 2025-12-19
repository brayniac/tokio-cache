use crate::cache::Cache;
use crate::commands::{Command, ParseError};
use crate::config::Config;
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
    let addr = &config.server.listen;
    let cache = Arc::new(cache);

    // Create listener with optimized settings
    let listener = create_listener(addr, &config)?;

    // Accept loop
    loop {
        let (socket, _addr) = listener.accept().await?;
        let cache = cache.clone();

        CONNECTIONS_ACCEPTED.increment();
        CONNECTIONS_ACTIVE.increment();

        tokio::spawn(handle_client(
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

async fn handle_client<C: Cache>(
    socket: TcpStream,
    cache: Arc<C>,
    buffer_size: usize,
    recv_buffer_size: usize,
    send_buffer_size: usize,
) {
    let _ = handle_client_inner(socket, cache, buffer_size, recv_buffer_size, send_buffer_size).await;
    CONNECTIONS_ACTIVE.decrement();
}

async fn handle_client_inner<C: Cache>(
    socket: TcpStream,
    cache: Arc<C>,
    buffer_size: usize,
    recv_buffer_size: usize,
    send_buffer_size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    socket.set_nodelay(true)?;

    // Configure socket buffers
    let sock_ref = socket2::SockRef::from(&socket);
    let _ = sock_ref.set_recv_buffer_size(recv_buffer_size);
    let _ = sock_ref.set_send_buffer_size(send_buffer_size);

    let mut buffer = BytesMut::with_capacity(buffer_size);
    let mut write_buf = BytesMut::with_capacity(256);

    loop {
        // Wait for socket to be readable
        socket.ready(Interest::READABLE).await?;

        // Try to read data without blocking
        match socket.try_read_buf(&mut buffer) {
            Ok(0) => {
                // Connection closed
                return Ok(());
            }
            Ok(_n) => {
                // Process all complete commands in buffer
                loop {
                    if buffer.is_empty() {
                        break;
                    }

                    match Command::parse_from_buffer(&buffer) {
                        Ok((cmd, consumed)) => {
                            write_buf.clear();
                            cmd.execute(&*cache, &mut write_buf).await;
                            buffer.advance(consumed);

                            // Write response
                            if !write_buf.is_empty() {
                                // Try non-blocking write first
                                let mut written = 0;
                                while written < write_buf.len() {
                                    match socket.try_write(&write_buf[written..]) {
                                        Ok(n) => written += n,
                                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                            // Wait for socket to be writable
                                            socket.ready(Interest::WRITABLE).await?;
                                        }
                                        Err(e) => return Err(e.into()),
                                    }
                                }
                            }
                        }
                        Err(ParseError::Incomplete) => {
                            // Need more data
                            break;
                        }
                        Err(ParseError::Invalid(msg)) => {
                            PROTOCOL_ERRORS.increment();
                            let error_msg = if msg.contains("Expected array") {
                                b"-ERR Protocol error: expected Redis RESP protocol\r\n".to_vec()
                            } else {
                                format!("-ERR {}\r\n", msg).into_bytes()
                            };
                            socket.writable().await?;
                            let _ = socket.try_write(&error_msg);
                            buffer.clear();
                            break;
                        }
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available, continue waiting
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}
