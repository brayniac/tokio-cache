use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metriken::{metric, Counter, Format, Gauge};
use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use tokio::net::TcpListener;

// Connection metrics
#[metric(
    name = "connections_accepted",
    description = "Total number of connections accepted"
)]
pub static CONNECTIONS_ACCEPTED: Counter = Counter::new();

#[metric(
    name = "connections_active",
    description = "Number of currently active connections"
)]
pub static CONNECTIONS_ACTIVE: Gauge = Gauge::new();

// Operation counters
#[metric(name = "cache_gets", description = "Total GET operations")]
pub static GETS: Counter = Counter::new();

#[metric(name = "cache_sets", description = "Total SET operations")]
pub static SETS: Counter = Counter::new();

#[metric(name = "cache_deletes", description = "Total DELETE operations")]
pub static DELETES: Counter = Counter::new();

#[metric(name = "cache_flushes", description = "Total FLUSH operations")]
pub static FLUSHES: Counter = Counter::new();

// Cache effectiveness
#[metric(name = "cache_hits", description = "Total cache hits")]
pub static HITS: Counter = Counter::new();

#[metric(name = "cache_misses", description = "Total cache misses")]
pub static MISSES: Counter = Counter::new();

// Errors
#[metric(name = "cache_set_errors", description = "Total SET errors (cache full)")]
pub static SET_ERRORS: Counter = Counter::new();

#[metric(
    name = "protocol_errors",
    description = "Total protocol parse errors"
)]
pub static PROTOCOL_ERRORS: Counter = Counter::new();

/// Format all metrics in Prometheus exposition format
fn prometheus_output() -> Vec<u8> {
    let mut output = Vec::new();
    let metrics = metriken::metrics();

    for entry in &metrics {
        // Get the formatted name (Prometheus style)
        let name = entry.formatted(Format::Prometheus);

        let Some(any) = entry.as_any() else {
            continue;
        };

        // Try to get the value based on metric type
        if let Some(counter) = any.downcast_ref::<Counter>() {
            let value = counter.value();
            if let Some(desc) = entry.description() {
                writeln!(output, "# HELP {} {}", name, desc).ok();
                writeln!(output, "# TYPE {} counter", name).ok();
            }
            writeln!(output, "{} {}", name, value).ok();
        } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
            let value = gauge.value();
            if let Some(desc) = entry.description() {
                writeln!(output, "# HELP {} {}", name, desc).ok();
                writeln!(output, "# TYPE {} gauge", name).ok();
            }
            writeln!(output, "{} {}", name, value).ok();
        }
    }

    output
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match req.uri().path() {
        "/metrics" => {
            let body = prometheus_output();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; version=0.0.4")
                .body(Full::new(Bytes::from(body)))
                .unwrap())
        }
        "/health" => Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from_static(b"OK")))
            .unwrap()),
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"Not Found")))
            .unwrap()),
    }
}

/// Spawn the metrics HTTP server on the given address.
/// This runs in the background and serves /metrics and /health endpoints.
pub async fn serve(addr: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = addr.parse()?;
    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            if let Err(_e) = http1::Builder::new()
                .serve_connection(io, service_fn(handle_request))
                .await
            {
                // Connection errors are expected (client disconnects, etc.)
            }
        });
    }
}
