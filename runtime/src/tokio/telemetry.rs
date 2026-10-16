//! Utilities for collecting and reporting telemetry data.

use super::{
    Context,
    tracing::{Config, export},
};
use crate::{Metrics as _, Spawner, Supervisor as _};
use axum::{
    Extension, Router,
    body::Body,
    http::{Response, StatusCode, header},
    routing::get,
    serve,
};
use cfg_if::cfg_if;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing::Level;
use tracing_subscriber::{Layer, Registry, filter::filter_fn, layer::SubscriberExt};

/// Log configuration.
pub struct Logs {
    /// The level of logging to use.
    pub level: Level,

    /// Whether to log in JSON format.
    ///
    /// This is useful for structured logging in server-based environments.
    /// If you are running things locally, it is recommended to use
    /// `json = false` to get a human-readable format.
    pub json: bool,
}

/// Initialize telemetry with the given configuration.
///
/// If `metrics` is provided, starts serving metrics at the given address at `/metrics`.
/// If `traces` is provided, enables OpenTelemetry trace export. When `None`, span
/// callsites in the runtime-provided subscriber are disabled and only log events
/// are emitted (per [`Logs::level`]).
pub fn init(context: Context, logs: Logs, metrics: Option<SocketAddr>, traces: Option<Config>) {
    // Create fmt layer for logging
    let log_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_thread_ids(true)
        .with_file(true);

    // Set the format to JSON (if specified)
    let log_layer = if logs.json {
        log_layer.json().boxed()
    } else {
        log_layer.compact().boxed()
    };
    let log_layer = match &traces {
        None => log_layer
            .with_filter(filter_fn(move |metadata| {
                metadata.is_event() && *metadata.level() <= logs.level
            }))
            .boxed(),
        Some(_) => log_layer
            .with_filter(tracing_subscriber::EnvFilter::new(logs.level.to_string()))
            .boxed(),
    };

    // Create OpenTelemetry layer for tracing
    let trace_layer = traces.map(|cfg| {
        let tracer = export(cfg).expect("Failed to initialize tracer");
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(tracing_subscriber::EnvFilter::new(logs.level.to_string()))
    });

    // Create tracing registry.
    cfg_if! {
        if #[cfg(feature = "tokio-console")] {
            let console_layer = console_subscriber::spawn();
            let registry = Registry::default()
                .with(log_layer)
                .with(trace_layer)
                .with(console_layer);
        } else {
            let registry = Registry::default().with(log_layer).with(trace_layer);
        }
    }

    // Set the global subscriber
    tracing::subscriber::set_global_default(registry).expect("Failed to set subscriber");

    // Expose metrics over HTTP
    if let Some(cfg) = metrics {
        context.child("metrics").spawn(move |context| async move {
            // Create a tokio listener for the metrics server.
            //
            // We explicitly avoid using a runtime `Listener` because
            // it will track bandwidth used for metrics and apply a policy
            // for read/write timeouts fit for a p2p network.
            let listener = TcpListener::bind(cfg)
                .await
                .expect("Failed to bind metrics server");

            // Create a router for the metrics server
            let shared = Arc::new(context);
            let app = Router::new()
                .route(
                    "/metrics",
                    get(|Extension(ctx): Extension<Arc<Context>>| async move {
                        Response::builder()
                            .status(StatusCode::OK)
                            .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
                            .body(Body::from(ctx.encode()))
                            .expect("Failed to create response")
                    }),
                )
                .layer(Extension(shared));

            // Serve the metrics over HTTP.
            //
            // `serve` will spawn its own tasks using `tokio::spawn` (and there is no way to specify
            // it to do otherwise). These tasks will not be tracked like metrics spawned using `Spawner`.
            serve(listener, app.into_make_service())
                .await
                .expect("Could not serve metrics");
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Clock as _, Error, Network as _, Runner as _, Sink as _, Stream,
        telemetry::metrics::raw::Counter, tokio::Runner,
    };
    use bytes::Bytes;
    use std::{collections::HashMap, str::FromStr, time::Duration};
    use tracing::error;

    #[test]
    fn test_telemetry() {
        let executor = Runner::default();
        executor.start(|context| async move {
            // Define the server address
            let address = SocketAddr::from_str("127.0.0.1:8000").unwrap();

            // Configure telemetry
            init(
                context.child("metrics"),
                Logs {
                    level: Level::INFO,
                    json: false,
                },
                Some(address),
                None,
            );

            // Register a test metric
            let counter: Counter<u64> = Counter::default();
            let _registered = context.register("test_counter", "Test counter", counter.clone());
            counter.inc();

            // Helper functions to parse HTTP response
            async fn read_line<St: Stream>(stream: &mut St) -> Result<String, Error> {
                let mut line = Vec::new();
                loop {
                    let received = stream.recv(1).await?;
                    let byte = received.coalesce().as_ref()[0];
                    if byte == b'\n' {
                        if line.last() == Some(&b'\r') {
                            line.pop(); // Remove trailing \r
                        }
                        break;
                    }
                    line.push(byte);
                }
                String::from_utf8(line).map_err(|_| Error::ReadFailed)
            }

            async fn read_headers<St: Stream>(
                stream: &mut St,
            ) -> Result<HashMap<String, String>, Error> {
                let mut headers = HashMap::new();
                loop {
                    let line = read_line(stream).await?;
                    if line.is_empty() {
                        break;
                    }
                    let parts: Vec<&str> = line.splitn(2, ": ").collect();
                    if parts.len() == 2 {
                        headers.insert(parts[0].to_string(), parts[1].to_string());
                    }
                }
                Ok(headers)
            }

            async fn read_body<St: Stream>(
                stream: &mut St,
                content_length: usize,
            ) -> Result<String, Error> {
                let received = stream.recv(content_length).await?;
                String::from_utf8(received.coalesce().into()).map_err(|_| Error::ReadFailed)
            }

            // Simulate a client connecting to the server
            let client_handle = context.child("client").spawn(move |context| async move {
                let (mut sink, mut stream) = loop {
                    match context.dial(address).await {
                        Ok((sink, stream)) => break (sink, stream),
                        Err(e) => {
                            // The client may be polled before the server is ready, that's alright!
                            error!(err =?e, "failed to connect");
                            context.sleep(Duration::from_millis(10)).await;
                        }
                    }
                };

                // Send a GET request to the server
                let request = format!(
                    "GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
                );
                sink.send(Bytes::from(request)).await.unwrap();

                // Read and verify the HTTP status line
                let status_line = read_line(&mut stream).await.unwrap();
                assert_eq!(status_line, "HTTP/1.1 200 OK");

                // Read and parse headers
                let headers = read_headers(&mut stream).await.unwrap();
                println!("Headers: {headers:?}");
                let content_length = headers
                    .get("content-length")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();

                // Read and verify the body
                let body = read_body(&mut stream, content_length).await.unwrap();
                assert!(body.contains("test_counter_total 1"));
            });

            // Wait for the client task to complete
            client_handle.await.unwrap();
        });
    }
}
