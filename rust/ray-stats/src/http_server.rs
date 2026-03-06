// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Prometheus-compatible HTTP metrics endpoint.
//!
//! Exposes `GET /metrics` returning Prometheus text exposition format.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::exporter::MetricsExporter;

/// Default Prometheus metrics HTTP port.
/// Matches `DEFAULT_METRICS_PORT` in `ray-common/constants.rs`.
const DEFAULT_METRICS_PORT: u16 = 8080;

/// Default bind address for servers.
/// Matches `DEFAULT_SERVER_BIND_ADDRESS` in `ray-common/constants.rs`.
const DEFAULT_SERVER_BIND_ADDRESS: &str = "0.0.0.0";

/// Configuration for the metrics HTTP server.
#[derive(Debug, Clone)]
pub struct MetricsHttpConfig {
    /// Port to listen on. Use 0 for an OS-assigned port.
    pub port: u16,
}

impl Default for MetricsHttpConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_METRICS_PORT,
        }
    }
}

/// Start a minimal HTTP server that serves Prometheus metrics.
///
/// Returns a `JoinHandle` for the server task and the actual bound port.
pub async fn start_metrics_server(
    config: MetricsHttpConfig,
    exporter: Arc<MetricsExporter>,
) -> Result<(tokio::task::JoinHandle<()>, u16), std::io::Error> {
    let listener =
        TcpListener::bind(format!("{}:{}", DEFAULT_SERVER_BIND_ADDRESS, config.port)).await?;
    let port = listener.local_addr()?.port();
    tracing::info!(port, "Prometheus metrics server started");

    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    let exporter = Arc::clone(&exporter);
                    tokio::spawn(async move {
                        const HTTP_READ_BUFFER_SIZE: usize = 4096;
                        let mut buf = vec![0u8; HTTP_READ_BUFFER_SIZE];
                        let n = match stream.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };
                        let request = String::from_utf8_lossy(&buf[..n]);

                        let response = if request.starts_with("GET /metrics") {
                            let body = exporter.to_prometheus_text();
                            format!(
                                "HTTP/1.1 200 OK\r\n\
                                 Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                                 Content-Length: {}\r\n\
                                 \r\n\
                                 {}",
                                body.len(),
                                body
                            )
                        } else {
                            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_string()
                        };

                        let _ = stream.write_all(response.as_bytes()).await;
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to accept metrics connection");
                }
            }
        }
    });

    Ok((handle, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporter::ExporterConfig;
    use crate::Counter;

    #[tokio::test]
    async fn test_metrics_endpoint_returns_prometheus_text() {
        let exporter = Arc::new(MetricsExporter::new(ExporterConfig::default()));
        let counter = Counter::new("test_http_counter", "test");
        counter.increment(&[], 42);
        exporter.register_counter(counter);

        let config = MetricsHttpConfig { port: 0 };
        let (_handle, port) = start_metrics_server(config, exporter).await.unwrap();

        // Connect and send a GET /metrics request.
        let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut response = vec![0u8; 4096];
        let n = stream.read(&mut response).await.unwrap();
        let response_str = String::from_utf8_lossy(&response[..n]);

        assert!(response_str.contains("HTTP/1.1 200 OK"));
        assert!(response_str.contains("text/plain"));
        assert!(response_str.contains("# TYPE test_http_counter counter"));
        assert!(response_str.contains("test_http_counter 42"));
    }

    #[tokio::test]
    async fn test_metrics_endpoint_404_for_unknown_path() {
        let exporter = Arc::new(MetricsExporter::new(ExporterConfig::default()));
        let config = MetricsHttpConfig { port: 0 };
        let (_handle, port) = start_metrics_server(config, exporter).await.unwrap();

        let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        stream
            .write_all(b"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let mut response = vec![0u8; 4096];
        let n = stream.read(&mut response).await.unwrap();
        let response_str = String::from_utf8_lossy(&response[..n]);

        assert!(response_str.contains("HTTP/1.1 404 Not Found"));
    }

    #[test]
    fn test_metrics_http_config_default_values() {
        let config = MetricsHttpConfig::default();
        assert_eq!(config.port, DEFAULT_METRICS_PORT);
        assert_eq!(config.port, 8080);
    }
}
