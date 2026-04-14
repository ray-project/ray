//! GCS server binary entry point.
//!
//! Maps C++ `gcs_server_main.cc`.
//! Parses gflags-style CLI arguments that Ray Python sends.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::info;

use gcs_server::{GcsServer, GcsServerConfig};

/// Parse gflags-style arguments (--key=value) into a map.
fn parse_gflags(args: impl Iterator<Item = String>) -> HashMap<String, String> {
    let mut flags = HashMap::new();
    for arg in args {
        if let Some(kv) = arg.strip_prefix("--") {
            if let Some((key, value)) = kv.split_once('=') {
                flags.insert(key.to_string(), value.to_string());
            }
        }
    }
    flags
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let flags = parse_gflags(std::env::args().skip(1));

    let port: u16 = flags
        .get("gcs_server_port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379);

    let session_dir = flags.get("session-dir").map(PathBuf::from);
    let node_id = flags.get("node-id").cloned();

    // Parse config_list: Python sends base64-encoded JSON. The C++ GCS decodes it
    // before storing, so the raylet receives decoded JSON via GetInternalConfig.
    let config_list = flags
        .get("config_list")
        .map(|b64| {
            use base64::Engine;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .unwrap_or_else(|_| b64.as_bytes().to_vec());
            String::from_utf8(decoded).unwrap_or_default()
        })
        .unwrap_or_default();

    // Create stdout/stderr log files that Ray Python expects.
    if let Some(path) = flags.get("stdout_filepath") {
        let _ = std::fs::File::create(path);
    }
    if let Some(path) = flags.get("stderr_filepath") {
        let _ = std::fs::File::create(path);
    }

    // Parse Redis configuration.
    let redis_address = flags.get("redis_address").cloned()
        .or_else(|| std::env::var("RAY_REDIS_ADDRESS").ok());
    let redis_password = flags.get("redis_password").cloned()
        .or_else(|| std::env::var("RAY_REDIS_PASSWORD").ok());
    let external_storage_namespace = flags
        .get("external_storage_namespace")
        .cloned()
        .unwrap_or_default();

    // Build Redis URL if address is provided.
    let redis_url = redis_address.map(|addr| {
        if addr.starts_with("redis://") || addr.starts_with("rediss://") {
            addr
        } else if let Some(password) = &redis_password {
            format!("redis://:{}@{}", password, addr)
        } else {
            format!("redis://{}", addr)
        }
    });

    let config = GcsServerConfig {
        grpc_port: port,
        raylet_config_list: config_list,
        redis_address: redis_url.clone(),
        external_storage_namespace: external_storage_namespace.clone(),
        ..Default::default()
    };

    info!(port, redis = redis_url.is_some(), "Rust GCS server starting");

    // Create server with appropriate storage backend.
    let server = if let Some(ref url) = redis_url {
        let redis_client = gcs_store::RedisStoreClient::connect(url, &external_storage_namespace)
            .await
            .expect("Failed to connect to Redis");
        let redis_client = std::sync::Arc::new(redis_client);
        GcsServer::new_with_redis(config, redis_client)
    } else {
        GcsServer::new(config)
    };

    // Bind to the port first so we know the actual port (handles port 0).
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    let actual_port = listener.local_addr()?.port();

    info!(actual_port, "Rust GCS server bound");

    // Write port file for Ray to discover us.
    if let (Some(dir), Some(nid)) = (&session_dir, &node_id) {
        let port_file = dir.join(format!("gcs_server_port_{}", nid));
        if let Err(e) = std::fs::write(&port_file, actual_port.to_string()) {
            tracing::warn!(?port_file, error = %e, "Failed to write GCS port file");
        } else {
            info!(?port_file, actual_port, "Wrote GCS port file");
        }
    }

    server.start_with_listener(listener).await?;

    Ok(())
}
