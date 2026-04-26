//! GCS server binary entry point.
//!
//! Maps C++ `gcs_server_main.cc`.
//! Parses gflags-style CLI arguments that Ray Python sends.
//!
//! ## Flags parsed (matching C++ DEFINE_* flags)
//!
//! | Flag | Type | Default | Description |
//! |------|------|---------|-------------|
//! | `gcs_server_port` | int | 0 | gRPC listen port (0 = OS-assigned, matches C++ `gcs_server_main.cc:48`) |
//! | `redis_address` | string | "" | Redis server address |
//! | `redis_password` | string | "" | Redis authentication |
//! | `redis_port` | int | -1 | Redis port (appended to address) |
//! | `redis_enable_ssl` | bool | false | Use TLS for Redis |
//! | `redis_username` | string | "" | Redis ACL username |
//! | `retry_redis` | bool | false | Retry Redis connection |
//! | `external_storage_namespace` | string | "" | Redis key namespace (falls back to `RayConfig::external_storage_namespace` = `"default"`) |
//! | `session_name` | string | "" | Ray session name |
//! | `session_dir` | string | "" | Session directory |
//! | `node_id` | string | "" | Head node ID |
//! | `node_ip_address` | string | "" | Node IP address |
//! | `log_dir` | string | "" | Log directory |
//! | `metrics_agent_port` | int | -1 | Metrics agent port |
//! | `config_list` | string | "" | Base64-encoded Ray config |
//! | `stdout_filepath` | string | "" | Stdout log file |
//! | `stderr_filepath` | string | "" | Stderr log file |
//! | `ray_commit` | string | "" | Ray git commit hash |

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::info;

use gcs_server::{GcsServer, GcsServerConfig};

/// Ray version string, matching C++ `kRayVersion` from `constants.h`.
const RAY_VERSION: &str = "3.0.0.dev0";

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

/// Strict base64 decode of the `config_list` flag. Mirrors C++
/// `gcs_server_main.cc:108-110`:
///
/// ```cpp
/// RAY_CHECK(absl::Base64Unescape(FLAGS_config_list, &config_list))
///     << "config_list is not a valid base64-encoded string.";
/// ```
///
/// A bad base64 payload is fatal — the server refuses to start instead
/// of silently falling back to the raw bytes (which would mask a bug
/// in whichever caller generated the flag).
fn decode_config_list(encoded: &str) -> Result<String> {
    use base64::Engine;
    if encoded.is_empty() {
        return Ok(String::new());
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| anyhow::anyhow!("--config_list is not valid base64: {e}"))?;
    String::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("--config_list decodes to non-UTF-8 bytes: {e}"))
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let flags = parse_gflags(std::env::args().skip(1));

    // RayConfig has to be initialized BEFORE we build the tokio runtime,
    // because the runtime's worker-thread count comes from
    // `gcs_server_rpc_server_thread_num`. C++ does the same:
    // `gcs_server_main.cc:120` (RayConfig init) precedes
    // `gcs_server_main.cc:165-166` (thread-num read for the gRPC
    // server). Strict base64 decode mirrors C++ line 109.
    let config_list_raw = flags.get("config_list").map(|s| s.as_str()).unwrap_or("");
    let config_list = decode_config_list(config_list_raw)?;
    ray_config::initialize(&config_list)
        .map_err(|e| anyhow::anyhow!("RayConfig::initialize failed: {e}"))?;

    // Build the tokio runtime with `gcs_server_rpc_server_thread_num`
    // worker threads. Matches C++ `gcs_server.cc` constructing the gRPC
    // server with `RayConfig::instance().gcs_server_rpc_server_thread_num()`
    // threads. Default 1 (`ray_config_def.h:382` would compute
    // hardware_concurrency/4, but the Rust default of 1 is conservative
    // and override-friendly via env or `config_list`).
    let worker_threads = ray_config::instance().gcs_server_rpc_server_thread_num as usize;
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();
    if worker_threads > 0 {
        rt_builder.worker_threads(worker_threads);
    }
    let runtime = rt_builder
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build tokio runtime: {e}"))?;

    runtime.block_on(run_server(flags, config_list))
}

async fn run_server(flags: HashMap<String, String>, config_list: String) -> Result<()> {

    // --- Parse all C++ gflags-equivalent flags ---

    // C++ default is 0 (OS-assigned), matching `gcs_server_main.cc:48`
    // (`DEFINE_int32(gcs_server_port, 0, ...)`). Pre-binding in
    // `start_with_listener` honors port 0 by reading `local_addr()`
    // after bind and writing the resulting port to the port file.
    let port: u16 = flags
        .get("gcs_server_port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let session_dir = flags.get("session_dir")
        .or_else(|| flags.get("session-dir"))
        .map(PathBuf::from);
    let node_id = flags.get("node_id")
        .or_else(|| flags.get("node-id"))
        .cloned();
    let node_ip_address = flags.get("node_ip_address").cloned().unwrap_or_default();
    let log_dir = flags.get("log_dir").cloned().unwrap_or_default();
    let metrics_agent_port: i32 = flags
        .get("metrics_agent_port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);
    let ray_commit = flags.get("ray_commit").cloned().unwrap_or_default();
    let session_name = flags.get("session_name").cloned().unwrap_or_default();

    // `config_list` was already decoded and `ray_config::initialize`
    // was already called by `main` (before the runtime was built, so
    // the runtime's `worker_threads` could read
    // `gcs_server_rpc_server_thread_num`). Reuse the decoded JSON.

    // Create stdout/stderr log files that Ray Python expects.
    // C++ redirects streams with rotation; Rust creates the files for
    // compatibility and logs via tracing-subscriber instead.
    if let Some(path) = flags.get("stdout_filepath") {
        let _ = std::fs::File::create(path);
    }
    if let Some(path) = flags.get("stderr_filepath") {
        let _ = std::fs::File::create(path);
    }

    // --- Startup logging (matching C++ RAY_LOG(INFO) with version/commit) ---
    info!(
        ray_version = RAY_VERSION,
        ray_commit = ray_commit.as_str(),
        "Ray cluster metadata"
    );

    // --- Parse Redis configuration ---
    let redis_address = flags.get("redis_address").cloned()
        .or_else(|| std::env::var("RAY_REDIS_ADDRESS").ok());
    let redis_password = flags.get("redis_password").cloned()
        .or_else(|| std::env::var("RAY_REDIS_PASSWORD").ok());
    let redis_username = flags.get("redis_username").cloned();
    let redis_port: Option<i32> = flags
        .get("redis_port")
        .and_then(|s| s.parse().ok())
        .filter(|&p| p > 0);
    let redis_enable_ssl = flags
        .get("redis_enable_ssl")
        .map(|s| s == "true" || s == "1")
        .unwrap_or(false);
    let _retry_redis = flags
        .get("retry_redis")
        .map(|s| s == "true" || s == "1")
        .unwrap_or(false);
    // `external_storage_namespace`: CLI flag wins when present, else
    // fall back to the initialized RayConfig value (which honors
    // `RAY_external_storage_namespace` env + `config_list` override).
    // Matches C++ `ray_config_def.h:866` default of `"default"`.
    let external_storage_namespace = flags
        .get("external_storage_namespace")
        .cloned()
        .unwrap_or_else(|| ray_config::instance().external_storage_namespace.clone());

    // Build Redis URL if address is provided.
    // Supports: plain address, redis:// URL, rediss:// URL, username+password auth.
    let redis_url = redis_address.map(|addr| {
        if addr.starts_with("redis://") || addr.starts_with("rediss://") {
            addr
        } else {
            let scheme = if redis_enable_ssl { "rediss" } else { "redis" };
            let host_port = if let Some(port) = redis_port {
                if addr.contains(':') {
                    addr // Already has port
                } else {
                    format!("{}:{}", addr, port)
                }
            } else {
                addr
            };
            match (&redis_username, &redis_password) {
                (Some(user), Some(pass)) => {
                    format!("{}://{}:{}@{}", scheme, user, pass, host_port)
                }
                (None, Some(pass)) => {
                    format!("{}://:{}@{}", scheme, pass, host_port)
                }
                _ => format!("{}://{}", scheme, host_port),
            }
        }
    });

    // Pull relevant settings from the initialized RayConfig. All
    // overrides — env vars and config_list — have been applied above.
    let rc = ray_config::snapshot();

    let config = GcsServerConfig {
        grpc_port: port,
        raylet_config_list: config_list,
        redis_address: redis_url.clone(),
        external_storage_namespace: external_storage_namespace.clone(),
        session_name,
        log_dir: if log_dir.is_empty() { None } else { Some(log_dir.clone()) },
        event_log_reporter_enabled: rc.event_log_reporter_enabled,
        // Propagate the CLI `--metrics_agent_port` into the server so
        // it can either init the metrics exporter eagerly (port > 0)
        // or wait for the head-node-register listener to supply the
        // port. Parity with C++ `gcs_server_main.cc` → `GcsServerConfig
        // metrics_agent_port` threading.
        metrics_agent_port,
        ..Default::default()
    };

    // Report the *effective* config at startup. Every value below has
    // passed through RayConfig's default → env → config_list merge —
    // the log line is therefore the authoritative record of what the
    // process is actually running with. Matches the detail level of
    // C++ `RayConfig::initialize` debug-log block
    // (`ray_config.cc:66-73`).
    info!(
        port,
        redis = redis_url.is_some(),
        node_ip_address = node_ip_address.as_str(),
        log_dir = log_dir.as_str(),
        metrics_agent_port,
        event_log_reporter_enabled = rc.event_log_reporter_enabled,
        emit_event_to_log_file = rc.emit_event_to_log_file,
        event_level = rc.event_level.as_str(),
        external_storage_namespace = external_storage_namespace.as_str(),
        health_check_period_ms = rc.health_check_period_ms,
        health_check_timeout_ms = rc.health_check_timeout_ms,
        health_check_failure_threshold = rc.health_check_failure_threshold,
        gcs_mark_task_failed_on_worker_dead_delay_ms = rc.gcs_mark_task_failed_on_worker_dead_delay_ms,
        gcs_mark_task_failed_on_job_done_delay_ms = rc.gcs_mark_task_failed_on_job_done_delay_ms,
        gcs_server_rpc_server_thread_num = rc.gcs_server_rpc_server_thread_num,
        "Rust GCS server starting"
    );

    // Create server with appropriate storage backend.
    // Construction is async: it calls KV to get-or-generate the cluster ID.
    let server = if let Some(ref url) = redis_url {
        let redis_client = gcs_store::RedisStoreClient::connect(url, &external_storage_namespace)
            .await
            .expect("Failed to connect to Redis");
        let redis_client = std::sync::Arc::new(redis_client);
        GcsServer::new_with_redis(config, redis_client).await
    } else {
        GcsServer::new(config).await
    };

    // Bind to the port first so we know the actual port (handles port 0).
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    let actual_port = listener.local_addr()?.port();

    info!(actual_port, "Rust GCS server bound");

    // Write port file for Ray to discover us (matching C++ PersistPort).
    if let (Some(dir), Some(nid)) = (&session_dir, &node_id) {
        let port_file = dir.join(format!("gcs_server_port_{}", nid));
        if let Err(e) = std::fs::write(&port_file, actual_port.to_string()) {
            tracing::warn!(?port_file, error = %e, "Failed to write GCS port file");
        } else {
            info!(?port_file, actual_port, "Wrote GCS port file");
        }
    }

    // --- Graceful shutdown on SIGTERM (matching C++ signal handler) ---
    let server = std::sync::Arc::new(server);
    let server_for_signal = server.clone();

    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::spawn(async move {
            sigterm.recv().await;
            info!("GCS server received SIGTERM, shutting down...");
            // The server will stop when the main task exits.
            drop(server_for_signal);
        });
    }

    server.start_with_listener(listener).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn decode_config_list_empty_is_empty() {
        assert_eq!(decode_config_list("").unwrap(), "");
    }

    #[test]
    fn decode_config_list_roundtrips_json() {
        let payload = r#"{"event_log_reporter_enabled": true}"#;
        let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
        assert_eq!(decode_config_list(&encoded).unwrap(), payload);
    }

    /// Parity with C++ `gcs_server_main.cc:109` —
    /// `RAY_CHECK(absl::Base64Unescape(...))`. An invalid base64 flag
    /// must be fatal, not silently fall back to raw bytes.
    #[test]
    fn decode_config_list_rejects_invalid_base64() {
        let err = decode_config_list("not valid base64!!!").unwrap_err().to_string();
        assert!(
            err.contains("not valid base64"),
            "expected base64-error, got: {err}"
        );
    }

    #[test]
    fn decode_config_list_rejects_non_utf8() {
        // Valid base64 encoding of invalid UTF-8 bytes.
        let bad = base64::engine::general_purpose::STANDARD.encode([0xFFu8, 0xFEu8, 0xFDu8]);
        let err = decode_config_list(&bad).unwrap_err().to_string();
        assert!(err.contains("non-UTF-8"), "got: {err}");
    }
}
