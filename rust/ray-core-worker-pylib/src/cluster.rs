// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! In-process cluster startup for Python integration.
//!
//! Provides `PyClusterHandle` and `start_cluster()` to start a GCS server
//! and Raylet entirely in-process, enabling Python scripts to exercise the
//! full Rust backend without any external processes.

use std::sync::Arc;

use ray_common::id::NodeID;
use tokio::sync::oneshot;

/// Handle to an in-process Ray cluster (GCS + Raylet).
///
/// Returned by `start_cluster()`. Call `shutdown()` to stop the GCS server.
#[cfg_attr(feature = "python", pyo3::pyclass(module = "_raylet"))]
pub struct PyClusterHandle {
    gcs_addr: String,
    node_id: NodeID,
    #[allow(dead_code)]
    shutdown_tx: Option<oneshot::Sender<()>>,
    #[allow(dead_code)]
    runtime: Arc<tokio::runtime::Runtime>,
}

impl PyClusterHandle {
    pub fn gcs_address(&self) -> &str {
        &self.gcs_addr
    }

    pub fn node_id(&self) -> &NodeID {
        &self.node_id
    }
}

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl PyClusterHandle {
    /// Get the GCS address (e.g. "127.0.0.1:12345").
    #[pyo3(name = "gcs_address")]
    fn py_gcs_address(&self) -> String {
        self.gcs_addr.clone()
    }

    /// Get the Raylet node ID.
    #[pyo3(name = "node_id")]
    fn py_node_id(&self) -> crate::ids::PyNodeID {
        crate::ids::PyNodeID::from_inner(self.node_id)
    }

    /// Shut down the GCS server.
    #[pyo3(name = "shutdown")]
    fn py_shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            tx.send(()).ok();
        }
    }
}

/// Start an in-process Ray cluster (GCS server + Raylet).
///
/// Returns a `PyClusterHandle` that holds the GCS address and node ID.
/// Call `handle.shutdown()` when done.
#[cfg(feature = "python")]
#[pyo3::pyfunction]
pub fn start_cluster() -> pyo3::PyResult<PyClusterHandle> {
    use std::collections::HashMap;
    use ray_raylet::node_manager::{NodeManager, RayletConfig};
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to create tokio runtime: {}",
                    e
                ))
            })?,
    );

    let rt = Arc::clone(&runtime);
    let handle = runtime.block_on(async move {
        // Start in-process GCS server.
        let gcs_server = ray_test_utils::start_test_gcs_server().await;
        let gcs_addr = format!("{}", gcs_server.addr);

        // Start Raylet (NodeManager).
        let node_id = NodeID::from_random();
        let nm = Arc::new(NodeManager::new(RayletConfig {
            node_ip_address: "127.0.0.1".to_string(),
            port: 0,
            object_store_socket: String::new(),
            gcs_address: format!("http://{}", gcs_server.addr),
            log_dir: None,
            ray_config: ray_common::config::RayConfig::default(),
            node_id: node_id.hex(),
            resources: HashMap::from([("CPU".to_string(), 4.0)]),
            labels: HashMap::new(),
            session_name: "python-actor-demo".to_string(),
            auth_token: None,
        }));
        let nm_clone = Arc::clone(&nm);
        tokio::spawn(async move { nm_clone.run().await });
        // Wait for NodeManager to start accepting connections.
        const CLUSTER_STARTUP_WAIT_MS: u64 = 500;
        tokio::time::sleep(std::time::Duration::from_millis(CLUSTER_STARTUP_WAIT_MS)).await;

        PyClusterHandle {
            gcs_addr,
            node_id,
            shutdown_tx: Some(gcs_server.shutdown_tx),
            runtime: rt,
        }
    });

    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_handle_accessors() {
        let node_id = NodeID::from_random();
        let (tx, _rx) = oneshot::channel();
        let rt = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let handle = PyClusterHandle {
            gcs_addr: "127.0.0.1:9999".to_string(),
            node_id,
            shutdown_tx: Some(tx),
            runtime: rt,
        };
        assert_eq!(handle.gcs_address(), "127.0.0.1:9999");
        assert_eq!(*handle.node_id(), node_id);
    }

    #[test]
    fn test_cluster_handle_shutdown_idempotent() {
        let node_id = NodeID::from_random();
        let (tx, _rx) = oneshot::channel();
        let rt = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let mut handle = PyClusterHandle {
            gcs_addr: "127.0.0.1:9999".to_string(),
            node_id,
            shutdown_tx: Some(tx),
            runtime: rt,
        };
        // First shutdown sends the signal.
        assert!(handle.shutdown_tx.is_some());
        handle.shutdown_tx.take().unwrap().send(()).ok();
        // Second call is a no-op.
        assert!(handle.shutdown_tx.is_none());
    }

    #[test]
    fn test_cluster_handle_accessors_with_different_addresses() {
        let node_id = NodeID::from_random();
        let (tx, _rx) = oneshot::channel();
        let rt = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );

        // Test with an IPv6 loopback address.
        let handle = PyClusterHandle {
            gcs_addr: "[::1]:6379".to_string(),
            node_id,
            shutdown_tx: Some(tx),
            runtime: rt,
        };
        assert_eq!(handle.gcs_address(), "[::1]:6379");
        // Node ID should be non-nil (random).
        assert_ne!(handle.node_id().hex(), "0".repeat(56));
    }

    #[test]
    fn test_cluster_handle_node_id_is_stable() {
        let node_id = NodeID::from_random();
        let hex = node_id.hex();
        let (tx, _rx) = oneshot::channel();
        let rt = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let handle = PyClusterHandle {
            gcs_addr: "127.0.0.1:0".to_string(),
            node_id,
            shutdown_tx: Some(tx),
            runtime: rt,
        };

        // Multiple calls to node_id() return the same value.
        assert_eq!(handle.node_id().hex(), hex);
        assert_eq!(handle.node_id().hex(), hex);
        // NodeID hex should be 56 chars (28 bytes).
        assert_eq!(hex.len(), 56);
    }
}
