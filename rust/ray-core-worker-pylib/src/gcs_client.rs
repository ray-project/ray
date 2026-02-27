// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Python-facing GCS client.
//!
//! Provides internal KV operations, cluster info queries, and health checks.
//! Owns a tokio runtime for bridging sync Python to async gRPC.

/// Python-facing GCS client.
///
/// Currently a stub that records the GCS address. Full gRPC integration
/// will be added when the GCS RPC client is connected.
pub struct PyGcsClient {
    gcs_address: String,
    runtime: tokio::runtime::Runtime,
}

impl PyGcsClient {
    /// Create a new PyGcsClient.
    pub fn new(gcs_address: String) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        Self {
            gcs_address,
            runtime,
        }
    }

    pub fn gcs_address(&self) -> &str {
        &self.gcs_address
    }

    // ─── Internal KV ─────────────────────────────────────────────────

    /// Get a value from the internal KV store.
    pub fn internal_kv_get(&self, _namespace: &str, _key: &str) -> Option<Vec<u8>> {
        tracing::debug!("internal_kv_get stub");
        None
    }

    /// Put a value into the internal KV store.
    pub fn internal_kv_put(
        &self,
        _namespace: &str,
        _key: &str,
        _value: &[u8],
        _overwrite: bool,
    ) -> bool {
        tracing::debug!("internal_kv_put stub");
        true
    }

    /// Delete a key from the internal KV store.
    pub fn internal_kv_del(&self, _namespace: &str, _key: &str) -> bool {
        tracing::debug!("internal_kv_del stub");
        true
    }

    /// List keys matching a prefix.
    pub fn internal_kv_keys(&self, _namespace: &str, _prefix: &str) -> Vec<String> {
        tracing::debug!("internal_kv_keys stub");
        Vec::new()
    }

    /// Check if a key exists.
    pub fn internal_kv_exists(&self, _namespace: &str, _key: &str) -> bool {
        tracing::debug!("internal_kv_exists stub");
        false
    }

    // ─── Cluster Info ────────────────────────────────────────────────

    /// Get info for all nodes in the cluster.
    pub fn get_all_node_info(&self) -> Vec<ray_proto::ray::rpc::GcsNodeInfo> {
        tracing::debug!("get_all_node_info stub");
        Vec::new()
    }

    /// Get info for all jobs.
    pub fn get_all_job_info(&self) -> Vec<ray_proto::ray::rpc::JobTableData> {
        tracing::debug!("get_all_job_info stub");
        Vec::new()
    }

    /// Get info for all actors.
    pub fn get_all_actor_info(&self) -> Vec<ray_proto::ray::rpc::ActorTableData> {
        tracing::debug!("get_all_actor_info stub");
        Vec::new()
    }

    // ─── Health ──────────────────────────────────────────────────────

    /// Check if the GCS server is alive.
    pub fn check_alive(&self, _node_ips: &[String]) -> Vec<bool> {
        tracing::debug!("check_alive stub");
        Vec::new()
    }

    /// Request nodes to drain.
    pub fn drain_nodes(&self, _node_ids: &[Vec<u8>]) -> Vec<bool> {
        tracing::debug!("drain_nodes stub");
        Vec::new()
    }

    /// Access the runtime (for advanced usage).
    pub fn runtime(&self) -> &tokio::runtime::Runtime {
        &self.runtime
    }
}
