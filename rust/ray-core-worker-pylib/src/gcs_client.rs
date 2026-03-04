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

use ray_gcs_rpc_client::{GcsClient, GcsRpcClient};
use ray_proto::ray::rpc;
use ray_rpc::client::RetryConfig;

/// Python-facing GCS client.
///
/// Wraps a real `GcsRpcClient` (via the `GcsClient` trait) and a tokio runtime.
/// All async gRPC calls are bridged to sync via `runtime.block_on()`.
#[cfg_attr(feature = "python", pyo3::pyclass(module = "_raylet"))]
pub struct PyGcsClient {
    gcs_address: String,
    client: Box<dyn GcsClient>,
    runtime: tokio::runtime::Runtime,
}

impl PyGcsClient {
    /// Create a new PyGcsClient that lazily connects to the GCS server.
    pub fn new(gcs_address: String) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");

        // Enter the runtime context so that tonic/hyper can find the Tokio reactor
        // when creating a lazy channel.
        let _guard = runtime.enter();
        let endpoint = format!("http://{}", gcs_address);
        let channel = tonic::transport::Channel::from_shared(endpoint)
            .expect("invalid GCS address")
            .connect_lazy();
        let client = GcsRpcClient::from_channel(channel, RetryConfig::default());

        Self {
            gcs_address,
            client: Box::new(client),
            runtime,
        }
    }

    /// Create a PyGcsClient with a custom GcsClient implementation (for testing).
    pub fn with_client(gcs_address: String, client: Box<dyn GcsClient>) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        Self {
            gcs_address,
            client,
            runtime,
        }
    }

    pub fn gcs_address(&self) -> &str {
        &self.gcs_address
    }

    // ─── Internal KV ─────────────────────────────────────────────────

    /// Get a value from the internal KV store.
    pub fn internal_kv_get(&self, namespace: &str, key: &str) -> Option<Vec<u8>> {
        let req = rpc::InternalKvGetRequest {
            key: key.as_bytes().to_vec(),
            namespace: namespace.as_bytes().to_vec(),
        };
        match self.runtime.block_on(self.client.internal_kv_get(req)) {
            Ok(reply) => {
                if reply.value.is_empty() {
                    None
                } else {
                    Some(reply.value)
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "internal_kv_get failed");
                None
            }
        }
    }

    /// Put a value into the internal KV store.
    pub fn internal_kv_put(
        &self,
        namespace: &str,
        key: &str,
        value: &[u8],
        overwrite: bool,
    ) -> bool {
        let req = rpc::InternalKvPutRequest {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
            overwrite,
            namespace: namespace.as_bytes().to_vec(),
        };
        match self.runtime.block_on(self.client.internal_kv_put(req)) {
            Ok(reply) => reply.added,
            Err(e) => {
                tracing::warn!(error = %e, "internal_kv_put failed");
                false
            }
        }
    }

    /// Delete a key from the internal KV store.
    pub fn internal_kv_del(&self, namespace: &str, key: &str) -> bool {
        let req = rpc::InternalKvDelRequest {
            key: key.as_bytes().to_vec(),
            namespace: namespace.as_bytes().to_vec(),
            del_by_prefix: false,
        };
        match self.runtime.block_on(self.client.internal_kv_del(req)) {
            Ok(reply) => reply.deleted_num > 0,
            Err(e) => {
                tracing::warn!(error = %e, "internal_kv_del failed");
                false
            }
        }
    }

    /// List keys matching a prefix.
    pub fn internal_kv_keys(&self, namespace: &str, prefix: &str) -> Vec<String> {
        let req = rpc::InternalKvKeysRequest {
            prefix: prefix.as_bytes().to_vec(),
            namespace: namespace.as_bytes().to_vec(),
        };
        match self.runtime.block_on(self.client.internal_kv_keys(req)) {
            Ok(reply) => reply
                .results
                .into_iter()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "internal_kv_keys failed");
                Vec::new()
            }
        }
    }

    /// Check if a key exists (via get).
    pub fn internal_kv_exists(&self, namespace: &str, key: &str) -> bool {
        self.internal_kv_get(namespace, key).is_some()
    }

    // ─── Cluster Info ────────────────────────────────────────────────

    /// Get info for all nodes in the cluster.
    pub fn get_all_node_info(&self) -> Vec<rpc::GcsNodeInfo> {
        let req = rpc::GetAllNodeInfoRequest::default();
        match self.runtime.block_on(self.client.get_all_node_info(req)) {
            Ok(reply) => reply.node_info_list,
            Err(e) => {
                tracing::warn!(error = %e, "get_all_node_info failed");
                Vec::new()
            }
        }
    }

    /// Get info for all jobs.
    pub fn get_all_job_info(&self) -> Vec<rpc::JobTableData> {
        let req = rpc::GetAllJobInfoRequest::default();
        match self.runtime.block_on(self.client.get_all_job_info(req)) {
            Ok(reply) => reply.job_info_list,
            Err(e) => {
                tracing::warn!(error = %e, "get_all_job_info failed");
                Vec::new()
            }
        }
    }

    /// Get info for all actors.
    pub fn get_all_actor_info(&self) -> Vec<rpc::ActorTableData> {
        let req = rpc::GetAllActorInfoRequest::default();
        match self.runtime.block_on(self.client.get_all_actor_info(req)) {
            Ok(reply) => reply.actor_table_data,
            Err(e) => {
                tracing::warn!(error = %e, "get_all_actor_info failed");
                Vec::new()
            }
        }
    }

    // ─── Health ──────────────────────────────────────────────────────

    /// Check if specific nodes are alive by node ID bytes.
    pub fn check_alive(&self, node_ids: &[Vec<u8>]) -> Vec<bool> {
        let req = rpc::CheckAliveRequest {
            node_ids: node_ids.to_vec(),
        };
        match self.runtime.block_on(self.client.check_alive(req)) {
            Ok(reply) => reply.raylet_alive,
            Err(e) => {
                tracing::warn!(error = %e, "check_alive failed");
                Vec::new()
            }
        }
    }

    /// Request nodes to drain (stub — drain RPC not yet in GcsClient trait).
    pub fn drain_nodes(&self, _node_ids: &[Vec<u8>]) -> Vec<bool> {
        tracing::debug!("drain_nodes: not yet wired to GCS RPC");
        Vec::new()
    }

    /// Access the runtime (for advanced usage).
    pub fn runtime(&self) -> &tokio::runtime::Runtime {
        &self.runtime
    }
}

// ─── PyO3 methods (only when "python" feature is enabled) ────────────

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl PyGcsClient {
    /// Connect to GCS at the given address (e.g. "127.0.0.1:6379").
    #[new]
    fn py_new(gcs_address: String) -> Self {
        Self::new(gcs_address)
    }

    /// Get the GCS address.
    #[pyo3(name = "address")]
    fn py_address(&self) -> String {
        self.gcs_address.clone()
    }

    // ─── Internal KV ─────────────────────────────────────────────────

    /// Get a value from the internal KV store.
    #[pyo3(name = "internal_kv_get")]
    fn py_internal_kv_get(&self, namespace: &str, key: &str) -> Option<Vec<u8>> {
        self.internal_kv_get(namespace, key)
    }

    /// Put a value into the internal KV store. Returns true if added.
    #[pyo3(name = "internal_kv_put")]
    fn py_internal_kv_put(
        &self,
        namespace: &str,
        key: &str,
        value: &[u8],
        overwrite: bool,
    ) -> bool {
        self.internal_kv_put(namespace, key, value, overwrite)
    }

    /// Delete a key from the internal KV store.
    #[pyo3(name = "internal_kv_del")]
    fn py_internal_kv_del(&self, namespace: &str, key: &str) -> bool {
        self.internal_kv_del(namespace, key)
    }

    /// List keys matching a prefix.
    #[pyo3(name = "internal_kv_keys")]
    fn py_internal_kv_keys(&self, namespace: &str, prefix: &str) -> Vec<String> {
        self.internal_kv_keys(namespace, prefix)
    }

    /// Check if a key exists.
    #[pyo3(name = "internal_kv_exists")]
    fn py_internal_kv_exists(&self, namespace: &str, key: &str) -> bool {
        self.internal_kv_exists(namespace, key)
    }

    // ─── Cluster Info ────────────────────────────────────────────────

    /// Get node count.
    #[pyo3(name = "get_node_count")]
    fn py_get_node_count(&self) -> usize {
        self.get_all_node_info().len()
    }

    /// Get all node IP addresses.
    #[pyo3(name = "get_node_addresses")]
    fn py_get_node_addresses(&self) -> Vec<String> {
        self.get_all_node_info()
            .iter()
            .map(|n| n.node_manager_address.clone())
            .collect()
    }

    /// Get all job info as a list of (job_id_bytes, is_dead) tuples.
    #[pyo3(name = "get_job_info")]
    fn py_get_job_info(&self) -> Vec<(Vec<u8>, bool)> {
        self.get_all_job_info()
            .into_iter()
            .map(|j| (j.job_id.clone(), j.is_dead))
            .collect()
    }

    /// Check if specific nodes are alive.
    #[pyo3(name = "check_alive")]
    fn py_check_alive(&self, node_ids: Vec<Vec<u8>>) -> Vec<bool> {
        self.check_alive(&node_ids)
    }

    /// Drain nodes (stub).
    #[pyo3(name = "drain_nodes")]
    fn py_drain_nodes(&self, node_ids: Vec<Vec<u8>>) -> Vec<bool> {
        self.drain_nodes(&node_ids)
    }

    /// Register a new actor with GCS and return its actor ID.
    ///
    /// Arguments:
    ///   name: the actor class name (e.g. "Counter")
    ///   namespace: the Ray namespace (e.g. "default")
    #[pyo3(name = "register_actor")]
    fn py_register_actor(
        &self,
        name: &str,
        namespace: &str,
    ) -> pyo3::PyResult<crate::ids::PyActorID> {
        use ray_common::id::{ActorID, TaskID};

        let actor_id = ActorID::from_random();
        let req = rpc::RegisterActorRequest {
            task_spec: Some(rpc::TaskSpec {
                task_id: TaskID::from_random().binary(),
                name: format!("{}.__init__", name),
                actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                    actor_id: actor_id.binary(),
                    name: name.to_string(),
                    ray_namespace: namespace.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };

        let reply = self
            .runtime
            .block_on(self.client.register_actor(req))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "register_actor failed: {}",
                    e
                ))
            })?;

        // Check status.
        if let Some(ref status) = reply.status {
            if status.code != 0 {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "register_actor returned error: {:?}",
                    status
                )));
            }
        }

        Ok(crate::ids::PyActorID::from_inner(actor_id))
    }

    /// Look up a named actor by name and namespace.
    ///
    /// Returns the actor ID if found, or None if no actor with that name exists.
    #[pyo3(name = "get_named_actor")]
    fn py_get_named_actor(
        &self,
        name: &str,
        namespace: &str,
    ) -> pyo3::PyResult<Option<crate::ids::PyActorID>> {
        use ray_common::id::ActorID;

        let req = rpc::GetNamedActorInfoRequest {
            name: name.to_string(),
            ray_namespace: namespace.to_string(),
        };
        match self.runtime.block_on(self.client.get_named_actor_info(req)) {
            Ok(reply) => {
                if let Some(ref info) = reply.actor_table_data {
                    if info.actor_id.is_empty() {
                        Ok(None)
                    } else {
                        let aid = ActorID::from_binary(&info.actor_id);
                        Ok(Some(crate::ids::PyActorID::from_inner(aid)))
                    }
                } else {
                    Ok(None)
                }
            }
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tonic::Status;

    /// A configurable fake GCS client for testing PyGcsClient.
    struct FakeGcs {
        kv_store: Mutex<HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>>,
        node_info: Mutex<Vec<rpc::GcsNodeInfo>>,
        job_info: Mutex<Vec<rpc::JobTableData>>,
        actor_info: Mutex<Vec<rpc::ActorTableData>>,
        alive_responses: Mutex<Vec<bool>>,
        fail_next: Mutex<Option<Status>>,
    }

    impl FakeGcs {
        fn new() -> Self {
            Self {
                kv_store: Mutex::new(HashMap::new()),
                node_info: Mutex::new(Vec::new()),
                job_info: Mutex::new(Vec::new()),
                actor_info: Mutex::new(Vec::new()),
                alive_responses: Mutex::new(Vec::new()),
                fail_next: Mutex::new(None),
            }
        }

        fn set_fail_next(&self, status: Status) {
            *self.fail_next.lock().unwrap() = Some(status);
        }

        fn check_fail(&self) -> Result<(), Status> {
            if let Some(status) = self.fail_next.lock().unwrap().take() {
                Err(status)
            } else {
                Ok(())
            }
        }
    }

    #[async_trait::async_trait]
    impl GcsClient for FakeGcs {
        async fn add_job(&self, _: rpc::AddJobRequest) -> Result<rpc::AddJobReply, Status> {
            self.check_fail()?;
            Ok(rpc::AddJobReply::default())
        }
        async fn mark_job_finished(
            &self,
            _: rpc::MarkJobFinishedRequest,
        ) -> Result<rpc::MarkJobFinishedReply, Status> {
            self.check_fail()?;
            Ok(rpc::MarkJobFinishedReply::default())
        }
        async fn get_all_job_info(
            &self,
            _: rpc::GetAllJobInfoRequest,
        ) -> Result<rpc::GetAllJobInfoReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetAllJobInfoReply {
                job_info_list: self.job_info.lock().unwrap().clone(),
                ..Default::default()
            })
        }
        async fn get_next_job_id(
            &self,
            _: rpc::GetNextJobIdRequest,
        ) -> Result<rpc::GetNextJobIdReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetNextJobIdReply {
                job_id: 1,
                status: None,
            })
        }
        async fn report_job_error(
            &self,
            _: rpc::ReportJobErrorRequest,
        ) -> Result<rpc::ReportJobErrorReply, Status> {
            self.check_fail()?;
            Ok(rpc::ReportJobErrorReply::default())
        }
        async fn register_node(
            &self,
            _: rpc::RegisterNodeRequest,
        ) -> Result<rpc::RegisterNodeReply, Status> {
            self.check_fail()?;
            Ok(rpc::RegisterNodeReply::default())
        }
        async fn unregister_node(
            &self,
            _: rpc::UnregisterNodeRequest,
        ) -> Result<rpc::UnregisterNodeReply, Status> {
            self.check_fail()?;
            Ok(rpc::UnregisterNodeReply::default())
        }
        async fn get_all_node_info(
            &self,
            _: rpc::GetAllNodeInfoRequest,
        ) -> Result<rpc::GetAllNodeInfoReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetAllNodeInfoReply {
                node_info_list: self.node_info.lock().unwrap().clone(),
                ..Default::default()
            })
        }
        async fn get_cluster_id(
            &self,
            _: rpc::GetClusterIdRequest,
        ) -> Result<rpc::GetClusterIdReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetClusterIdReply::default())
        }
        async fn check_alive(
            &self,
            _: rpc::CheckAliveRequest,
        ) -> Result<rpc::CheckAliveReply, Status> {
            self.check_fail()?;
            Ok(rpc::CheckAliveReply {
                raylet_alive: self.alive_responses.lock().unwrap().clone(),
                ..Default::default()
            })
        }
        async fn register_actor(
            &self,
            _: rpc::RegisterActorRequest,
        ) -> Result<rpc::RegisterActorReply, Status> {
            self.check_fail()?;
            Ok(rpc::RegisterActorReply::default())
        }
        async fn create_actor(
            &self,
            _: rpc::CreateActorRequest,
        ) -> Result<rpc::CreateActorReply, Status> {
            self.check_fail()?;
            Ok(rpc::CreateActorReply::default())
        }
        async fn get_actor_info(
            &self,
            _: rpc::GetActorInfoRequest,
        ) -> Result<rpc::GetActorInfoReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetActorInfoReply::default())
        }
        async fn get_named_actor_info(
            &self,
            _: rpc::GetNamedActorInfoRequest,
        ) -> Result<rpc::GetNamedActorInfoReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetNamedActorInfoReply::default())
        }
        async fn get_all_actor_info(
            &self,
            _: rpc::GetAllActorInfoRequest,
        ) -> Result<rpc::GetAllActorInfoReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetAllActorInfoReply {
                actor_table_data: self.actor_info.lock().unwrap().clone(),
                ..Default::default()
            })
        }
        async fn kill_actor_via_gcs(
            &self,
            _: rpc::KillActorViaGcsRequest,
        ) -> Result<rpc::KillActorViaGcsReply, Status> {
            self.check_fail()?;
            Ok(rpc::KillActorViaGcsReply::default())
        }
        async fn report_worker_failure(
            &self,
            _: rpc::ReportWorkerFailureRequest,
        ) -> Result<rpc::ReportWorkerFailureReply, Status> {
            self.check_fail()?;
            Ok(rpc::ReportWorkerFailureReply::default())
        }
        async fn add_worker_info(
            &self,
            _: rpc::AddWorkerInfoRequest,
        ) -> Result<rpc::AddWorkerInfoReply, Status> {
            self.check_fail()?;
            Ok(rpc::AddWorkerInfoReply::default())
        }
        async fn get_all_worker_info(
            &self,
            _: rpc::GetAllWorkerInfoRequest,
        ) -> Result<rpc::GetAllWorkerInfoReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetAllWorkerInfoReply::default())
        }
        async fn get_all_resource_usage(
            &self,
            _: rpc::GetAllResourceUsageRequest,
        ) -> Result<rpc::GetAllResourceUsageReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetAllResourceUsageReply::default())
        }
        async fn create_placement_group(
            &self,
            _: rpc::CreatePlacementGroupRequest,
        ) -> Result<rpc::CreatePlacementGroupReply, Status> {
            self.check_fail()?;
            Ok(rpc::CreatePlacementGroupReply::default())
        }
        async fn remove_placement_group(
            &self,
            _: rpc::RemovePlacementGroupRequest,
        ) -> Result<rpc::RemovePlacementGroupReply, Status> {
            self.check_fail()?;
            Ok(rpc::RemovePlacementGroupReply::default())
        }
        async fn get_all_placement_group(
            &self,
            _: rpc::GetAllPlacementGroupRequest,
        ) -> Result<rpc::GetAllPlacementGroupReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetAllPlacementGroupReply::default())
        }
        async fn internal_kv_get(
            &self,
            req: rpc::InternalKvGetRequest,
        ) -> Result<rpc::InternalKvGetReply, Status> {
            self.check_fail()?;
            let store = self.kv_store.lock().unwrap();
            let value = store
                .get(&(req.namespace.clone(), req.key.clone()))
                .cloned()
                .unwrap_or_default();
            Ok(rpc::InternalKvGetReply {
                value,
                ..Default::default()
            })
        }
        async fn internal_kv_put(
            &self,
            req: rpc::InternalKvPutRequest,
        ) -> Result<rpc::InternalKvPutReply, Status> {
            self.check_fail()?;
            let mut store = self.kv_store.lock().unwrap();
            let key = (req.namespace.clone(), req.key.clone());
            let exists = store.contains_key(&key);
            if !exists || req.overwrite {
                store.insert(key, req.value);
                Ok(rpc::InternalKvPutReply {
                    added: !exists,
                    ..Default::default()
                })
            } else {
                Ok(rpc::InternalKvPutReply {
                    added: false,
                    ..Default::default()
                })
            }
        }
        async fn internal_kv_del(
            &self,
            req: rpc::InternalKvDelRequest,
        ) -> Result<rpc::InternalKvDelReply, Status> {
            self.check_fail()?;
            let mut store = self.kv_store.lock().unwrap();
            let removed = store
                .remove(&(req.namespace.clone(), req.key.clone()))
                .is_some();
            Ok(rpc::InternalKvDelReply {
                deleted_num: if removed { 1 } else { 0 },
                ..Default::default()
            })
        }
        async fn internal_kv_keys(
            &self,
            req: rpc::InternalKvKeysRequest,
        ) -> Result<rpc::InternalKvKeysReply, Status> {
            self.check_fail()?;
            let store = self.kv_store.lock().unwrap();
            let prefix = &req.prefix;
            let ns = &req.namespace;
            let results: Vec<Vec<u8>> = store
                .keys()
                .filter(|(n, k)| n == ns && k.starts_with(prefix))
                .map(|(_, k)| k.clone())
                .collect();
            Ok(rpc::InternalKvKeysReply {
                results,
                ..Default::default()
            })
        }
        async fn add_task_event_data(
            &self,
            _: rpc::AddTaskEventDataRequest,
        ) -> Result<rpc::AddTaskEventDataReply, Status> {
            self.check_fail()?;
            Ok(rpc::AddTaskEventDataReply::default())
        }
        async fn get_task_events(
            &self,
            _: rpc::GetTaskEventsRequest,
        ) -> Result<rpc::GetTaskEventsReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetTaskEventsReply::default())
        }
        async fn drain_node(
            &self,
            _: rpc::DrainNodeRequest,
        ) -> Result<rpc::DrainNodeReply, Status> {
            self.check_fail()?;
            Ok(rpc::DrainNodeReply::default())
        }
        async fn get_placement_group(
            &self,
            _: rpc::GetPlacementGroupRequest,
        ) -> Result<rpc::GetPlacementGroupReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetPlacementGroupReply::default())
        }
        async fn get_named_placement_group(
            &self,
            _: rpc::GetNamedPlacementGroupRequest,
        ) -> Result<rpc::GetNamedPlacementGroupReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetNamedPlacementGroupReply::default())
        }
        async fn wait_placement_group_until_ready(
            &self,
            _: rpc::WaitPlacementGroupUntilReadyRequest,
        ) -> Result<rpc::WaitPlacementGroupUntilReadyReply, Status> {
            self.check_fail()?;
            Ok(rpc::WaitPlacementGroupUntilReadyReply::default())
        }
        async fn list_named_actors(
            &self,
            _: rpc::ListNamedActorsRequest,
        ) -> Result<rpc::ListNamedActorsReply, Status> {
            self.check_fail()?;
            Ok(rpc::ListNamedActorsReply::default())
        }
        async fn internal_kv_multi_get(
            &self,
            _: rpc::InternalKvMultiGetRequest,
        ) -> Result<rpc::InternalKvMultiGetReply, Status> {
            self.check_fail()?;
            Ok(rpc::InternalKvMultiGetReply::default())
        }
        async fn internal_kv_exists(
            &self,
            req: rpc::InternalKvExistsRequest,
        ) -> Result<rpc::InternalKvExistsReply, Status> {
            self.check_fail()?;
            let store = self.kv_store.lock().unwrap();
            let exists = store.contains_key(&(req.namespace, req.key));
            Ok(rpc::InternalKvExistsReply {
                exists,
                ..Default::default()
            })
        }
        async fn get_all_available_resources(
            &self,
            _: rpc::GetAllAvailableResourcesRequest,
        ) -> Result<rpc::GetAllAvailableResourcesReply, Status> {
            self.check_fail()?;
            Ok(rpc::GetAllAvailableResourcesReply::default())
        }
    }

    fn make_client() -> PyGcsClient {
        PyGcsClient::with_client("fake:0".into(), Box::new(FakeGcs::new()))
    }

    #[test]
    fn test_gcs_address() {
        let client = make_client();
        assert_eq!(client.gcs_address(), "fake:0");
    }

    #[test]
    fn test_kv_get_missing_key() {
        let client = make_client();
        assert!(client.internal_kv_get("ns", "missing").is_none());
    }

    #[test]
    fn test_kv_put_and_get() {
        let fake = FakeGcs::new();
        // Pre-populate
        fake.kv_store.lock().unwrap().insert(
            (b"ns".to_vec(), b"key1".to_vec()),
            b"value1".to_vec(),
        );
        let client = PyGcsClient::with_client("fake:0".into(), Box::new(fake));

        let val = client.internal_kv_get("ns", "key1");
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_kv_put_new_key() {
        let client = make_client();
        let added = client.internal_kv_put("ns", "key1", b"hello", false);
        assert!(added);

        let val = client.internal_kv_get("ns", "key1");
        assert_eq!(val, Some(b"hello".to_vec()));
    }

    #[test]
    fn test_kv_put_no_overwrite() {
        let client = make_client();
        assert!(client.internal_kv_put("ns", "k", b"v1", false));
        // Second put without overwrite should return false (not added)
        assert!(!client.internal_kv_put("ns", "k", b"v2", false));
        // Original value preserved
        assert_eq!(client.internal_kv_get("ns", "k"), Some(b"v1".to_vec()));
    }

    #[test]
    fn test_kv_put_with_overwrite() {
        let client = make_client();
        client.internal_kv_put("ns", "k", b"v1", false);
        // Overwrite=true replaces the value
        client.internal_kv_put("ns", "k", b"v2", true);
        assert_eq!(client.internal_kv_get("ns", "k"), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_kv_del() {
        let client = make_client();
        client.internal_kv_put("ns", "k", b"v", false);
        assert!(client.internal_kv_del("ns", "k"));
        assert!(client.internal_kv_get("ns", "k").is_none());
    }

    #[test]
    fn test_kv_del_missing() {
        let client = make_client();
        assert!(!client.internal_kv_del("ns", "missing"));
    }

    #[test]
    fn test_kv_keys() {
        let client = make_client();
        client.internal_kv_put("ns", "prefix/a", b"1", false);
        client.internal_kv_put("ns", "prefix/b", b"2", false);
        client.internal_kv_put("ns", "other/c", b"3", false);

        let mut keys = client.internal_kv_keys("ns", "prefix/");
        keys.sort();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"prefix/a".to_string()));
        assert!(keys.contains(&"prefix/b".to_string()));
    }

    #[test]
    fn test_kv_exists() {
        let client = make_client();
        assert!(!client.internal_kv_exists("ns", "k"));
        client.internal_kv_put("ns", "k", b"v", false);
        assert!(client.internal_kv_exists("ns", "k"));
    }

    #[test]
    fn test_get_all_node_info_empty() {
        let client = make_client();
        let nodes = client.get_all_node_info();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_get_all_node_info_with_data() {
        let fake = FakeGcs::new();
        fake.node_info.lock().unwrap().push(rpc::GcsNodeInfo {
            node_id: vec![1u8; 28],
            node_name: "node-1".into(),
            ..Default::default()
        });
        let client = PyGcsClient::with_client("fake:0".into(), Box::new(fake));

        let nodes = client.get_all_node_info();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_name, "node-1");
    }

    #[test]
    fn test_get_all_job_info_empty() {
        let client = make_client();
        let jobs = client.get_all_job_info();
        assert!(jobs.is_empty());
    }

    #[test]
    fn test_get_all_actor_info_empty() {
        let client = make_client();
        let actors = client.get_all_actor_info();
        assert!(actors.is_empty());
    }

    #[test]
    fn test_check_alive() {
        let fake = FakeGcs::new();
        *fake.alive_responses.lock().unwrap() = vec![true, false, true];
        let client = PyGcsClient::with_client("fake:0".into(), Box::new(fake));

        let alive = client.check_alive(&[vec![1], vec![2], vec![3]]);
        assert_eq!(alive, vec![true, false, true]);
    }

    #[test]
    fn test_drain_nodes_stub() {
        let client = make_client();
        let result = client.drain_nodes(&[vec![1]]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_kv_get_on_error_returns_none() {
        let fake = FakeGcs::new();
        fake.set_fail_next(Status::unavailable("GCS down"));
        let client = PyGcsClient::with_client("fake:0".into(), Box::new(fake));

        assert!(client.internal_kv_get("ns", "key").is_none());
    }

    #[test]
    fn test_kv_put_on_error_returns_false() {
        let fake = FakeGcs::new();
        fake.set_fail_next(Status::unavailable("GCS down"));
        let client = PyGcsClient::with_client("fake:0".into(), Box::new(fake));

        assert!(!client.internal_kv_put("ns", "key", b"val", false));
    }

    #[test]
    fn test_node_info_on_error_returns_empty() {
        let fake = FakeGcs::new();
        fake.set_fail_next(Status::internal("crash"));
        let client = PyGcsClient::with_client("fake:0".into(), Box::new(fake));

        assert!(client.get_all_node_info().is_empty());
    }

    #[test]
    fn test_namespace_isolation() {
        let client = make_client();
        client.internal_kv_put("ns1", "key", b"val1", false);
        client.internal_kv_put("ns2", "key", b"val2", false);

        assert_eq!(client.internal_kv_get("ns1", "key"), Some(b"val1".to_vec()));
        assert_eq!(client.internal_kv_get("ns2", "key"), Some(b"val2".to_vec()));
    }
}
