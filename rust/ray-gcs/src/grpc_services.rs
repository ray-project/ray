// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! gRPC service implementations for GCS.
//!
//! Replaces `src/ray/gcs/grpc_services.h/cc`.
//!
//! Each struct implements handler methods that correspond to the
//! tonic-generated service traits, delegating to GCS managers.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::actor_manager::GcsActorManager;
use crate::autoscaler_state_manager::GcsAutoscalerStateManager;
use crate::job_manager::GcsJobManager;
use crate::kv_manager::GcsInternalKVManager;
use crate::node_manager::GcsNodeManager;
use crate::placement_group_manager::GcsPlacementGroupManager;
use crate::pubsub_handler::InternalPubSubHandler;
use crate::resource_manager::GcsResourceManager;
use crate::task_manager::GcsTaskManager;
use crate::worker_manager::GcsWorkerManager;

use ray_proto::ray::rpc;

// ─── JobInfoGcsService ─────────────────────────────────────────────────────

pub struct JobInfoGcsServiceImpl {
    pub job_manager: Arc<GcsJobManager>,
    pub kv_manager: Option<Arc<crate::kv_manager::GcsInternalKVManager>>,
    pub worker_client: Option<Arc<dyn crate::actor_scheduler::CoreWorkerClient>>,
}

impl JobInfoGcsServiceImpl {
    pub async fn add_job(&self, request: rpc::AddJobRequest) -> Result<rpc::AddJobReply, Status> {
        if let Some(data) = request.data {
            self.job_manager.handle_add_job(data).await?;
        }
        Ok(rpc::AddJobReply::default())
    }

    pub async fn mark_job_finished(
        &self,
        request: rpc::MarkJobFinishedRequest,
    ) -> Result<rpc::MarkJobFinishedReply, Status> {
        self.job_manager
            .handle_mark_job_finished(&request.job_id)
            .await?;
        Ok(rpc::MarkJobFinishedReply::default())
    }

    pub async fn get_all_job_info(
        &self,
        request: rpc::GetAllJobInfoRequest,
    ) -> Result<rpc::GetAllJobInfoReply, Status> {
        // C++ contract: negative limit → invalid error; 0 → return 0 jobs; absent → no limit.
        if let Some(l) = request.limit {
            if l < 0 {
                return Err(Status::invalid_argument(format!(
                    "Invalid limit {} specified in GetAllJobInfoRequest, must be nonnegative.",
                    l
                )));
            }
        }
        let limit = request.limit.map(|l| l.max(0) as usize);
        let job_or_submission_id = request.job_or_submission_id.as_deref();
        let skip_submission = request.skip_submission_job_info_field.unwrap_or(false);
        let skip_running_tasks = request.skip_is_running_tasks_field.unwrap_or(false);
        let mut jobs = self.job_manager.handle_get_all_job_info(
            limit,
            job_or_submission_id,
            skip_submission,
            skip_running_tasks,
        );

        // C++ contract: enrich job_info from internal KV for Jobs API submissions.
        // Keys are in namespace "job" with key prefix "__ray_internal__job_info_" + submission_id.
        if !skip_submission {
            if let Some(kv) = &self.kv_manager {
                let job_namespace = b"job";
                let key_prefix = "__ray_internal__job_info_";
                // Collect submission IDs that need enrichment
                let mut enrichment_keys: Vec<(usize, String)> = Vec::new();
                for (i, job) in jobs.iter().enumerate() {
                    if job.job_info.is_some() {
                        continue; // already has job_info
                    }
                    if let Some(sid) = job
                        .config
                        .as_ref()
                        .and_then(|cfg| cfg.metadata.get("job_submission_id"))
                    {
                        enrichment_keys.push((i, format!("{}{}", key_prefix, sid)));
                    }
                }
                // Fetch from KV
                for (idx, key) in &enrichment_keys {
                    if let Ok(Some(value)) = kv.handle_get(job_namespace, key.as_bytes()).await {
                        // C++ uses protobuf::util::JsonStringToMessage to populate the full
                        // JobsAPIInfo proto. We parse all known fields from JSON.
                        if let Ok(j) = serde_json::from_slice::<serde_json::Value>(&value) {
                            let mut api_info = rpc::JobsApiInfo::default();
                            if let Some(v) = j.get("status").and_then(|v| v.as_str()) {
                                api_info.status = v.to_string();
                            }
                            if let Some(v) = j.get("entrypoint").and_then(|v| v.as_str()) {
                                api_info.entrypoint = v.to_string();
                            }
                            if let Some(v) = j.get("message").and_then(|v| v.as_str()) {
                                api_info.message = Some(v.to_string());
                            }
                            if let Some(v) = j.get("error_type").and_then(|v| v.as_str()) {
                                api_info.error_type = Some(v.to_string());
                            }
                            if let Some(v) = j.get("start_time").and_then(|v| v.as_u64()) {
                                api_info.start_time = Some(v);
                            }
                            if let Some(v) = j.get("end_time").and_then(|v| v.as_u64()) {
                                api_info.end_time = Some(v);
                            }
                            if let Some(obj) = j.get("metadata").and_then(|v| v.as_object()) {
                                for (mk, mv) in obj {
                                    if let Some(s) = mv.as_str() {
                                        api_info.metadata.insert(mk.clone(), s.to_string());
                                    }
                                }
                            }
                            if let Some(v) = j.get("runtime_env_json").and_then(|v| v.as_str()) {
                                api_info.runtime_env_json = Some(v.to_string());
                            }
                            if let Some(v) = j.get("entrypoint_num_cpus").and_then(|v| v.as_f64()) {
                                api_info.entrypoint_num_cpus = Some(v);
                            }
                            if let Some(v) = j.get("entrypoint_num_gpus").and_then(|v| v.as_f64()) {
                                api_info.entrypoint_num_gpus = Some(v);
                            }
                            if let Some(obj) = j.get("entrypoint_resources").and_then(|v| v.as_object()) {
                                for (rk, rv) in obj {
                                    if let Some(f) = rv.as_f64() {
                                        api_info.entrypoint_resources.insert(rk.clone(), f);
                                    }
                                }
                            }
                            if let Some(v) = j.get("driver_agent_http_address").and_then(|v| v.as_str()) {
                                api_info.driver_agent_http_address = Some(v.to_string());
                            }
                            if let Some(v) = j.get("driver_node_id").and_then(|v| v.as_str()) {
                                api_info.driver_node_id = Some(v.to_string());
                            }
                            if let Some(v) = j.get("driver_exit_code").and_then(|v| v.as_i64()) {
                                api_info.driver_exit_code = Some(v as i32);
                            }
                            if let Some(v) = j.get("entrypoint_memory").and_then(|v| v.as_u64()) {
                                api_info.entrypoint_memory = Some(v);
                            }
                            jobs[*idx].job_info = Some(api_info);
                        }
                    }
                }
            }
        }

        // C++ contract: resolve is_running_tasks from worker RPCs.
        // Dead jobs → false. Alive jobs → NumPendingTasksRequest to driver.
        // RPC failure → leave is_running_tasks unset (matching C++ clear_is_running_tasks).
        if !skip_running_tasks {
            for job in &mut jobs {
                if job.is_running_tasks.is_some() {
                    continue;
                }
                if job.is_dead {
                    job.is_running_tasks = Some(false);
                    continue;
                }
                // Alive job: query the driver for pending task count.
                if let Some(ref wc) = self.worker_client {
                    if let Some(ref addr) = job.driver_address {
                        if addr.port > 0 {
                            let driver_addr = format!("{}:{}", addr.ip_address, addr.port);
                            match wc
                                .num_pending_tasks(
                                    &driver_addr,
                                    rpc::NumPendingTasksRequest::default(),
                                )
                                .await
                            {
                                Ok(reply) => {
                                    job.is_running_tasks =
                                        Some(reply.num_pending_tasks > 0);
                                }
                                Err(_) => {
                                    // C++ clears the field on RPC failure
                                    job.is_running_tasks = None;
                                }
                            }
                        } else {
                            // No port — can't contact driver
                            job.is_running_tasks = None;
                        }
                    } else {
                        // No driver address
                        job.is_running_tasks = None;
                    }
                } else {
                    // No worker client available — leave unset
                    job.is_running_tasks = None;
                }
            }
        }

        Ok(rpc::GetAllJobInfoReply {
            job_info_list: jobs,
            ..Default::default()
        })
    }

    pub async fn get_next_job_id(&self) -> Result<rpc::GetNextJobIdReply, Status> {
        let job_id = self.job_manager.handle_get_next_job_id().await?;
        Ok(rpc::GetNextJobIdReply {
            job_id,
            ..Default::default()
        })
    }
}

#[tonic::async_trait]
impl rpc::job_info_gcs_service_server::JobInfoGcsService for JobInfoGcsServiceImpl {
    async fn add_job(
        &self,
        req: Request<rpc::AddJobRequest>,
    ) -> Result<Response<rpc::AddJobReply>, Status> {
        self.add_job(req.into_inner()).await.map(Response::new)
    }

    async fn mark_job_finished(
        &self,
        req: Request<rpc::MarkJobFinishedRequest>,
    ) -> Result<Response<rpc::MarkJobFinishedReply>, Status> {
        self.mark_job_finished(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_all_job_info(
        &self,
        req: Request<rpc::GetAllJobInfoRequest>,
    ) -> Result<Response<rpc::GetAllJobInfoReply>, Status> {
        self.get_all_job_info(req.into_inner()).await.map(Response::new)
    }

    async fn report_job_error(
        &self,
        req: Request<rpc::ReportJobErrorRequest>,
    ) -> Result<Response<rpc::ReportJobErrorReply>, Status> {
        if let Some(job_error) = req.into_inner().job_error {
            self.job_manager.handle_report_job_error(job_error)?;
        }
        Ok(Response::new(rpc::ReportJobErrorReply::default()))
    }

    async fn get_next_job_id(
        &self,
        _req: Request<rpc::GetNextJobIdRequest>,
    ) -> Result<Response<rpc::GetNextJobIdReply>, Status> {
        self.get_next_job_id().await.map(Response::new)
    }
}

// ─── NodeInfoGcsService ────────────────────────────────────────────────────

pub struct NodeInfoGcsServiceImpl {
    pub node_manager: Arc<GcsNodeManager>,
    pub autoscaler_state_manager: Option<Arc<GcsAutoscalerStateManager>>,
}

impl NodeInfoGcsServiceImpl {
    pub fn get_cluster_id(&self) -> Result<rpc::GetClusterIdReply, Status> {
        let cluster_id = self.node_manager.handle_get_cluster_id();
        Ok(rpc::GetClusterIdReply {
            cluster_id,
            ..Default::default()
        })
    }

    pub async fn register_node(
        &self,
        request: rpc::RegisterNodeRequest,
    ) -> Result<rpc::RegisterNodeReply, Status> {
        if let Some(node_info) = request.node_info {
            self.node_manager.handle_register_node(node_info).await?;
        }
        Ok(rpc::RegisterNodeReply::default())
    }

    pub async fn unregister_node(
        &self,
        request: rpc::UnregisterNodeRequest,
    ) -> Result<rpc::UnregisterNodeReply, Status> {
        self.node_manager
            .handle_unregister_node(&request.node_id, request.node_death_info)
            .await?;
        Ok(rpc::UnregisterNodeReply::default())
    }

    pub fn get_all_node_info(&self) -> Result<rpc::GetAllNodeInfoReply, Status> {
        let nodes = self.node_manager.handle_get_all_node_info();
        Ok(rpc::GetAllNodeInfoReply {
            node_info_list: nodes,
            ..Default::default()
        })
    }
}

#[tonic::async_trait]
impl rpc::node_info_gcs_service_server::NodeInfoGcsService for NodeInfoGcsServiceImpl {
    async fn get_cluster_id(
        &self,
        _req: Request<rpc::GetClusterIdRequest>,
    ) -> Result<Response<rpc::GetClusterIdReply>, Status> {
        self.get_cluster_id().map(Response::new)
    }

    async fn register_node(
        &self,
        req: Request<rpc::RegisterNodeRequest>,
    ) -> Result<Response<rpc::RegisterNodeReply>, Status> {
        self.register_node(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn unregister_node(
        &self,
        req: Request<rpc::UnregisterNodeRequest>,
    ) -> Result<Response<rpc::UnregisterNodeReply>, Status> {
        self.unregister_node(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn drain_node(
        &self,
        req: Request<rpc::DrainNodeRequest>,
    ) -> Result<Response<rpc::DrainNodeReply>, Status> {
        let request = req.into_inner();
        let mut drain_node_status = Vec::new();
        for data in &request.drain_node_data {
            let node_id = ray_common::id::NodeID::from_binary(data.node_id.as_slice());
            self.node_manager.handle_drain_node(&node_id, 0);
            // Also update the autoscaler drain state — both drain RPC surfaces
            // must produce the same observable state (C++ parity).
            if let Some(ref autoscaler) = self.autoscaler_state_manager {
                autoscaler.drain_node_with_deadline(&data.node_id, 0);
            }
            drain_node_status.push(rpc::DrainNodeStatus {
                node_id: data.node_id.clone(),
            });
        }
        Ok(Response::new(rpc::DrainNodeReply {
            drain_node_status,
            ..Default::default()
        }))
    }

    async fn get_all_node_info(
        &self,
        _req: Request<rpc::GetAllNodeInfoRequest>,
    ) -> Result<Response<rpc::GetAllNodeInfoReply>, Status> {
        self.get_all_node_info().map(Response::new)
    }

    async fn get_all_node_address_and_liveness(
        &self,
        req: Request<rpc::GetAllNodeAddressAndLivenessRequest>,
    ) -> Result<Response<rpc::GetAllNodeAddressAndLivenessReply>, Status> {
        let request = req.into_inner();
        let all_nodes = self.node_manager.handle_get_all_node_info();

        // Build a set of requested node IDs for filtering (if non-empty)
        let filter_by_ids = !request.node_ids.is_empty();
        let requested_ids: std::collections::HashSet<Vec<u8>> = if filter_by_ids {
            request.node_ids.into_iter().collect()
        } else {
            std::collections::HashSet::new()
        };

        // Optional state filter
        let state_filter = request.state_filter;

        let mut node_info_list: Vec<rpc::GcsNodeAddressAndLiveness> = all_nodes
            .into_iter()
            .filter(|node| {
                // Filter by node_ids if specified
                if filter_by_ids && !requested_ids.contains(&node.node_id) {
                    return false;
                }
                // Filter by state if specified
                if let Some(state) = state_filter {
                    if node.state != state {
                        return false;
                    }
                }
                true
            })
            .map(|node| rpc::GcsNodeAddressAndLiveness {
                node_id: node.node_id.clone(),
                node_manager_address: node.node_manager_address.clone(),
                node_manager_port: node.node_manager_port,
                object_manager_port: node.object_manager_port,
                state: node.state,
                death_info: node.death_info.clone(),
            })
            .collect();

        // Apply limit if specified
        if let Some(limit) = request.limit {
            if limit > 0 {
                node_info_list.truncate(limit as usize);
            }
        }

        Ok(Response::new(rpc::GetAllNodeAddressAndLivenessReply {
            node_info_list,
            ..Default::default()
        }))
    }

    async fn check_alive(
        &self,
        req: Request<rpc::CheckAliveRequest>,
    ) -> Result<Response<rpc::CheckAliveReply>, Status> {
        let request = req.into_inner();
        // Return alive=true for each node that's registered as alive
        let raylet_alive: Vec<bool> = request
            .node_ids
            .iter()
            .map(|node_id_bytes| {
                let node_id = ray_common::id::NodeID::from_binary(node_id_bytes.as_slice());
                self.node_manager.is_node_alive(&node_id)
            })
            .collect();
        Ok(Response::new(rpc::CheckAliveReply {
            raylet_alive,
            ray_version: env!("CARGO_PKG_VERSION").to_string(),
            ..Default::default()
        }))
    }
}

// ─── ActorInfoGcsService ───────────────────────────────────────────────────

pub struct ActorInfoGcsServiceImpl {
    pub actor_manager: Arc<GcsActorManager>,
}

impl ActorInfoGcsServiceImpl {
    pub async fn register_actor(
        &self,
        request: rpc::RegisterActorRequest,
    ) -> Result<rpc::RegisterActorReply, Status> {
        if let Some(task_spec) = request.task_spec {
            self.actor_manager.handle_register_actor(task_spec).await?;
        }
        Ok(rpc::RegisterActorReply::default())
    }

    pub fn get_actor_info(
        &self,
        request: rpc::GetActorInfoRequest,
    ) -> Result<rpc::GetActorInfoReply, Status> {
        let actor = self.actor_manager.handle_get_actor_info(&request.actor_id);
        Ok(rpc::GetActorInfoReply {
            actor_table_data: actor,
            ..Default::default()
        })
    }

    pub fn get_named_actor_info(
        &self,
        request: rpc::GetNamedActorInfoRequest,
    ) -> Result<rpc::GetNamedActorInfoReply, Status> {
        let result = self
            .actor_manager
            .handle_get_named_actor_info(&request.name, &request.ray_namespace);
        match result {
            Some((actor_data, task_spec)) => Ok(rpc::GetNamedActorInfoReply {
                actor_table_data: Some(actor_data),
                task_spec,
                ..Default::default()
            }),
            None => Ok(rpc::GetNamedActorInfoReply::default()),
        }
    }

    pub fn get_all_actor_info(
        &self,
        request: rpc::GetAllActorInfoRequest,
    ) -> Result<rpc::GetAllActorInfoReply, Status> {
        let limit = request.limit.filter(|&l| l > 0).map(|l| l as usize);
        let actors = self
            .actor_manager
            .handle_get_all_actor_info(limit, None, None);
        Ok(rpc::GetAllActorInfoReply {
            actor_table_data: actors,
            ..Default::default()
        })
    }

    pub async fn kill_actor_via_gcs(
        &self,
        request: rpc::KillActorViaGcsRequest,
    ) -> Result<rpc::KillActorViaGcsReply, Status> {
        self.actor_manager
            .handle_kill_actor(&request.actor_id)
            .await?;
        Ok(rpc::KillActorViaGcsReply::default())
    }
}

#[tonic::async_trait]
impl rpc::actor_info_gcs_service_server::ActorInfoGcsService for ActorInfoGcsServiceImpl {
    async fn register_actor(
        &self,
        req: Request<rpc::RegisterActorRequest>,
    ) -> Result<Response<rpc::RegisterActorReply>, Status> {
        self.register_actor(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn create_actor(
        &self,
        req: Request<rpc::CreateActorRequest>,
    ) -> Result<Response<rpc::CreateActorReply>, Status> {
        let request = req.into_inner();
        let task_spec = request
            .task_spec
            .ok_or_else(|| Status::invalid_argument("missing task_spec"))?;
        let rx = self.actor_manager.handle_create_actor(task_spec).await?;
        // Await until actor is ALIVE or creation fails
        let reply = rx
            .await
            .map_err(|_| Status::internal("actor creation cancelled"))??;
        Ok(Response::new(reply))
    }

    async fn restart_actor_for_lineage_reconstruction(
        &self,
        req: Request<rpc::RestartActorForLineageReconstructionRequest>,
    ) -> Result<Response<rpc::RestartActorForLineageReconstructionReply>, Status> {
        let request = req.into_inner();
        self.actor_manager
            .handle_restart_actor_for_lineage_reconstruction(
                &request.actor_id,
                request.num_restarts_due_to_lineage_reconstruction,
            )
            .await?;
        Ok(Response::new(
            rpc::RestartActorForLineageReconstructionReply::default(),
        ))
    }

    async fn get_actor_info(
        &self,
        req: Request<rpc::GetActorInfoRequest>,
    ) -> Result<Response<rpc::GetActorInfoReply>, Status> {
        self.get_actor_info(req.into_inner()).map(Response::new)
    }

    async fn get_named_actor_info(
        &self,
        req: Request<rpc::GetNamedActorInfoRequest>,
    ) -> Result<Response<rpc::GetNamedActorInfoReply>, Status> {
        self.get_named_actor_info(req.into_inner())
            .map(Response::new)
    }

    async fn list_named_actors(
        &self,
        req: Request<rpc::ListNamedActorsRequest>,
    ) -> Result<Response<rpc::ListNamedActorsReply>, Status> {
        let request = req.into_inner();
        let names = self
            .actor_manager
            .handle_list_named_actors(&request.ray_namespace, request.all_namespaces);
        Ok(Response::new(rpc::ListNamedActorsReply {
            named_actors_list: names
                .into_iter()
                .map(|(ns, name)| rpc::NamedActorInfo {
                    ray_namespace: ns,
                    name,
                })
                .collect(),
            ..Default::default()
        }))
    }

    async fn get_all_actor_info(
        &self,
        req: Request<rpc::GetAllActorInfoRequest>,
    ) -> Result<Response<rpc::GetAllActorInfoReply>, Status> {
        self.get_all_actor_info(req.into_inner()).map(Response::new)
    }

    async fn kill_actor_via_gcs(
        &self,
        req: Request<rpc::KillActorViaGcsRequest>,
    ) -> Result<Response<rpc::KillActorViaGcsReply>, Status> {
        self.kill_actor_via_gcs(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn report_actor_out_of_scope(
        &self,
        req: Request<rpc::ReportActorOutOfScopeRequest>,
    ) -> Result<Response<rpc::ReportActorOutOfScopeReply>, Status> {
        let request = req.into_inner();
        self.actor_manager.handle_report_actor_out_of_scope(
            &request.actor_id,
            request.num_restarts_due_to_lineage_reconstruction,
        )?;
        Ok(Response::new(rpc::ReportActorOutOfScopeReply::default()))
    }
}

// ─── InternalKVGcsService ──────────────────────────────────────────────────

pub struct InternalKVGcsServiceImpl {
    pub kv_manager: Arc<GcsInternalKVManager>,
}

impl InternalKVGcsServiceImpl {
    pub async fn internal_kv_get(
        &self,
        request: rpc::InternalKvGetRequest,
    ) -> Result<rpc::InternalKvGetReply, Status> {
        let value = self
            .kv_manager
            .handle_get(&request.namespace, &request.key)
            .await?;
        match value {
            Some(v) => Ok(rpc::InternalKvGetReply {
                value: v,
                ..Default::default()
            }),
            None => Ok(rpc::InternalKvGetReply {
                status: Some(rpc::GcsStatus {
                    code: 17, // NotFound
                    message: "Failed to find the key".to_string(),
                }),
                ..Default::default()
            }),
        }
    }

    pub async fn internal_kv_put(
        &self,
        request: rpc::InternalKvPutRequest,
    ) -> Result<rpc::InternalKvPutReply, Status> {
        let added = self
            .kv_manager
            .handle_put(
                &request.namespace,
                &request.key,
                request.value,
                request.overwrite,
            )
            .await?;
        Ok(rpc::InternalKvPutReply {
            added,
            ..Default::default()
        })
    }

    pub async fn internal_kv_del(
        &self,
        request: rpc::InternalKvDelRequest,
    ) -> Result<rpc::InternalKvDelReply, Status> {
        let num_deleted = self
            .kv_manager
            .handle_del(&request.namespace, &request.key, request.del_by_prefix)
            .await?;
        Ok(rpc::InternalKvDelReply {
            deleted_num: num_deleted as i32,
            ..Default::default()
        })
    }

    pub async fn internal_kv_exists(
        &self,
        request: rpc::InternalKvExistsRequest,
    ) -> Result<rpc::InternalKvExistsReply, Status> {
        let exists = self
            .kv_manager
            .handle_exists(&request.namespace, &request.key)
            .await?;
        Ok(rpc::InternalKvExistsReply {
            exists,
            ..Default::default()
        })
    }

    pub async fn internal_kv_keys(
        &self,
        request: rpc::InternalKvKeysRequest,
    ) -> Result<rpc::InternalKvKeysReply, Status> {
        let keys = self
            .kv_manager
            .handle_keys(&request.namespace, &request.prefix)
            .await?;
        Ok(rpc::InternalKvKeysReply {
            results: keys,
            ..Default::default()
        })
    }

    pub fn get_internal_config(&self) -> Result<rpc::GetInternalConfigReply, Status> {
        Ok(rpc::GetInternalConfigReply {
            config: self.kv_manager.raylet_config_list().to_string(),
            ..Default::default()
        })
    }
}

#[tonic::async_trait]
impl rpc::internal_kv_gcs_service_server::InternalKvGcsService for InternalKVGcsServiceImpl {
    async fn internal_kv_get(
        &self,
        req: Request<rpc::InternalKvGetRequest>,
    ) -> Result<Response<rpc::InternalKvGetReply>, Status> {
        self.internal_kv_get(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn internal_kv_multi_get(
        &self,
        req: Request<rpc::InternalKvMultiGetRequest>,
    ) -> Result<Response<rpc::InternalKvMultiGetReply>, Status> {
        let request = req.into_inner();
        let mut results = Vec::new();
        for key in &request.keys {
            if let Some(value) = self.kv_manager.handle_get(&request.namespace, key).await? {
                results.push(rpc::MapFieldEntry {
                    key: key.clone(),
                    value,
                });
            }
        }
        Ok(Response::new(rpc::InternalKvMultiGetReply {
            results,
            ..Default::default()
        }))
    }

    async fn internal_kv_put(
        &self,
        req: Request<rpc::InternalKvPutRequest>,
    ) -> Result<Response<rpc::InternalKvPutReply>, Status> {
        self.internal_kv_put(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn internal_kv_del(
        &self,
        req: Request<rpc::InternalKvDelRequest>,
    ) -> Result<Response<rpc::InternalKvDelReply>, Status> {
        self.internal_kv_del(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn internal_kv_exists(
        &self,
        req: Request<rpc::InternalKvExistsRequest>,
    ) -> Result<Response<rpc::InternalKvExistsReply>, Status> {
        self.internal_kv_exists(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn internal_kv_keys(
        &self,
        req: Request<rpc::InternalKvKeysRequest>,
    ) -> Result<Response<rpc::InternalKvKeysReply>, Status> {
        self.internal_kv_keys(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_internal_config(
        &self,
        _req: Request<rpc::GetInternalConfigRequest>,
    ) -> Result<Response<rpc::GetInternalConfigReply>, Status> {
        self.get_internal_config().map(Response::new)
    }
}

// ─── WorkerInfoGcsService ──────────────────────────────────────────────────

pub struct WorkerInfoGcsServiceImpl {
    pub worker_manager: Arc<GcsWorkerManager>,
}

#[tonic::async_trait]
impl rpc::worker_info_gcs_service_server::WorkerInfoGcsService for WorkerInfoGcsServiceImpl {
    async fn report_worker_failure(
        &self,
        req: Request<rpc::ReportWorkerFailureRequest>,
    ) -> Result<Response<rpc::ReportWorkerFailureReply>, Status> {
        let request = req.into_inner();
        if let Some(worker_failure) = request.worker_failure {
            self.worker_manager
                .handle_report_worker_failure(worker_failure)
                .await?;
        }
        Ok(Response::new(rpc::ReportWorkerFailureReply::default()))
    }

    async fn get_worker_info(
        &self,
        req: Request<rpc::GetWorkerInfoRequest>,
    ) -> Result<Response<rpc::GetWorkerInfoReply>, Status> {
        let request = req.into_inner();
        let worker_table_data = self
            .worker_manager
            .handle_get_worker_info(&request.worker_id)
            .await?;
        Ok(Response::new(rpc::GetWorkerInfoReply {
            worker_table_data,
            ..Default::default()
        }))
    }

    async fn get_all_worker_info(
        &self,
        req: Request<rpc::GetAllWorkerInfoRequest>,
    ) -> Result<Response<rpc::GetAllWorkerInfoReply>, Status> {
        let request = req.into_inner();
        let limit = request.limit.map(|l| l.max(0) as usize);
        let filters = request.filters.unwrap_or_default();
        let filter_exist_paused_threads = filters.exist_paused_threads.unwrap_or(false);
        let filter_is_alive = filters.is_alive.unwrap_or(false);
        let (worker_table_data, total, num_filtered) = self
            .worker_manager
            .handle_get_all_worker_info(limit, filter_exist_paused_threads, filter_is_alive)
            .await?;
        Ok(Response::new(rpc::GetAllWorkerInfoReply {
            worker_table_data,
            total,
            num_filtered,
            ..Default::default()
        }))
    }

    async fn add_worker_info(
        &self,
        req: Request<rpc::AddWorkerInfoRequest>,
    ) -> Result<Response<rpc::AddWorkerInfoReply>, Status> {
        let request = req.into_inner();
        if let Some(worker_data) = request.worker_data {
            self.worker_manager
                .handle_add_worker_info(worker_data)
                .await?;
        }
        Ok(Response::new(rpc::AddWorkerInfoReply::default()))
    }

    async fn update_worker_debugger_port(
        &self,
        req: Request<rpc::UpdateWorkerDebuggerPortRequest>,
    ) -> Result<Response<rpc::UpdateWorkerDebuggerPortReply>, Status> {
        let request = req.into_inner();
        self.worker_manager
            .handle_update_worker_debugger_port(&request.worker_id, request.debugger_port)
            .await?;
        Ok(Response::new(rpc::UpdateWorkerDebuggerPortReply::default()))
    }

    async fn update_worker_num_paused_threads(
        &self,
        req: Request<rpc::UpdateWorkerNumPausedThreadsRequest>,
    ) -> Result<Response<rpc::UpdateWorkerNumPausedThreadsReply>, Status> {
        let request = req.into_inner();
        self.worker_manager
            .handle_update_worker_num_paused_threads(
                &request.worker_id,
                request.num_paused_threads_delta,
            )
            .await?;
        Ok(Response::new(
            rpc::UpdateWorkerNumPausedThreadsReply::default(),
        ))
    }
}

// ─── PlacementGroupInfoGcsService ──────────────────────────────────────────

pub struct PlacementGroupInfoGcsServiceImpl {
    pub placement_group_manager: Arc<GcsPlacementGroupManager>,
}

#[tonic::async_trait]
impl rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService
    for PlacementGroupInfoGcsServiceImpl
{
    async fn create_placement_group(
        &self,
        req: Request<rpc::CreatePlacementGroupRequest>,
    ) -> Result<Response<rpc::CreatePlacementGroupReply>, Status> {
        let request = req.into_inner();
        if let Some(spec) = request.placement_group_spec {
            // Convert PlacementGroupSpec to PlacementGroupTableData for storage.
            let _pg_id_bytes = spec.placement_group_id.clone();
            let pg_data = rpc::PlacementGroupTableData {
                placement_group_id: spec.placement_group_id,
                name: spec.name,
                bundles: spec.bundles,
                strategy: spec.strategy,
                creator_job_id: spec.creator_job_id,
                creator_actor_id: spec.creator_actor_id,
                creator_job_dead: spec.creator_job_dead,
                creator_actor_dead: spec.creator_actor_dead,
                is_detached: spec.is_detached,
                // Initial state is PENDING (proper state machine).
                state: 0, // PlacementGroupState::Pending
                ..Default::default()
            };
            self.placement_group_manager
                .handle_create_placement_group(pg_data)
                .await?;

            // C++ contract: PG stays in PENDING state after creation.
            // Transitions to CREATED only when the scheduler successfully
            // places all bundles (OnPlacementGroupCreationSuccess).
            // The mark_placement_group_created() call must come from the
            // scheduling path, not from the gRPC handler.
        }
        Ok(Response::new(rpc::CreatePlacementGroupReply::default()))
    }

    async fn remove_placement_group(
        &self,
        req: Request<rpc::RemovePlacementGroupRequest>,
    ) -> Result<Response<rpc::RemovePlacementGroupReply>, Status> {
        let request = req.into_inner();
        self.placement_group_manager
            .handle_remove_placement_group(&request.placement_group_id)
            .await?;
        Ok(Response::new(rpc::RemovePlacementGroupReply::default()))
    }

    async fn get_placement_group(
        &self,
        req: Request<rpc::GetPlacementGroupRequest>,
    ) -> Result<Response<rpc::GetPlacementGroupReply>, Status> {
        let request = req.into_inner();
        let pg = self
            .placement_group_manager
            .handle_get_placement_group(&request.placement_group_id);
        Ok(Response::new(rpc::GetPlacementGroupReply {
            placement_group_table_data: pg,
            ..Default::default()
        }))
    }

    async fn get_named_placement_group(
        &self,
        req: Request<rpc::GetNamedPlacementGroupRequest>,
    ) -> Result<Response<rpc::GetNamedPlacementGroupReply>, Status> {
        let request = req.into_inner();
        let pg = self
            .placement_group_manager
            .handle_get_named_placement_group(&request.name, &request.ray_namespace);
        Ok(Response::new(rpc::GetNamedPlacementGroupReply {
            placement_group_table_data: pg,
            ..Default::default()
        }))
    }

    async fn get_all_placement_group(
        &self,
        req: Request<rpc::GetAllPlacementGroupRequest>,
    ) -> Result<Response<rpc::GetAllPlacementGroupReply>, Status> {
        let request = req.into_inner();
        let limit = request.limit.filter(|&l| l > 0).map(|l| l as usize);
        let pgs = self
            .placement_group_manager
            .handle_get_all_placement_groups(limit);
        Ok(Response::new(rpc::GetAllPlacementGroupReply {
            placement_group_table_data: pgs,
            ..Default::default()
        }))
    }

    async fn wait_placement_group_until_ready(
        &self,
        req: Request<rpc::WaitPlacementGroupUntilReadyRequest>,
    ) -> Result<Response<rpc::WaitPlacementGroupUntilReadyReply>, Status> {
        let request = req.into_inner();
        // Wait up to 30 seconds for the PG to become ready
        let timeout = std::time::Duration::from_secs(30);
        let ready = self
            .placement_group_manager
            .wait_until_ready(&request.placement_group_id, timeout)
            .await;
        let status = if ready {
            Some(rpc::GcsStatus {
                code: 0,
                message: "ready".into(),
            })
        } else {
            Some(rpc::GcsStatus {
                code: 1,
                message: "not ready".into(),
            })
        };
        Ok(Response::new(rpc::WaitPlacementGroupUntilReadyReply {
            status,
        }))
    }
}

// ─── NodeResourceInfoGcsService ────────────────────────────────────────────

pub struct NodeResourceInfoGcsServiceImpl {
    pub resource_manager: Arc<GcsResourceManager>,
}

#[tonic::async_trait]
impl rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsService
    for NodeResourceInfoGcsServiceImpl
{
    async fn get_all_available_resources(
        &self,
        _req: Request<rpc::GetAllAvailableResourcesRequest>,
    ) -> Result<Response<rpc::GetAllAvailableResourcesReply>, Status> {
        let resources = self.resource_manager.handle_get_all_available_resources();
        let resources_list = resources
            .into_iter()
            .map(|(node_id, available)| rpc::AvailableResources {
                node_id: node_id.binary().to_vec(),
                resources_available: available,
            })
            .collect();
        Ok(Response::new(rpc::GetAllAvailableResourcesReply {
            resources_list,
            ..Default::default()
        }))
    }

    async fn get_all_total_resources(
        &self,
        _req: Request<rpc::GetAllTotalResourcesRequest>,
    ) -> Result<Response<rpc::GetAllTotalResourcesReply>, Status> {
        let resources = self.resource_manager.handle_get_all_total_resources();
        let resources_list = resources
            .into_iter()
            .map(|(node_id, total)| rpc::TotalResources {
                node_id: node_id.binary().to_vec(),
                resources_total: total,
            })
            .collect();
        Ok(Response::new(rpc::GetAllTotalResourcesReply {
            resources_list,
            ..Default::default()
        }))
    }

    async fn get_all_resource_usage(
        &self,
        _req: Request<rpc::GetAllResourceUsageRequest>,
    ) -> Result<Response<rpc::GetAllResourceUsageReply>, Status> {
        // Return a batch of resource usage, one per node.
        let all_usage = self.resource_manager.get_all_resource_usage();
        let batch = rpc::ResourceUsageBatchData {
            batch: all_usage
                .into_iter()
                .map(|(node_id, usage)| rpc::ResourcesData {
                    node_id: node_id.binary().to_vec(),
                    resources_available: usage.available_resources,
                    resources_total: usage.total_resources,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        Ok(Response::new(rpc::GetAllResourceUsageReply {
            resource_usage_data: Some(batch),
            ..Default::default()
        }))
    }

    async fn get_draining_nodes(
        &self,
        _req: Request<rpc::GetDrainingNodesRequest>,
    ) -> Result<Response<rpc::GetDrainingNodesReply>, Status> {
        let draining = self.resource_manager.handle_get_draining_nodes();
        let draining_nodes = draining
            .into_iter()
            .map(|nid| rpc::DrainingNode {
                node_id: nid.binary().to_vec(),
                draining_deadline_timestamp_ms: 0,
            })
            .collect();
        Ok(Response::new(rpc::GetDrainingNodesReply {
            draining_nodes,
            ..Default::default()
        }))
    }
}

// ─── RuntimeEnvGcsService ──────────────────────────────────────────────────

pub struct RuntimeEnvGcsServiceImpl;

#[tonic::async_trait]
impl rpc::runtime_env_gcs_service_server::RuntimeEnvGcsService for RuntimeEnvGcsServiceImpl {
    async fn pin_runtime_env_uri(
        &self,
        _req: Request<rpc::PinRuntimeEnvUriRequest>,
    ) -> Result<Response<rpc::PinRuntimeEnvUriReply>, Status> {
        Ok(Response::new(rpc::PinRuntimeEnvUriReply::default()))
    }
}

// ─── InternalPubSubGcsService ──────────────────────────────────────────────

pub struct InternalPubSubGcsServiceImpl {
    pub pubsub_handler: Arc<InternalPubSubHandler>,
    /// Publisher ID (GCS node ID, 28 raw bytes). Must be non-empty.
    pub publisher_id: Vec<u8>,
}

#[tonic::async_trait]
impl rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsService
    for InternalPubSubGcsServiceImpl
{
    async fn gcs_publish(
        &self,
        req: Request<rpc::GcsPublishRequest>,
    ) -> Result<Response<rpc::GcsPublishReply>, Status> {
        let request = req.into_inner();
        self.pubsub_handler.handle_publish(request.pub_messages);
        Ok(Response::new(rpc::GcsPublishReply::default()))
    }

    async fn gcs_subscriber_poll(
        &self,
        req: Request<rpc::GcsSubscriberPollRequest>,
    ) -> Result<Response<rpc::GcsSubscriberPollReply>, Status> {
        let request = req.into_inner();
        // Long-poll with a timeout: wait for messages or 1 second
        let messages = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            self.pubsub_handler
                .handle_subscriber_poll(&request.subscriber_id, request.max_processed_sequence_id),
        )
        .await
        .unwrap_or_default();

        Ok(Response::new(rpc::GcsSubscriberPollReply {
            pub_messages: messages,
            publisher_id: self.publisher_id.clone(),
            ..Default::default()
        }))
    }

    async fn gcs_subscriber_command_batch(
        &self,
        req: Request<rpc::GcsSubscriberCommandBatchRequest>,
    ) -> Result<Response<rpc::GcsSubscriberCommandBatchReply>, Status> {
        let request = req.into_inner();
        // Use subscriber_id for subscription registration (matches C++ Publisher behavior).
        // sender_id is only used for the sender→subscriber mapping (for unregister-by-sender).
        let subscriber_id = request.subscriber_id.clone();

        for command in &request.commands {
            if command.command_message_one_of.is_some()
                && matches!(
                    command.command_message_one_of,
                    Some(rpc::command::CommandMessageOneOf::UnsubscribeMessage(_))
                )
            {
                self.pubsub_handler
                    .handle_unsubscribe_command(&subscriber_id, command.channel_type);
            } else {
                // Subscribe command
                self.pubsub_handler.handle_subscribe_command(
                    subscriber_id.clone(),
                    command.channel_type,
                    command.key_id.clone(),
                );
            }
        }

        Ok(Response::new(rpc::GcsSubscriberCommandBatchReply::default()))
    }
}

// ─── TaskInfoGcsService ────────────────────────────────────────────────────

pub struct TaskInfoGcsServiceImpl {
    pub task_manager: Arc<GcsTaskManager>,
}

#[tonic::async_trait]
impl rpc::task_info_gcs_service_server::TaskInfoGcsService for TaskInfoGcsServiceImpl {
    async fn add_task_event_data(
        &self,
        req: Request<rpc::AddTaskEventDataRequest>,
    ) -> Result<Response<rpc::AddTaskEventDataReply>, Status> {
        let request = req.into_inner();
        if let Some(data) = request.data {
            self.task_manager.handle_add_task_event_data(data);
        }
        Ok(Response::new(rpc::AddTaskEventDataReply::default()))
    }

    async fn get_task_events(
        &self,
        req: Request<rpc::GetTaskEventsRequest>,
    ) -> Result<Response<rpc::GetTaskEventsReply>, Status> {
        let request = req.into_inner();

        let limit = request.limit.filter(|&l| l > 0).map(|l| l as usize);

        // Extract job_id and task_ids from filters if present.
        let mut job_id = None;
        let mut task_ids_vec: Vec<ray_common::id::TaskID> = Vec::new();

        if let Some(ref filters) = request.filters {
            // Use the first job filter if present.
            if let Some(jf) = filters.job_filters.first() {
                if !jf.job_id.is_empty() {
                    job_id = Some(ray_common::id::JobID::from_binary(jf.job_id.as_slice()));
                }
            }
            // Collect task filters.
            for tf in &filters.task_filters {
                if !tf.task_id.is_empty() {
                    task_ids_vec.push(ray_common::id::TaskID::from_binary(tf.task_id.as_slice()));
                }
            }
        }

        let task_ids = if task_ids_vec.is_empty() {
            None
        } else {
            Some(task_ids_vec)
        };

        let events =
            self.task_manager
                .handle_get_task_events(job_id.as_ref(), task_ids.as_deref(), limit);
        Ok(Response::new(rpc::GetTaskEventsReply {
            events_by_task: events,
            ..Default::default()
        }))
    }
}

// ─── AutoscalerStateService ────────────────────────────────────────────────

pub struct AutoscalerStateServiceImpl {
    pub autoscaler_state_manager: Arc<GcsAutoscalerStateManager>,
    pub node_manager: Arc<GcsNodeManager>,
    pub placement_group_manager: Arc<GcsPlacementGroupManager>,
    /// Raylet client for forwarding drain requests (C++ parity: `raylet_client_pool_`).
    pub raylet_client: Option<Arc<dyn crate::actor_scheduler::RayletClient>>,
    /// Actor manager for preemption side effects (C++ parity: `gcs_actor_manager_`).
    pub actor_manager: Option<Arc<crate::actor_manager::GcsActorManager>>,
}

#[tonic::async_trait]
impl rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService
    for AutoscalerStateServiceImpl
{
    async fn get_cluster_resource_state(
        &self,
        _req: Request<rpc::autoscaler::GetClusterResourceStateRequest>,
    ) -> Result<Response<rpc::autoscaler::GetClusterResourceStateReply>, Status> {
        let (version, node_states) = self
            .autoscaler_state_manager
            .handle_get_cluster_resource_state();
        Ok(Response::new(
            rpc::autoscaler::GetClusterResourceStateReply {
                cluster_resource_state: Some(rpc::autoscaler::ClusterResourceState {
                    cluster_resource_state_version: version,
                    node_states,
                    ..Default::default()
                }),
            },
        ))
    }

    async fn report_autoscaling_state(
        &self,
        req: Request<rpc::autoscaler::ReportAutoscalingStateRequest>,
    ) -> Result<Response<rpc::autoscaler::ReportAutoscalingStateReply>, Status> {
        let inner = req.into_inner();
        if let Some(state) = inner.autoscaling_state {
            let version = state.autoscaler_state_version;
            let encoded = prost::Message::encode_to_vec(&state);
            self.autoscaler_state_manager
                .handle_report_autoscaling_state(encoded, version);
        }
        Ok(Response::new(
            rpc::autoscaler::ReportAutoscalingStateReply::default(),
        ))
    }

    async fn request_cluster_resource_constraint(
        &self,
        req: Request<rpc::autoscaler::RequestClusterResourceConstraintRequest>,
    ) -> Result<Response<rpc::autoscaler::RequestClusterResourceConstraintReply>, Status> {
        let inner = req.into_inner();
        // C++ contract: stores *request.mutable_cluster_resource_constraint(),
        // which is the inner ClusterResourceConstraint, NOT the whole request.
        let constraint = inner.cluster_resource_constraint.unwrap_or_default();
        let encoded = prost::Message::encode_to_vec(&constraint);
        self.autoscaler_state_manager
            .handle_request_cluster_resource_constraint(encoded);
        Ok(Response::new(
            rpc::autoscaler::RequestClusterResourceConstraintReply::default(),
        ))
    }

    async fn report_cluster_config(
        &self,
        _req: Request<rpc::autoscaler::ReportClusterConfigRequest>,
    ) -> Result<Response<rpc::autoscaler::ReportClusterConfigReply>, Status> {
        Ok(Response::new(
            rpc::autoscaler::ReportClusterConfigReply::default(),
        ))
    }

    async fn get_cluster_status(
        &self,
        _req: Request<rpc::autoscaler::GetClusterStatusRequest>,
    ) -> Result<Response<rpc::autoscaler::GetClusterStatusReply>, Status> {
        let (version, node_states) = self
            .autoscaler_state_manager
            .handle_get_cluster_resource_state();

        // Decode the last reported autoscaling state (if any).
        let autoscaling_state: Option<rpc::autoscaler::AutoscalingState> = self
            .autoscaler_state_manager
            .get_autoscaling_state()
            .and_then(|bytes| {
                prost::Message::decode(bytes.as_slice()).ok()
            });

        // Build pending_resource_requests from aggregated demands (C++ GetPendingResourceRequests).
        let pending_resource_requests: Vec<rpc::autoscaler::ResourceRequestByCount> = self
            .autoscaler_state_manager
            .get_aggregated_demands()
            .into_iter()
            .filter(|entry| entry.count + entry.infeasible_count > 0)
            .map(|entry| {
                let mut resources_bundle = std::collections::HashMap::new();
                for (name, amount) in &entry.shape {
                    resources_bundle.insert(name.clone(), *amount);
                }
                rpc::autoscaler::ResourceRequestByCount {
                    request: Some(rpc::autoscaler::ResourceRequest {
                        resources_bundle,
                        ..Default::default()
                    }),
                    count: (entry.count + entry.infeasible_count) as i64,
                }
            })
            .collect();

        // Build cluster_resource_constraints (C++ GetClusterResourceConstraints).
        let cluster_resource_constraints: Vec<rpc::autoscaler::ClusterResourceConstraint> = self
            .autoscaler_state_manager
            .get_cluster_resource_constraint()
            .and_then(|bytes| {
                prost::Message::decode(bytes.as_slice()).ok()
            })
            .into_iter()
            .collect();

        // Build pending_gang_resource_requests from placement group load
        // (C++ GetPendingGangResourceRequests).
        let pending_gang_resource_requests: Vec<rpc::autoscaler::GangResourceRequest> = self
            .placement_group_manager
            .get_placement_group_load()
            .into_iter()
            .map(|pg_data| {
                let pg_id_hex = hex::encode(&pg_data.placement_group_id);
                let mut gang_req = rpc::autoscaler::GangResourceRequest {
                    details: format!(
                        "placement_group:{}:{}:{}",
                        pg_data.name, pg_id_hex, pg_data.strategy
                    ),
                    ..Default::default()
                };
                let mut bundle_selector = rpc::autoscaler::BundleSelector::default();

                for bundle in &pg_data.bundles {
                    // C++: skip placed bundles (non-nil node_id)
                    if !bundle.node_id.is_empty()
                        && bundle.node_id.iter().any(|&b| b != 0)
                    {
                        continue;
                    }
                    let resources_bundle = bundle.unit_resources.clone();
                    // Legacy field
                    gang_req.requests.push(rpc::autoscaler::ResourceRequest {
                        resources_bundle: resources_bundle.clone(),
                        ..Default::default()
                    });
                    // BundleSelector field
                    bundle_selector
                        .resource_requests
                        .push(rpc::autoscaler::ResourceRequest {
                            resources_bundle,
                            ..Default::default()
                        });
                }
                gang_req.bundle_selectors.push(bundle_selector);
                gang_req
            })
            .collect();

        let cluster_resource_state = rpc::autoscaler::ClusterResourceState {
            cluster_resource_state_version: version,
            last_seen_autoscaler_state_version: self
                .autoscaler_state_manager
                .last_seen_autoscaler_version(),
            node_states,
            pending_resource_requests,
            pending_gang_resource_requests,
            cluster_resource_constraints,
            cluster_session_name: self.autoscaler_state_manager.session_name().to_string(),
        };

        Ok(Response::new(rpc::autoscaler::GetClusterStatusReply {
            autoscaling_state,
            cluster_resource_state: Some(cluster_resource_state),
            ..Default::default()
        }))
    }

    async fn drain_node(
        &self,
        req: Request<rpc::autoscaler::DrainNodeRequest>,
    ) -> Result<Response<rpc::autoscaler::DrainNodeReply>, Status> {
        let request = req.into_inner();
        let node_id = ray_common::id::NodeID::from_binary(request.node_id.as_slice());

        let draining_deadline_timestamp_ms = request.deadline_timestamp_ms;

        // C++ parity: reject negative deadlines (Status::Invalid in C++).
        if draining_deadline_timestamp_ms < 0 {
            return Err(Status::invalid_argument(format!(
                "Draining deadline must be non-negative, received {}",
                draining_deadline_timestamp_ms
            )));
        }

        // C++ parity: if node is not alive, check if it's dead or unknown.
        // Both cases return is_accepted=true (node is already gone).
        let maybe_node = self.node_manager.get_alive_node(&node_id);
        if maybe_node.is_none() {
            if self.node_manager.is_node_dead(&node_id) {
                tracing::info!(?node_id, "Request to drain a dead node, treat it as drained");
            } else {
                tracing::warn!(?node_id, "Request to drain an unknown node");
            }
            return Ok(Response::new(rpc::autoscaler::DrainNodeReply {
                is_accepted: true,
                ..Default::default()
            }));
        }
        let node = maybe_node.unwrap();

        // C++ parity: mark actors as preempted BEFORE forwarding to raylet.
        if let Some(ref actor_manager) = self.actor_manager {
            actor_manager.set_preempted_and_publish(&node_id);
        }

        // C++ parity: forward drain to raylet, gate acceptance on raylet reply.
        if let Some(ref raylet_client) = self.raylet_client {
            let addr = format!(
                "{}:{}",
                node.node_manager_address, node.node_manager_port
            );
            let raylet_request = rpc::DrainRayletRequest {
                reason: request.reason,
                reason_message: request.reason_message.clone(),
                deadline_timestamp_ms: draining_deadline_timestamp_ms,
                ..Default::default()
            };
            match raylet_client.drain_raylet(&addr, raylet_request).await {
                Ok(raylet_reply) => {
                    if raylet_reply.is_accepted {
                        // Only commit drain state after raylet accepts
                        self.node_manager.set_node_draining(
                            &node_id,
                            crate::node_manager::DrainNodeRequestInfo {
                                reason: request.reason,
                                reason_message: request.reason_message.clone(),
                                deadline_timestamp_ms: draining_deadline_timestamp_ms,
                            },
                        );
                        self.autoscaler_state_manager
                            .drain_node_with_deadline(&request.node_id, draining_deadline_timestamp_ms);
                        return Ok(Response::new(rpc::autoscaler::DrainNodeReply {
                            is_accepted: true,
                            ..Default::default()
                        }));
                    } else {
                        tracing::info!(
                            ?node_id,
                            reason = raylet_reply.rejection_reason_message,
                            "Node drain rejected by raylet"
                        );
                        return Ok(Response::new(rpc::autoscaler::DrainNodeReply {
                            is_accepted: false,
                            rejection_reason_message: raylet_reply.rejection_reason_message,
                        }));
                    }
                }
                Err(status) => {
                    tracing::warn!(?node_id, ?status, "Failed to contact raylet for drain");
                    return Err(status);
                }
            }
        }

        // Fallback: no raylet client configured (e.g., tests without raylet wiring).
        // Commit drain state directly (preserves backward compat with existing tests).
        self.node_manager.set_node_draining(
            &node_id,
            crate::node_manager::DrainNodeRequestInfo {
                reason: request.reason,
                reason_message: request.reason_message.clone(),
                deadline_timestamp_ms: draining_deadline_timestamp_ms,
            },
        );
        self.autoscaler_state_manager
            .drain_node_with_deadline(&request.node_id, draining_deadline_timestamp_ms);

        Ok(Response::new(rpc::autoscaler::DrainNodeReply {
            is_accepted: true,
            ..Default::default()
        }))
    }
}

#[cfg(test)]
#[allow(clippy::needless_update)]
mod tests {
    use super::*;
    use crate::autoscaler_state_manager::GcsAutoscalerStateManager;
    use crate::pubsub_handler::InternalPubSubHandler;
    use crate::resource_manager::GcsResourceManager;
    use crate::store_client::{InMemoryInternalKV, InMemoryStoreClient};
    use crate::table_storage::GcsTableStorage;
    use crate::task_manager::GcsTaskManager;

    // ─── Helpers ───────────────────────────────────────────────────────

    fn make_store() -> (Arc<InMemoryStoreClient>, Arc<GcsTableStorage>) {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store.clone()));
        (store, storage)
    }

    fn make_node_info(id: u8) -> rpc::GcsNodeInfo {
        let mut node_id = vec![0u8; 28];
        node_id[0] = id;
        rpc::GcsNodeInfo {
            node_id,
            node_name: format!("node-{}", id),
            node_manager_address: "127.0.0.1".to_string(),
            node_manager_port: 10000 + id as i32,
            object_manager_port: 20000 + id as i32,
            state: 0, // ALIVE
            ..Default::default()
        }
    }

    fn node_id_bytes(id: u8) -> Vec<u8> {
        let mut v = vec![0u8; 28];
        v[0] = id;
        v
    }

    fn make_autoscaler_svc(
        storage: Arc<GcsTableStorage>,
        node_manager: Arc<GcsNodeManager>,
    ) -> (
        AutoscalerStateServiceImpl,
        Arc<GcsAutoscalerStateManager>,
        Arc<GcsPlacementGroupManager>,
    ) {
        let resource_manager = Arc::new(GcsResourceManager::new());
        let autoscaler_state_manager = Arc::new(GcsAutoscalerStateManager::new(
            "test".to_string(),
            node_manager.clone(),
            resource_manager,
        ));
        let placement_group_manager = Arc::new(GcsPlacementGroupManager::new(storage));
        let svc = AutoscalerStateServiceImpl {
            autoscaler_state_manager: autoscaler_state_manager.clone(),
            node_manager,
            placement_group_manager: placement_group_manager.clone(),
            raylet_client: None,
            actor_manager: None,
        };
        (svc, autoscaler_state_manager, placement_group_manager)
    }

    // ─── KV Service ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_kv_grpc_service() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let kv_manager = Arc::new(GcsInternalKVManager::new(kv, "config".into()));
        let svc = InternalKVGcsServiceImpl { kv_manager };

        // Put
        let put_req = rpc::InternalKvPutRequest {
            namespace: b"ns".to_vec(),
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
            overwrite: true,
            ..Default::default()
        };
        let reply = svc.internal_kv_put(put_req).await.unwrap();
        assert!(reply.added);

        // Get
        let get_req = rpc::InternalKvGetRequest {
            namespace: b"ns".to_vec(),
            key: b"key1".to_vec(),
            ..Default::default()
        };
        let reply = svc.internal_kv_get(get_req).await.unwrap();
        assert_eq!(reply.value, b"val1");

        // Exists
        let exists_req = rpc::InternalKvExistsRequest {
            namespace: b"ns".to_vec(),
            key: b"key1".to_vec(),
            ..Default::default()
        };
        let reply = svc.internal_kv_exists(exists_req).await.unwrap();
        assert!(reply.exists);
    }

    #[tokio::test]
    async fn test_kv_grpc_get_not_found() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let kv_manager = Arc::new(GcsInternalKVManager::new(kv, "cfg".into()));
        let svc = InternalKVGcsServiceImpl { kv_manager };

        let reply = svc
            .internal_kv_get(rpc::InternalKvGetRequest {
                namespace: b"ns".to_vec(),
                key: b"missing".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        // Should return NotFound status (code 17)
        assert!(reply.status.is_some());
        assert_eq!(reply.status.unwrap().code, 17);
    }

    #[tokio::test]
    async fn test_kv_grpc_del() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let kv_manager = Arc::new(GcsInternalKVManager::new(kv, "cfg".into()));
        let svc = InternalKVGcsServiceImpl { kv_manager };

        // Put two keys
        svc.internal_kv_put(rpc::InternalKvPutRequest {
            namespace: b"ns".to_vec(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            overwrite: true,
            ..Default::default()
        })
        .await
        .unwrap();
        svc.internal_kv_put(rpc::InternalKvPutRequest {
            namespace: b"ns".to_vec(),
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
            overwrite: true,
            ..Default::default()
        })
        .await
        .unwrap();

        // Delete one
        let reply = svc
            .internal_kv_del(rpc::InternalKvDelRequest {
                namespace: b"ns".to_vec(),
                key: b"k1".to_vec(),
                del_by_prefix: false,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(reply.deleted_num, 1);

        // k1 gone, k2 still there
        let reply = svc
            .internal_kv_exists(rpc::InternalKvExistsRequest {
                namespace: b"ns".to_vec(),
                key: b"k1".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!reply.exists);
        let reply = svc
            .internal_kv_exists(rpc::InternalKvExistsRequest {
                namespace: b"ns".to_vec(),
                key: b"k2".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(reply.exists);
    }

    #[tokio::test]
    async fn test_kv_grpc_keys() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let kv_manager = Arc::new(GcsInternalKVManager::new(kv, "cfg".into()));
        let svc = InternalKVGcsServiceImpl { kv_manager };

        for i in 0..3 {
            svc.internal_kv_put(rpc::InternalKvPutRequest {
                namespace: b"ns".to_vec(),
                key: format!("prefix/{}", i).into_bytes(),
                value: b"v".to_vec(),
                overwrite: true,
                ..Default::default()
            })
            .await
            .unwrap();
        }
        svc.internal_kv_put(rpc::InternalKvPutRequest {
            namespace: b"ns".to_vec(),
            key: b"other".to_vec(),
            value: b"v".to_vec(),
            overwrite: true,
            ..Default::default()
        })
        .await
        .unwrap();

        let reply = svc
            .internal_kv_keys(rpc::InternalKvKeysRequest {
                namespace: b"ns".to_vec(),
                prefix: b"prefix/".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(reply.results.len(), 3);
    }

    #[tokio::test]
    async fn test_kv_grpc_multi_get() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let kv_manager = Arc::new(GcsInternalKVManager::new(kv, "cfg".into()));
        let svc = InternalKVGcsServiceImpl {
            kv_manager: kv_manager.clone(),
        };

        // Put two keys
        kv_manager
            .handle_put(b"ns", b"a", b"1".to_vec(), true)
            .await
            .unwrap();
        kv_manager
            .handle_put(b"ns", b"b", b"2".to_vec(), true)
            .await
            .unwrap();

        // Multi-get via tonic trait
        use rpc::internal_kv_gcs_service_server::InternalKvGcsService;
        let reply = svc
            .internal_kv_multi_get(Request::new(rpc::InternalKvMultiGetRequest {
                namespace: b"ns".to_vec(),
                keys: vec![b"a".to_vec(), b"b".to_vec(), b"missing".to_vec()],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.results.len(), 2);
    }

    #[tokio::test]
    async fn test_kv_grpc_get_internal_config() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let kv_manager = Arc::new(GcsInternalKVManager::new(kv, "my_config_data".into()));
        let svc = InternalKVGcsServiceImpl { kv_manager };

        let reply = svc.get_internal_config().unwrap();
        assert_eq!(reply.config, "my_config_data");
    }

    // ─── Job Service ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_job_grpc_service() {
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let svc = JobInfoGcsServiceImpl { job_manager, kv_manager: None, worker_client: None };

        let add_req = rpc::AddJobRequest {
            data: Some(rpc::JobTableData {
                job_id: vec![1, 0, 0, 0],
                ..Default::default()
            }),
            ..Default::default()
        };
        svc.add_job(add_req).await.unwrap();

        let get_req = rpc::GetAllJobInfoRequest::default();
        let reply = svc.get_all_job_info(get_req).await.unwrap();
        assert_eq!(reply.job_info_list.len(), 1);
    }

    #[tokio::test]
    async fn test_job_grpc_mark_finished() {
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let svc = JobInfoGcsServiceImpl {
            job_manager: job_manager.clone(),
            kv_manager: None,
            worker_client: None,
        };

        svc.add_job(rpc::AddJobRequest {
            data: Some(rpc::JobTableData {
                job_id: vec![1, 0, 0, 0],
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        svc.mark_job_finished(rpc::MarkJobFinishedRequest {
            job_id: vec![1, 0, 0, 0],
            ..Default::default()
        })
        .await
        .unwrap();

        assert_eq!(job_manager.num_running_jobs(), 0);
        assert_eq!(job_manager.finished_jobs_count(), 1);
    }

    #[tokio::test]
    async fn test_job_grpc_get_next_job_id() {
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let svc = JobInfoGcsServiceImpl { job_manager, kv_manager: None, worker_client: None };

        let reply1 = svc.get_next_job_id().await.unwrap();
        let reply2 = svc.get_next_job_id().await.unwrap();
        assert_eq!(reply2.job_id, reply1.job_id + 1);
    }

    #[tokio::test]
    async fn test_job_grpc_get_all_with_limit() {
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let svc = JobInfoGcsServiceImpl { job_manager, kv_manager: None, worker_client: None };

        for i in 1..=5u8 {
            svc.add_job(rpc::AddJobRequest {
                data: Some(rpc::JobTableData {
                    job_id: vec![i, 0, 0, 0],
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        }

        let reply = svc
            .get_all_job_info(rpc::GetAllJobInfoRequest {
                limit: Some(3),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(reply.job_info_list.len(), 3);
    }

    #[tokio::test]
    async fn test_job_grpc_report_job_error_publishes_pubsub_message() {
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let pubsub_handler = Arc::new(InternalPubSubHandler::new());
        job_manager.set_pubsub_handler(pubsub_handler.clone());
        let svc = JobInfoGcsServiceImpl { job_manager, kv_manager: None, worker_client: None };

        let subscriber_id = b"job-error-sub".to_vec();
        pubsub_handler.handle_subscribe_command(
            subscriber_id.clone(),
            crate::pubsub_handler::ChannelType::RayErrorInfoChannel as i32,
            vec![],
        );

        use rpc::job_info_gcs_service_server::JobInfoGcsService;
        svc.report_job_error(Request::new(rpc::ReportJobErrorRequest {
            job_error: Some(rpc::ErrorTableData {
                job_id: vec![9, 0, 0, 0],
                r#type: "task_error".into(),
                error_message: "traceback".into(),
                timestamp: 456.0,
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let pending = pubsub_handler.pending_messages_for_test(&subscriber_id);
        assert_eq!(pending.len(), 1);
        let (msg, _) = &pending[0];
        assert_eq!(
            msg.channel_type,
            crate::pubsub_handler::ChannelType::RayErrorInfoChannel as i32
        );
        match msg.inner_message.as_ref() {
            Some(rpc::pub_message::InnerMessage::ErrorInfoMessage(error)) => {
                assert_eq!(error.r#type, "task_error");
                assert_eq!(error.error_message, "traceback");
                assert_eq!(error.job_id, vec![9, 0, 0, 0]);
            }
            other => panic!("expected error info message, got {other:?}"),
        }
    }

    // ─── GCS-4 Round 5: is_running_tasks from real driver RPC ────────

    /// C++ contract: alive job with no pending tasks → is_running_tasks = false.
    /// This proves the heuristic (!is_dead → true) has been replaced with a real signal.
    #[tokio::test]
    async fn test_get_all_job_info_alive_job_with_no_pending_tasks_reports_false() {
        use crate::actor_scheduler::tests::MockCoreWorkerClient;
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let mock_client = Arc::new(MockCoreWorkerClient::new());
        // Driver reports 0 pending tasks
        mock_client.push_pending_tasks_reply(Ok(rpc::NumPendingTasksReply {
            num_pending_tasks: 0,
        }));

        let svc = JobInfoGcsServiceImpl {
            job_manager: job_manager.clone(),
            kv_manager: None,
            worker_client: Some(mock_client),
        };

        // Add an alive job with a driver address
        svc.add_job(rpc::AddJobRequest {
            data: Some(rpc::JobTableData {
                job_id: vec![1, 0, 0, 0],
                driver_address: Some(rpc::Address {
                    ip_address: "127.0.0.1".to_string(),
                    port: 50000,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        let reply = svc
            .get_all_job_info(rpc::GetAllJobInfoRequest::default())
            .await
            .unwrap();
        assert_eq!(reply.job_info_list.len(), 1);
        // The job is alive, but driver reports 0 pending tasks → false
        assert_eq!(
            reply.job_info_list[0].is_running_tasks,
            Some(false),
            "alive job with 0 pending tasks must report is_running_tasks=false"
        );
    }

    /// C++ contract: alive job with pending tasks → is_running_tasks = true
    /// via NumPendingTasksRequest RPC to the driver.
    #[tokio::test]
    async fn test_get_all_job_info_uses_driver_pending_tasks_rpc_for_is_running_tasks() {
        use crate::actor_scheduler::tests::MockCoreWorkerClient;
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let mock_client = Arc::new(MockCoreWorkerClient::new());
        // Driver reports 5 pending tasks
        mock_client.push_pending_tasks_reply(Ok(rpc::NumPendingTasksReply {
            num_pending_tasks: 5,
        }));

        let svc = JobInfoGcsServiceImpl {
            job_manager: job_manager.clone(),
            kv_manager: None,
            worker_client: Some(mock_client),
        };

        svc.add_job(rpc::AddJobRequest {
            data: Some(rpc::JobTableData {
                job_id: vec![2, 0, 0, 0],
                driver_address: Some(rpc::Address {
                    ip_address: "127.0.0.1".to_string(),
                    port: 50001,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        let reply = svc
            .get_all_job_info(rpc::GetAllJobInfoRequest::default())
            .await
            .unwrap();
        assert_eq!(reply.job_info_list.len(), 1);
        assert_eq!(
            reply.job_info_list[0].is_running_tasks,
            Some(true),
            "alive job with pending tasks must report is_running_tasks=true"
        );
    }

    // ─── GCS-4 Round 6: limit semantics + full JobsAPIInfo enrichment ──

    /// C++ contract: limit == 0 → return zero jobs.
    #[tokio::test]
    async fn test_get_all_job_info_limit_zero_returns_zero_jobs() {
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let svc = JobInfoGcsServiceImpl { job_manager: job_manager.clone(), kv_manager: None, worker_client: None };

        // Add 3 jobs
        for i in 1..=3u8 {
            svc.add_job(rpc::AddJobRequest {
                data: Some(rpc::JobTableData { job_id: vec![i, 0, 0, 0], ..Default::default() }),
                ..Default::default()
            }).await.unwrap();
        }

        let reply = svc.get_all_job_info(rpc::GetAllJobInfoRequest {
            limit: Some(0),
            ..Default::default()
        }).await.unwrap();
        assert_eq!(reply.job_info_list.len(), 0, "limit=0 must return zero jobs");
    }

    /// C++ contract: negative limit → Status::Invalid error.
    #[tokio::test]
    async fn test_get_all_job_info_negative_limit_is_invalid() {
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let svc = JobInfoGcsServiceImpl { job_manager, kv_manager: None, worker_client: None };

        let result = svc.get_all_job_info(rpc::GetAllJobInfoRequest {
            limit: Some(-1),
            ..Default::default()
        }).await;
        assert!(result.is_err(), "negative limit must be rejected");
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    /// C++ contract: JobsAPIInfo enrichment from KV populates ALL proto fields,
    /// not just status/entrypoint/message.
    #[tokio::test]
    async fn test_get_all_job_info_enriches_full_jobs_api_info_from_internal_kv() {
        use crate::store_client::{InMemoryInternalKV, InternalKVInterface};
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let kv: Arc<InMemoryInternalKV> = Arc::new(InMemoryInternalKV::new());
        let kv_mgr = Arc::new(crate::kv_manager::GcsInternalKVManager::new(kv.clone() as Arc<dyn InternalKVInterface>, String::new()));

        // Store full JobsAPIInfo JSON in KV
        let job_json = serde_json::json!({
            "status": "RUNNING",
            "entrypoint": "python train.py",
            "message": "Job is running",
            "error_type": "RUNTIME_ENV_SETUP_FAILED",
            "start_time": 1710000000000u64,
            "end_time": 1710003600000u64,
            "metadata": {"user": "alice", "team": "ml"},
            "runtime_env_json": "{\"pip\": [\"torch\"]}",
            "entrypoint_num_cpus": 4.0,
            "entrypoint_num_gpus": 2.0,
            "entrypoint_resources": {"TPU": 1.0},
            "driver_agent_http_address": "http://127.0.0.1:8265",
            "driver_node_id": "abc123",
            "driver_exit_code": 0,
            "entrypoint_memory": 8589934592u64
        });
        kv.put(b"job", b"__ray_internal__job_info_sub001", serde_json::to_vec(&job_json).unwrap(), true).await.unwrap();

        let svc = JobInfoGcsServiceImpl {
            job_manager: job_manager.clone(),
            kv_manager: Some(kv_mgr),
            worker_client: None,
        };

        // Add job with submission ID matching the KV key
        svc.add_job(rpc::AddJobRequest {
            data: Some(rpc::JobTableData {
                job_id: vec![1, 0, 0, 0],
                config: Some(rpc::JobConfig {
                    metadata: std::collections::HashMap::from([("job_submission_id".to_string(), "sub001".to_string())]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }).await.unwrap();

        let reply = svc.get_all_job_info(rpc::GetAllJobInfoRequest {
            skip_is_running_tasks_field: Some(true),
            ..Default::default()
        }).await.unwrap();

        assert_eq!(reply.job_info_list.len(), 1);
        let info = reply.job_info_list[0].job_info.as_ref().expect("job_info must be enriched");
        assert_eq!(info.status, "RUNNING");
        assert_eq!(info.entrypoint, "python train.py");
        assert_eq!(info.message.as_deref(), Some("Job is running"));
        assert_eq!(info.error_type.as_deref(), Some("RUNTIME_ENV_SETUP_FAILED"));
        assert_eq!(info.start_time, Some(1710000000000));
        assert_eq!(info.end_time, Some(1710003600000));
        assert_eq!(info.runtime_env_json.as_deref(), Some("{\"pip\": [\"torch\"]}"));
        assert_eq!(info.entrypoint_num_cpus, Some(4.0));
        assert_eq!(info.entrypoint_num_gpus, Some(2.0));
        assert!((info.entrypoint_resources.get("TPU").copied().unwrap_or(0.0) - 1.0).abs() < 0.001);
        assert_eq!(info.driver_agent_http_address.as_deref(), Some("http://127.0.0.1:8265"));
        assert_eq!(info.driver_node_id.as_deref(), Some("abc123"));
        assert_eq!(info.driver_exit_code, Some(0));
        assert_eq!(info.entrypoint_memory, Some(8589934592));
        assert_eq!(info.metadata.get("user").map(|s| s.as_str()), Some("alice"));
        assert_eq!(info.metadata.get("team").map(|s| s.as_str()), Some("ml"));
    }

    // ─── Node Service ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_node_grpc_register_and_get_all() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        svc.register_node(rpc::RegisterNodeRequest {
            node_info: Some(make_node_info(1)),
            ..Default::default()
        })
        .await
        .unwrap();
        svc.register_node(rpc::RegisterNodeRequest {
            node_info: Some(make_node_info(2)),
            ..Default::default()
        })
        .await
        .unwrap();

        let reply = svc.get_all_node_info().unwrap();
        assert_eq!(reply.node_info_list.len(), 2);
    }

    #[tokio::test]
    async fn test_node_grpc_unregister() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        svc.register_node(rpc::RegisterNodeRequest {
            node_info: Some(make_node_info(1)),
            ..Default::default()
        })
        .await
        .unwrap();

        svc.unregister_node(rpc::UnregisterNodeRequest {
            node_id: node_id_bytes(1),
            ..Default::default()
        })
        .await
        .unwrap();

        // Should still appear in get_all (alive + dead)
        let reply = svc.get_all_node_info().unwrap();
        assert_eq!(reply.node_info_list.len(), 1);
        assert_eq!(reply.node_info_list[0].state, 1); // DEAD
    }

    #[tokio::test]
    async fn test_node_grpc_get_cluster_id() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        let cluster_id = vec![42u8; 28];
        node_manager.set_cluster_id(cluster_id.clone());

        let reply = svc.get_cluster_id().unwrap();
        assert_eq!(reply.cluster_id, cluster_id);
    }

    #[tokio::test]
    async fn test_node_grpc_check_alive() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();

        use rpc::node_info_gcs_service_server::NodeInfoGcsService;
        let reply = svc
            .check_alive(Request::new(rpc::CheckAliveRequest {
                node_ids: vec![node_id_bytes(1), node_id_bytes(99)],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.raylet_alive.len(), 2);
        assert!(reply.raylet_alive[0]); // node 1 alive
        assert!(!reply.raylet_alive[1]); // node 99 not registered
        assert!(!reply.ray_version.is_empty());
    }

    #[tokio::test]
    async fn test_node_grpc_drain_node() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();

        use rpc::node_info_gcs_service_server::NodeInfoGcsService;
        let reply = svc
            .drain_node(Request::new(rpc::DrainNodeRequest {
                drain_node_data: vec![rpc::DrainNodeData {
                    node_id: node_id_bytes(1),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.drain_node_status.len(), 1);
        assert_eq!(reply.drain_node_status[0].node_id, node_id_bytes(1));
    }

    #[tokio::test]
    async fn test_node_grpc_get_all_address_and_liveness() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();

        use rpc::node_info_gcs_service_server::NodeInfoGcsService;
        let reply = svc
            .get_all_node_address_and_liveness(Request::new(
                rpc::GetAllNodeAddressAndLivenessRequest::default(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 1);
        assert_eq!(reply.node_info_list[0].node_manager_port, 10001);
        assert_eq!(reply.node_info_list[0].state, 0); // ALIVE
    }

    // ─── Actor Service ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_actor_grpc_register_and_get() {
        let (_, storage) = make_store();
        let actor_manager = Arc::new(GcsActorManager::new(storage));
        let svc = ActorInfoGcsServiceImpl {
            actor_manager: actor_manager.clone(),
        };

        let mut actor_id = vec![0u8; 16];
        actor_id[0] = 1;
        svc.register_actor(rpc::RegisterActorRequest {
            task_spec: Some(rpc::TaskSpec {
                actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                    actor_id: actor_id.clone(),
                    name: "my_actor".to_string(),
                    ray_namespace: "default".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        let reply = svc
            .get_actor_info(rpc::GetActorInfoRequest {
                actor_id: actor_id.clone(),
                ..Default::default()
            })
            .unwrap();
        assert!(reply.actor_table_data.is_some());
        assert_eq!(reply.actor_table_data.unwrap().name, "my_actor");
    }

    #[tokio::test]
    async fn test_actor_grpc_get_named_actor_info() {
        let (_, storage) = make_store();
        let actor_manager = Arc::new(GcsActorManager::new(storage));
        let svc = ActorInfoGcsServiceImpl {
            actor_manager: actor_manager.clone(),
        };

        let mut actor_id = vec![0u8; 16];
        actor_id[0] = 1;
        svc.register_actor(rpc::RegisterActorRequest {
            task_spec: Some(rpc::TaskSpec {
                actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                    actor_id,
                    name: "named".to_string(),
                    ray_namespace: "ns1".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        let reply = svc
            .get_named_actor_info(rpc::GetNamedActorInfoRequest {
                name: "named".to_string(),
                ray_namespace: "ns1".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert!(reply.actor_table_data.is_some());

        // Different namespace should not find it
        let reply = svc
            .get_named_actor_info(rpc::GetNamedActorInfoRequest {
                name: "named".to_string(),
                ray_namespace: "ns2".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert!(reply.actor_table_data.is_none());
    }

    #[tokio::test]
    async fn test_actor_grpc_list_named_actors() {
        let (_, storage) = make_store();
        let actor_manager = Arc::new(GcsActorManager::new(storage));
        let svc = ActorInfoGcsServiceImpl {
            actor_manager: actor_manager.clone(),
        };

        for i in 1..=3u8 {
            let mut aid = vec![0u8; 16];
            aid[0] = i;
            svc.register_actor(rpc::RegisterActorRequest {
                task_spec: Some(rpc::TaskSpec {
                    actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                        actor_id: aid,
                        name: format!("actor_{}", i),
                        ray_namespace: "default".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        }

        use rpc::actor_info_gcs_service_server::ActorInfoGcsService;
        let reply = svc
            .list_named_actors(Request::new(rpc::ListNamedActorsRequest {
                ray_namespace: "default".to_string(),
                all_namespaces: false,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.named_actors_list.len(), 3);
    }

    #[tokio::test]
    async fn test_actor_grpc_get_all_actor_info() {
        let (_, storage) = make_store();
        let actor_manager = Arc::new(GcsActorManager::new(storage));
        let svc = ActorInfoGcsServiceImpl {
            actor_manager: actor_manager.clone(),
        };

        for i in 1..=4u8 {
            let mut aid = vec![0u8; 16];
            aid[0] = i;
            svc.register_actor(rpc::RegisterActorRequest {
                task_spec: Some(rpc::TaskSpec {
                    actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                        actor_id: aid,
                        name: format!("a{}", i),
                        ray_namespace: "default".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        }

        let reply = svc
            .get_all_actor_info(rpc::GetAllActorInfoRequest::default())
            .unwrap();
        assert_eq!(reply.actor_table_data.len(), 4);

        // With limit
        let reply = svc
            .get_all_actor_info(rpc::GetAllActorInfoRequest {
                limit: Some(2),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(reply.actor_table_data.len(), 2);
    }

    #[tokio::test]
    async fn test_actor_grpc_kill_actor() {
        let (_, storage) = make_store();
        let actor_manager = Arc::new(GcsActorManager::new(storage));
        let svc = ActorInfoGcsServiceImpl {
            actor_manager: actor_manager.clone(),
        };

        let mut actor_id = vec![0u8; 16];
        actor_id[0] = 1;
        svc.register_actor(rpc::RegisterActorRequest {
            task_spec: Some(rpc::TaskSpec {
                actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                    actor_id: actor_id.clone(),
                    name: "killme".to_string(),
                    ray_namespace: "default".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        svc.kill_actor_via_gcs(rpc::KillActorViaGcsRequest {
            actor_id: actor_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

        assert_eq!(actor_manager.num_registered_actors(), 0);
    }

    #[tokio::test]
    async fn test_actor_grpc_report_out_of_scope_marks_dead() {
        let (_, storage) = make_store();
        let actor_manager = Arc::new(GcsActorManager::new(storage));
        let svc = ActorInfoGcsServiceImpl {
            actor_manager: actor_manager.clone(),
        };

        let mut actor_id = vec![0u8; 16];
        actor_id[0] = 7;
        svc.register_actor(rpc::RegisterActorRequest {
            task_spec: Some(rpc::TaskSpec {
                actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                    actor_id: actor_id.clone(),
                    name: "scoped_actor".to_string(),
                    ray_namespace: "default".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        use rpc::actor_info_gcs_service_server::ActorInfoGcsService;
        svc.report_actor_out_of_scope(Request::new(rpc::ReportActorOutOfScopeRequest {
            actor_id: actor_id.clone(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();

        let actor = actor_manager.handle_get_actor_info(&actor_id).unwrap();
        assert_eq!(actor.state, crate::actor_manager::ActorState::Dead as i32);
        assert!(actor_manager
            .handle_get_named_actor_info("scoped_actor", "default")
            .is_none());
    }

    #[tokio::test]
    async fn test_actor_grpc_restart_lineage_reconstruction() {
        use crate::actor_scheduler::tests::{MockCoreWorkerClient, MockRayletClient};
        use crate::node_manager::GcsNodeManager;

        let (_, storage) = make_store();
        let actor_manager = GcsActorManager::new(storage.clone());
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        node_manager
            .handle_register_node(rpc::GcsNodeInfo {
                node_id: node_id.clone(),
                node_manager_address: "127.0.0.1".to_string(),
                node_manager_port: 10001,
                state: 0,
                ..Default::default()
            })
            .await
            .unwrap();

        let raylet = Arc::new(MockRayletClient::new());
        raylet.push_reply(Ok(rpc::RequestWorkerLeaseReply {
            worker_address: Some(rpc::Address {
                node_id: node_id.clone(),
                ip_address: "127.0.0.1".to_string(),
                port: 20001,
                worker_id: vec![11u8; 28],
            }),
            worker_pid: 9876,
            ..Default::default()
        }));
        let worker = Arc::new(MockCoreWorkerClient::new());
        worker.push_reply(Ok(rpc::PushTaskReply::default()));
        actor_manager.set_actor_scheduler(Arc::new(crate::actor_scheduler::GcsActorScheduler::new(
            node_manager, raylet, worker,
        )));
        let actor_manager = Arc::new(actor_manager);
        let svc = ActorInfoGcsServiceImpl {
            actor_manager: actor_manager.clone(),
        };

        let mut actor_id = vec![0u8; 16];
        actor_id[0] = 8;
        svc.register_actor(rpc::RegisterActorRequest {
            task_spec: Some(rpc::TaskSpec {
                actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
                    actor_id: actor_id.clone(),
                    name: "restart_me".to_string(),
                    ray_namespace: "default".to_string(),
                    max_actor_restarts: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        actor_manager.force_move_registered_actor_to_dead_for_test(&actor_id);

        use rpc::actor_info_gcs_service_server::ActorInfoGcsService;
        svc.restart_actor_for_lineage_reconstruction(Request::new(
            rpc::RestartActorForLineageReconstructionRequest {
                actor_id: actor_id.clone(),
                num_restarts_due_to_lineage_reconstruction: 1,
            },
        ))
        .await
        .unwrap();

        let actor = actor_manager.handle_get_actor_info(&actor_id).unwrap();
        assert_eq!(actor.state, crate::actor_manager::ActorState::Alive as i32);
        assert_eq!(actor.num_restarts_due_to_lineage_reconstruction, 1);
        assert_eq!(actor.pid, 9876);
    }

    // ─── Worker Service ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_worker_grpc_add_and_get_all() {
        let (_, storage) = make_store();
        let worker_manager = Arc::new(GcsWorkerManager::new(storage));
        let svc = WorkerInfoGcsServiceImpl {
            worker_manager: worker_manager.clone(),
        };

        use rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
        svc.add_worker_info(Request::new(rpc::AddWorkerInfoRequest {
            worker_data: Some(rpc::WorkerTableData {
                worker_address: Some(rpc::Address {
                    worker_id: vec![1, 2, 3],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        let reply = svc
            .get_all_worker_info(Request::new(rpc::GetAllWorkerInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.len(), 1);
        assert_eq!(reply.total, 1);
        assert_eq!(reply.num_filtered, 0);
    }

    #[tokio::test]
    async fn test_worker_grpc_report_failure() {
        let (_, storage) = make_store();
        let worker_manager = Arc::new(GcsWorkerManager::new(storage));
        let svc = WorkerInfoGcsServiceImpl {
            worker_manager: worker_manager.clone(),
        };

        use rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
        svc.report_worker_failure(Request::new(rpc::ReportWorkerFailureRequest {
            worker_failure: Some(rpc::WorkerTableData {
                worker_address: Some(rpc::Address {
                    worker_id: vec![1],
                    ..Default::default()
                }),
                exit_type: Some(4), // SYSTEM_ERROR
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        assert_eq!(worker_manager.system_error_count(), 1);
    }

    #[tokio::test]
    async fn test_worker_grpc_get_all_with_limit() {
        let (_, storage) = make_store();
        let worker_manager = Arc::new(GcsWorkerManager::new(storage));
        let svc = WorkerInfoGcsServiceImpl {
            worker_manager: worker_manager.clone(),
        };

        use rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
        for i in 1..=5u8 {
            svc.add_worker_info(Request::new(rpc::AddWorkerInfoRequest {
                worker_data: Some(rpc::WorkerTableData {
                    worker_address: Some(rpc::Address {
                        worker_id: vec![i],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        }

        let reply = svc
            .get_all_worker_info(Request::new(rpc::GetAllWorkerInfoRequest {
                limit: Some(3),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.len(), 3);
        assert_eq!(reply.total, 5);
        assert_eq!(reply.num_filtered, 0);
    }

    #[tokio::test]
    async fn test_worker_grpc_get_worker_info() {
        let (_, storage) = make_store();
        let worker_manager = Arc::new(GcsWorkerManager::new(storage));
        let svc = WorkerInfoGcsServiceImpl {
            worker_manager: worker_manager.clone(),
        };

        use rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
        svc.add_worker_info(Request::new(rpc::AddWorkerInfoRequest {
            worker_data: Some(rpc::WorkerTableData {
                worker_address: Some(rpc::Address {
                    worker_id: vec![9, 8, 7],
                    ..Default::default()
                }),
                debugger_port: Some(7331),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        let reply = svc
            .get_worker_info(Request::new(rpc::GetWorkerInfoRequest {
                worker_id: vec![9, 8, 7],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.unwrap().debugger_port, Some(7331));
    }

    #[tokio::test]
    async fn test_worker_grpc_update_debugger_port() {
        let (_, storage) = make_store();
        let worker_manager = Arc::new(GcsWorkerManager::new(storage));
        let svc = WorkerInfoGcsServiceImpl {
            worker_manager: worker_manager.clone(),
        };

        use rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
        svc.add_worker_info(Request::new(rpc::AddWorkerInfoRequest {
            worker_data: Some(rpc::WorkerTableData {
                worker_address: Some(rpc::Address {
                    worker_id: vec![3, 2, 1],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        svc.update_worker_debugger_port(Request::new(rpc::UpdateWorkerDebuggerPortRequest {
            worker_id: vec![3, 2, 1],
            debugger_port: 8123,
        }))
        .await
        .unwrap();

        let reply = svc
            .get_worker_info(Request::new(rpc::GetWorkerInfoRequest {
                worker_id: vec![3, 2, 1],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.unwrap().debugger_port, Some(8123));
    }

    #[tokio::test]
    async fn test_worker_grpc_update_num_paused_threads() {
        let (_, storage) = make_store();
        let worker_manager = Arc::new(GcsWorkerManager::new(storage));
        let svc = WorkerInfoGcsServiceImpl {
            worker_manager: worker_manager.clone(),
        };

        use rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
        svc.add_worker_info(Request::new(rpc::AddWorkerInfoRequest {
            worker_data: Some(rpc::WorkerTableData {
                worker_address: Some(rpc::Address {
                    worker_id: vec![5, 5, 5],
                    ..Default::default()
                }),
                num_paused_threads: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        svc.update_worker_num_paused_threads(Request::new(
            rpc::UpdateWorkerNumPausedThreadsRequest {
                worker_id: vec![5, 5, 5],
                num_paused_threads_delta: 4,
            },
        ))
        .await
        .unwrap();

        let reply = svc
            .get_worker_info(Request::new(rpc::GetWorkerInfoRequest {
                worker_id: vec![5, 5, 5],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.unwrap().num_paused_threads, Some(6));
    }

    #[tokio::test]
    async fn test_worker_grpc_get_all_filters_and_counts() {
        let (_, storage) = make_store();
        let worker_manager = Arc::new(GcsWorkerManager::new(storage));
        let svc = WorkerInfoGcsServiceImpl {
            worker_manager: worker_manager.clone(),
        };

        use rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
        for (worker_id, paused, is_alive) in [(1u8, 0, true), (2u8, 2, true), (3u8, 1, false)] {
            svc.add_worker_info(Request::new(rpc::AddWorkerInfoRequest {
                worker_data: Some(rpc::WorkerTableData {
                    worker_address: Some(rpc::Address {
                        worker_id: vec![worker_id],
                        ..Default::default()
                    }),
                    num_paused_threads: Some(paused),
                    is_alive,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        }

        let paused_only = svc
            .get_all_worker_info(Request::new(rpc::GetAllWorkerInfoRequest {
                filters: Some(rpc::get_all_worker_info_request::Filters {
                    exist_paused_threads: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(paused_only.total, 3);
        assert_eq!(paused_only.num_filtered, 1);
        assert_eq!(paused_only.worker_table_data.len(), 2);

        let alive_and_paused = svc
            .get_all_worker_info(Request::new(rpc::GetAllWorkerInfoRequest {
                limit: Some(0),
                filters: Some(rpc::get_all_worker_info_request::Filters {
                    exist_paused_threads: Some(true),
                    is_alive: Some(true),
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(alive_and_paused.total, 3);
        assert_eq!(alive_and_paused.num_filtered, 2);
        assert!(alive_and_paused.worker_table_data.is_empty());
    }

    // ─── Placement Group Service ───────────────────────────────────────

    #[tokio::test]
    async fn test_pg_grpc_create_and_get() {
        let (_, storage) = make_store();
        let pg_manager = Arc::new(GcsPlacementGroupManager::new(storage));
        let svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager: pg_manager.clone(),
        };

        let mut pg_id = vec![0u8; 18];
        pg_id[0] = 1;

        use rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
        svc.create_placement_group(Request::new(rpc::CreatePlacementGroupRequest {
            placement_group_spec: Some(rpc::PlacementGroupSpec {
                placement_group_id: pg_id.clone(),
                name: "my_pg".to_string(),
                strategy: 0, // PACK
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        let reply = svc
            .get_placement_group(Request::new(rpc::GetPlacementGroupRequest {
                placement_group_id: pg_id.clone(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.placement_group_table_data.is_some());
        assert_eq!(reply.placement_group_table_data.unwrap().name, "my_pg");
    }

    #[tokio::test]
    async fn test_pg_grpc_get_named() {
        let (_, storage) = make_store();
        let pg_manager = Arc::new(GcsPlacementGroupManager::new(storage));
        let svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager: pg_manager.clone(),
        };

        let mut pg_id = vec![0u8; 18];
        pg_id[0] = 1;

        use rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
        svc.create_placement_group(Request::new(rpc::CreatePlacementGroupRequest {
            placement_group_spec: Some(rpc::PlacementGroupSpec {
                placement_group_id: pg_id.clone(),
                name: "named_pg".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        let reply = svc
            .get_named_placement_group(Request::new(rpc::GetNamedPlacementGroupRequest {
                name: "named_pg".to_string(),
                ray_namespace: String::new(), // default namespace since spec doesn't have ray_namespace
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.placement_group_table_data.is_some());
    }

    #[tokio::test]
    async fn test_pg_grpc_remove() {
        let (_, storage) = make_store();
        let pg_manager = Arc::new(GcsPlacementGroupManager::new(storage));
        let svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager: pg_manager.clone(),
        };

        let mut pg_id = vec![0u8; 18];
        pg_id[0] = 1;

        use rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
        svc.create_placement_group(Request::new(rpc::CreatePlacementGroupRequest {
            placement_group_spec: Some(rpc::PlacementGroupSpec {
                placement_group_id: pg_id.clone(),
                name: "removeme".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        svc.remove_placement_group(Request::new(rpc::RemovePlacementGroupRequest {
            placement_group_id: pg_id.clone(),
            ..Default::default()
        }))
        .await
        .unwrap();

        // GCS-13: PG persists with REMOVED state instead of being deleted
        assert_eq!(pg_manager.num_placement_groups(), 1);
        let pg = pg_manager
            .handle_get_placement_group(&pg_id)
            .expect("removed PG should still be queryable");
        assert_eq!(pg.state, 2); // 2 = PlacementGroupState::Removed
    }

    #[tokio::test]
    async fn test_pg_grpc_get_all_with_limit() {
        let (_, storage) = make_store();
        let pg_manager = Arc::new(GcsPlacementGroupManager::new(storage));
        let svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager: pg_manager.clone(),
        };

        use rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
        for i in 1..=4u8 {
            let mut pg_id = vec![0u8; 18];
            pg_id[0] = i;
            svc.create_placement_group(Request::new(rpc::CreatePlacementGroupRequest {
                placement_group_spec: Some(rpc::PlacementGroupSpec {
                    placement_group_id: pg_id,
                    name: format!("pg_{}", i),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        }

        let reply = svc
            .get_all_placement_group(Request::new(rpc::GetAllPlacementGroupRequest {
                limit: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.placement_group_table_data.len(), 2);
    }

    #[tokio::test]
    async fn test_pg_grpc_wait_until_ready() {
        let (_, storage) = make_store();
        let pg_manager = Arc::new(GcsPlacementGroupManager::new(storage));
        let svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager: pg_manager.clone(),
        };

        let mut pg_id = vec![0u8; 18];
        pg_id[0] = 1;

        use rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
        // Create PG (starts as PENDING)
        svc.create_placement_group(Request::new(rpc::CreatePlacementGroupRequest {
            placement_group_spec: Some(rpc::PlacementGroupSpec {
                placement_group_id: pg_id.clone(),
                name: "ready_pg".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        // Explicitly mark as Created (simulates scheduler success)
        let pg_id_typed = ray_common::id::PlacementGroupID::from_binary(
            pg_id.as_slice().try_into().unwrap(),
        );
        pg_manager.mark_placement_group_created(&pg_id_typed);

        let reply = svc
            .wait_placement_group_until_ready(Request::new(
                rpc::WaitPlacementGroupUntilReadyRequest {
                    placement_group_id: pg_id.clone(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        // State 1 = Created = ready
        assert!(reply.status.is_some());
        assert_eq!(reply.status.unwrap().code, 0); // ready

        // Non-existent PG should not be ready
        let reply = svc
            .wait_placement_group_until_ready(Request::new(
                rpc::WaitPlacementGroupUntilReadyRequest {
                    placement_group_id: vec![0u8; 18],
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.status.is_some());
        assert_eq!(reply.status.unwrap().code, 1); // not ready
    }

    // ─── Resource Service ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_resource_grpc_get_all_resources() {
        let resource_manager = Arc::new(GcsResourceManager::new());
        let svc = NodeResourceInfoGcsServiceImpl {
            resource_manager: resource_manager.clone(),
        };

        let nid = ray_common::id::NodeID::from_binary(&{
            let mut v = [0u8; 28];
            v[0] = 1;
            v
        });
        resource_manager.on_node_add(&nid);
        resource_manager.update_resource_usage(
            &nid,
            crate::resource_manager::NodeResourceUsage {
                total_resources: [("CPU".to_string(), 8.0)].into_iter().collect(),
                available_resources: [("CPU".to_string(), 4.0)].into_iter().collect(),
                ..Default::default()
            },
        );

        use rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsService;
        let reply = svc
            .get_all_available_resources(Request::new(
                rpc::GetAllAvailableResourcesRequest::default(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.resources_list.len(), 1);
        assert!(reply.resources_list[0]
            .resources_available
            .contains_key("CPU"));

        let reply = svc
            .get_all_total_resources(Request::new(rpc::GetAllTotalResourcesRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.resources_list.len(), 1);
        assert_eq!(
            *reply.resources_list[0].resources_total.get("CPU").unwrap(),
            8.0
        );
    }

    #[tokio::test]
    async fn test_resource_grpc_get_all_resource_usage() {
        let resource_manager = Arc::new(GcsResourceManager::new());
        let svc = NodeResourceInfoGcsServiceImpl {
            resource_manager: resource_manager.clone(),
        };

        use rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsService;
        let reply = svc
            .get_all_resource_usage(Request::new(rpc::GetAllResourceUsageRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.resource_usage_data.is_some());
        assert!(reply.resource_usage_data.unwrap().batch.is_empty());
    }

    #[tokio::test]
    async fn test_resource_grpc_get_draining_nodes() {
        let resource_manager = Arc::new(GcsResourceManager::new());
        let svc = NodeResourceInfoGcsServiceImpl {
            resource_manager: resource_manager.clone(),
        };

        let nid = ray_common::id::NodeID::from_binary(&{
            let mut v = [0u8; 28];
            v[0] = 1;
            v
        });
        resource_manager.on_node_add(&nid);
        resource_manager.set_node_draining(&nid, true, 0);

        use rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsService;
        let reply = svc
            .get_draining_nodes(Request::new(rpc::GetDrainingNodesRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.draining_nodes.len(), 1);
    }

    // ─── PubSub Service ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_pubsub_grpc_subscribe_and_poll() {
        let pubsub_handler = Arc::new(InternalPubSubHandler::new());
        let svc = InternalPubSubGcsServiceImpl {
            pubsub_handler: pubsub_handler.clone(),
            publisher_id: vec![1u8; 28],
        };

        use rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsService;

        // Subscribe
        svc.gcs_subscriber_command_batch(Request::new(rpc::GcsSubscriberCommandBatchRequest {
            subscriber_id: b"sub1".to_vec(),
            commands: vec![rpc::Command {
                channel_type: 1, // some channel
                key_id: vec![],
                command_message_one_of: Some(rpc::command::CommandMessageOneOf::SubscribeMessage(
                    rpc::SubMessage {
                        sub_message_one_of: None,
                    },
                )),
            }],
            ..Default::default()
        }))
        .await
        .unwrap();

        // Publish
        svc.gcs_publish(Request::new(rpc::GcsPublishRequest {
            pub_messages: vec![rpc::PubMessage {
                channel_type: 1,
                key_id: b"test_key".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await
        .unwrap();

        // Poll
        let reply = svc
            .gcs_subscriber_poll(Request::new(rpc::GcsSubscriberPollRequest {
                subscriber_id: b"sub1".to_vec(),
                max_processed_sequence_id: 0,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.publisher_id, vec![1u8; 28]);
        // Messages may or may not be available depending on channel match
    }

    #[tokio::test]
    async fn test_pubsub_grpc_unsubscribe() {
        let pubsub_handler = Arc::new(InternalPubSubHandler::new());
        let svc = InternalPubSubGcsServiceImpl {
            pubsub_handler: pubsub_handler.clone(),
            publisher_id: vec![1u8; 28],
        };

        use rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsService;

        // Subscribe then unsubscribe
        svc.gcs_subscriber_command_batch(Request::new(rpc::GcsSubscriberCommandBatchRequest {
            subscriber_id: b"sub1".to_vec(),
            commands: vec![rpc::Command {
                channel_type: 1,
                key_id: vec![],
                command_message_one_of: Some(
                    rpc::command::CommandMessageOneOf::UnsubscribeMessage(
                        rpc::UnsubscribeMessage {},
                    ),
                ),
            }],
            ..Default::default()
        }))
        .await
        .unwrap();
        // Should not panic
    }

    // ─── Task Service ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_task_grpc_add_and_get_events() {
        let task_manager = Arc::new(GcsTaskManager::new(None));
        let svc = TaskInfoGcsServiceImpl {
            task_manager: task_manager.clone(),
        };

        let mut task_id = vec![0u8; 24];
        task_id[0] = 1;

        use rpc::task_info_gcs_service_server::TaskInfoGcsService;
        svc.add_task_event_data(Request::new(rpc::AddTaskEventDataRequest {
            data: Some(rpc::TaskEventData {
                events_by_task: vec![rpc::TaskEvents {
                    task_id: task_id.clone(),
                    attempt_number: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        let reply = svc
            .get_task_events(Request::new(rpc::GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.events_by_task.len(), 1);
    }

    #[tokio::test]
    async fn test_task_grpc_get_events_with_limit() {
        let task_manager = Arc::new(GcsTaskManager::new(None));
        let svc = TaskInfoGcsServiceImpl {
            task_manager: task_manager.clone(),
        };

        use rpc::task_info_gcs_service_server::TaskInfoGcsService;
        for i in 1..=5u8 {
            let mut task_id = vec![0u8; 24];
            task_id[0] = i;
            svc.add_task_event_data(Request::new(rpc::AddTaskEventDataRequest {
                data: Some(rpc::TaskEventData {
                    events_by_task: vec![rpc::TaskEvents {
                        task_id,
                        attempt_number: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap();
        }

        let reply = svc
            .get_task_events(Request::new(rpc::GetTaskEventsRequest {
                limit: Some(3),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.events_by_task.len(), 3);
    }

    // ─── Autoscaler Service ────────────────────────────────────────────

    #[tokio::test]
    async fn test_autoscaler_grpc_get_cluster_resource_state() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let (svc, _, _) = make_autoscaler_svc(storage, node_manager);

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .get_cluster_resource_state(Request::new(
                rpc::autoscaler::GetClusterResourceStateRequest::default(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.cluster_resource_state.is_some());
    }

    #[tokio::test]
    async fn test_autoscaler_grpc_drain_node() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let (svc, _, _) = make_autoscaler_svc(storage, node_manager.clone());

        // Register node first
        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                reason: 0,
                reason_message: "scale down".to_string(),
                deadline_timestamp_ms: ray_util::time::current_time_ms() as i64 + 60_000,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);
    }

    // ─── Runtime Env Service (stub) ────────────────────────────────────

    #[tokio::test]
    async fn test_runtime_env_grpc_stub() {
        let svc = RuntimeEnvGcsServiceImpl;

        use rpc::runtime_env_gcs_service_server::RuntimeEnvGcsService;
        let reply = svc
            .pin_runtime_env_uri(Request::new(rpc::PinRuntimeEnvUriRequest::default()))
            .await
            .unwrap()
            .into_inner();
        // Just verify it doesn't panic and returns default
        let _ = reply;
    }

    // ─── GCS-5: UnregisterNode preserves death_info ─────────────────

    #[tokio::test]
    async fn test_unregister_node_passes_death_info_to_manager() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        svc.register_node(rpc::RegisterNodeRequest {
            node_info: Some(make_node_info(1)),
            ..Default::default()
        })
        .await
        .unwrap();

        svc.unregister_node(rpc::UnregisterNodeRequest {
            node_id: node_id_bytes(1),
            node_death_info: Some(rpc::NodeDeathInfo {
                reason: 1,
                reason_message: "graceful shutdown".to_string(),
            }),
        })
        .await
        .unwrap();

        let all = svc.get_all_node_info().unwrap();
        assert_eq!(all.node_info_list.len(), 1);
        let death_info = all.node_info_list[0]
            .death_info
            .as_ref()
            .expect("death_info should be preserved");
        assert_eq!(death_info.reason, 1);
        assert_eq!(death_info.reason_message, "graceful shutdown");
    }

    // ─── GCS-7: GetAllNodeAddressAndLiveness filters ────────────────

    #[tokio::test]
    async fn test_get_all_node_address_and_liveness_filter_by_ids() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        for i in 1..=3u8 {
            svc.register_node(rpc::RegisterNodeRequest {
                node_info: Some(make_node_info(i)),
                ..Default::default()
            })
            .await
            .unwrap();
        }

        use rpc::node_info_gcs_service_server::NodeInfoGcsService;
        let reply = svc
            .get_all_node_address_and_liveness(Request::new(
                rpc::GetAllNodeAddressAndLivenessRequest {
                    node_ids: vec![node_id_bytes(1), node_id_bytes(3)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 2);
    }

    #[tokio::test]
    async fn test_get_all_node_address_and_liveness_with_limit() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        for i in 1..=5u8 {
            svc.register_node(rpc::RegisterNodeRequest {
                node_info: Some(make_node_info(i)),
                ..Default::default()
            })
            .await
            .unwrap();
        }

        use rpc::node_info_gcs_service_server::NodeInfoGcsService;
        let reply = svc
            .get_all_node_address_and_liveness(Request::new(
                rpc::GetAllNodeAddressAndLivenessRequest {
                    limit: Some(2),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 2);
    }

    #[tokio::test]
    async fn test_get_all_node_address_and_liveness_no_filter() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: None,
        };

        for i in 1..=3u8 {
            svc.register_node(rpc::RegisterNodeRequest {
                node_info: Some(make_node_info(i)),
                ..Default::default()
            })
            .await
            .unwrap();
        }

        use rpc::node_info_gcs_service_server::NodeInfoGcsService;
        let reply = svc
            .get_all_node_address_and_liveness(Request::new(
                rpc::GetAllNodeAddressAndLivenessRequest::default(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 3);
    }

    // ─── GCS-18: Autoscaler DrainNode validation ────────────────────

    /// C++ parity: draining an unknown/dead node returns is_accepted=true.
    #[tokio::test]
    async fn test_autoscaler_drain_node_not_alive_accepted() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let (svc, _, _) = make_autoscaler_svc(storage, node_manager);

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(99), // non-existent node
                deadline_timestamp_ms: 0,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        // C++ parity: unknown node is treated as already drained
        assert!(reply.is_accepted);
    }

    /// C++ parity: negative deadline is rejected with InvalidArgument status.
    #[tokio::test]
    async fn test_autoscaler_drain_node_negative_deadline_rejected() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();
        let (svc, _, _) = make_autoscaler_svc(storage, node_manager);

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let result = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: -1, // negative deadline
                ..Default::default()
            }))
            .await;
        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("non-negative"));
    }

    #[tokio::test]
    async fn test_autoscaler_drain_node_valid_accepted() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();
        let (svc, _, _) = make_autoscaler_svc(storage, node_manager);

        let future_deadline = ray_util::time::current_time_ms() as i64 + 60_000;
        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: future_deadline,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);
    }

    #[tokio::test]
    async fn test_autoscaler_drain_node_zero_deadline_accepted() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();
        let (svc, _, _) = make_autoscaler_svc(storage, node_manager);

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: 0, // no deadline
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);
    }

    // ─── GCS-6: Drain node updates autoscaler drain state ────────────

    #[tokio::test]
    async fn test_drain_node_updates_autoscaler_drain_state() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();
        let resource_manager = Arc::new(crate::resource_manager::GcsResourceManager::new());
        let autoscaler_state_manager = Arc::new(GcsAutoscalerStateManager::new(
            "test".to_string(),
            node_manager.clone(),
            resource_manager,
        ));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: Some(autoscaler_state_manager.clone()),
        };

        use rpc::node_info_gcs_service_server::NodeInfoGcsService;
        svc.drain_node(Request::new(rpc::DrainNodeRequest {
            drain_node_data: vec![rpc::DrainNodeData {
                node_id: node_id_bytes(1),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await
        .unwrap();

        // Verify autoscaler state manager was updated with drain status
        use crate::autoscaler_state_manager::DrainStatus;
        assert_eq!(
            autoscaler_state_manager.get_drain_status(&node_id_bytes(1)),
            DrainStatus::Draining,
        );
    }

    #[tokio::test]
    async fn test_autoscaler_drain_rpc_updates_same_state_as_node_drain() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        node_manager
            .handle_register_node(make_node_info(2))
            .await
            .unwrap();
        let (svc, autoscaler_state_manager, _) = make_autoscaler_svc(storage, node_manager.clone());

        // Use the autoscaler drain RPC (not the node-service drain)
        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(2),
                deadline_timestamp_ms: ray_util::time::current_time_ms() as i64 + 60_000,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.is_accepted);

        // Verify autoscaler state manager was also updated
        use crate::autoscaler_state_manager::DrainStatus;
        assert_eq!(
            autoscaler_state_manager.get_drain_status(&node_id_bytes(2)),
            DrainStatus::Draining,
        );
        // Deadline should be non-zero (it was set to future time)
        assert!(
            autoscaler_state_manager.get_drain_deadline(&node_id_bytes(2)) > 0,
            "drain deadline should be preserved"
        );
    }

    #[tokio::test]
    async fn test_drain_node_deadline_is_preserved_consistently() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        node_manager
            .handle_register_node(make_node_info(3))
            .await
            .unwrap();
        let resource_manager = Arc::new(crate::resource_manager::GcsResourceManager::new());
        let autoscaler_state_manager = Arc::new(GcsAutoscalerStateManager::new(
            "test".to_string(),
            node_manager.clone(),
            resource_manager,
        ));

        // Use node-service drain (no deadline in proto)
        let node_svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: Some(autoscaler_state_manager.clone()),
        };

        use rpc::node_info_gcs_service_server::NodeInfoGcsService;
        node_svc
            .drain_node(Request::new(rpc::DrainNodeRequest {
                drain_node_data: vec![rpc::DrainNodeData {
                    node_id: node_id_bytes(3),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await
            .unwrap();

        // Node-service drain has no deadline (0)
        use crate::autoscaler_state_manager::DrainStatus;
        assert_eq!(
            autoscaler_state_manager.get_drain_status(&node_id_bytes(3)),
            DrainStatus::Draining,
        );
        assert_eq!(
            autoscaler_state_manager.get_drain_deadline(&node_id_bytes(3)),
            0,
        );
    }

    // ─── GCS-6: Drain node cross-manager side effects (C++ parity) ──

    /// C++ parity: autoscaler drain on dead node returns is_accepted=true.
    #[tokio::test]
    async fn test_gcs_drain_node_dead_node_accepted() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let (svc, _, _) = make_autoscaler_svc(storage, node_manager.clone());

        // Register then kill the node
        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();
        node_manager
            .handle_unregister_node(&node_id_bytes(1), None)
            .await
            .unwrap();
        assert!(node_manager.is_node_dead(&ray_common::id::NodeID::from_binary(&node_id_bytes(1))));

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: 0,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        // C++ parity: dead node is treated as already drained
        assert!(reply.is_accepted);
    }

    /// C++ parity: SetNodeDraining stores full drain request info and fires
    /// node_draining_listeners (cross-manager side effect).
    #[tokio::test]
    async fn test_gcs_drain_node_cross_manager_side_effects_match_cpp() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));

        // Register a draining listener (C++ parity: node_draining_listeners_)
        let listener_calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls_clone = listener_calls.clone();
        node_manager.add_node_draining_listener(Box::new(move |node_id, is_draining, deadline| {
            calls_clone.lock().unwrap().push((*node_id, is_draining, deadline));
        }));

        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();

        let (svc, _, _) = make_autoscaler_svc(storage, node_manager.clone());

        let future_deadline = ray_util::time::current_time_ms() as i64 + 60_000;
        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                reason: 1,
                reason_message: "scale down".to_string(),
                deadline_timestamp_ms: future_deadline,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);

        // Verify: node_manager has the full drain request stored
        let nid = ray_common::id::NodeID::from_binary(&node_id_bytes(1));
        let drain_info = node_manager.get_drain_request(&nid).expect("drain request should be stored");
        assert_eq!(drain_info.reason, 1);
        assert_eq!(drain_info.reason_message, "scale down");
        assert_eq!(drain_info.deadline_timestamp_ms, future_deadline);

        // Verify: draining listener was called (C++ parity: node_draining_listeners_)
        let calls = listener_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, nid);
        assert!(calls[0].1); // is_draining = true
        assert_eq!(calls[0].2, future_deadline);
    }

    /// C++ parity: SetNodeDraining + autoscaler drain state are both updated
    /// and draining state is cleaned up on node death.
    #[tokio::test]
    async fn test_gcs_drain_node_matches_autoscaler_and_node_manager_state_transitions() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let resource_manager = Arc::new(crate::resource_manager::GcsResourceManager::new());
        let autoscaler_state_manager = Arc::new(GcsAutoscalerStateManager::new(
            "test".to_string(),
            node_manager.clone(),
            resource_manager,
        ));

        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();

        let placement_group_manager =
            Arc::new(crate::placement_group_manager::GcsPlacementGroupManager::new(storage.clone()));
        let svc = AutoscalerStateServiceImpl {
            node_manager: node_manager.clone(),
            autoscaler_state_manager: autoscaler_state_manager.clone(),
            placement_group_manager,
            raylet_client: None,
            actor_manager: None,
        };

        let future_deadline = ray_util::time::current_time_ms() as i64 + 60_000;
        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        svc.drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
            node_id: node_id_bytes(1),
            reason: 2,
            reason_message: "preemption".to_string(),
            deadline_timestamp_ms: future_deadline,
            ..Default::default()
        }))
        .await
        .unwrap();

        // Both managers must agree the node is draining
        let nid = ray_common::id::NodeID::from_binary(&node_id_bytes(1));
        assert!(node_manager.is_node_draining(&nid));
        use crate::autoscaler_state_manager::DrainStatus;
        assert_eq!(
            autoscaler_state_manager.get_drain_status(&node_id_bytes(1)),
            DrainStatus::Draining,
        );
        assert_eq!(
            autoscaler_state_manager.get_drain_deadline(&node_id_bytes(1)),
            future_deadline,
        );

        // Node death should clean up node_manager draining state
        node_manager
            .handle_unregister_node(&node_id_bytes(1), None)
            .await
            .unwrap();
        assert!(!node_manager.is_node_draining(&nid));
        assert!(node_manager.get_draining_nodes().is_empty());
    }

    /// C++ parity: verify full observable effects of the drain path.
    /// - Negative deadline → InvalidArgument error (not OK with is_accepted=false)
    /// - Dead node → is_accepted=true
    /// - Unknown node → is_accepted=true
    /// - Alive node → is_accepted=true + draining state set + listeners fired
    /// - Overwrite: second drain on same node overwrites request
    #[tokio::test]
    async fn test_gcs_drain_node_runtime_observable_effects_match_cpp() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));

        let listener_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let count_clone = listener_count.clone();
        node_manager.add_node_draining_listener(Box::new(move |_, _, _| {
            count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }));

        node_manager
            .handle_register_node(make_node_info(1))
            .await
            .unwrap();
        node_manager
            .handle_register_node(make_node_info(2))
            .await
            .unwrap();

        // Kill node 2 to make it dead
        node_manager
            .handle_unregister_node(&node_id_bytes(2), None)
            .await
            .unwrap();

        let (svc, _, _) = make_autoscaler_svc(storage, node_manager.clone());
        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;

        // 1. Negative deadline → error
        let result = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: -5,
                ..Default::default()
            }))
            .await;
        assert!(result.is_err());

        // 2. Dead node → is_accepted=true
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(2),
                deadline_timestamp_ms: 0,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);

        // 3. Unknown node → is_accepted=true
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(99),
                deadline_timestamp_ms: 0,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);

        // 4. Alive node → is_accepted=true + draining + listener
        let future_deadline = ray_util::time::current_time_ms() as i64 + 60_000;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                reason: 1,
                reason_message: "first drain".to_string(),
                deadline_timestamp_ms: future_deadline,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);
        let nid1 = ray_common::id::NodeID::from_binary(&node_id_bytes(1));
        assert!(node_manager.is_node_draining(&nid1));
        assert_eq!(listener_count.load(std::sync::atomic::Ordering::Relaxed), 1);

        // 5. Overwrite: second drain on same node overwrites (C++ behavior)
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                reason: 2,
                reason_message: "second drain".to_string(),
                deadline_timestamp_ms: future_deadline + 1000,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);
        let drain_info = node_manager.get_drain_request(&nid1).unwrap();
        assert_eq!(drain_info.reason, 2);
        assert_eq!(drain_info.reason_message, "second drain");
        // Listener fired again (overwrite triggers listener)
        assert_eq!(listener_count.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    // ─── GCS-6 Round 2: Live wiring + raylet gating + actor preemption ──

    /// Helper to construct an AutoscalerStateServiceImpl wired with a mock raylet client
    /// and a real actor manager for full drain-path testing.
    fn make_wired_autoscaler_svc(
        storage: Arc<GcsTableStorage>,
        node_manager: Arc<GcsNodeManager>,
        raylet_client: Arc<dyn crate::actor_scheduler::RayletClient>,
    ) -> (
        AutoscalerStateServiceImpl,
        Arc<GcsAutoscalerStateManager>,
        Arc<crate::actor_manager::GcsActorManager>,
        Arc<GcsResourceManager>,
    ) {
        let resource_manager = Arc::new(GcsResourceManager::new());
        let autoscaler_state_manager = Arc::new(GcsAutoscalerStateManager::new(
            "test".to_string(),
            node_manager.clone(),
            resource_manager.clone(),
        ));
        let placement_group_manager = Arc::new(crate::placement_group_manager::GcsPlacementGroupManager::new(storage.clone()));
        let actor_manager = Arc::new(crate::actor_manager::GcsActorManager::new(storage));
        let svc = AutoscalerStateServiceImpl {
            autoscaler_state_manager: autoscaler_state_manager.clone(),
            node_manager: node_manager.clone(),
            placement_group_manager,
            raylet_client: Some(raylet_client),
            actor_manager: Some(actor_manager.clone()),
        };
        (svc, autoscaler_state_manager, actor_manager, resource_manager)
    }

    // -- 1. Live resource-manager wiring --

    #[tokio::test]
    async fn test_autoscaler_drain_updates_resource_manager_in_live_server_wiring() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let resource_manager = Arc::new(GcsResourceManager::new());

        // Wire the draining listener just like the live server does
        {
            let rm = resource_manager.clone();
            node_manager.add_node_draining_listener(Box::new(move |node_id, is_draining, deadline_ms| {
                rm.set_node_draining(node_id, is_draining, deadline_ms);
            }));
        }

        let nid = ray_common::id::NodeID::from_binary(&node_id_bytes(1));
        node_manager.handle_register_node(make_node_info(1)).await.unwrap();
        resource_manager.on_node_add(&nid);

        // Use a mock raylet that accepts the drain
        let mock_raylet = Arc::new(crate::actor_scheduler::tests::MockRayletClient::new());
        mock_raylet.push_drain_reply(Ok(rpc::DrainRayletReply {
            is_accepted: true,
            ..Default::default()
        }));

        let autoscaler_state_manager = Arc::new(GcsAutoscalerStateManager::new(
            "test".to_string(),
            node_manager.clone(),
            resource_manager.clone(),
        ));
        let svc = AutoscalerStateServiceImpl {
            autoscaler_state_manager,
            node_manager: node_manager.clone(),
            placement_group_manager: Arc::new(crate::placement_group_manager::GcsPlacementGroupManager::new(storage)),
            raylet_client: Some(mock_raylet as Arc<dyn crate::actor_scheduler::RayletClient>),
            actor_manager: None,
        };

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: 99999,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);

        // The resource_manager must now see the node as draining via the live listener wiring
        let draining = resource_manager.handle_get_draining_nodes();
        assert_eq!(draining.len(), 1, "resource_manager should see draining node via live listener");
        assert_eq!(draining[0], nid);
    }

    #[tokio::test]
    async fn test_get_draining_nodes_reflects_autoscaler_drain_without_test_local_listener() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let resource_manager = Arc::new(GcsResourceManager::new());

        // Wire the listener (simulating what server.rs does)
        {
            let rm = resource_manager.clone();
            node_manager.add_node_draining_listener(Box::new(move |node_id, is_draining, deadline_ms| {
                rm.set_node_draining(node_id, is_draining, deadline_ms);
            }));
        }

        let nid = ray_common::id::NodeID::from_binary(&node_id_bytes(1));
        node_manager.handle_register_node(make_node_info(1)).await.unwrap();
        resource_manager.on_node_add(&nid);

        // Drain directly through node_manager — should propagate to resource_manager
        node_manager.set_node_draining(
            &nid,
            crate::node_manager::DrainNodeRequestInfo {
                reason: 0,
                reason_message: String::new(),
                deadline_timestamp_ms: 5000,
            },
        );

        assert_eq!(resource_manager.handle_get_draining_nodes().len(), 1);
        assert_eq!(resource_manager.handle_get_draining_nodes()[0], nid);
    }

    // -- 2. Raylet-gated acceptance/rejection --

    #[tokio::test]
    async fn test_autoscaler_drain_busy_node_rejected_by_raylet() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let mut info = make_node_info(1);
        info.node_manager_address = "127.0.0.1".to_string();
        info.node_manager_port = 10001;
        node_manager.handle_register_node(info).await.unwrap();

        let mock_raylet = Arc::new(crate::actor_scheduler::tests::MockRayletClient::new());
        mock_raylet.push_drain_reply(Ok(rpc::DrainRayletReply {
            is_accepted: false,
            rejection_reason_message: "Node has 3 active worker lease(s)".to_string(),
        }));

        let (svc, _, _, _) = make_wired_autoscaler_svc(
            storage,
            node_manager.clone(),
            mock_raylet as Arc<dyn crate::actor_scheduler::RayletClient>,
        );

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: 99999,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!reply.is_accepted, "drain should be rejected when raylet rejects");
        assert!(reply.rejection_reason_message.contains("active worker lease"));
    }

    #[tokio::test]
    async fn test_autoscaler_drain_idle_node_accepted_by_raylet() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let mut info = make_node_info(1);
        info.node_manager_address = "127.0.0.1".to_string();
        info.node_manager_port = 10001;
        node_manager.handle_register_node(info).await.unwrap();

        let mock_raylet = Arc::new(crate::actor_scheduler::tests::MockRayletClient::new());
        mock_raylet.push_drain_reply(Ok(rpc::DrainRayletReply {
            is_accepted: true,
            ..Default::default()
        }));

        let (svc, _, _, _) = make_wired_autoscaler_svc(
            storage,
            node_manager.clone(),
            mock_raylet as Arc<dyn crate::actor_scheduler::RayletClient>,
        );

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: 99999,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);
    }

    #[tokio::test]
    async fn test_autoscaler_drain_rejection_reason_propagated() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let mut info = make_node_info(1);
        info.node_manager_address = "127.0.0.1".to_string();
        info.node_manager_port = 10001;
        node_manager.handle_register_node(info).await.unwrap();

        let mock_raylet = Arc::new(crate::actor_scheduler::tests::MockRayletClient::new());
        let specific_reason = "custom rejection: GPU memory pressure".to_string();
        mock_raylet.push_drain_reply(Ok(rpc::DrainRayletReply {
            is_accepted: false,
            rejection_reason_message: specific_reason.clone(),
        }));

        let (svc, _, _, _) = make_wired_autoscaler_svc(
            storage,
            node_manager.clone(),
            mock_raylet as Arc<dyn crate::actor_scheduler::RayletClient>,
        );

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: 99999,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!reply.is_accepted);
        assert_eq!(reply.rejection_reason_message, specific_reason);
    }

    #[tokio::test]
    async fn test_autoscaler_drain_state_committed_only_after_raylet_accepts() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let mut info = make_node_info(1);
        info.node_manager_address = "127.0.0.1".to_string();
        info.node_manager_port = 10001;
        node_manager.handle_register_node(info).await.unwrap();

        // Raylet rejects
        let mock_raylet = Arc::new(crate::actor_scheduler::tests::MockRayletClient::new());
        mock_raylet.push_drain_reply(Ok(rpc::DrainRayletReply {
            is_accepted: false,
            rejection_reason_message: "busy".to_string(),
        }));

        let (svc, autoscaler_state_manager, _, _) = make_wired_autoscaler_svc(
            storage,
            node_manager.clone(),
            mock_raylet as Arc<dyn crate::actor_scheduler::RayletClient>,
        );

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: 99999,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!reply.is_accepted);

        // After rejection, drain state must NOT be committed
        let nid = ray_common::id::NodeID::from_binary(&node_id_bytes(1));
        assert!(!node_manager.is_node_draining(&nid), "node_manager should not have drain state after rejection");
        use crate::autoscaler_state_manager::DrainStatus;
        assert_eq!(
            autoscaler_state_manager.get_drain_status(&node_id_bytes(1)),
            DrainStatus::Running,
            "autoscaler state should not show draining after rejection"
        );
    }

    // -- 3. Actor-preemption side effects --

    #[tokio::test]
    async fn test_autoscaler_drain_marks_node_actors_preempted() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let mut info = make_node_info(1);
        info.node_manager_address = "127.0.0.1".to_string();
        info.node_manager_port = 10001;
        node_manager.handle_register_node(info).await.unwrap();

        let mock_raylet = Arc::new(crate::actor_scheduler::tests::MockRayletClient::new());
        mock_raylet.push_drain_reply(Ok(rpc::DrainRayletReply {
            is_accepted: true,
            ..Default::default()
        }));

        let (svc, _, actor_manager, _) = make_wired_autoscaler_svc(
            storage,
            node_manager.clone(),
            mock_raylet as Arc<dyn crate::actor_scheduler::RayletClient>,
        );

        // Register an actor on node 1
        let actor_id = ray_common::id::ActorID::from_binary(&[1u8; 16]);
        let nid = ray_common::id::NodeID::from_binary(&node_id_bytes(1));
        {
            let mut actor_data = ray_proto::ray::rpc::ActorTableData::default();
            actor_data.state = 2; // ALIVE
            actor_data.node_id = Some(node_id_bytes(1));
            actor_data.preempted = false;
            actor_manager.registered_actors.insert(actor_id, std::sync::Arc::new(actor_data));
            actor_manager.actors_by_node.entry(nid).or_default().push(actor_id);
        }

        // Drain the node
        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: 99999,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);

        // The actor should now be marked as preempted
        let actor = actor_manager.registered_actors.get(&actor_id).unwrap();
        assert!(actor.preempted, "actor should be marked preempted after drain");
    }

    #[tokio::test]
    async fn test_autoscaler_drain_publishes_actor_preemption_state() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let mut info = make_node_info(1);
        info.node_manager_address = "127.0.0.1".to_string();
        info.node_manager_port = 10001;
        node_manager.handle_register_node(info).await.unwrap();

        let mock_raylet = Arc::new(crate::actor_scheduler::tests::MockRayletClient::new());
        mock_raylet.push_drain_reply(Ok(rpc::DrainRayletReply {
            is_accepted: true,
            ..Default::default()
        }));

        let (svc, _, actor_manager, _) = make_wired_autoscaler_svc(
            storage,
            node_manager.clone(),
            mock_raylet as Arc<dyn crate::actor_scheduler::RayletClient>,
        );

        // Set up pubsub handler to capture published messages
        let pubsub_handler = Arc::new(crate::pubsub_handler::InternalPubSubHandler::new());
        actor_manager.set_pubsub_handler(pubsub_handler.clone());

        // Subscribe to actor channel
        pubsub_handler.handle_subscribe_command(
            b"test_sub".to_vec(),
            crate::pubsub_handler::ChannelType::GcsActorChannel as i32,
            vec![],
        );

        // Register an actor on node 1
        let actor_id = ray_common::id::ActorID::from_binary(&[2u8; 16]);
        let nid = ray_common::id::NodeID::from_binary(&node_id_bytes(1));
        {
            let mut actor_data = ray_proto::ray::rpc::ActorTableData::default();
            actor_data.state = 2; // ALIVE
            actor_data.node_id = Some(node_id_bytes(1));
            actor_data.preempted = false;
            actor_manager.registered_actors.insert(actor_id, std::sync::Arc::new(actor_data));
            actor_manager.actors_by_node.entry(nid).or_default().push(actor_id);
        }

        // Drain the node
        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .drain_node(Request::new(rpc::autoscaler::DrainNodeRequest {
                node_id: node_id_bytes(1),
                deadline_timestamp_ms: 99999,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);

        // Verify that the preemption was published via pubsub
        let messages = pubsub_handler.handle_subscriber_poll(b"test_sub", 0).await;
        assert!(!messages.is_empty(), "actor preemption state should be published");
        // The published message should contain the actor with preempted=true
        let found_preempted = messages.iter().any(|msg| {
            match &msg.inner_message {
                Some(ray_proto::ray::rpc::pub_message::InnerMessage::ActorMessage(actor_data)) => {
                    actor_data.preempted
                }
                _ => false,
            }
        });
        assert!(found_preempted, "published actor message should have preempted=true");
    }

    // ─── GCS-17: Cluster status payload parity ──────────────────────

    #[tokio::test]
    async fn test_get_cluster_status_includes_autoscaling_state_and_session() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let resource_manager = Arc::new(crate::resource_manager::GcsResourceManager::new());
        let autoscaler_state_manager = Arc::new(GcsAutoscalerStateManager::new(
            "my-session".to_string(),
            node_manager.clone(),
            resource_manager,
        ));
        let placement_group_manager = Arc::new(GcsPlacementGroupManager::new(storage));

        // Report autoscaling state
        let as_state = rpc::autoscaler::AutoscalingState {
            autoscaler_state_version: 7,
            last_seen_cluster_resource_state_version: 3,
            ..Default::default()
        };
        let encoded = prost::Message::encode_to_vec(&as_state);
        autoscaler_state_manager.handle_report_autoscaling_state(encoded, 7);

        let svc = AutoscalerStateServiceImpl {
            autoscaler_state_manager,
            node_manager,
            placement_group_manager,
            raylet_client: None,
            actor_manager: None,
        };

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .get_cluster_status(Request::new(rpc::autoscaler::GetClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();

        // Verify autoscaling_state is present
        let autoscaling_state = reply.autoscaling_state.expect("autoscaling_state should be present");
        assert_eq!(autoscaling_state.autoscaler_state_version, 7);
        assert_eq!(autoscaling_state.last_seen_cluster_resource_state_version, 3);

        // Verify cluster_session_name is present
        let cluster_state = reply.cluster_resource_state.expect("cluster_resource_state should be present");
        assert_eq!(cluster_state.cluster_session_name, "my-session");
    }

    #[tokio::test]
    async fn test_get_cluster_status_includes_last_seen_autoscaler_state_version() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let (svc, autoscaler_state_manager, _) = make_autoscaler_svc(storage, node_manager);

        // Report autoscaling state with version 42
        autoscaler_state_manager.handle_report_autoscaling_state(b"state".to_vec(), 42);

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .get_cluster_status(Request::new(rpc::autoscaler::GetClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();

        let cluster_state = reply.cluster_resource_state.expect("cluster_resource_state");
        assert_eq!(cluster_state.last_seen_autoscaler_state_version, 42);
    }

    #[tokio::test]
    async fn test_get_cluster_status_includes_pending_resource_requests() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let (svc, autoscaler_state_manager, _) = make_autoscaler_svc(storage, node_manager);

        // Add resource demands
        use crate::autoscaler_state_manager::{NodeResourceDemand, ResourceDemandEntry};
        autoscaler_state_manager.update_resource_demands(
            vec![1u8; 28],
            NodeResourceDemand {
                resource_demands: vec![ResourceDemandEntry {
                    shape: std::collections::HashMap::from([("CPU".to_string(), 4.0)]),
                    count: 10,
                    infeasible_count: 3,
                }],
                backlog_size: 10,
            },
        );

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .get_cluster_status(Request::new(rpc::autoscaler::GetClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();

        let cluster_state = reply.cluster_resource_state.expect("cluster_resource_state");
        assert!(!cluster_state.pending_resource_requests.is_empty(),
            "pending_resource_requests should be populated");
        let req = &cluster_state.pending_resource_requests[0];
        assert_eq!(req.count, 13); // 10 count + 3 infeasible
        let bundle = req.request.as_ref().expect("request");
        assert_eq!(*bundle.resources_bundle.get("CPU").unwrap(), 4.0);
    }

    #[tokio::test]
    async fn test_get_cluster_status_includes_cluster_resource_constraints() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let (svc, autoscaler_state_manager, _) = make_autoscaler_svc(storage, node_manager);

        // Set a cluster resource constraint
        let constraint = rpc::autoscaler::ClusterResourceConstraint {
            resource_requests: vec![rpc::autoscaler::ResourceRequestByCount {
                request: Some(rpc::autoscaler::ResourceRequest {
                    resources_bundle: std::collections::HashMap::from([("GPU".to_string(), 8.0)]),
                    ..Default::default()
                }),
                count: 1,
            }],
        };
        let encoded = prost::Message::encode_to_vec(&constraint);
        autoscaler_state_manager.handle_request_cluster_resource_constraint(encoded);

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .get_cluster_status(Request::new(rpc::autoscaler::GetClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();

        let cluster_state = reply.cluster_resource_state.expect("cluster_resource_state");
        assert!(!cluster_state.cluster_resource_constraints.is_empty(),
            "cluster_resource_constraints should be populated");
        let c = &cluster_state.cluster_resource_constraints[0];
        assert_eq!(c.resource_requests.len(), 1);
    }

    // ─── GCS-17 Round 4: constraint RPC roundtrip + pending gang requests ────

    #[tokio::test]
    async fn test_request_cluster_resource_constraint_roundtrips_through_real_rpc() {
        // This test exercises the REAL RPC path: send RequestClusterResourceConstraint,
        // then call GetClusterStatus, and verify the constraint appears correctly.
        // This catches the Round 3 bug where the full request was stored instead of
        // the inner ClusterResourceConstraint.
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let (svc, _, _) = make_autoscaler_svc(storage, node_manager);

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;

        // Send constraint via the real RPC path
        let constraint = rpc::autoscaler::ClusterResourceConstraint {
            resource_requests: vec![rpc::autoscaler::ResourceRequestByCount {
                request: Some(rpc::autoscaler::ResourceRequest {
                    resources_bundle: std::collections::HashMap::from([
                        ("CPU".to_string(), 16.0),
                        ("GPU".to_string(), 4.0),
                    ]),
                    ..Default::default()
                }),
                count: 2,
            }],
        };
        svc.request_cluster_resource_constraint(Request::new(
            rpc::autoscaler::RequestClusterResourceConstraintRequest {
                cluster_resource_constraint: Some(constraint.clone()),
            },
        ))
        .await
        .unwrap();

        // Now get cluster status via the real RPC path
        let reply = svc
            .get_cluster_status(Request::new(rpc::autoscaler::GetClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();

        let cluster_state = reply.cluster_resource_state.expect("cluster_resource_state");
        assert_eq!(
            cluster_state.cluster_resource_constraints.len(),
            1,
            "Should have exactly one cluster resource constraint"
        );
        let c = &cluster_state.cluster_resource_constraints[0];
        assert_eq!(c.resource_requests.len(), 1);
        let req = c.resource_requests[0].request.as_ref().unwrap();
        assert_eq!(*req.resources_bundle.get("CPU").unwrap(), 16.0);
        assert_eq!(*req.resources_bundle.get("GPU").unwrap(), 4.0);
        assert_eq!(c.resource_requests[0].count, 2);
    }

    #[tokio::test]
    async fn test_get_cluster_status_includes_pending_gang_resource_requests_from_pg_load() {
        // C++ populates pending_gang_resource_requests from placement group load.
        // PGs in PENDING or RESCHEDULING state should contribute gang resource requests.
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage.clone()));
        let (svc, _, pg_manager) = make_autoscaler_svc(storage, node_manager);

        // Create a PENDING PG with bundles
        let mut pg_id = vec![0u8; 18];
        pg_id[0] = 42;
        let pg_data = rpc::PlacementGroupTableData {
            placement_group_id: pg_id.clone(),
            name: "gang_test_pg".to_string(),
            state: 0, // PENDING
            strategy: 1, // PACK
            bundles: vec![
                rpc::Bundle {
                    unit_resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 4.0),
                        ("GPU".to_string(), 1.0),
                    ]),
                    node_id: vec![], // unplaced
                    ..Default::default()
                },
                rpc::Bundle {
                    unit_resources: std::collections::HashMap::from([
                        ("CPU".to_string(), 4.0),
                        ("GPU".to_string(), 1.0),
                    ]),
                    node_id: vec![], // unplaced
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        pg_manager.handle_create_placement_group(pg_data).await.unwrap();

        use rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
        let reply = svc
            .get_cluster_status(Request::new(rpc::autoscaler::GetClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();

        let cluster_state = reply.cluster_resource_state.expect("cluster_resource_state");
        assert!(
            !cluster_state.pending_gang_resource_requests.is_empty(),
            "pending_gang_resource_requests should be populated from PG load"
        );
        let gang_req = &cluster_state.pending_gang_resource_requests[0];
        assert_eq!(gang_req.requests.len(), 2, "Should have 2 bundle resource requests");
        assert!(!gang_req.details.is_empty(), "Should have details string");
        assert_eq!(gang_req.bundle_selectors.len(), 1, "Should have 1 bundle selector");
        assert_eq!(
            gang_req.bundle_selectors[0].resource_requests.len(),
            2,
            "Bundle selector should have 2 resource requests"
        );
    }

    // ─── GCS-12: PG create lifecycle ────────────────────────────────

    #[tokio::test]
    async fn test_pg_grpc_create_starts_pending_then_created() {
        let (_, storage) = make_store();
        let pg_manager = Arc::new(GcsPlacementGroupManager::new(storage));
        let svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager: pg_manager.clone(),
        };

        let mut pg_id = vec![0u8; 18];
        pg_id[0] = 1;

        use rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
        svc.create_placement_group(Request::new(rpc::CreatePlacementGroupRequest {
            placement_group_spec: Some(rpc::PlacementGroupSpec {
                placement_group_id: pg_id.clone(),
                name: "lifecycle_pg".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        // C++ parity: PG remains PENDING after create_placement_group RPC.
        // Transition to CREATED only happens via explicit scheduler callback.
        let pg = pg_manager.handle_get_placement_group(&pg_id).unwrap();
        assert_eq!(pg.state, 0, "PG should remain PENDING (0) after creation, not auto-transition");
    }

    // ─── GCS-12: Placement group remains pending until scheduler success ────

    #[tokio::test]
    async fn test_pg_create_remains_pending_until_scheduler_success() {
        let (_, storage) = make_store();
        let pg_manager = Arc::new(GcsPlacementGroupManager::new(storage));
        let svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager: pg_manager.clone(),
        };

        let mut pg_id = vec![0u8; 18];
        pg_id[0] = 1;

        use rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
        svc.create_placement_group(Request::new(rpc::CreatePlacementGroupRequest {
            placement_group_spec: Some(rpc::PlacementGroupSpec {
                placement_group_id: pg_id.clone(),
                name: "pending_pg".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        // PG should be PENDING (0) — not Created
        let pg = pg_manager.handle_get_placement_group(&pg_id).unwrap();
        assert_eq!(pg.state, 0, "PG must stay PENDING after gRPC create");

        // Explicitly mark as created (simulates scheduler success)
        let pg_id_typed = ray_common::id::PlacementGroupID::from_binary(
            pg_id.as_slice().try_into().unwrap(),
        );
        pg_manager.mark_placement_group_created(&pg_id_typed);

        // Now it should be Created
        let pg = pg_manager.handle_get_placement_group(&pg_id).unwrap();
        assert_eq!(pg.state, 1, "PG should be CREATED (1) after explicit transition");
    }

    #[tokio::test]
    async fn test_wait_placement_group_until_ready_waits_for_explicit_transition() {
        let (_, storage) = make_store();
        let pg_manager = Arc::new(GcsPlacementGroupManager::new(storage));
        let svc = PlacementGroupInfoGcsServiceImpl {
            placement_group_manager: pg_manager.clone(),
        };

        let mut pg_id = vec![0u8; 18];
        pg_id[0] = 2;

        use rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
        svc.create_placement_group(Request::new(rpc::CreatePlacementGroupRequest {
            placement_group_spec: Some(rpc::PlacementGroupSpec {
                placement_group_id: pg_id.clone(),
                name: "wait_pg".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

        // PG is PENDING — wait should NOT immediately succeed
        let pg = pg_manager.handle_get_placement_group(&pg_id).unwrap();
        assert_eq!(pg.state, 0, "PG should be PENDING before scheduler");

        // Wait with short timeout — should timeout (not ready)
        let ready = pg_manager.wait_until_ready(
            &pg_id,
            std::time::Duration::from_millis(50),
        ).await;
        assert!(!ready, "wait should timeout when PG is still PENDING");

        // Now mark created
        let pg_id_typed = ray_common::id::PlacementGroupID::from_binary(
            pg_id.as_slice().try_into().unwrap(),
        );
        pg_manager.mark_placement_group_created(&pg_id_typed);

        // Now wait should succeed
        let ready = pg_manager.wait_until_ready(
            &pg_id,
            std::time::Duration::from_millis(100),
        ).await;
        assert!(ready, "wait should succeed after mark_placement_group_created");
    }
}
