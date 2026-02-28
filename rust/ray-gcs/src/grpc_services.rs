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
}

impl JobInfoGcsServiceImpl {
    pub async fn add_job(
        &self,
        request: rpc::AddJobRequest,
    ) -> Result<rpc::AddJobReply, Status> {
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

    pub fn get_all_job_info(
        &self,
        request: rpc::GetAllJobInfoRequest,
    ) -> Result<rpc::GetAllJobInfoReply, Status> {
        let limit = request.limit.filter(|&l| l > 0).map(|l| l as usize);
        let jobs = self.job_manager.handle_get_all_job_info(limit);
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
        self.get_all_job_info(req.into_inner()).map(Response::new)
    }

    async fn report_job_error(
        &self,
        _req: Request<rpc::ReportJobErrorRequest>,
    ) -> Result<Response<rpc::ReportJobErrorReply>, Status> {
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
            .handle_unregister_node(&request.node_id)
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
        self.register_node(req.into_inner()).await.map(Response::new)
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
        _req: Request<rpc::DrainNodeRequest>,
    ) -> Result<Response<rpc::DrainNodeReply>, Status> {
        Ok(Response::new(rpc::DrainNodeReply::default()))
    }

    async fn get_all_node_info(
        &self,
        _req: Request<rpc::GetAllNodeInfoRequest>,
    ) -> Result<Response<rpc::GetAllNodeInfoReply>, Status> {
        self.get_all_node_info().map(Response::new)
    }

    async fn get_all_node_address_and_liveness(
        &self,
        _req: Request<rpc::GetAllNodeAddressAndLivenessRequest>,
    ) -> Result<Response<rpc::GetAllNodeAddressAndLivenessReply>, Status> {
        Ok(Response::new(
            rpc::GetAllNodeAddressAndLivenessReply::default(),
        ))
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
                let node_id =
                    ray_common::id::NodeID::from_binary(node_id_bytes.as_slice());
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
        let actor = self
            .actor_manager
            .handle_get_named_actor_info(&request.name, &request.ray_namespace);
        Ok(rpc::GetNamedActorInfoReply {
            actor_table_data: actor,
            ..Default::default()
        })
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
        _req: Request<rpc::CreateActorRequest>,
    ) -> Result<Response<rpc::CreateActorReply>, Status> {
        Ok(Response::new(rpc::CreateActorReply::default()))
    }

    async fn restart_actor_for_lineage_reconstruction(
        &self,
        _req: Request<rpc::RestartActorForLineageReconstructionRequest>,
    ) -> Result<Response<rpc::RestartActorForLineageReconstructionReply>, Status> {
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
        _req: Request<rpc::ListNamedActorsRequest>,
    ) -> Result<Response<rpc::ListNamedActorsReply>, Status> {
        Ok(Response::new(rpc::ListNamedActorsReply::default()))
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
        _req: Request<rpc::ReportActorOutOfScopeRequest>,
    ) -> Result<Response<rpc::ReportActorOutOfScopeReply>, Status> {
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

    pub fn get_internal_config(
        &self,
    ) -> Result<rpc::GetInternalConfigReply, Status> {
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
        _req: Request<rpc::InternalKvMultiGetRequest>,
    ) -> Result<Response<rpc::InternalKvMultiGetReply>, Status> {
        Ok(Response::new(rpc::InternalKvMultiGetReply::default()))
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
        _req: Request<rpc::ReportWorkerFailureRequest>,
    ) -> Result<Response<rpc::ReportWorkerFailureReply>, Status> {
        Ok(Response::new(rpc::ReportWorkerFailureReply::default()))
    }

    async fn get_worker_info(
        &self,
        _req: Request<rpc::GetWorkerInfoRequest>,
    ) -> Result<Response<rpc::GetWorkerInfoReply>, Status> {
        Ok(Response::new(rpc::GetWorkerInfoReply::default()))
    }

    async fn get_all_worker_info(
        &self,
        _req: Request<rpc::GetAllWorkerInfoRequest>,
    ) -> Result<Response<rpc::GetAllWorkerInfoReply>, Status> {
        Ok(Response::new(rpc::GetAllWorkerInfoReply::default()))
    }

    async fn add_worker_info(
        &self,
        _req: Request<rpc::AddWorkerInfoRequest>,
    ) -> Result<Response<rpc::AddWorkerInfoReply>, Status> {
        Ok(Response::new(rpc::AddWorkerInfoReply::default()))
    }

    async fn update_worker_debugger_port(
        &self,
        _req: Request<rpc::UpdateWorkerDebuggerPortRequest>,
    ) -> Result<Response<rpc::UpdateWorkerDebuggerPortReply>, Status> {
        Ok(Response::new(
            rpc::UpdateWorkerDebuggerPortReply::default(),
        ))
    }

    async fn update_worker_num_paused_threads(
        &self,
        _req: Request<rpc::UpdateWorkerNumPausedThreadsRequest>,
    ) -> Result<Response<rpc::UpdateWorkerNumPausedThreadsReply>, Status> {
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
        _req: Request<rpc::CreatePlacementGroupRequest>,
    ) -> Result<Response<rpc::CreatePlacementGroupReply>, Status> {
        Ok(Response::new(rpc::CreatePlacementGroupReply::default()))
    }

    async fn remove_placement_group(
        &self,
        _req: Request<rpc::RemovePlacementGroupRequest>,
    ) -> Result<Response<rpc::RemovePlacementGroupReply>, Status> {
        Ok(Response::new(rpc::RemovePlacementGroupReply::default()))
    }

    async fn get_placement_group(
        &self,
        _req: Request<rpc::GetPlacementGroupRequest>,
    ) -> Result<Response<rpc::GetPlacementGroupReply>, Status> {
        Ok(Response::new(rpc::GetPlacementGroupReply::default()))
    }

    async fn get_named_placement_group(
        &self,
        _req: Request<rpc::GetNamedPlacementGroupRequest>,
    ) -> Result<Response<rpc::GetNamedPlacementGroupReply>, Status> {
        Ok(Response::new(rpc::GetNamedPlacementGroupReply::default()))
    }

    async fn get_all_placement_group(
        &self,
        _req: Request<rpc::GetAllPlacementGroupRequest>,
    ) -> Result<Response<rpc::GetAllPlacementGroupReply>, Status> {
        Ok(Response::new(rpc::GetAllPlacementGroupReply::default()))
    }

    async fn wait_placement_group_until_ready(
        &self,
        _req: Request<rpc::WaitPlacementGroupUntilReadyRequest>,
    ) -> Result<Response<rpc::WaitPlacementGroupUntilReadyReply>, Status> {
        Ok(Response::new(
            rpc::WaitPlacementGroupUntilReadyReply::default(),
        ))
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
        Ok(Response::new(
            rpc::GetAllAvailableResourcesReply::default(),
        ))
    }

    async fn get_all_total_resources(
        &self,
        _req: Request<rpc::GetAllTotalResourcesRequest>,
    ) -> Result<Response<rpc::GetAllTotalResourcesReply>, Status> {
        Ok(Response::new(rpc::GetAllTotalResourcesReply::default()))
    }

    async fn get_all_resource_usage(
        &self,
        _req: Request<rpc::GetAllResourceUsageRequest>,
    ) -> Result<Response<rpc::GetAllResourceUsageReply>, Status> {
        Ok(Response::new(rpc::GetAllResourceUsageReply::default()))
    }

    async fn get_draining_nodes(
        &self,
        _req: Request<rpc::GetDrainingNodesRequest>,
    ) -> Result<Response<rpc::GetDrainingNodesReply>, Status> {
        Ok(Response::new(rpc::GetDrainingNodesReply::default()))
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
        _req: Request<rpc::GcsPublishRequest>,
    ) -> Result<Response<rpc::GcsPublishReply>, Status> {
        Ok(Response::new(rpc::GcsPublishReply::default()))
    }

    async fn gcs_subscriber_poll(
        &self,
        _req: Request<rpc::GcsSubscriberPollRequest>,
    ) -> Result<Response<rpc::GcsSubscriberPollReply>, Status> {
        // Long-poll: sleep briefly to avoid busy-loop, then return with publisher_id.
        // The C++ subscriber will re-poll when it gets an empty response.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(Response::new(rpc::GcsSubscriberPollReply {
            publisher_id: self.publisher_id.clone(),
            ..Default::default()
        }))
    }

    async fn gcs_subscriber_command_batch(
        &self,
        _req: Request<rpc::GcsSubscriberCommandBatchRequest>,
    ) -> Result<Response<rpc::GcsSubscriberCommandBatchReply>, Status> {
        Ok(Response::new(
            rpc::GcsSubscriberCommandBatchReply::default(),
        ))
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
        _req: Request<rpc::AddTaskEventDataRequest>,
    ) -> Result<Response<rpc::AddTaskEventDataReply>, Status> {
        Ok(Response::new(rpc::AddTaskEventDataReply::default()))
    }

    async fn get_task_events(
        &self,
        _req: Request<rpc::GetTaskEventsRequest>,
    ) -> Result<Response<rpc::GetTaskEventsReply>, Status> {
        Ok(Response::new(rpc::GetTaskEventsReply::default()))
    }
}

// ─── AutoscalerStateService ────────────────────────────────────────────────

pub struct AutoscalerStateServiceImpl {
    pub autoscaler_state_manager: Arc<GcsAutoscalerStateManager>,
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
            let encoded = prost::Message::encode_to_vec(&state);
            self.autoscaler_state_manager
                .handle_report_autoscaling_state(encoded, 0);
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
        let encoded = prost::Message::encode_to_vec(&inner);
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
        Ok(Response::new(
            rpc::autoscaler::GetClusterStatusReply::default(),
        ))
    }

    async fn drain_node(
        &self,
        _req: Request<rpc::autoscaler::DrainNodeRequest>,
    ) -> Result<Response<rpc::autoscaler::DrainNodeReply>, Status> {
        Ok(Response::new(rpc::autoscaler::DrainNodeReply::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_client::{InMemoryInternalKV, InMemoryStoreClient};
    use crate::table_storage::GcsTableStorage;

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
    async fn test_job_grpc_service() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let svc = JobInfoGcsServiceImpl { job_manager };

        let add_req = rpc::AddJobRequest {
            data: Some(rpc::JobTableData {
                job_id: vec![1, 0, 0, 0],
                ..Default::default()
            }),
            ..Default::default()
        };
        svc.add_job(add_req).await.unwrap();

        let get_req = rpc::GetAllJobInfoRequest::default();
        let reply = svc.get_all_job_info(get_req).unwrap();
        assert_eq!(reply.job_info_list.len(), 1);
    }
}
