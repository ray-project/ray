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
        _req: Request<rpc::GetAllNodeAddressAndLivenessRequest>,
    ) -> Result<Response<rpc::GetAllNodeAddressAndLivenessReply>, Status> {
        let all_nodes = self.node_manager.handle_get_all_node_info();
        let node_info_list = all_nodes
            .into_iter()
            .map(|node| rpc::GcsNodeAddressAndLiveness {
                node_id: node.node_id.clone(),
                node_manager_address: node.node_manager_address.clone(),
                node_manager_port: node.node_manager_port,
                object_manager_port: node.object_manager_port,
                state: node.state,
                death_info: node.death_info.clone(),
            })
            .collect();
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
        _req: Request<rpc::GetWorkerInfoRequest>,
    ) -> Result<Response<rpc::GetWorkerInfoReply>, Status> {
        // Single worker lookup not yet supported; return empty.
        Ok(Response::new(rpc::GetWorkerInfoReply::default()))
    }

    async fn get_all_worker_info(
        &self,
        req: Request<rpc::GetAllWorkerInfoRequest>,
    ) -> Result<Response<rpc::GetAllWorkerInfoReply>, Status> {
        let request = req.into_inner();
        let limit = request.limit.filter(|&l| l > 0).map(|l| l as usize);
        let workers = self
            .worker_manager
            .handle_get_all_worker_info(limit)
            .await?;
        Ok(Response::new(rpc::GetAllWorkerInfoReply {
            worker_table_data: workers,
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
        _req: Request<rpc::UpdateWorkerDebuggerPortRequest>,
    ) -> Result<Response<rpc::UpdateWorkerDebuggerPortReply>, Status> {
        Ok(Response::new(rpc::UpdateWorkerDebuggerPortReply::default()))
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
        req: Request<rpc::CreatePlacementGroupRequest>,
    ) -> Result<Response<rpc::CreatePlacementGroupReply>, Status> {
        let request = req.into_inner();
        if let Some(spec) = request.placement_group_spec {
            // Convert PlacementGroupSpec to PlacementGroupTableData for storage.
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
                // Mark as Created immediately (single-node: no 2PC needed).
                state: 1, // PlacementGroupState::Created
                ..Default::default()
            };
            self.placement_group_manager
                .handle_create_placement_group(pg_data)
                .await?;
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
        let pg = self
            .placement_group_manager
            .handle_get_placement_group(&request.placement_group_id);
        // Ready if state == Created (1). Encode in GcsStatus: code=0 means ready.
        let ready = pg.map(|p| p.state == 1).unwrap_or(false);
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
                    .handle_unsubscribe_command(&subscriber_id);
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
        let (version, node_states) = self
            .autoscaler_state_manager
            .handle_get_cluster_resource_state();
        Ok(Response::new(rpc::autoscaler::GetClusterStatusReply {
            cluster_resource_state: Some(rpc::autoscaler::ClusterResourceState {
                cluster_resource_state_version: version,
                node_states,
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    async fn drain_node(
        &self,
        req: Request<rpc::autoscaler::DrainNodeRequest>,
    ) -> Result<Response<rpc::autoscaler::DrainNodeReply>, Status> {
        let request = req.into_inner();
        let node_id = ray_common::id::NodeID::from_binary(request.node_id.as_slice());
        self.node_manager
            .handle_drain_node(&node_id, request.deadline_timestamp_ms);
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

    #[tokio::test]
    async fn test_job_grpc_mark_finished() {
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let svc = JobInfoGcsServiceImpl {
            job_manager: job_manager.clone(),
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
        let svc = JobInfoGcsServiceImpl { job_manager };

        let reply1 = svc.get_next_job_id().await.unwrap();
        let reply2 = svc.get_next_job_id().await.unwrap();
        assert_eq!(reply2.job_id, reply1.job_id + 1);
    }

    #[tokio::test]
    async fn test_job_grpc_get_all_with_limit() {
        let (_, storage) = make_store();
        let job_manager = Arc::new(GcsJobManager::new(storage));
        let svc = JobInfoGcsServiceImpl { job_manager };

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
            .unwrap();
        assert_eq!(reply.job_info_list.len(), 3);
    }

    // ─── Node Service ──────────────────────────────────────────────────

    #[tokio::test]
    async fn test_node_grpc_register_and_get_all() {
        let (_, storage) = make_store();
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let svc = NodeInfoGcsServiceImpl {
            node_manager: node_manager.clone(),
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

        assert_eq!(pg_manager.num_placement_groups(), 0);
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
        // PG is created with state=1 (Created) by the service handler
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
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let resource_manager = Arc::new(GcsResourceManager::new());
        let autoscaler = Arc::new(GcsAutoscalerStateManager::new(
            "test".into(),
            node_manager.clone(),
            resource_manager.clone(),
        ));
        let svc = AutoscalerStateServiceImpl {
            autoscaler_state_manager: autoscaler,
            node_manager,
        };

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
        let node_manager = Arc::new(GcsNodeManager::new(storage));
        let resource_manager = Arc::new(GcsResourceManager::new());
        let autoscaler = Arc::new(GcsAutoscalerStateManager::new(
            "test".into(),
            node_manager.clone(),
            resource_manager.clone(),
        ));
        let svc = AutoscalerStateServiceImpl {
            autoscaler_state_manager: autoscaler,
            node_manager: node_manager.clone(),
        };

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
                deadline_timestamp_ms: 5000,
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
}
