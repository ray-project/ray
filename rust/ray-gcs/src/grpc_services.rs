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

use tonic::Status;

use crate::actor_manager::GcsActorManager;
use crate::job_manager::GcsJobManager;
use crate::kv_manager::GcsInternalKVManager;
use crate::node_manager::GcsNodeManager;
use crate::placement_group_manager::GcsPlacementGroupManager;
use crate::pubsub_handler::InternalPubSubHandler;
use crate::resource_manager::GcsResourceManager;
use crate::task_manager::GcsTaskManager;
use crate::worker_manager::GcsWorkerManager;

// ─── JobInfoGcsService ─────────────────────────────────────────────────────

pub struct JobInfoGcsServiceImpl {
    pub job_manager: Arc<GcsJobManager>,
}

impl JobInfoGcsServiceImpl {
    pub async fn add_job(
        &self,
        request: ray_proto::ray::rpc::AddJobRequest,
    ) -> Result<ray_proto::ray::rpc::AddJobReply, Status> {
        if let Some(data) = request.data {
            self.job_manager.handle_add_job(data).await?;
        }
        Ok(ray_proto::ray::rpc::AddJobReply::default())
    }

    pub async fn mark_job_finished(
        &self,
        request: ray_proto::ray::rpc::MarkJobFinishedRequest,
    ) -> Result<ray_proto::ray::rpc::MarkJobFinishedReply, Status> {
        self.job_manager
            .handle_mark_job_finished(&request.job_id)
            .await?;
        Ok(ray_proto::ray::rpc::MarkJobFinishedReply::default())
    }

    pub fn get_all_job_info(
        &self,
        request: ray_proto::ray::rpc::GetAllJobInfoRequest,
    ) -> Result<ray_proto::ray::rpc::GetAllJobInfoReply, Status> {
        let limit = request.limit.filter(|&l| l > 0).map(|l| l as usize);
        let jobs = self.job_manager.handle_get_all_job_info(limit);
        Ok(ray_proto::ray::rpc::GetAllJobInfoReply {
            job_info_list: jobs,
            ..Default::default()
        })
    }

    pub async fn get_next_job_id(&self) -> Result<ray_proto::ray::rpc::GetNextJobIdReply, Status> {
        let job_id = self.job_manager.handle_get_next_job_id().await?;
        Ok(ray_proto::ray::rpc::GetNextJobIdReply {
            job_id,
            ..Default::default()
        })
    }
}

// ─── NodeInfoGcsService ────────────────────────────────────────────────────

pub struct NodeInfoGcsServiceImpl {
    pub node_manager: Arc<GcsNodeManager>,
}

impl NodeInfoGcsServiceImpl {
    pub fn get_cluster_id(&self) -> Result<ray_proto::ray::rpc::GetClusterIdReply, Status> {
        let cluster_id = self.node_manager.handle_get_cluster_id();
        Ok(ray_proto::ray::rpc::GetClusterIdReply {
            cluster_id: cluster_id.into_bytes(),
            ..Default::default()
        })
    }

    pub async fn register_node(
        &self,
        request: ray_proto::ray::rpc::RegisterNodeRequest,
    ) -> Result<ray_proto::ray::rpc::RegisterNodeReply, Status> {
        if let Some(node_info) = request.node_info {
            self.node_manager.handle_register_node(node_info).await?;
        }
        Ok(ray_proto::ray::rpc::RegisterNodeReply::default())
    }

    pub async fn unregister_node(
        &self,
        request: ray_proto::ray::rpc::UnregisterNodeRequest,
    ) -> Result<ray_proto::ray::rpc::UnregisterNodeReply, Status> {
        self.node_manager
            .handle_unregister_node(&request.node_id)
            .await?;
        Ok(ray_proto::ray::rpc::UnregisterNodeReply::default())
    }

    pub fn get_all_node_info(&self) -> Result<ray_proto::ray::rpc::GetAllNodeInfoReply, Status> {
        let nodes = self.node_manager.handle_get_all_node_info();
        Ok(ray_proto::ray::rpc::GetAllNodeInfoReply {
            node_info_list: nodes,
            ..Default::default()
        })
    }
}

// ─── ActorInfoGcsService ───────────────────────────────────────────────────

pub struct ActorInfoGcsServiceImpl {
    pub actor_manager: Arc<GcsActorManager>,
}

impl ActorInfoGcsServiceImpl {
    pub async fn register_actor(
        &self,
        request: ray_proto::ray::rpc::RegisterActorRequest,
    ) -> Result<ray_proto::ray::rpc::RegisterActorReply, Status> {
        if let Some(task_spec) = request.task_spec {
            self.actor_manager.handle_register_actor(task_spec).await?;
        }
        Ok(ray_proto::ray::rpc::RegisterActorReply::default())
    }

    pub fn get_actor_info(
        &self,
        request: ray_proto::ray::rpc::GetActorInfoRequest,
    ) -> Result<ray_proto::ray::rpc::GetActorInfoReply, Status> {
        let actor = self.actor_manager.handle_get_actor_info(&request.actor_id);
        Ok(ray_proto::ray::rpc::GetActorInfoReply {
            actor_table_data: actor,
            ..Default::default()
        })
    }

    pub fn get_named_actor_info(
        &self,
        request: ray_proto::ray::rpc::GetNamedActorInfoRequest,
    ) -> Result<ray_proto::ray::rpc::GetNamedActorInfoReply, Status> {
        let actor = self
            .actor_manager
            .handle_get_named_actor_info(&request.name, &request.ray_namespace);
        Ok(ray_proto::ray::rpc::GetNamedActorInfoReply {
            actor_table_data: actor,
            ..Default::default()
        })
    }

    pub fn get_all_actor_info(
        &self,
        request: ray_proto::ray::rpc::GetAllActorInfoRequest,
    ) -> Result<ray_proto::ray::rpc::GetAllActorInfoReply, Status> {
        let limit = request.limit.filter(|&l| l > 0).map(|l| l as usize);
        let actors = self
            .actor_manager
            .handle_get_all_actor_info(limit, None, None);
        Ok(ray_proto::ray::rpc::GetAllActorInfoReply {
            actor_table_data: actors,
            ..Default::default()
        })
    }

    pub async fn kill_actor_via_gcs(
        &self,
        request: ray_proto::ray::rpc::KillActorViaGcsRequest,
    ) -> Result<ray_proto::ray::rpc::KillActorViaGcsReply, Status> {
        self.actor_manager
            .handle_kill_actor(&request.actor_id)
            .await?;
        Ok(ray_proto::ray::rpc::KillActorViaGcsReply::default())
    }
}

// ─── InternalKVGcsService ──────────────────────────────────────────────────

pub struct InternalKVGcsServiceImpl {
    pub kv_manager: Arc<GcsInternalKVManager>,
}

impl InternalKVGcsServiceImpl {
    pub async fn internal_kv_get(
        &self,
        request: ray_proto::ray::rpc::InternalKvGetRequest,
    ) -> Result<ray_proto::ray::rpc::InternalKvGetReply, Status> {
        let ns = String::from_utf8_lossy(&request.namespace).to_string();
        let key = String::from_utf8_lossy(&request.key).to_string();
        let value = self.kv_manager.handle_get(&ns, &key).await?;
        Ok(ray_proto::ray::rpc::InternalKvGetReply {
            value: value.unwrap_or_default().into_bytes(),
            ..Default::default()
        })
    }

    pub async fn internal_kv_put(
        &self,
        request: ray_proto::ray::rpc::InternalKvPutRequest,
    ) -> Result<ray_proto::ray::rpc::InternalKvPutReply, Status> {
        let ns = String::from_utf8_lossy(&request.namespace).to_string();
        let key = String::from_utf8_lossy(&request.key).to_string();
        let value = String::from_utf8_lossy(&request.value).to_string();
        let added = self
            .kv_manager
            .handle_put(&ns, &key, value, request.overwrite)
            .await?;
        Ok(ray_proto::ray::rpc::InternalKvPutReply {
            added,
            ..Default::default()
        })
    }

    pub async fn internal_kv_del(
        &self,
        request: ray_proto::ray::rpc::InternalKvDelRequest,
    ) -> Result<ray_proto::ray::rpc::InternalKvDelReply, Status> {
        let ns = String::from_utf8_lossy(&request.namespace).to_string();
        let key = String::from_utf8_lossy(&request.key).to_string();
        let num_deleted = self
            .kv_manager
            .handle_del(&ns, &key, request.del_by_prefix)
            .await?;
        Ok(ray_proto::ray::rpc::InternalKvDelReply {
            deleted_num: num_deleted as i32,
            ..Default::default()
        })
    }

    pub async fn internal_kv_exists(
        &self,
        request: ray_proto::ray::rpc::InternalKvExistsRequest,
    ) -> Result<ray_proto::ray::rpc::InternalKvExistsReply, Status> {
        let ns = String::from_utf8_lossy(&request.namespace).to_string();
        let key = String::from_utf8_lossy(&request.key).to_string();
        let exists = self.kv_manager.handle_exists(&ns, &key).await?;
        Ok(ray_proto::ray::rpc::InternalKvExistsReply {
            exists,
            ..Default::default()
        })
    }

    pub async fn internal_kv_keys(
        &self,
        request: ray_proto::ray::rpc::InternalKvKeysRequest,
    ) -> Result<ray_proto::ray::rpc::InternalKvKeysReply, Status> {
        let ns = String::from_utf8_lossy(&request.namespace).to_string();
        let prefix = String::from_utf8_lossy(&request.prefix).to_string();
        let keys = self.kv_manager.handle_keys(&ns, &prefix).await?;
        Ok(ray_proto::ray::rpc::InternalKvKeysReply {
            results: keys.into_iter().map(|k| k.into_bytes()).collect(),
            ..Default::default()
        })
    }

    pub fn get_internal_config(
        &self,
    ) -> Result<ray_proto::ray::rpc::GetInternalConfigReply, Status> {
        Ok(ray_proto::ray::rpc::GetInternalConfigReply {
            config: self.kv_manager.raylet_config_list().to_string(),
            ..Default::default()
        })
    }
}

// ─── WorkerInfoGcsService ──────────────────────────────────────────────────

#[allow(dead_code)]
pub struct WorkerInfoGcsServiceImpl {
    pub worker_manager: Arc<GcsWorkerManager>,
}

// ─── PlacementGroupInfoGcsService ──────────────────────────────────────────

#[allow(dead_code)]
pub struct PlacementGroupInfoGcsServiceImpl {
    pub placement_group_manager: Arc<GcsPlacementGroupManager>,
}

// ─── NodeResourceInfoGcsService ────────────────────────────────────────────

#[allow(dead_code)]
pub struct NodeResourceInfoGcsServiceImpl {
    pub resource_manager: Arc<GcsResourceManager>,
}

// ─── TaskInfoGcsService ────────────────────────────────────────────────────

#[allow(dead_code)]
pub struct TaskInfoGcsServiceImpl {
    pub task_manager: Arc<GcsTaskManager>,
}

// ─── InternalPubSubGcsService ──────────────────────────────────────────────

#[allow(dead_code)]
pub struct InternalPubSubGcsServiceImpl {
    pub pubsub_handler: Arc<InternalPubSubHandler>,
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
        let put_req = ray_proto::ray::rpc::InternalKvPutRequest {
            namespace: b"ns".to_vec(),
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
            overwrite: true,
            ..Default::default()
        };
        let reply = svc.internal_kv_put(put_req).await.unwrap();
        assert!(reply.added);

        // Get
        let get_req = ray_proto::ray::rpc::InternalKvGetRequest {
            namespace: b"ns".to_vec(),
            key: b"key1".to_vec(),
            ..Default::default()
        };
        let reply = svc.internal_kv_get(get_req).await.unwrap();
        assert_eq!(reply.value, b"val1");

        // Exists
        let exists_req = ray_proto::ray::rpc::InternalKvExistsRequest {
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

        let add_req = ray_proto::ray::rpc::AddJobRequest {
            data: Some(ray_proto::ray::rpc::JobTableData {
                job_id: vec![1, 0, 0, 0],
                ..Default::default()
            }),
            ..Default::default()
        };
        svc.add_job(add_req).await.unwrap();

        let get_req = ray_proto::ray::rpc::GetAllJobInfoRequest::default();
        let reply = svc.get_all_job_info(get_req).unwrap();
        assert_eq!(reply.job_info_list.len(), 1);
    }
}
