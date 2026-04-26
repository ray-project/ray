//! GCS Resource Manager -- tracks node resources.
//!
//! Maps C++ `GcsResourceManager` from `src/ray/gcs/gcs_resource_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tonic::{Request, Response, Status};

use gcs_proto::ray::rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsService;
use gcs_proto::ray::rpc::syncer::ResourceViewSyncMessage;
use gcs_proto::ray::rpc::*;

fn ok_status() -> GcsStatus {
    GcsStatus { code: 0, message: String::new() }
}

/// GCS Resource Manager. Maps C++ `GcsResourceManager`.
pub struct GcsResourceManager {
    /// Per-node resource usage data.
    node_resources: DashMap<Vec<u8>, ResourcesData>,
    /// Nodes being drained.
    draining_nodes: DashMap<Vec<u8>, i64>, // node_id -> deadline_timestamp_ms
}

impl GcsResourceManager {
    pub fn new() -> Self {
        Self {
            node_resources: DashMap::new(),
            draining_nodes: DashMap::new(),
        }
    }

    /// Update resource usage for a node.
    pub fn update_node_resources(&self, node_id: Vec<u8>, resources: ResourcesData) {
        self.node_resources.insert(node_id, resources);
    }

    /// Mark a node as draining.
    pub fn set_node_draining(&self, node_id: Vec<u8>, deadline_ms: i64) {
        self.draining_nodes.insert(node_id, deadline_ms);
    }

    /// Called when a node dies.
    pub fn on_node_dead(&self, node_id: &[u8]) {
        self.node_resources.remove(node_id);
        self.draining_nodes.remove(node_id);
    }

    /// Copy the `resource_load` and `resource_load_by_shape` fields from a
    /// fresh raylet-reported `ResourcesData` into the stored row. Other
    /// fields are left untouched, matching C++
    /// `GcsResourceManager::UpdateResourceLoads`
    /// (`gcs/gcs_resource_manager.cc:147-156`).
    ///
    /// If the node is unknown (not yet registered or already removed) the
    /// update is dropped, same as the C++ early-return.
    pub fn update_resource_loads(&self, data: &ResourcesData) {
        if let Some(mut entry) = self.node_resources.get_mut(&data.node_id) {
            entry.resource_load = data.resource_load.clone();
            entry.resource_load_by_shape = data.resource_load_by_shape.clone();
        }
    }

    /// Consume a `ResourceViewSyncMessage` from a remote node.
    ///
    /// Maps C++ `GcsResourceManager::UpdateFromResourceView`
    /// (`gcs/gcs_resource_manager.cc:129-145`). We flatten the syncer-level
    /// `ResourceViewSyncMessage` into the richer `ResourcesData` structure the
    /// Rust handler already stores per node, so the node resource RPCs and the
    /// syncer share one source of truth.
    pub fn consume_resource_view(
        &self,
        node_id: Vec<u8>,
        message: ResourceViewSyncMessage,
    ) {
        let data = ResourcesData {
            node_id: node_id.clone(),
            resources_available: message.resources_available,
            resources_total: message.resources_total,
            object_pulls_queued: message.object_pulls_queued,
            idle_duration_ms: message.idle_duration_ms,
            is_draining: message.is_draining,
            draining_deadline_timestamp_ms: message.draining_deadline_timestamp_ms,
            labels: message.labels,
            ..Default::default()
        };
        if message.is_draining {
            self.draining_nodes
                .insert(node_id.clone(), message.draining_deadline_timestamp_ms);
        } else {
            self.draining_nodes.remove(&node_id);
        }
        self.node_resources.insert(node_id, data);
    }
}

#[tonic::async_trait]
impl NodeResourceInfoGcsService for GcsResourceManager {
    async fn get_all_available_resources(
        &self,
        _request: Request<GetAllAvailableResourcesRequest>,
    ) -> Result<Response<GetAllAvailableResourcesReply>, Status> {
        let resources: Vec<AvailableResources> = self
            .node_resources
            .iter()
            .map(|entry| AvailableResources {
                node_id: entry.key().clone(),
                resources_available: entry.value().resources_available.clone(),
            })
            .collect();

        Ok(Response::new(GetAllAvailableResourcesReply {
            status: Some(ok_status()),
            resources_list: resources,
        }))
    }

    async fn get_all_total_resources(
        &self,
        _request: Request<GetAllTotalResourcesRequest>,
    ) -> Result<Response<GetAllTotalResourcesReply>, Status> {
        let resources: Vec<TotalResources> = self
            .node_resources
            .iter()
            .map(|entry| TotalResources {
                node_id: entry.key().clone(),
                resources_total: entry.value().resources_total.clone(),
            })
            .collect();

        Ok(Response::new(GetAllTotalResourcesReply {
            status: Some(ok_status()),
            resources_list: resources,
        }))
    }

    async fn get_draining_nodes(
        &self,
        _request: Request<GetDrainingNodesRequest>,
    ) -> Result<Response<GetDrainingNodesReply>, Status> {
        let draining: Vec<DrainingNode> = self
            .draining_nodes
            .iter()
            .map(|e| DrainingNode {
                node_id: e.key().clone(),
                draining_deadline_timestamp_ms: *e.value(),
            })
            .collect();

        Ok(Response::new(GetDrainingNodesReply {
            status: Some(ok_status()),
            draining_nodes: draining,
        }))
    }

    async fn get_all_resource_usage(
        &self,
        _request: Request<GetAllResourceUsageRequest>,
    ) -> Result<Response<GetAllResourceUsageReply>, Status> {
        let batch = ResourceUsageBatchData {
            batch: self
                .node_resources
                .iter()
                .map(|entry| entry.value().clone())
                .collect(),
            ..Default::default()
        };

        Ok(Response::new(GetAllResourceUsageReply {
            status: Some(ok_status()),
            resource_usage_data: Some(batch),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_update_and_get_resources() {
        let mgr = GcsResourceManager::new();
        mgr.update_node_resources(b"node1".to_vec(), ResourcesData::default());

        let reply = mgr
            .get_all_available_resources(Request::new(GetAllAvailableResourcesRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.resources_list.len(), 1);
    }

    #[tokio::test]
    async fn test_draining_nodes() {
        let mgr = GcsResourceManager::new();
        mgr.set_node_draining(b"node1".to_vec(), 12345);

        let reply = mgr
            .get_draining_nodes(Request::new(GetDrainingNodesRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.draining_nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_on_node_dead() {
        let mgr = GcsResourceManager::new();
        mgr.update_node_resources(b"n1".to_vec(), ResourcesData::default());
        mgr.set_node_draining(b"n1".to_vec(), 0);

        mgr.on_node_dead(b"n1");

        let reply = mgr
            .get_all_resource_usage(Request::new(GetAllResourceUsageRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.resource_usage_data.unwrap().batch.is_empty());
    }

    #[tokio::test]
    async fn test_get_all_total_resources() {
        let mgr = GcsResourceManager::new();

        let mut total_map = std::collections::HashMap::new();
        total_map.insert("CPU".to_string(), 8.0);
        total_map.insert("GPU".to_string(), 2.0);

        mgr.update_node_resources(
            b"node1".to_vec(),
            ResourcesData {
                resources_total: total_map,
                ..Default::default()
            },
        );

        let reply = mgr
            .get_all_total_resources(Request::new(GetAllTotalResourcesRequest {}))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.resources_list.len(), 1);
        let entry = &reply.resources_list[0];
        assert_eq!(entry.node_id, b"node1");
        assert_eq!(entry.resources_total.get("CPU"), Some(&8.0));
        assert_eq!(entry.resources_total.get("GPU"), Some(&2.0));
    }
}
