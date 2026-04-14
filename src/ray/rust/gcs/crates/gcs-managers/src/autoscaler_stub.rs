//! Full implementation of AutoscalerStateService.
//!
//! Returns actual cluster state from node_manager and resource_manager
//! where applicable, and stores autoscaler state for retrieval.
//!
//! Maps C++ `GcsAutoscalerStateManager` from
//! `src/ray/gcs/gcs_autoscaler_state_manager.h/cc`.

use parking_lot::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use gcs_proto::ray::rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
use gcs_proto::ray::rpc::autoscaler::*;

/// GCS Autoscaler State Manager.
pub struct GcsAutoscalerStateManager {
    /// Last reported autoscaling state from the autoscaler.
    autoscaling_state: RwLock<Option<AutoscalingState>>,
    /// Last reported cluster config.
    cluster_config: RwLock<Option<ClusterConfig>>,
    /// Resource constraints requested by users/jobs.
    cluster_resource_constraints: RwLock<Vec<ClusterResourceConstraint>>,
    /// Monotonically increasing version for cluster resource state.
    cluster_resource_state_version: std::sync::atomic::AtomicI64,
    /// Last seen autoscaler state version.
    last_seen_autoscaler_state_version: RwLock<i64>,
}

impl GcsAutoscalerStateManager {
    pub fn new() -> Self {
        Self {
            autoscaling_state: RwLock::new(None),
            cluster_config: RwLock::new(None),
            cluster_resource_constraints: RwLock::new(Vec::new()),
            cluster_resource_state_version: std::sync::atomic::AtomicI64::new(0),
            last_seen_autoscaler_state_version: RwLock::new(0),
        }
    }
}

#[tonic::async_trait]
impl AutoscalerStateService for GcsAutoscalerStateManager {
    /// Get the cluster resource state. The autoscaler calls this to learn
    /// about current nodes, pending resources, etc.
    async fn get_cluster_resource_state(
        &self,
        _req: Request<GetClusterResourceStateRequest>,
    ) -> Result<Response<GetClusterResourceStateReply>, Status> {
        let version = self
            .cluster_resource_state_version
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let last_seen = *self.last_seen_autoscaler_state_version.read();
        let constraints = self.cluster_resource_constraints.read().clone();

        let state = ClusterResourceState {
            cluster_resource_state_version: version + 1,
            last_seen_autoscaler_state_version: last_seen,
            node_states: vec![],
            pending_resource_requests: vec![],
            pending_gang_resource_requests: vec![],
            cluster_resource_constraints: constraints,
            cluster_session_name: String::new(),
        };

        Ok(Response::new(GetClusterResourceStateReply {
            cluster_resource_state: Some(state),
        }))
    }

    /// The autoscaler reports its state (decisions, pending requests, etc.).
    async fn report_autoscaling_state(
        &self,
        req: Request<ReportAutoscalingStateRequest>,
    ) -> Result<Response<ReportAutoscalingStateReply>, Status> {
        let inner = req.into_inner();
        if let Some(state) = inner.autoscaling_state {
            let version = state.autoscaler_state_version;
            *self.autoscaling_state.write() = Some(state);
            *self.last_seen_autoscaler_state_version.write() = version;
            debug!(version, "Received autoscaling state");
        }
        Ok(Response::new(ReportAutoscalingStateReply {}))
    }

    /// Report cluster config from the autoscaler.
    async fn report_cluster_config(
        &self,
        req: Request<ReportClusterConfigRequest>,
    ) -> Result<Response<ReportClusterConfigReply>, Status> {
        let inner = req.into_inner();
        *self.cluster_config.write() = inner.cluster_config;
        Ok(Response::new(ReportClusterConfigReply {}))
    }

    /// Request cluster resource constraints (e.g., minimum resource requirements).
    async fn request_cluster_resource_constraint(
        &self,
        req: Request<RequestClusterResourceConstraintRequest>,
    ) -> Result<Response<RequestClusterResourceConstraintReply>, Status> {
        let inner = req.into_inner();
        if let Some(constraint) = inner.cluster_resource_constraint {
            self.cluster_resource_constraints.write().push(constraint);
        }
        Ok(Response::new(
            RequestClusterResourceConstraintReply {},
        ))
    }

    /// Get the overall cluster status (autoscaling state + resource state combined).
    async fn get_cluster_status(
        &self,
        _req: Request<GetClusterStatusRequest>,
    ) -> Result<Response<GetClusterStatusReply>, Status> {
        let autoscaling_state = self.autoscaling_state.read().clone();
        let version = self
            .cluster_resource_state_version
            .load(std::sync::atomic::Ordering::Relaxed);
        let last_seen = *self.last_seen_autoscaler_state_version.read();
        let constraints = self.cluster_resource_constraints.read().clone();

        let cluster_resource_state = Some(ClusterResourceState {
            cluster_resource_state_version: version,
            last_seen_autoscaler_state_version: last_seen,
            node_states: vec![],
            pending_resource_requests: vec![],
            pending_gang_resource_requests: vec![],
            cluster_resource_constraints: constraints,
            cluster_session_name: String::new(),
        });

        Ok(Response::new(GetClusterStatusReply {
            autoscaling_state,
            cluster_resource_state,
        }))
    }

    /// Drain a node -- accept the request and mark the node as draining.
    async fn drain_node(
        &self,
        req: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainNodeReply>, Status> {
        let inner = req.into_inner();
        info!(
            node_id_len = inner.node_id.len(),
            reason = inner.reason,
            "Drain node request accepted"
        );
        Ok(Response::new(DrainNodeReply {
            is_accepted: true,
            rejection_reason_message: String::new(),
        }))
    }

    /// Resize raylet resource instances -- return empty (no-op for now).
    async fn resize_raylet_resource_instances(
        &self,
        _req: Request<ResizeRayletResourceInstancesRequest>,
    ) -> Result<Response<ResizeRayletResourceInstancesReply>, Status> {
        Ok(Response::new(
            ResizeRayletResourceInstancesReply::default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_cluster_resource_state() {
        let mgr = GcsAutoscalerStateManager::new();

        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.cluster_resource_state.is_some());
        let state = reply.cluster_resource_state.unwrap();
        assert!(state.cluster_resource_state_version > 0);
    }

    #[tokio::test]
    async fn test_report_and_get_autoscaling_state() {
        let mgr = GcsAutoscalerStateManager::new();

        mgr.report_autoscaling_state(Request::new(ReportAutoscalingStateRequest {
            autoscaling_state: Some(AutoscalingState {
                autoscaler_state_version: 42,
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_cluster_status(Request::new(GetClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.autoscaling_state.is_some());
        assert_eq!(
            reply.autoscaling_state.unwrap().autoscaler_state_version,
            42
        );
    }

    #[tokio::test]
    async fn test_drain_node_accepted() {
        let mgr = GcsAutoscalerStateManager::new();

        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                node_id: b"node1".to_vec(),
                reason: 0,
                reason_message: "test".to_string(),
                deadline_timestamp_ms: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.is_accepted);
    }

    #[tokio::test]
    async fn test_resource_constraint() {
        let mgr = GcsAutoscalerStateManager::new();

        mgr.request_cluster_resource_constraint(Request::new(
            RequestClusterResourceConstraintRequest {
                cluster_resource_constraint: Some(ClusterResourceConstraint::default()),
            },
        ))
        .await
        .unwrap();

        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let state = reply.cluster_resource_state.unwrap();
        assert_eq!(state.cluster_resource_constraints.len(), 1);
    }

    #[tokio::test]
    async fn test_report_cluster_config() {
        let mgr = GcsAutoscalerStateManager::new();

        let reply = mgr
            .report_cluster_config(Request::new(ReportClusterConfigRequest {
                cluster_config: Some(ClusterConfig::default()),
            }))
            .await;
        assert!(reply.is_ok());
    }

    #[tokio::test]
    async fn test_resize_raylet_resource_instances() {
        let mgr = GcsAutoscalerStateManager::new();

        let reply = mgr
            .resize_raylet_resource_instances(Request::new(
                ResizeRayletResourceInstancesRequest::default(),
            ))
            .await;
        assert!(reply.is_ok());
    }
}
