//! GCS Node Manager -- manages node registration, health, and lifecycle.
//!
//! Maps C++ `GcsNodeManager` from `src/ray/gcs/gcs_node_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use prost::Message;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use gcs_proto::ray::rpc::node_info_gcs_service_server::NodeInfoGcsService;
use gcs_proto::ray::rpc::*;
use gcs_pubsub::GcsPublisher;
use gcs_table_storage::GcsTableStorage;

/// GCS Node Manager. Maps C++ `GcsNodeManager`.
pub struct GcsNodeManager {
    alive_nodes: DashMap<Vec<u8>, GcsNodeInfo>,
    dead_nodes: DashMap<Vec<u8>, GcsNodeInfo>,
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    cluster_id: Vec<u8>,
    node_added_tx: broadcast::Sender<GcsNodeInfo>,
    node_removed_tx: broadcast::Sender<GcsNodeInfo>,
}

impl GcsNodeManager {
    pub fn new(
        table_storage: Arc<GcsTableStorage>,
        publisher: Arc<GcsPublisher>,
        cluster_id: Vec<u8>,
    ) -> Self {
        let (node_added_tx, _) = broadcast::channel(256);
        let (node_removed_tx, _) = broadcast::channel(256);
        Self {
            alive_nodes: DashMap::new(),
            dead_nodes: DashMap::new(),
            table_storage,
            publisher,
            cluster_id,
            node_added_tx,
            node_removed_tx,
        }
    }

    pub fn subscribe_node_added(&self) -> broadcast::Receiver<GcsNodeInfo> {
        self.node_added_tx.subscribe()
    }

    pub fn subscribe_node_removed(&self) -> broadcast::Receiver<GcsNodeInfo> {
        self.node_removed_tx.subscribe()
    }

    pub fn get_alive_node(&self, node_id: &[u8]) -> Option<GcsNodeInfo> {
        self.alive_nodes.get(node_id).map(|r| r.value().clone())
    }

    pub fn get_all_alive_nodes(&self) -> HashMap<Vec<u8>, GcsNodeInfo> {
        self.alive_nodes
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    pub fn is_node_alive(&self, node_id: &[u8]) -> bool {
        self.alive_nodes.contains_key(node_id)
    }

    /// Add a node to alive cache and persist.
    pub async fn add_node(&self, node: GcsNodeInfo) {
        let node_id = node.node_id.clone();
        self.table_storage
            .node_table()
            .put(&hex::encode(&node_id), &node)
            .await;
        self.alive_nodes.insert(node_id.clone(), node.clone());
        let _ = self.node_added_tx.send(node.clone());
        self.publisher
            .publish_node_info(&node_id, node.encode_to_vec());
        info!(node_id = hex::encode(&node_id), "Node registered");
    }

    /// Remove a node (move from alive to dead).
    pub async fn remove_node(&self, node_id: &[u8]) -> Option<GcsNodeInfo> {
        if let Some((_, mut node)) = self.alive_nodes.remove(node_id) {
            node.state = gcs_node_info::GcsNodeState::Dead as i32;
            self.dead_nodes.insert(node_id.to_vec(), node.clone());
            self.table_storage
                .node_table()
                .put(&hex::encode(node_id), &node)
                .await;
            let _ = self.node_removed_tx.send(node.clone());
            self.publisher
                .publish_node_info(node_id, node.encode_to_vec());
            info!(node_id = hex::encode(node_id), "Node removed");
            Some(node)
        } else {
            warn!(node_id = hex::encode(node_id), "Tried to remove unknown node");
            None
        }
    }

    /// Initialize from persisted data.
    pub async fn initialize(&self, nodes: &HashMap<String, GcsNodeInfo>) {
        for (_key, node) in nodes {
            if node.state == gcs_node_info::GcsNodeState::Alive as i32 {
                self.alive_nodes.insert(node.node_id.clone(), node.clone());
            } else {
                self.dead_nodes.insert(node.node_id.clone(), node.clone());
            }
        }
        info!(
            alive = self.alive_nodes.len(),
            dead = self.dead_nodes.len(),
            "Node manager initialized"
        );
    }

    /// Start a background health check loop that periodically probes alive nodes.
    /// If a node fails `failure_threshold` consecutive checks, it is marked dead.
    ///
    /// Maps C++ `GcsHealthCheckManager`.
    pub fn start_health_check_loop(
        self: &Arc<Self>,
        period_ms: u64,
        timeout_ms: u64,
        failure_threshold: u32,
    ) {
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            // Track consecutive failure counts per node.
            let failure_counts: DashMap<Vec<u8>, u32> = DashMap::new();

            loop {
                tokio::time::sleep(Duration::from_millis(period_ms)).await;

                // Snapshot alive nodes.
                let nodes: Vec<(Vec<u8>, GcsNodeInfo)> = mgr
                    .alive_nodes
                    .iter()
                    .map(|e| (e.key().clone(), e.value().clone()))
                    .collect();

                for (node_id, node_info) in nodes {
                    let addr = format!(
                        "http://{}:{}",
                        node_info.node_manager_address, node_info.node_manager_port
                    );

                    let is_healthy = tokio::time::timeout(
                        Duration::from_millis(timeout_ms),
                        async {
                            // Try to connect and send a health check.
                            let channel = tonic::transport::Channel::from_shared(addr.clone())
                                .map_err(|_| ())?
                                .connect()
                                .await
                                .map_err(|_| ())?;
                            let mut client =
                                tonic_health::pb::health_client::HealthClient::new(channel);
                            client
                                .check(tonic_health::pb::HealthCheckRequest {
                                    service: String::new(),
                                })
                                .await
                                .map_err(|_| ())?;
                            Ok::<(), ()>(())
                        },
                    )
                    .await
                    .map(|r| r.is_ok())
                    .unwrap_or(false);

                    if is_healthy {
                        failure_counts.remove(&node_id);
                    } else {
                        let mut count = failure_counts
                            .entry(node_id.clone())
                            .or_insert(0);
                        *count += 1;
                        debug!(
                            node_id = hex::encode(&node_id),
                            failures = *count,
                            threshold = failure_threshold,
                            "Health check failed for node"
                        );
                        if *count >= failure_threshold {
                            drop(count);
                            failure_counts.remove(&node_id);
                            info!(
                                node_id = hex::encode(&node_id),
                                "Node marked dead by health check"
                            );
                            mgr.remove_node(&node_id).await;
                        }
                    }
                }
            }
        });
    }
}

// We need hex encoding for node IDs in storage keys -- use a simple implementation.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[tonic::async_trait]
impl NodeInfoGcsService for GcsNodeManager {
    async fn get_cluster_id(
        &self,
        _request: Request<GetClusterIdRequest>,
    ) -> Result<Response<GetClusterIdReply>, Status> {
        Ok(Response::new(GetClusterIdReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
            cluster_id: self.cluster_id.clone(),
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeReply>, Status> {
        let req = request.into_inner();
        if let Some(node) = req.node_info {
            self.add_node(node).await;
        }
        Ok(Response::new(RegisterNodeReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
        }))
    }

    async fn unregister_node(
        &self,
        request: Request<UnregisterNodeRequest>,
    ) -> Result<Response<UnregisterNodeReply>, Status> {
        let req = request.into_inner();
        self.remove_node(&req.node_id).await;
        Ok(Response::new(UnregisterNodeReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
        }))
    }

    async fn get_all_node_info(
        &self,
        _request: Request<GetAllNodeInfoRequest>,
    ) -> Result<Response<GetAllNodeInfoReply>, Status> {
        let mut infos = Vec::new();
        for entry in self.alive_nodes.iter() {
            infos.push(entry.value().clone());
        }
        for entry in self.dead_nodes.iter() {
            infos.push(entry.value().clone());
        }
        let total = infos.len() as i64;
        Ok(Response::new(GetAllNodeInfoReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
            node_info_list: infos,
            total,
            num_filtered: 0,
        }))
    }

    async fn check_alive(
        &self,
        request: Request<CheckAliveRequest>,
    ) -> Result<Response<CheckAliveReply>, Status> {
        let req = request.into_inner();
        let results: Vec<bool> = req
            .node_ids
            .iter()
            .map(|id| self.alive_nodes.contains_key(id.as_slice()))
            .collect();
        Ok(Response::new(CheckAliveReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
            ray_version: String::new(),
            raylet_alive: results,
        }))
    }

    async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainNodeReply>, Status> {
        let _req = request.into_inner();
        Ok(Response::new(DrainNodeReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
            drain_node_status: vec![DrainNodeStatus {
                node_id: vec![],
            }],
        }))
    }

    async fn get_all_node_address_and_liveness(
        &self,
        _request: Request<GetAllNodeAddressAndLivenessRequest>,
    ) -> Result<Response<GetAllNodeAddressAndLivenessReply>, Status> {
        let mut entries = Vec::new();
        for entry in self.alive_nodes.iter() {
            let node = entry.value();
            entries.push(GcsNodeAddressAndLiveness {
                node_id: node.node_id.clone(),
                node_manager_address: node.node_manager_address.clone(),
                node_manager_port: node.node_manager_port,
                object_manager_port: node.object_manager_port,
                state: gcs_node_info::GcsNodeState::Alive as i32,
                death_info: None,
            });
        }
        Ok(Response::new(GetAllNodeAddressAndLivenessReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
            node_info_list: entries,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;

    fn make_manager() -> GcsNodeManager {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(GcsPublisher::new(64));
        GcsNodeManager::new(table_storage, publisher, b"cluster_1".to_vec())
    }

    fn make_node(id: &[u8]) -> GcsNodeInfo {
        GcsNodeInfo {
            node_id: id.to_vec(),
            state: gcs_node_info::GcsNodeState::Alive as i32,
            node_manager_address: "127.0.0.1".into(),
            node_manager_port: 12345,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_add_and_get_node() {
        let mgr = make_manager();
        let node = make_node(b"node1");
        mgr.add_node(node.clone()).await;

        assert!(mgr.is_node_alive(b"node1"));
        let got = mgr.get_alive_node(b"node1").unwrap();
        assert_eq!(got.node_manager_address, "127.0.0.1");
    }

    #[tokio::test]
    async fn test_remove_node() {
        let mgr = make_manager();
        mgr.add_node(make_node(b"node1")).await;
        assert!(mgr.is_node_alive(b"node1"));

        let removed = mgr.remove_node(b"node1").await;
        assert!(removed.is_some());
        assert!(!mgr.is_node_alive(b"node1"));
    }

    #[tokio::test]
    async fn test_remove_nonexistent_node() {
        let mgr = make_manager();
        let removed = mgr.remove_node(b"nope").await;
        assert!(removed.is_none());
    }

    #[tokio::test]
    async fn test_get_all_alive_nodes() {
        let mgr = make_manager();
        mgr.add_node(make_node(b"n1")).await;
        mgr.add_node(make_node(b"n2")).await;
        assert_eq!(mgr.get_all_alive_nodes().len(), 2);
    }

    #[tokio::test]
    async fn test_node_added_listener() {
        let mgr = make_manager();
        let mut rx = mgr.subscribe_node_added();
        mgr.add_node(make_node(b"n1")).await;
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.node_id, b"n1");
    }

    #[tokio::test]
    async fn test_node_removed_listener() {
        let mgr = make_manager();
        let mut rx = mgr.subscribe_node_removed();
        mgr.add_node(make_node(b"n1")).await;
        mgr.remove_node(b"n1").await;
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.node_id, b"n1");
    }

    // ─── gRPC handler tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn test_grpc_get_cluster_id() {
        let mgr = make_manager();
        let reply = mgr
            .get_cluster_id(Request::new(GetClusterIdRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.cluster_id, b"cluster_1");
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_grpc_register_node() {
        let mgr = make_manager();
        let node = make_node(b"node_r1");
        mgr.register_node(Request::new(RegisterNodeRequest {
            node_info: Some(node),
        }))
        .await
        .unwrap();

        assert!(mgr.is_node_alive(b"node_r1"));
    }

    #[tokio::test]
    async fn test_grpc_unregister_node() {
        let mgr = make_manager();
        mgr.register_node(Request::new(RegisterNodeRequest {
            node_info: Some(make_node(b"node_u1")),
        }))
        .await
        .unwrap();
        assert!(mgr.is_node_alive(b"node_u1"));

        mgr.unregister_node(Request::new(UnregisterNodeRequest {
            node_id: b"node_u1".to_vec(),
            node_death_info: None,
        }))
        .await
        .unwrap();
        assert!(!mgr.is_node_alive(b"node_u1"));
    }

    #[tokio::test]
    async fn test_grpc_get_all_node_info() {
        let mgr = make_manager();
        for i in 0..2 {
            mgr.register_node(Request::new(RegisterNodeRequest {
                node_info: Some(make_node(format!("n{i}").as_bytes())),
            }))
            .await
            .unwrap();
        }

        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 2);
        assert_eq!(reply.total, 2);
    }

    #[tokio::test]
    async fn test_grpc_check_alive() {
        let mgr = make_manager();
        mgr.register_node(Request::new(RegisterNodeRequest {
            node_info: Some(make_node(b"alive_node")),
        }))
        .await
        .unwrap();

        let reply = mgr
            .check_alive(Request::new(CheckAliveRequest {
                node_ids: vec![b"alive_node".to_vec(), b"random_node".to_vec()],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.raylet_alive, vec![true, false]);
    }

    #[tokio::test]
    async fn test_grpc_drain_node() {
        let mgr = make_manager();
        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                drain_node_data: vec![],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_grpc_get_all_node_address_and_liveness() {
        let mgr = make_manager();
        let mut node = make_node(b"addr_node");
        node.node_manager_address = "10.0.0.1".into();
        node.node_manager_port = 9999;
        mgr.register_node(Request::new(RegisterNodeRequest {
            node_info: Some(node),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_all_node_address_and_liveness(Request::new(
                GetAllNodeAddressAndLivenessRequest {
                    node_ids: vec![],
                    limit: None,
                    state_filter: None,
                },
            ))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.node_info_list.len(), 1);
        let entry = &reply.node_info_list[0];
        assert_eq!(entry.node_manager_address, "10.0.0.1");
        assert_eq!(entry.node_manager_port, 9999);
        assert_eq!(entry.state, gcs_node_info::GcsNodeState::Alive as i32);
    }

    #[tokio::test]
    async fn test_initialize() {
        let mgr = make_manager();

        let mut nodes = HashMap::new();
        let mut alive_node = make_node(b"n_alive");
        alive_node.state = gcs_node_info::GcsNodeState::Alive as i32;
        nodes.insert("n_alive".to_string(), alive_node);

        let mut dead_node = make_node(b"n_dead");
        dead_node.state = gcs_node_info::GcsNodeState::Dead as i32;
        nodes.insert("n_dead".to_string(), dead_node);

        mgr.initialize(&nodes).await;

        assert!(mgr.is_node_alive(b"n_alive"));
        assert!(!mgr.is_node_alive(b"n_dead"));

        // get_all_node_info should return both alive and dead.
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 2);
    }
}
