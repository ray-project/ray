//! GCS Node Manager -- manages node registration, health, and lifecycle.
//!
//! Maps C++ `GcsNodeManager` from `src/ray/gcs/gcs_node_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use prost::Message;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use gcs_proto::ray::rpc::node_info_gcs_service_server::NodeInfoGcsService;
use gcs_proto::ray::rpc::*;
use gcs_pubsub::GcsPublisher;
use gcs_table_storage::GcsTableStorage;

use crate::export_event_writer::{ExportEventManager, ExportNodeData, ExportNodeDeathInfo};

/// Ray version string, matching C++ `kRayVersion` from `src/ray/common/constants.h`.
const RAY_VERSION: &str = "3.0.0.dev0";

/// Drain request info stored per node.
/// Maps C++ `rpc::autoscaler::DrainNodeRequest` stored in `GcsNodeManager::draining_nodes_`.
#[derive(Clone, Debug)]
pub struct DrainNodeInfo {
    pub reason: i32,
    pub reason_message: String,
    pub deadline_timestamp_ms: i64,
}

/// Draining event: (node_id, deadline_timestamp_ms).
/// Maps C++ `node_draining_listeners_` callback `(NodeID, bool, int64_t)`.
pub type DrainEvent = (Vec<u8>, i64);

/// GCS Node Manager. Maps C++ `GcsNodeManager`.
pub struct GcsNodeManager {
    alive_nodes: DashMap<Vec<u8>, GcsNodeInfo>,
    dead_nodes: DashMap<Vec<u8>, GcsNodeInfo>,
    /// Drain requests for nodes that have received drain orders.
    /// Invariant: keys must be a subset of `alive_nodes` keys.
    /// Entries are removed when a node is removed from `alive_nodes`.
    /// Maps C++ `GcsNodeManager::draining_nodes_`.
    draining_nodes: DashMap<Vec<u8>, DrainNodeInfo>,
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    cluster_id: Vec<u8>,
    node_added_tx: broadcast::Sender<GcsNodeInfo>,
    node_removed_tx: broadcast::Sender<GcsNodeInfo>,
    /// Maps C++ `node_draining_listeners_`.
    node_draining_tx: broadcast::Sender<DrainEvent>,
    /// Optional export-event sink. When set, node lifecycle changes write
    /// `EXPORT_NODE` JSON-line events. Maps C++ `RayExportEvent` calls in
    /// `gcs_node_manager.cc:60-90` (gated by `export_event_write_enabled_`).
    export_events: Arc<ExportEventManager>,
}

impl GcsNodeManager {
    /// Backwards-compatible constructor. Export events are disabled — use
    /// `with_export_events` to wire a real writer.
    pub fn new(
        table_storage: Arc<GcsTableStorage>,
        publisher: Arc<GcsPublisher>,
        cluster_id: Vec<u8>,
    ) -> Self {
        Self::with_export_events(
            table_storage,
            publisher,
            cluster_id,
            ExportEventManager::disabled(),
        )
    }

    /// Construct with an export-event writer. When the writer is enabled,
    /// every `add_node`/`remove_node` emits an `EXPORT_NODE` JSON line.
    pub fn with_export_events(
        table_storage: Arc<GcsTableStorage>,
        publisher: Arc<GcsPublisher>,
        cluster_id: Vec<u8>,
        export_events: Arc<ExportEventManager>,
    ) -> Self {
        let (node_added_tx, _) = broadcast::channel(256);
        let (node_removed_tx, _) = broadcast::channel(256);
        let (node_draining_tx, _) = broadcast::channel(256);
        Self {
            alive_nodes: DashMap::new(),
            dead_nodes: DashMap::new(),
            draining_nodes: DashMap::new(),
            table_storage,
            publisher,
            cluster_id,
            node_added_tx,
            node_removed_tx,
            node_draining_tx,
            export_events,
        }
    }

    /// Build an `ExportNodeData` payload from a `GcsNodeInfo`.
    /// Mirrors C++ `gcs_node_manager.cc:68-88`.
    fn node_to_export(node: &GcsNodeInfo) -> ExportNodeData {
        ExportNodeData {
            node_id: hex::encode(&node.node_id),
            node_manager_address: node.node_manager_address.clone(),
            resources_total: node.resources_total.clone(),
            node_name: node.node_name.clone(),
            start_time_ms: node.start_time_ms as i64,
            end_time_ms: node.end_time_ms as i64,
            is_head_node: node.is_head_node,
            labels: node.labels.clone(),
            state: match gcs_node_info::GcsNodeState::try_from(node.state) {
                Ok(gcs_node_info::GcsNodeState::Alive) => "ALIVE".into(),
                Ok(gcs_node_info::GcsNodeState::Dead) => "DEAD".into(),
                _ => "UNSPECIFIED".into(),
            },
            death_info: node.death_info.as_ref().and_then(|di| {
                if di.reason_message.is_empty() && di.reason == 0 {
                    None
                } else {
                    Some(ExportNodeDeathInfo {
                        reason: di.reason.to_string(),
                        reason_message: di.reason_message.clone(),
                    })
                }
            }),
        }
    }

    pub fn subscribe_node_added(&self) -> broadcast::Receiver<GcsNodeInfo> {
        self.node_added_tx.subscribe()
    }

    pub fn subscribe_node_removed(&self) -> broadcast::Receiver<GcsNodeInfo> {
        self.node_removed_tx.subscribe()
    }

    /// Subscribe to draining events: `(node_id, deadline_timestamp_ms)`.
    /// Maps C++ `AddNodeDrainingListener`.
    pub fn subscribe_node_draining(&self) -> broadcast::Receiver<DrainEvent> {
        self.node_draining_tx.subscribe()
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

    /// Get all dead nodes. Maps C++ `GcsNodeManager::GetAllDeadNodes`.
    pub fn get_all_dead_nodes(&self) -> HashMap<Vec<u8>, GcsNodeInfo> {
        self.dead_nodes
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    pub fn is_node_alive(&self, node_id: &[u8]) -> bool {
        self.alive_nodes.contains_key(node_id)
    }

    /// Check if a node is known to be dead.
    /// Maps C++ `GcsNodeManager::IsNodeDead`.
    pub fn is_node_dead(&self, node_id: &[u8]) -> bool {
        self.dead_nodes.contains_key(node_id)
    }

    /// Whether a node currently has a recorded drain request.
    /// Maps C++ `draining_nodes_.contains(node_id)`.
    pub fn is_node_draining(&self, node_id: &[u8]) -> bool {
        self.draining_nodes.contains_key(node_id)
    }

    /// Set a node to draining state.
    /// Maps C++ `GcsNodeManager::SetNodeDraining`.
    ///
    /// The node must be alive. If not alive, the call is a no-op (logged).
    /// If a drain request already exists, it is overwritten.
    /// Notifies draining listeners (maps C++ `node_draining_listeners_`).
    pub fn set_node_draining(&self, node_id: Vec<u8>, info: DrainNodeInfo) {
        if !self.alive_nodes.contains_key(node_id.as_slice()) {
            info!(
                node_id = hex::encode(&node_id),
                "Skip setting node to be draining, which is already removed"
            );
            return;
        }

        let deadline_ms = info.deadline_timestamp_ms;

        if let Some(mut existing) = self.draining_nodes.get_mut(node_id.as_slice()) {
            info!(
                node_id = hex::encode(&node_id),
                "Drain request for node already exists. Overwriting."
            );
            *existing = info;
        } else {
            info!(
                node_id = hex::encode(&node_id),
                reason = info.reason,
                deadline_ms = info.deadline_timestamp_ms,
                "Set node to be draining"
            );
            self.draining_nodes.insert(node_id.clone(), info);
        }

        // Notify draining listeners (maps C++ node_draining_listeners_ callback).
        let _ = self.node_draining_tx.send((node_id, deadline_ms));
    }

    /// Infer the death info for a node based on its draining state.
    ///
    /// Maps C++ `GcsNodeManager::InferDeathInfo` (gcs_node_manager.cc:541-567).
    ///
    /// - Not draining → UNEXPECTED_TERMINATION (heartbeat failure)
    /// - Draining with no deadline (0) → UNEXPECTED_TERMINATION
    /// - Draining with expired deadline + PREEMPTION reason → AUTOSCALER_DRAIN_PREEMPTED
    /// - Otherwise → UNEXPECTED_TERMINATION
    pub fn infer_death_info(&self, node_id: &[u8]) -> NodeDeathInfo {
        // DrainNodeReason::Preemption == 2 (from autoscaler.proto).
        const PREEMPTION_REASON: i32 = 2;

        let expect_force_termination = self
            .draining_nodes
            .get(node_id)
            .map(|entry| {
                let drain = entry.value();
                drain.deadline_timestamp_ms != 0
                    && (current_time_ms() as i64 > drain.deadline_timestamp_ms)
                    && drain.reason == PREEMPTION_REASON
            })
            .unwrap_or(false);

        if expect_force_termination {
            let reason_message = self
                .draining_nodes
                .get(node_id)
                .map(|e| e.value().reason_message.clone())
                .unwrap_or_default();
            info!(
                node_id = hex::encode(node_id),
                "Node was forcibly preempted"
            );
            NodeDeathInfo {
                reason: node_death_info::Reason::AutoscalerDrainPreempted as i32,
                reason_message,
            }
        } else {
            NodeDeathInfo {
                reason: node_death_info::Reason::UnexpectedTermination as i32,
                reason_message: "health check failed due to missing too many heartbeats"
                    .to_string(),
            }
        }
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
        // Emit EXPORT_NODE event (parity with C++ WriteExportNodeEvent in
        // gcs_node_manager.cc:60-90).
        self.export_events
            .report_node_event(&Self::node_to_export(&node));
        info!(node_id = hex::encode(&node_id), "Node registered");
    }

    /// Remove a node (move from alive to dead).
    ///
    /// Maps C++ `GcsNodeManager::RemoveNodeFromCache`. Sets `death_info`,
    /// `state = DEAD`, and `end_time_ms` on the node before adding it to the
    /// dead cache, persisting, and publishing.
    pub async fn remove_node(
        &self,
        node_id: &[u8],
        death_info: NodeDeathInfo,
    ) -> Option<GcsNodeInfo> {
        if let Some((_, mut node)) = self.alive_nodes.remove(node_id) {
            // Clean up draining state (invariant: draining_nodes keys ⊆ alive_nodes keys).
            self.draining_nodes.remove(node_id);
            node.state = gcs_node_info::GcsNodeState::Dead as i32;
            node.death_info = Some(death_info);
            node.end_time_ms = current_time_ms();
            self.dead_nodes.insert(node_id.to_vec(), node.clone());
            self.table_storage
                .node_table()
                .put(&hex::encode(node_id), &node)
                .await;
            let _ = self.node_removed_tx.send(node.clone());
            self.publisher
                .publish_node_info(node_id, node.encode_to_vec());
            // Emit EXPORT_NODE (DEAD) event.
            self.export_events
                .report_node_event(&Self::node_to_export(&node));
            info!(node_id = hex::encode(node_id), "Node removed");
            Some(node)
        } else {
            warn!(node_id = hex::encode(node_id), "Tried to remove unknown node");
            None
        }
    }

    /// Initialize from persisted data.
    ///
    /// Fires `node_added_tx` for each recovered alive node, matching C++
    /// `GcsNodeManager::Initialize` which calls `AddNodeToCache` → fires
    /// `node_added_listeners_` for each alive node.
    pub async fn initialize(&self, nodes: &HashMap<String, GcsNodeInfo>) {
        for (_key, node) in nodes {
            if node.state == gcs_node_info::GcsNodeState::Alive as i32 {
                self.alive_nodes.insert(node.node_id.clone(), node.clone());
                let _ = self.node_added_tx.send(node.clone());
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
                            // Infer death reason from draining state, matching
                            // C++ OnNodeFailure → InferDeathInfo → RemoveNode.
                            let death_info = mgr.infer_death_info(&node_id);
                            mgr.remove_node(&node_id, death_info).await;
                        }
                    }
                }
            }
        });
    }
}

/// Current system time in milliseconds since UNIX epoch.
/// Maps C++ `current_sys_time_ms()`.
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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
        // Pass death_info from request (default if absent), matching C++
        // HandleUnregisterNode which passes request.node_death_info().
        let death_info = req.node_death_info.unwrap_or_default();
        self.remove_node(&req.node_id, death_info).await;
        Ok(Response::new(UnregisterNodeReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
        }))
    }

    /// Get all node info with filtering support.
    ///
    /// Matches C++ HandleGetAllNodeInfo (gcs_node_manager.cc:237-378):
    /// - state_filter: only return ALIVE or DEAD nodes
    /// - node_selectors: filter by node_id, node_name, node_ip_address, is_head_node (OR logic)
    /// - limit: max number of results
    /// - num_filtered: total - returned (accurate count of excluded nodes)
    async fn get_all_node_info(
        &self,
        request: Request<GetAllNodeInfoRequest>,
    ) -> Result<Response<GetAllNodeInfoReply>, Status> {
        let req = request.into_inner();
        let total = (self.alive_nodes.len() + self.dead_nodes.len()) as i64;
        let limit = req.limit.filter(|&l| l > 0).map(|l| l as usize);
        let state_filter = req.state_filter; // Option<i32>: 0=ALIVE, 1=DEAD
        let has_selectors = !req.node_selectors.is_empty();

        // Pre-extract selector values for efficient matching (C++ does this too).
        use gcs_proto::ray::rpc::get_all_node_info_request::node_selector::NodeSelector as NS;
        let mut filter_node_ids: Vec<Vec<u8>> = Vec::new();
        let mut filter_node_names: Vec<String> = Vec::new();
        let mut filter_ip_addresses: Vec<String> = Vec::new();
        let mut filter_is_head_node: Option<bool> = None;

        for selector in &req.node_selectors {
            if let Some(ref ns) = selector.node_selector {
                match ns {
                    NS::NodeId(id) => filter_node_ids.push(id.clone()),
                    NS::NodeName(name) => filter_node_names.push(name.clone()),
                    NS::NodeIpAddress(ip) => filter_ip_addresses.push(ip.clone()),
                    NS::IsHeadNode(v) => filter_is_head_node = Some(*v),
                }
            }
        }

        // Check if a node matches any of the selectors (OR logic, matching C++).
        let matches_selectors = |node: &GcsNodeInfo| -> bool {
            if !has_selectors {
                return true; // No selectors = include all.
            }
            if filter_node_ids.iter().any(|id| *id == node.node_id) {
                return true;
            }
            if filter_node_names.iter().any(|n| *n == node.node_name) {
                return true;
            }
            if filter_ip_addresses.iter().any(|ip| *ip == node.node_manager_address) {
                return true;
            }
            if let Some(head) = filter_is_head_node {
                if node.is_head_node == head {
                    return true;
                }
            }
            false
        };

        let mut infos = Vec::new();
        let include_alive = state_filter.is_none()
            || state_filter == Some(gcs_node_info::GcsNodeState::Alive as i32);
        let include_dead = state_filter.is_none()
            || state_filter == Some(gcs_node_info::GcsNodeState::Dead as i32);

        // Iterate alive nodes first, then dead (matching C++ order).
        if include_alive {
            for entry in self.alive_nodes.iter() {
                if let Some(lim) = limit {
                    if infos.len() >= lim {
                        break;
                    }
                }
                if matches_selectors(entry.value()) {
                    infos.push(entry.value().clone());
                }
            }
        }
        if include_dead {
            for entry in self.dead_nodes.iter() {
                if let Some(lim) = limit {
                    if infos.len() >= lim {
                        break;
                    }
                }
                if matches_selectors(entry.value()) {
                    infos.push(entry.value().clone());
                }
            }
        }

        let num_filtered = total - infos.len() as i64;

        Ok(Response::new(GetAllNodeInfoReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
            node_info_list: infos,
            total,
            num_filtered,
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
            ray_version: RAY_VERSION.to_string(),
            raylet_alive: results,
        }))
    }

    /// Drain nodes — acknowledge each requested node for draining.
    ///
    /// Matches C++ HandleDrainNode (gcs_node_manager.cc:199-214):
    /// - Iterates every entry in drain_node_data
    /// - For each node: calls DrainNode helper, adds DrainNodeStatus with node_id
    /// - DrainNode checks alive_nodes; if not found, logs warning and skips
    /// - If found, C++ calls ShutdownRaylet(graceful=true) — we log but skip
    ///   the raylet call since no raylet client pool exists in Rust yet
    async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainNodeReply>, Status> {
        let req = request.into_inner();
        let mut statuses = Vec::with_capacity(req.drain_node_data.len());

        for drain_data in &req.drain_node_data {
            let node_id = &drain_data.node_id;

            if self.alive_nodes.contains_key(node_id.as_slice()) {
                info!(node_id = hex::encode(node_id), "DrainNode() for node");
                // C++ would call raylet_client->ShutdownRaylet(node_id, graceful=true)
                // here. We log the drain but skip the raylet call since no
                // raylet client pool exists in Rust yet.
            } else {
                warn!(
                    node_id = hex::encode(node_id),
                    "Skip draining node which is already removed"
                );
            }

            statuses.push(DrainNodeStatus {
                node_id: node_id.clone(),
            });
        }

        Ok(Response::new(DrainNodeReply {
            status: Some(GcsStatus {
                code: 0,
                message: String::new(),
            }),
            drain_node_status: statuses,
        }))
    }

    /// Get node address and liveness info with filtering support.
    ///
    /// Maps C++ HandleGetAllNodeAddressAndLiveness + GetAllNodeAddressAndLiveness
    /// (gcs_node_manager.cc:395-485):
    /// - node_ids: if non-empty, only return those specific nodes (optimized path)
    /// - state_filter: filter by ALIVE or DEAD
    /// - limit: max number of results (0 or absent = unlimited)
    /// - Returns both alive and dead nodes with state and death_info from GcsNodeInfo
    async fn get_all_node_address_and_liveness(
        &self,
        request: Request<GetAllNodeAddressAndLivenessRequest>,
    ) -> Result<Response<GetAllNodeAddressAndLivenessReply>, Status> {
        let req = request.into_inner();
        let limit = if req.limit.map_or(true, |l| l <= 0) {
            usize::MAX
        } else {
            req.limit.unwrap() as usize
        };
        let state_filter = req.state_filter; // Option<i32>: 0=ALIVE, 1=DEAD
        let include_alive = state_filter.is_none()
            || state_filter == Some(gcs_node_info::GcsNodeState::Alive as i32);
        let include_dead = state_filter.is_none()
            || state_filter == Some(gcs_node_info::GcsNodeState::Dead as i32);

        let convert = |node: &GcsNodeInfo| -> GcsNodeAddressAndLiveness {
            GcsNodeAddressAndLiveness {
                node_id: node.node_id.clone(),
                node_manager_address: node.node_manager_address.clone(),
                node_manager_port: node.node_manager_port,
                object_manager_port: node.object_manager_port,
                state: node.state,
                death_info: node.death_info.clone(),
            }
        };

        let mut entries = Vec::new();

        if !req.node_ids.is_empty() {
            // Optimized path: look up only requested node_ids.
            // Matches C++ GetAllNodeAddressAndLiveness optimized path.
            for nid in &req.node_ids {
                if entries.len() >= limit {
                    break;
                }
                if include_alive {
                    if let Some(node) = self.alive_nodes.get(nid.as_slice()) {
                        entries.push(convert(node.value()));
                    }
                }
                if entries.len() >= limit {
                    break;
                }
                if include_dead {
                    if let Some(node) = self.dead_nodes.get(nid.as_slice()) {
                        entries.push(convert(node.value()));
                    }
                }
            }
        } else {
            // Full scan path with state filter.
            if include_alive {
                for entry in self.alive_nodes.iter() {
                    if entries.len() >= limit {
                        break;
                    }
                    entries.push(convert(entry.value()));
                }
            }
            if include_dead {
                for entry in self.dead_nodes.iter() {
                    if entries.len() >= limit {
                        break;
                    }
                    entries.push(convert(entry.value()));
                }
            }
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

    fn temp_export_dir() -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!("gcs_node_mgr_export_{nanos}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn make_manager_with_export(
        dir: &std::path::Path,
    ) -> GcsNodeManager {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(GcsPublisher::new(64));
        let export = crate::export_event_writer::ExportEventManager::new(dir).unwrap();
        GcsNodeManager::with_export_events(
            table_storage,
            publisher,
            b"cluster_1".to_vec(),
            export,
        )
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

        let removed = mgr
            .remove_node(b"node1", NodeDeathInfo::default())
            .await;
        assert!(removed.is_some());
        assert!(!mgr.is_node_alive(b"node1"));
    }

    #[tokio::test]
    async fn test_remove_nonexistent_node() {
        let mgr = make_manager();
        let removed = mgr
            .remove_node(b"nope", NodeDeathInfo::default())
            .await;
        assert!(removed.is_none());
    }

    #[tokio::test]
    async fn test_remove_node_stores_death_info() {
        let mgr = make_manager();
        mgr.add_node(make_node(b"n1")).await;
        let removed = mgr
            .remove_node(
                b"n1",
                NodeDeathInfo {
                    reason: node_death_info::Reason::ExpectedTermination as i32,
                    reason_message: "graceful".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(removed.state, gcs_node_info::GcsNodeState::Dead as i32);
        let di = removed.death_info.unwrap();
        assert_eq!(
            di.reason,
            node_death_info::Reason::ExpectedTermination as i32
        );
        assert_eq!(di.reason_message, "graceful");
        assert!(removed.end_time_ms > 0);
    }

    #[tokio::test]
    async fn test_remove_node_cleans_draining_state() {
        let mgr = make_manager();
        mgr.add_node(make_node(b"n1")).await;
        mgr.set_node_draining(
            b"n1".to_vec(),
            DrainNodeInfo {
                reason: 1,
                reason_message: "idle".into(),
                deadline_timestamp_ms: 0,
            },
        );
        // After removal, draining state should be cleaned up.
        mgr.remove_node(b"n1", NodeDeathInfo::default()).await;
        assert!(!mgr.is_node_alive(b"n1"));
        assert!(mgr.is_node_dead(b"n1"));
    }

    #[tokio::test]
    async fn test_infer_death_info_not_draining() {
        // Node not in draining_nodes → UNEXPECTED_TERMINATION.
        let mgr = make_manager();
        mgr.add_node(make_node(b"n1")).await;
        let di = mgr.infer_death_info(b"n1");
        assert_eq!(
            di.reason,
            node_death_info::Reason::UnexpectedTermination as i32
        );
        assert!(di.reason_message.contains("heartbeats"));
    }

    #[tokio::test]
    async fn test_infer_death_info_draining_no_deadline() {
        // Draining with deadline=0 → UNEXPECTED_TERMINATION (no force termination).
        let mgr = make_manager();
        mgr.add_node(make_node(b"n1")).await;
        mgr.set_node_draining(
            b"n1".to_vec(),
            DrainNodeInfo {
                reason: 2, // Preemption
                reason_message: "preempt".into(),
                deadline_timestamp_ms: 0,
            },
        );
        let di = mgr.infer_death_info(b"n1");
        assert_eq!(
            di.reason,
            node_death_info::Reason::UnexpectedTermination as i32
        );
    }

    #[tokio::test]
    async fn test_infer_death_info_draining_preempted() {
        // Draining with expired deadline + PREEMPTION → AUTOSCALER_DRAIN_PREEMPTED.
        let mgr = make_manager();
        mgr.add_node(make_node(b"n1")).await;
        mgr.set_node_draining(
            b"n1".to_vec(),
            DrainNodeInfo {
                reason: 2, // Preemption
                reason_message: "spot instance preempted".into(),
                deadline_timestamp_ms: 1, // far in the past
            },
        );
        let di = mgr.infer_death_info(b"n1");
        assert_eq!(
            di.reason,
            node_death_info::Reason::AutoscalerDrainPreempted as i32
        );
        assert_eq!(di.reason_message, "spot instance preempted");
    }

    #[tokio::test]
    async fn test_infer_death_info_draining_idle_not_preempted() {
        // Draining with expired deadline + IDLE_TERMINATION → UNEXPECTED_TERMINATION.
        // Only PREEMPTION reason triggers AUTOSCALER_DRAIN_PREEMPTED.
        let mgr = make_manager();
        mgr.add_node(make_node(b"n1")).await;
        mgr.set_node_draining(
            b"n1".to_vec(),
            DrainNodeInfo {
                reason: 1, // IdleTermination
                reason_message: "idle".into(),
                deadline_timestamp_ms: 1, // far in the past
            },
        );
        let di = mgr.infer_death_info(b"n1");
        assert_eq!(
            di.reason,
            node_death_info::Reason::UnexpectedTermination as i32
        );
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
        mgr.remove_node(b"n1", NodeDeathInfo::default()).await;
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
        assert_eq!(reply.ray_version, RAY_VERSION);
    }

    #[tokio::test]
    async fn test_grpc_drain_node_empty() {
        let mgr = make_manager();
        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                drain_node_data: vec![],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
        assert!(reply.drain_node_status.is_empty());
    }

    #[tokio::test]
    async fn test_grpc_drain_node_echoes_node_ids() {
        let mgr = make_manager();
        mgr.register_node(Request::new(RegisterNodeRequest {
            node_info: Some(make_node(b"n1")),
        }))
        .await
        .unwrap();
        mgr.register_node(Request::new(RegisterNodeRequest {
            node_info: Some(make_node(b"n2")),
        }))
        .await
        .unwrap();

        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                drain_node_data: vec![
                    DrainNodeData { node_id: b"n1".to_vec() },
                    DrainNodeData { node_id: b"n2".to_vec() },
                ],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.drain_node_status.len(), 2);
        assert_eq!(reply.drain_node_status[0].node_id, b"n1");
        assert_eq!(reply.drain_node_status[1].node_id, b"n2");
    }

    #[tokio::test]
    async fn test_grpc_drain_node_unknown_node_still_echoed() {
        let mgr = make_manager();
        // Drain a node that doesn't exist — C++ logs warning but still adds status.
        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                drain_node_data: vec![
                    DrainNodeData { node_id: b"unknown".to_vec() },
                ],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.drain_node_status.len(), 1);
        assert_eq!(reply.drain_node_status[0].node_id, b"unknown");
    }

    // ─── get_all_node_address_and_liveness tests ─────────────────────────

    #[tokio::test]
    async fn test_liveness_alive_node() {
        let mgr = make_manager();
        let mut node = make_node(b"addr_node");
        node.node_manager_address = "10.0.0.1".into();
        node.node_manager_port = 9999;
        node.object_manager_port = 8888;
        mgr.add_node(node).await;

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
        assert_eq!(entry.object_manager_port, 8888);
        assert_eq!(entry.state, gcs_node_info::GcsNodeState::Alive as i32);
        // Alive node: death_info is None (default/unset).
        assert!(entry.death_info.is_none());
    }

    #[tokio::test]
    async fn test_liveness_includes_dead_nodes() {
        // Both alive and dead should be returned when no state_filter.
        let mgr = make_manager();
        mgr.add_node(make_node(b"a1")).await;
        mgr.add_node(make_node(b"d1")).await;
        mgr.remove_node(
            b"d1",
            NodeDeathInfo {
                reason: node_death_info::Reason::UnexpectedTermination as i32,
                reason_message: "heartbeat".into(),
            },
        )
        .await;

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

        assert_eq!(reply.node_info_list.len(), 2);
        let alive: Vec<_> = reply
            .node_info_list
            .iter()
            .filter(|e| e.state == gcs_node_info::GcsNodeState::Alive as i32)
            .collect();
        let dead: Vec<_> = reply
            .node_info_list
            .iter()
            .filter(|e| e.state == gcs_node_info::GcsNodeState::Dead as i32)
            .collect();
        assert_eq!(alive.len(), 1);
        assert_eq!(dead.len(), 1);
    }

    #[tokio::test]
    async fn test_liveness_dead_node_has_death_info() {
        let mgr = make_manager();
        mgr.add_node(make_node(b"n1")).await;
        mgr.remove_node(
            b"n1",
            NodeDeathInfo {
                reason: node_death_info::Reason::UnexpectedTermination as i32,
                reason_message: "health check failed due to missing too many heartbeats"
                    .to_string(),
            },
        )
        .await;

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
        assert_eq!(entry.state, gcs_node_info::GcsNodeState::Dead as i32);
        let di = entry.death_info.as_ref().unwrap();
        assert_eq!(
            di.reason,
            node_death_info::Reason::UnexpectedTermination as i32
        );
        assert!(di.reason_message.contains("heartbeats"));
    }

    #[tokio::test]
    async fn test_liveness_state_filter_alive() {
        let mgr = make_manager();
        mgr.add_node(make_node(b"a1")).await;
        mgr.add_node(make_node(b"d1")).await;
        mgr.remove_node(b"d1", NodeDeathInfo::default()).await;

        let reply = mgr
            .get_all_node_address_and_liveness(Request::new(
                GetAllNodeAddressAndLivenessRequest {
                    node_ids: vec![],
                    limit: None,
                    state_filter: Some(gcs_node_info::GcsNodeState::Alive as i32),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.node_info_list.len(), 1);
        assert_eq!(
            reply.node_info_list[0].state,
            gcs_node_info::GcsNodeState::Alive as i32
        );
    }

    #[tokio::test]
    async fn test_liveness_state_filter_dead() {
        let mgr = make_manager();
        mgr.add_node(make_node(b"a1")).await;
        mgr.add_node(make_node(b"d1")).await;
        mgr.remove_node(
            b"d1",
            NodeDeathInfo {
                reason: node_death_info::Reason::UnexpectedTermination as i32,
                reason_message: "gone".into(),
            },
        )
        .await;

        let reply = mgr
            .get_all_node_address_and_liveness(Request::new(
                GetAllNodeAddressAndLivenessRequest {
                    node_ids: vec![],
                    limit: None,
                    state_filter: Some(gcs_node_info::GcsNodeState::Dead as i32),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.node_info_list.len(), 1);
        let entry = &reply.node_info_list[0];
        assert_eq!(entry.state, gcs_node_info::GcsNodeState::Dead as i32);
        let di = entry.death_info.as_ref().unwrap();
        assert_eq!(
            di.reason,
            node_death_info::Reason::UnexpectedTermination as i32
        );
    }

    #[tokio::test]
    async fn test_liveness_node_ids_filter() {
        let mgr = make_manager();
        mgr.add_node(make_node(b"a1")).await;
        mgr.add_node(make_node(b"a2")).await;
        mgr.add_node(make_node(b"a3")).await;

        let reply = mgr
            .get_all_node_address_and_liveness(Request::new(
                GetAllNodeAddressAndLivenessRequest {
                    node_ids: vec![b"a2".to_vec()],
                    limit: None,
                    state_filter: None,
                },
            ))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.node_info_list.len(), 1);
        assert_eq!(reply.node_info_list[0].node_id, b"a2");
    }

    #[tokio::test]
    async fn test_liveness_node_ids_filter_alive_and_dead() {
        // Optimized path should check both alive and dead for each node_id.
        let mgr = make_manager();
        mgr.add_node(make_node(b"a1")).await;
        mgr.add_node(make_node(b"d1")).await;
        mgr.remove_node(b"d1", NodeDeathInfo::default()).await;

        let reply = mgr
            .get_all_node_address_and_liveness(Request::new(
                GetAllNodeAddressAndLivenessRequest {
                    node_ids: vec![b"a1".to_vec(), b"d1".to_vec()],
                    limit: None,
                    state_filter: None,
                },
            ))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.node_info_list.len(), 2);
    }

    #[tokio::test]
    async fn test_liveness_limit() {
        let mgr = make_manager();
        for i in 0..5 {
            mgr.add_node(make_node(format!("n{i}").as_bytes())).await;
        }

        let reply = mgr
            .get_all_node_address_and_liveness(Request::new(
                GetAllNodeAddressAndLivenessRequest {
                    node_ids: vec![],
                    limit: Some(2),
                    state_filter: None,
                },
            ))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.node_info_list.len(), 2);
    }

    #[tokio::test]
    async fn test_liveness_unregister_with_death_info() {
        // Unregistering via gRPC with death_info should store it.
        let mgr = make_manager();
        mgr.add_node(make_node(b"n1")).await;
        mgr.unregister_node(Request::new(UnregisterNodeRequest {
            node_id: b"n1".to_vec(),
            node_death_info: Some(NodeDeathInfo {
                reason: node_death_info::Reason::ExpectedTermination as i32,
                reason_message: "graceful shutdown".into(),
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_all_node_address_and_liveness(Request::new(
                GetAllNodeAddressAndLivenessRequest {
                    node_ids: vec![],
                    limit: None,
                    state_filter: Some(gcs_node_info::GcsNodeState::Dead as i32),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.node_info_list.len(), 1);
        let entry = &reply.node_info_list[0];
        assert_eq!(entry.state, gcs_node_info::GcsNodeState::Dead as i32);
        let di = entry.death_info.as_ref().unwrap();
        assert_eq!(
            di.reason,
            node_death_info::Reason::ExpectedTermination as i32
        );
        assert_eq!(di.reason_message, "graceful shutdown");
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

    // --- GetAllNodeInfo filtering tests ---

    use gcs_proto::ray::rpc::get_all_node_info_request::{self, node_selector};

    fn make_named_node(id: &[u8], name: &str, ip: &str, is_head: bool) -> GcsNodeInfo {
        GcsNodeInfo {
            node_id: id.to_vec(),
            state: gcs_node_info::GcsNodeState::Alive as i32,
            node_manager_address: ip.to_string(),
            node_name: name.to_string(),
            is_head_node: is_head,
            node_manager_port: 12345,
            ..Default::default()
        }
    }

    /// Set up a manager with 3 alive nodes and 1 dead node for filter tests.
    async fn setup_filter_test() -> GcsNodeManager {
        let mgr = make_manager();
        // Alive nodes.
        mgr.add_node(make_named_node(b"n1", "node-1", "10.0.0.1", true)).await;
        mgr.add_node(make_named_node(b"n2", "node-2", "10.0.0.2", false)).await;
        mgr.add_node(make_named_node(b"n3", "node-3", "10.0.0.3", false)).await;
        // Dead node.
        mgr.add_node(make_named_node(b"n4", "node-4", "10.0.0.4", false)).await;
        mgr.remove_node(
            b"n4",
            NodeDeathInfo {
                reason: node_death_info::Reason::UnexpectedTermination as i32,
                reason_message: "health check failed due to missing too many heartbeats"
                    .to_string(),
            },
        )
        .await;
        mgr
    }

    #[tokio::test]
    async fn test_get_all_no_filters() {
        let mgr = setup_filter_test().await;
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.total, 4);
        assert_eq!(reply.node_info_list.len(), 4);
        assert_eq!(reply.num_filtered, 0);
    }

    #[tokio::test]
    async fn test_state_filter_alive() {
        let mgr = setup_filter_test().await;
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                state_filter: Some(gcs_node_info::GcsNodeState::Alive as i32),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.total, 4);
        assert_eq!(reply.node_info_list.len(), 3); // 3 alive
        assert_eq!(reply.num_filtered, 1); // 1 dead filtered out
        for node in &reply.node_info_list {
            assert_eq!(node.state, gcs_node_info::GcsNodeState::Alive as i32);
        }
    }

    #[tokio::test]
    async fn test_state_filter_dead() {
        let mgr = setup_filter_test().await;
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                state_filter: Some(gcs_node_info::GcsNodeState::Dead as i32),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.total, 4);
        assert_eq!(reply.node_info_list.len(), 1); // 1 dead
        assert_eq!(reply.num_filtered, 3); // 3 alive filtered out
        assert_eq!(reply.node_info_list[0].node_id, b"n4");
    }

    #[tokio::test]
    async fn test_limit() {
        let mgr = setup_filter_test().await;
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                limit: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.total, 4);
        assert_eq!(reply.node_info_list.len(), 2);
        assert_eq!(reply.num_filtered, 2); // 2 excluded by limit
    }

    #[tokio::test]
    async fn test_node_id_selector() {
        let mgr = setup_filter_test().await;
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                node_selectors: vec![
                    get_all_node_info_request::NodeSelector {
                        node_selector: Some(node_selector::NodeSelector::NodeId(b"n2".to_vec())),
                    },
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.total, 4);
        assert_eq!(reply.node_info_list.len(), 1);
        assert_eq!(reply.node_info_list[0].node_id, b"n2");
        assert_eq!(reply.num_filtered, 3);
    }

    #[tokio::test]
    async fn test_node_name_selector() {
        let mgr = setup_filter_test().await;
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                node_selectors: vec![
                    get_all_node_info_request::NodeSelector {
                        node_selector: Some(node_selector::NodeSelector::NodeName("node-3".to_string())),
                    },
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 1);
        assert_eq!(reply.node_info_list[0].node_name, "node-3");
        assert_eq!(reply.num_filtered, 3);
    }

    #[tokio::test]
    async fn test_ip_address_selector() {
        let mgr = setup_filter_test().await;
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                node_selectors: vec![
                    get_all_node_info_request::NodeSelector {
                        node_selector: Some(node_selector::NodeSelector::NodeIpAddress("10.0.0.1".to_string())),
                    },
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 1);
        assert_eq!(reply.node_info_list[0].node_manager_address, "10.0.0.1");
    }

    #[tokio::test]
    async fn test_is_head_node_selector() {
        let mgr = setup_filter_test().await;
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                node_selectors: vec![
                    get_all_node_info_request::NodeSelector {
                        node_selector: Some(node_selector::NodeSelector::IsHeadNode(true)),
                    },
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 1);
        assert!(reply.node_info_list[0].is_head_node);
        assert_eq!(reply.node_info_list[0].node_id, b"n1");
        assert_eq!(reply.num_filtered, 3);
    }

    #[tokio::test]
    async fn test_multiple_selectors_or_logic() {
        let mgr = setup_filter_test().await;
        // Select by node_id=n1 OR node_name=node-3 → should return 2 nodes.
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                node_selectors: vec![
                    get_all_node_info_request::NodeSelector {
                        node_selector: Some(node_selector::NodeSelector::NodeId(b"n1".to_vec())),
                    },
                    get_all_node_info_request::NodeSelector {
                        node_selector: Some(node_selector::NodeSelector::NodeName("node-3".to_string())),
                    },
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 2);
        assert_eq!(reply.num_filtered, 2);
    }

    #[tokio::test]
    async fn test_state_filter_with_selectors() {
        let mgr = setup_filter_test().await;
        // Select dead nodes + filter by node_id=n4 → should return 1.
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                state_filter: Some(gcs_node_info::GcsNodeState::Dead as i32),
                node_selectors: vec![
                    get_all_node_info_request::NodeSelector {
                        node_selector: Some(node_selector::NodeSelector::NodeId(b"n4".to_vec())),
                    },
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 1);
        assert_eq!(reply.node_info_list[0].node_id, b"n4");
    }

    #[tokio::test]
    async fn test_state_filter_alive_with_selector_for_dead_node() {
        let mgr = setup_filter_test().await;
        // Filter for ALIVE state but selector targets dead node n4 → 0 results.
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                state_filter: Some(gcs_node_info::GcsNodeState::Alive as i32),
                node_selectors: vec![
                    get_all_node_info_request::NodeSelector {
                        node_selector: Some(node_selector::NodeSelector::NodeId(b"n4".to_vec())),
                    },
                ],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_info_list.len(), 0);
        assert_eq!(reply.num_filtered, 4);
    }

    #[tokio::test]
    async fn test_limit_with_state_filter() {
        let mgr = setup_filter_test().await;
        // Alive nodes with limit=1.
        let reply = mgr
            .get_all_node_info(Request::new(GetAllNodeInfoRequest {
                state_filter: Some(gcs_node_info::GcsNodeState::Alive as i32),
                limit: Some(1),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.total, 4);
        assert_eq!(reply.node_info_list.len(), 1);
        assert_eq!(reply.num_filtered, 3);
    }

    /// Verifies parity with C++ `gcs_node_manager.cc:WriteExportNodeEvent`:
    /// add_node + remove_node both emit JSON-line entries to
    /// `<log_dir>/export_events/event_EXPORT_NODE.log`.
    #[tokio::test]
    async fn test_add_node_emits_export_node_event() {
        let dir = temp_export_dir();
        let mgr = make_manager_with_export(&dir);

        let mut node = make_node(b"node_export_1");
        node.is_head_node = true;
        node.start_time_ms = 1700000000123;
        node.resources_total.insert("CPU".into(), 4.0);
        mgr.add_node(node).await;

        let path = dir.join("export_events").join("event_EXPORT_NODE.log");
        let content = std::fs::read_to_string(&path).expect("file written");
        let line = content.lines().next().expect("at least one event");
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(v["source_type"], "EXPORT_NODE");
        assert_eq!(v["event_data"]["state"], "ALIVE");
        assert_eq!(v["event_data"]["is_head_node"], true);
        assert_eq!(v["event_data"]["start_time_ms"], 1700000000123_i64);
        assert_eq!(v["event_data"]["resources_total"]["CPU"], 4.0);
    }

    #[tokio::test]
    async fn test_remove_node_emits_export_node_event() {
        let dir = temp_export_dir();
        let mgr = make_manager_with_export(&dir);

        mgr.add_node(make_node(b"node_export_2")).await;
        mgr.remove_node(b"node_export_2", NodeDeathInfo::default())
            .await;

        let path = dir.join("export_events").join("event_EXPORT_NODE.log");
        let content = std::fs::read_to_string(&path).expect("file written");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "add + remove should each emit one event");
        let last: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(last["source_type"], "EXPORT_NODE");
        assert_eq!(last["event_data"]["state"], "DEAD");
    }

    #[tokio::test]
    async fn test_disabled_export_writes_nothing() {
        let mgr = make_manager(); // disabled writer by default
        mgr.add_node(make_node(b"node_disabled")).await;
        // No file is written because the writer is disabled. The test passes
        // by virtue of not crashing — there is no path to check.
    }
}
