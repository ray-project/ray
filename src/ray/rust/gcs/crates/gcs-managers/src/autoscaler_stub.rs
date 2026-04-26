//! Full implementation of AutoscalerStateService.
//!
//! Returns actual cluster state from node_manager and resource_manager
//! where applicable, and stores autoscaler state for retrieval.
//!
//! Maps C++ `GcsAutoscalerStateManager` from
//! `src/ray/gcs/gcs_autoscaler_state_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use prost::Message;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use gcs_kv::InternalKVInterface;
use gcs_proto::ray::rpc::autoscaler::autoscaler_state_service_server::AutoscalerStateService;
use gcs_proto::ray::rpc::autoscaler::*;
use gcs_proto::ray::rpc::node_manager_service_client::NodeManagerServiceClient;
use gcs_proto::ray::rpc::placement_group_table_data::PlacementGroupState;
use gcs_proto::ray::rpc::{
    DrainRayletReply as NodeDrainRayletReply, DrainRayletRequest as NodeDrainRayletRequest,
    GcsNodeInfo, KillLocalActorRequest, LabelSelector, PlacementGroupLoad,
    PlacementGroupTableData, PlacementStrategy, ResizeLocalResourceInstancesRequest,
    ResourcesData,
};

use crate::actor_stub::GcsActorManager;
use crate::node_manager::{DrainNodeInfo, GcsNodeManager};
use crate::placement_group_stub::GcsPlacementGroupManager;

/// Source of placement-group load used by the autoscaler.
///
/// Maps C++ `GcsPlacementGroupManager::GetPlacementGroupLoad()` —
/// the autoscaler state manager only needs this one method, so we
/// abstract it for testability and to avoid cyclic Arc ownership.
pub trait PlacementGroupLoadSource: Send + Sync {
    fn get_placement_group_load(&self) -> PlacementGroupLoad;
}

/// Adapter implementation backed by `GcsPlacementGroupManager`.
impl PlacementGroupLoadSource for GcsPlacementGroupManager {
    fn get_placement_group_load(&self) -> PlacementGroupLoad {
        GcsPlacementGroupManager::get_placement_group_load(self)
    }
}

/// KV namespace for autoscaler state persistence.
/// Maps C++ `kGcsAutoscalerStateNamespace` from `constants.h:82`.
const AUTOSCALER_STATE_NAMESPACE: &str = "__autoscaler";

/// KV key for persisted cluster config.
/// Maps C++ `kGcsAutoscalerClusterConfigKey` from `constants.h:84`.
const AUTOSCALER_CLUSTER_CONFIG_KEY: &str = "__autoscaler_cluster_config";

/// Expected node ID size in bytes. Maps C++ `NodeID::Size()` (kUniqueIDSize = 28).
const NODE_ID_SIZE: usize = 28;

/// Label prefix used to generate placement-group constraints.
/// Maps C++ `kPlacementGroupConstraintKeyPrefix` from `constants.h:103`.
const PLACEMENT_GROUP_CONSTRAINT_KEY_PREFIX: &str = "_PG_";

/// Raylet client pool interface — abstracts ResizeLocalResourceInstances so
/// production code talks to real raylets while tests inject mocks.
///
/// Maps C++ `RayletClientPool` (`src/ray/rpc/node_manager/raylet_client_pool.h`)
/// narrowed to the single method the autoscaler actually calls.
#[async_trait]
pub trait RayletClientPool: Send + Sync {
    /// Call ResizeLocalResourceInstances on the raylet at (address, port).
    ///
    /// Returns the raylet's updated total_resources on success.
    async fn resize_local_resource_instances(
        &self,
        address: &str,
        port: u16,
        resources: HashMap<String, f64>,
    ) -> Result<HashMap<String, f64>, Status>;

    /// Call `NodeManagerService::DrainRaylet` on the raylet at
    /// `(address, port)`. Maps C++ `RayletClient::DrainRaylet` invoked
    /// from `GcsAutoscalerStateManager::HandleDrainNode`
    /// (`gcs_autoscaler_state_manager.cc:504-520`).
    ///
    /// The raylet may *accept* (`is_accepted = true`) or *reject*
    /// (`is_accepted = false` with a `rejection_reason_message`). The
    /// GCS must propagate both outcomes back to the caller — before this
    /// commit the Rust GCS accepted alive-node drains unconditionally,
    /// which diverged from the raylet's policy.
    async fn drain_raylet(
        &self,
        address: &str,
        port: u16,
        request: NodeDrainRayletRequest,
    ) -> Result<NodeDrainRayletReply, Status>;

    /// Call `NodeManagerService::KillLocalActor` on the raylet at
    /// `(address, port)`. Maps C++ `RayletClient::KillLocalActor`
    /// invoked from `GcsActorManager::NotifyRayletToKillActor`
    /// (`gcs_actor_manager.cc:1843-1875`).
    ///
    /// Fire-and-forget from the GCS perspective: C++ logs both success
    /// and failure but changes no state based on the reply (which is an
    /// empty `KillLocalActorReply {}`). We therefore return
    /// `Result<(), Status>` so the caller can emit the same
    /// "Node with actor is already dead" INFO log on failure without
    /// branching on reply fields.
    ///
    /// The `force_kill` bit inside `request` is the whole point of
    /// this method: it tells the raylet whether to send the worker a
    /// graceful `Exit` (false) or SIGKILL (true). Before this trait
    /// method existed, the Rust GCS silently dropped the bit, collapsing
    /// two distinct C++ behaviors into one and breaking parity with
    /// the public `KillActorViaGcs` RPC contract.
    async fn kill_local_actor(
        &self,
        address: &str,
        port: u16,
        request: KillLocalActorRequest,
    ) -> Result<(), Status>;
}

/// Production raylet client pool using tonic gRPC.
///
/// Connects lazily per call (matching the pattern in `pg_scheduler.rs`).
/// This mirrors C++ `RayletClientPool::GetOrConnectByAddress` — a real pool
/// could cache channels per node, but lazy-connect is correct for parity.
pub struct TonicRayletClientPool;

impl TonicRayletClientPool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TonicRayletClientPool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RayletClientPool for TonicRayletClientPool {
    async fn resize_local_resource_instances(
        &self,
        address: &str,
        port: u16,
        resources: HashMap<String, f64>,
    ) -> Result<HashMap<String, f64>, Status> {
        let endpoint = format!("http://{address}:{port}");
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| Status::invalid_argument(format!("Invalid raylet endpoint: {e}")))?
            .connect()
            .await
            .map_err(|e| {
                Status::unavailable(format!("Failed to connect to raylet {endpoint}: {e}"))
            })?;

        let mut client = NodeManagerServiceClient::new(channel);
        let reply = client
            .resize_local_resource_instances(ResizeLocalResourceInstancesRequest { resources })
            .await?;
        Ok(reply.into_inner().total_resources)
    }

    async fn drain_raylet(
        &self,
        address: &str,
        port: u16,
        request: NodeDrainRayletRequest,
    ) -> Result<NodeDrainRayletReply, Status> {
        let endpoint = format!("http://{address}:{port}");
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| Status::invalid_argument(format!("Invalid raylet endpoint: {e}")))?
            .connect()
            .await
            .map_err(|e| {
                Status::unavailable(format!("Failed to connect to raylet {endpoint}: {e}"))
            })?;

        let mut client = NodeManagerServiceClient::new(channel);
        Ok(client.drain_raylet(request).await?.into_inner())
    }

    async fn kill_local_actor(
        &self,
        address: &str,
        port: u16,
        request: KillLocalActorRequest,
    ) -> Result<(), Status> {
        let endpoint = format!("http://{address}:{port}");
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| Status::invalid_argument(format!("Invalid raylet endpoint: {e}")))?
            .connect()
            .await
            .map_err(|e| {
                Status::unavailable(format!("Failed to connect to raylet {endpoint}: {e}"))
            })?;

        let mut client = NodeManagerServiceClient::new(channel);
        // Reply is empty — discard and collapse to unit.
        let _ = client.kill_local_actor(request).await?;
        Ok(())
    }
}

/// GCS Autoscaler State Manager.
///
/// Maps C++ `GcsAutoscalerStateManager` which holds references to
/// `GcsNodeManager`, `GcsActorManager`, `GcsPlacementGroupManager`,
/// `RayletClientPool`, and `InternalKVInterface`.
pub struct GcsAutoscalerStateManager {
    /// Reference to the node manager for checking node state and recording drains.
    node_manager: Arc<GcsNodeManager>,
    /// Actor manager — used by `drain_node` to mark alive actors on the
    /// target node as preempted and publish them. Maps C++
    /// `gcs_actor_manager_` member referenced at
    /// `gcs_autoscaler_state_manager.h:189-190` and invoked from
    /// `HandleDrainNode` (`gcs_autoscaler_state_manager.cc:498`).
    ///
    /// Wrapped in `RwLock<Option<...>>` rather than made a constructor
    /// arg so the server can wire the dependency after the autoscaler is
    /// already `Arc`-shared — the actor manager itself depends on the
    /// node manager, so they can't both be passed into one constructor
    /// without a layering problem. `drain_node` reads this once per call;
    /// no hot-path contention.
    actor_manager: parking_lot::RwLock<Option<Arc<GcsActorManager>>>,
    /// Placement-group load source — feeds `pending_gang_resource_requests`.
    /// In production this is the `GcsPlacementGroupManager`; tests can swap it.
    /// Maps C++ `gcs_placement_group_manager_`.
    placement_group_load_source: Arc<dyn PlacementGroupLoadSource>,
    /// Raylet client pool — used to forward ResizeLocalResourceInstances
    /// and DrainRaylet. Maps C++ `raylet_client_pool_`.
    ///
    /// Wrapped in `RwLock` so tests can swap in a mock after the
    /// autoscaler is already `Arc`-shared by the server.
    raylet_client_pool: parking_lot::RwLock<Arc<dyn RayletClientPool>>,
    /// KV store for persisting cluster config across GCS restarts.
    /// Maps C++ `kv_` member.
    kv: Arc<dyn InternalKVInterface>,
    /// Cluster session name. Maps C++ `session_name_`.
    session_name: String,
    /// Autoscaler's own per-node resource cache.
    /// Maps C++ `node_resource_info_: HashMap<NodeID, (Time, ResourcesData)>`.
    node_resource_info: DashMap<Vec<u8>, (Instant, ResourcesData)>,
    /// Last reported autoscaling state from the autoscaler.
    autoscaling_state: RwLock<Option<AutoscalingState>>,
    /// Last reported cluster config (also persisted to KV).
    cluster_config: RwLock<Option<ClusterConfig>>,
    /// Most recent resource constraint from `request_resources()`.
    /// Maps C++ `cluster_resource_constraint_` (std::optional, not vector).
    cluster_resource_constraint: RwLock<Option<ClusterResourceConstraint>>,
    /// Monotonically increasing version for cluster resource state.
    cluster_resource_state_version: std::sync::atomic::AtomicI64,
    /// Last seen autoscaler state version.
    last_seen_autoscaler_state_version: RwLock<i64>,
}

impl GcsAutoscalerStateManager {
    pub fn new(
        node_manager: Arc<GcsNodeManager>,
        placement_group_load_source: Arc<dyn PlacementGroupLoadSource>,
        raylet_client_pool: Arc<dyn RayletClientPool>,
        kv: Arc<dyn InternalKVInterface>,
        session_name: String,
    ) -> Self {
        Self {
            node_manager,
            actor_manager: parking_lot::RwLock::new(None),
            placement_group_load_source,
            raylet_client_pool: parking_lot::RwLock::new(raylet_client_pool),
            kv,
            session_name,
            node_resource_info: DashMap::new(),
            autoscaling_state: RwLock::new(None),
            cluster_config: RwLock::new(None),
            cluster_resource_constraint: RwLock::new(None),
            cluster_resource_state_version: std::sync::atomic::AtomicI64::new(0),
            last_seen_autoscaler_state_version: RwLock::new(0),
        }
    }

    /// Wire the actor manager after construction. The server sets this
    /// immediately after building the autoscaler so `drain_node` can
    /// publish preemption state. Kept as a separate setter (rather than
    /// a constructor argument) to avoid a circular construction order
    /// with the actor manager, which itself depends on the node manager.
    ///
    /// Safe to call on an `Arc`-shared autoscaler because the field uses
    /// interior mutability (`RwLock`).
    pub fn set_actor_manager(&self, actor_manager: Arc<GcsActorManager>) {
        *self.actor_manager.write() = Some(actor_manager);
    }

    /// Replace the raylet client pool (tests). Production passes a
    /// `TonicRayletClientPool` at construction; integration tests that
    /// can't stand up a real raylet swap in a scripted pool here.
    pub fn set_raylet_client_pool(&self, pool: Arc<dyn RayletClientPool>) {
        *self.raylet_client_pool.write() = pool;
    }

    /// Initialize the node_resource_info cache for a newly added node.
    /// Sets resources_total = resources_available and copies labels.
    ///
    /// Maps C++ `GcsAutoscalerStateManager::OnNodeAdd`
    /// (gcs_autoscaler_state_manager.cc:271-288).
    pub fn on_node_add(&self, node: &GcsNodeInfo) {
        let node_id = &node.node_id;
        if self.node_resource_info.contains_key(node_id) {
            return; // Already known.
        }
        let resources_data = ResourcesData {
            node_id: node_id.clone(),
            resources_total: node.resources_total.clone(),
            resources_available: node.resources_total.clone(),
            labels: node.labels.clone(),
            ..Default::default()
        };
        self.node_resource_info
            .insert(node_id.clone(), (Instant::now(), resources_data));
    }

    /// Remove a node from the resource info cache.
    /// Maps C++ `GcsAutoscalerStateManager::OnNodeDead` (header line 95).
    pub fn on_node_dead(&self, node_id: &[u8]) {
        self.node_resource_info.remove(node_id);
    }

    /// Replace a node's ResourcesData with fresh data from a raylet report.
    /// Maps C++ `UpdateResourceLoadAndUsage` (gcs_autoscaler_state_manager.cc:290-304).
    ///
    /// No callers exist yet (requires raylet resource reporting), but the
    /// method is ready for when that path is wired.
    pub fn update_resource_load_and_usage(&self, data: ResourcesData) {
        let node_id = data.node_id.clone();
        if let Some(mut entry) = self.node_resource_info.get_mut(&node_id) {
            entry.1 = data;
            entry.0 = Instant::now();
        } else {
            warn!(
                node_id = hex::encode(&node_id),
                "Ignoring resource usage for node that is not alive."
            );
        }
    }

    /// Mark a node as draining in the autoscaler's resource cache.
    ///
    /// Without a raylet client, the raylet can't report `is_draining=true`
    /// via resource sync. This method bridges that gap so the autoscaler
    /// sees DRAINING status after a drain_node call.
    pub fn set_node_draining_in_resource_info(&self, node_id: &[u8], deadline_ms: i64) {
        if let Some(mut entry) = self.node_resource_info.get_mut(node_id) {
            entry.1.is_draining = true;
            entry.1.draining_deadline_timestamp_ms = deadline_ms;
            entry.0 = Instant::now();
        }
    }

    /// Read the `ResourcesData` currently cached for a node.
    /// Returns `None` if the autoscaler has no row for that node (e.g.
    /// the node is dead or was never added). Used by the raylet-load
    /// pull-loop tests and by debug-dump paths to observe what the
    /// autoscaler currently knows.
    pub fn get_node_resource_info(&self, node_id: &[u8]) -> Option<ResourcesData> {
        self.node_resource_info
            .get(node_id)
            .map(|e| e.value().1.clone())
    }

    // ─── Internal helpers ─────────────────────────────────────────────

    /// Build the complete ClusterResourceState.
    /// Maps C++ `MakeClusterResourceStateInternal`
    /// (gcs_autoscaler_state_manager.cc:178-190).
    fn make_cluster_resource_state(&self) -> ClusterResourceState {
        let version = self
            .cluster_resource_state_version
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let last_seen = *self.last_seen_autoscaler_state_version.read();

        ClusterResourceState {
            cluster_resource_state_version: version,
            last_seen_autoscaler_state_version: last_seen,
            node_states: self.get_node_states(version),
            pending_resource_requests: self.get_pending_resource_requests(),
            pending_gang_resource_requests: self.get_pending_gang_resource_requests(),
            cluster_resource_constraints: self.get_cluster_resource_constraints(),
            cluster_session_name: self.session_name.clone(),
        }
    }

    /// Populate node_states for all alive and dead nodes.
    /// Maps C++ `GetNodeStates` (gcs_autoscaler_state_manager.cc:348-455).
    fn get_node_states(&self, version: i64) -> Vec<NodeState> {
        let mut node_states = Vec::new();

        // Alive nodes — full resource + status info.
        let alive_nodes = self.node_manager.get_all_alive_nodes();
        for (_nid, gcs_node) in &alive_nodes {
            let mut ns = NodeState {
                node_id: gcs_node.node_id.clone(),
                instance_id: gcs_node.instance_id.clone(),
                ray_node_type_name: gcs_node.node_type_name.clone(),
                node_state_version: version,
                node_ip_address: gcs_node.node_manager_address.clone(),
                instance_type_name: gcs_node.instance_type_name.clone(),
                ..Default::default()
            };

            // Copy node_activity from state_snapshot (C++ line 375-376).
            if let Some(ref snapshot) = gcs_node.state_snapshot {
                ns.node_activity = snapshot.node_activity.clone();
            }

            // Look up autoscaler's resource cache. If missing (race), add it (C++ line 388-391).
            if !self.node_resource_info.contains_key(&gcs_node.node_id) {
                self.on_node_add(gcs_node);
            }

            if let Some(entry) = self.node_resource_info.get(&gcs_node.node_id) {
                let (last_update, ref res_data) = *entry;

                // Status determination (C++ lines 396-414).
                if res_data.is_draining {
                    ns.status = NodeStatus::Draining as i32;
                } else if res_data.idle_duration_ms > 0 {
                    ns.status = NodeStatus::Idle as i32;
                    // Adjust idle duration: reported + time since last report (C++ lines 410-412).
                    let elapsed_ms =
                        Instant::now().duration_since(last_update).as_millis() as i64;
                    ns.idle_duration_ms = res_data.idle_duration_ms + elapsed_ms;
                } else {
                    ns.status = NodeStatus::Running as i32;
                }

                ns.available_resources = res_data.resources_available.clone();
                ns.total_resources = res_data.resources_total.clone();
            }

            // Copy labels from GcsNodeInfo (C++ lines 437-438).
            ns.labels = gcs_node.labels.clone();

            node_states.push(ns);
        }

        // Dead nodes — only metadata + DEAD status (C++ lines 369-373).
        let dead_nodes = self.node_manager.get_all_dead_nodes();
        for (_nid, gcs_node) in &dead_nodes {
            node_states.push(NodeState {
                node_id: gcs_node.node_id.clone(),
                instance_id: gcs_node.instance_id.clone(),
                ray_node_type_name: gcs_node.node_type_name.clone(),
                node_state_version: version,
                node_ip_address: gcs_node.node_manager_address.clone(),
                instance_type_name: gcs_node.instance_type_name.clone(),
                status: NodeStatus::Dead as i32,
                ..Default::default()
            });
        }

        node_states
    }

    /// Aggregate pending resource requests from all nodes' resource load.
    /// Maps C++ `GetPendingResourceRequests` + `GetAggregatedResourceLoad`
    /// (gcs_autoscaler_state_manager.cc:306-346).
    fn get_pending_resource_requests(&self) -> Vec<ResourceRequestByCount> {
        // Aggregate resource demands across all nodes keyed by shape.
        // C++ uses ResourceDemandKey (shape + label_selectors); we use a
        // canonical byte key for HashMap grouping.
        let mut aggregate: HashMap<Vec<u8>, (HashMap<String, f64>, Vec<LabelSelector>, i64)> =
            HashMap::new();

        for entry in self.node_resource_info.iter() {
            let (_, ref res_data) = *entry.value();
            if let Some(ref load) = res_data.resource_load_by_shape {
                for demand in &load.resource_demands {
                    let key = demand_key(&demand.shape, &demand.label_selectors);
                    let count = demand.num_infeasible_requests_queued as i64
                        + demand.backlog_size
                        + demand.num_ready_requests_queued as i64;
                    let agg = aggregate.entry(key).or_insert_with(|| {
                        (
                            demand.shape.clone(),
                            demand.label_selectors.clone(),
                            0,
                        )
                    });
                    agg.2 += count;
                }
            }
        }

        aggregate
            .into_values()
            .filter(|(_, _, count)| *count > 0)
            .map(|(shape, selectors, count)| ResourceRequestByCount {
                request: Some(ResourceRequest {
                    resources_bundle: shape,
                    label_selectors: selectors,
                    ..Default::default()
                }),
                count,
            })
            .collect()
    }

    /// Build pending gang resource requests from the placement-group manager's
    /// load. PENDING / RESCHEDULING PGs become one `GangResourceRequest` each.
    ///
    /// Maps C++ `GetPendingGangResourceRequests`
    /// (gcs_autoscaler_state_manager.cc:192-260).
    fn get_pending_gang_resource_requests(&self) -> Vec<GangResourceRequest> {
        let load = self.placement_group_load_source.get_placement_group_load();
        if load.placement_group_data.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::new();
        for pg_data in &load.placement_group_data {
            let state = pg_data.state;
            // Only PENDING / RESCHEDULING PGs contribute to load (C++ 209-212).
            if state != PlacementGroupState::Pending as i32
                && state != PlacementGroupState::Rescheduling as i32
            {
                continue;
            }

            let pg_id_hex = hex::encode(&pg_data.placement_group_id);
            let pg_constraint = gen_placement_constraint_for_pg(&pg_id_hex, pg_data.strategy);

            let mut req = GangResourceRequest {
                requests: Vec::new(),
                details: format_placement_group_details(pg_data),
                bundle_selectors: Vec::new(),
            };

            // Only one BundleSelector for now (C++ 220-222 — fallback TBD).
            let mut bundle_selector = BundleSelector::default();

            for bundle in &pg_data.bundles {
                // Skip already-placed bundles when RESCHEDULING; only count
                // unplaced (node_id empty) bundles as load (C++ 226-234).
                if !bundle.node_id.is_empty() {
                    debug_assert!(
                        state == PlacementGroupState::Rescheduling as i32,
                        "Placed bundle found in non-rescheduling PG {pg_id_hex}"
                    );
                    continue;
                }

                // Legacy per-bundle ResourceRequest (C++ 239-241).
                let mut legacy_req = ResourceRequest {
                    resources_bundle: bundle.unit_resources.clone(),
                    placement_constraints: Vec::new(),
                    label_selectors: Vec::new(),
                };

                // BundleSelector ResourceRequest (C++ 243-245).
                let mut selector_req = ResourceRequest {
                    resources_bundle: bundle.unit_resources.clone(),
                    placement_constraints: Vec::new(),
                    label_selectors: Vec::new(),
                };

                // Bundle label selector → LabelSelector proto on the new
                // BundleSelector request (C++ 247-251). The bundle.label_selector
                // map in the Rust proto is already in map<string,string> form;
                // we wrap it into a LabelSelector via equality constraints.
                if !bundle.label_selector.is_empty() {
                    if let Some(sel) = label_selector_from_map(&bundle.label_selector) {
                        selector_req.label_selectors.push(sel);
                    }
                }

                // Placement constraint for strict strategies (C++ 253-257).
                if let Some(ref constraint) = pg_constraint {
                    legacy_req.placement_constraints.push(constraint.clone());
                    selector_req.placement_constraints.push(constraint.clone());
                }

                req.requests.push(legacy_req);
                bundle_selector.resource_requests.push(selector_req);
            }

            // Always include the selector even when empty to match C++
            // (`add_bundle_selectors()` is called unconditionally, but we only
            // emit the request if at least one unplaced bundle exists).
            if !req.requests.is_empty() {
                req.bundle_selectors.push(bundle_selector);
                out.push(req);
            }
        }

        out
    }

    /// Return the cached constraint as a single-element-or-empty vec.
    /// Maps C++ `GetClusterResourceConstraints`
    /// (gcs_autoscaler_state_manager.cc:262-269).
    fn get_cluster_resource_constraints(&self) -> Vec<ClusterResourceConstraint> {
        match self.cluster_resource_constraint.read().as_ref() {
            Some(c) => vec![c.clone()],
            None => vec![],
        }
    }
}

/// Build a canonical byte key for grouping resource demands by shape + label selectors.
fn demand_key(
    shape: &HashMap<String, f64>,
    selectors: &[LabelSelector],
) -> Vec<u8> {
    // Sort shape entries for deterministic ordering.
    let mut pairs: Vec<_> = shape.iter().collect();
    pairs.sort_by_key(|(k, _)| k.clone());
    let mut key = Vec::new();
    for (k, v) in pairs {
        key.extend_from_slice(k.as_bytes());
        key.extend_from_slice(&v.to_le_bytes());
    }
    // Append serialized label selectors.
    for sel in selectors {
        key.extend_from_slice(&Message::encode_to_vec(sel));
    }
    key
}

/// Format placement-group details string for observability.
/// Maps C++ `FormatPlacementGroupDetails` (protobuf_utils.h:160-165):
///     <pg_id_hex>:<STRATEGY>|<STATE>
fn format_placement_group_details(pg: &PlacementGroupTableData) -> String {
    let strategy = PlacementStrategy::try_from(pg.strategy)
        .map(|s| s.as_str_name().to_string())
        .unwrap_or_else(|_| format!("UNKNOWN_STRATEGY_{}", pg.strategy));
    let state = PlacementGroupState::try_from(pg.state)
        .map(|s| s.as_str_name().to_string())
        .unwrap_or_else(|_| format!("UNKNOWN_STATE_{}", pg.state));
    format!(
        "{}:{}|{}",
        hex::encode(&pg.placement_group_id),
        strategy,
        state
    )
}

/// Generate a placement constraint for a placement group based on its strategy.
/// Maps C++ `GenPlacementConstraintForPlacementGroup` (protobuf_utils.cc:387-414).
///
/// STRICT_SPREAD → anti-affinity on "_PG_<pg_id_hex>".
/// STRICT_PACK   → affinity on "_PG_<pg_id_hex>".
/// PACK / SPREAD → None (no label constraint needed).
fn gen_placement_constraint_for_pg(
    pg_id_hex: &str,
    strategy: i32,
) -> Option<PlacementConstraint> {
    let label_name = format!("{PLACEMENT_GROUP_CONSTRAINT_KEY_PREFIX}{pg_id_hex}");
    match PlacementStrategy::try_from(strategy).ok()? {
        PlacementStrategy::StrictSpread => Some(PlacementConstraint {
            anti_affinity: Some(AntiAffinityConstraint {
                label_name,
                label_value: String::new(),
            }),
            affinity: None,
        }),
        PlacementStrategy::StrictPack => Some(PlacementConstraint {
            anti_affinity: None,
            affinity: Some(AffinityConstraint {
                label_name,
                label_value: String::new(),
            }),
        }),
        PlacementStrategy::Pack | PlacementStrategy::Spread => None,
    }
}

/// Convert a bundle's `label_selector` map (key → value) into a LabelSelector
/// proto with IN-operator constraints. Returns None if the map is empty.
///
/// This mirrors C++ `ray::LabelSelector(map).ToProto(...)` —
/// each key/value becomes an equality (LABEL_OPERATOR_IN with [value]) constraint.
fn label_selector_from_map(map: &HashMap<String, String>) -> Option<LabelSelector> {
    use gcs_proto::ray::rpc::{LabelSelectorConstraint, LabelSelectorOperator};
    if map.is_empty() {
        return None;
    }
    let mut entries: Vec<(&String, &String)> = map.iter().collect();
    entries.sort_by_key(|(k, _)| k.as_str());
    let mut sel = LabelSelector::default();
    for (k, v) in entries {
        sel.label_constraints.push(LabelSelectorConstraint {
            label_key: k.clone(),
            operator: LabelSelectorOperator::LabelOperatorIn as i32,
            label_values: vec![v.clone()],
        });
    }
    Some(sel)
}

mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[tonic::async_trait]
impl AutoscalerStateService for GcsAutoscalerStateManager {
    /// Get the cluster resource state.
    /// Maps C++ `HandleGetClusterResourceState`
    /// (gcs_autoscaler_state_manager.cc:48-62).
    async fn get_cluster_resource_state(
        &self,
        _req: Request<GetClusterResourceStateRequest>,
    ) -> Result<Response<GetClusterResourceStateReply>, Status> {
        Ok(Response::new(GetClusterResourceStateReply {
            cluster_resource_state: Some(self.make_cluster_resource_state()),
        }))
    }

    /// The autoscaler reports its state.
    /// Maps C++ `HandleReportAutoscalingState`
    /// (gcs_autoscaler_state_manager.cc:64-133).
    ///
    /// Discards incoming state if its version is older than the cached version.
    async fn report_autoscaling_state(
        &self,
        req: Request<ReportAutoscalingStateRequest>,
    ) -> Result<Response<ReportAutoscalingStateReply>, Status> {
        let inner = req.into_inner();
        if let Some(incoming) = inner.autoscaling_state {
            let incoming_version = incoming.autoscaler_state_version;

            let mut guard = self.autoscaling_state.write();
            if let Some(ref current) = *guard {
                if incoming_version < current.autoscaler_state_version {
                    info!(
                        current_version = current.autoscaler_state_version,
                        incoming_version,
                        "Received outdated autoscaling state, discarding"
                    );
                    return Ok(Response::new(ReportAutoscalingStateReply {}));
                }
            }
            *guard = Some(incoming);
            drop(guard);

            *self.last_seen_autoscaler_state_version.write() = incoming_version;
            debug!(version = incoming_version, "Received autoscaling state");
        }
        Ok(Response::new(ReportAutoscalingStateReply {}))
    }

    /// Report cluster config from the autoscaler — persist to KV.
    /// Maps C++ `HandleReportClusterConfig`
    /// (gcs_autoscaler_state_manager.cc:148-162).
    async fn report_cluster_config(
        &self,
        req: Request<ReportClusterConfigRequest>,
    ) -> Result<Response<ReportClusterConfigReply>, Status> {
        let inner = req.into_inner();
        if let Some(ref config) = inner.cluster_config {
            // Persist to KV for GCS fault-tolerance recovery.
            let serialized = config.encode_to_vec();
            let value = unsafe { String::from_utf8_unchecked(serialized) };
            self.kv
                .put(
                    AUTOSCALER_STATE_NAMESPACE,
                    AUTOSCALER_CLUSTER_CONFIG_KEY,
                    value,
                    true, // overwrite
                )
                .await;
        }
        *self.cluster_config.write() = inner.cluster_config;
        Ok(Response::new(ReportClusterConfigReply {}))
    }

    /// Request cluster resource constraints — replaces previous constraint.
    /// Maps C++ `HandleRequestClusterResourceConstraint`
    /// (gcs_autoscaler_state_manager.cc:135-146).
    async fn request_cluster_resource_constraint(
        &self,
        req: Request<RequestClusterResourceConstraintRequest>,
    ) -> Result<Response<RequestClusterResourceConstraintReply>, Status> {
        let inner = req.into_inner();
        *self.cluster_resource_constraint.write() = inner.cluster_resource_constraint;
        Ok(Response::new(RequestClusterResourceConstraintReply {}))
    }

    /// Get the overall cluster status (autoscaling state + resource state).
    /// Maps C++ `HandleGetClusterStatus`
    /// (gcs_autoscaler_state_manager.cc:164-176).
    async fn get_cluster_status(
        &self,
        _req: Request<GetClusterStatusRequest>,
    ) -> Result<Response<GetClusterStatusReply>, Status> {
        let autoscaling_state = self.autoscaling_state.read().clone();
        let cluster_resource_state = Some(self.make_cluster_resource_state());

        Ok(Response::new(GetClusterStatusReply {
            autoscaling_state,
            cluster_resource_state,
        }))
    }

    /// Drain a node.
    ///
    /// Faithful port of C++ `GcsAutoscalerStateManager::HandleDrainNode`
    /// (`gcs_autoscaler_state_manager.cc:457-523`). The flow is:
    ///
    /// 1. Validate the deadline (non-negative).
    /// 2. If the node is not alive, short-circuit with
    ///    `is_accepted=true` — dead/unknown nodes are already "drained"
    ///    from the cluster's perspective.
    /// 3. Mark alive actors on the target node as preempted *and*
    ///    republish them (C++ line 498). This happens before the raylet
    ///    RPC so the preemption signal reaches subscribers even if the
    ///    raylet is slow or unreachable.
    /// 4. Forward the request to the target raylet via `DrainRaylet`.
    ///    The raylet's `is_accepted` decision is the authority.
    /// 5. Only record node draining state *when the raylet accepts*
    ///    (C++ lines 510-514) — matches the documented semantic that
    ///    the raylet can refuse a drain (e.g. node has non-preemptible
    ///    work).
    /// 6. Propagate the raylet's `rejection_reason_message` back to the
    ///    caller unchanged on rejection (C++ lines 516-519).
    async fn drain_node(
        &self,
        req: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainNodeReply>, Status> {
        let inner = req.into_inner();
        let node_id = inner.node_id;
        let deadline_ms = inner.deadline_timestamp_ms;

        info!(
            node_id = hex::encode(&node_id),
            reason = inner.reason,
            reason_message = inner.reason_message,
            deadline_ms,
            "HandleDrainNode"
        );

        // 1. Validate deadline (must be non-negative).
        if deadline_ms < 0 {
            let msg = format!(
                "Draining deadline must be non-negative, received {}",
                deadline_ms
            );
            warn!("{}", msg);
            return Err(Status::invalid_argument(msg));
        }

        // 2. Dead/unknown node fast path — C++ lines 480-496.
        let Some(node) = self.node_manager.get_alive_node(&node_id) else {
            if self.node_manager.is_node_dead(&node_id) {
                info!(
                    node_id = hex::encode(&node_id),
                    "Request to drain a dead node, treat it as drained"
                );
            } else {
                warn!(
                    node_id = hex::encode(&node_id),
                    "Request to drain an unknown node"
                );
            }
            return Ok(Response::new(DrainNodeReply {
                is_accepted: true,
                rejection_reason_message: String::new(),
            }));
        };

        // 3. Mark actors on this node as preempted and publish. Parity
        //    with C++ line 498:
        //    `gcs_actor_manager_.SetPreemptedAndPublish(node_id)`. We
        //    do this *before* the raylet RPC so subscribers see the
        //    preemption signal regardless of raylet latency.
        //
        //    Snapshot the Arc under the read lock and drop the guard
        //    before awaiting — holding a parking_lot lock across an
        //    await would make the outer future `!Send`.
        let actor_manager = self.actor_manager.read().clone();
        if let Some(am) = actor_manager {
            am.set_preempted_and_publish(&node_id).await;
        }

        // 4. Forward the drain request to the target raylet.
        let raylet_request = NodeDrainRayletRequest {
            reason: inner.reason,
            reason_message: inner.reason_message.clone(),
            deadline_timestamp_ms: deadline_ms,
        };
        // Snapshot the Arc under the lock, drop the guard before await.
        let pool = self.raylet_client_pool.read().clone();
        let raylet_reply = match pool
            .drain_raylet(
                &node.node_manager_address,
                node.node_manager_port as u16,
                raylet_request,
            )
            .await
        {
            Ok(r) => r,
            Err(status) => {
                // C++ forwards the raylet's `Status` to the caller via
                // `send_reply_callback(status, nullptr, nullptr)`
                // (line 521). A transport failure is not an
                // autoscaler-side argument error; propagate it as-is so
                // the caller can retry.
                warn!(
                    node_id = hex::encode(&node_id),
                    error = %status,
                    "DrainRaylet RPC failed"
                );
                return Err(status);
            }
        };

        // 5-6. Record draining only on acceptance; propagate rejection.
        if raylet_reply.is_accepted {
            self.node_manager.set_node_draining(
                node_id.clone(),
                DrainNodeInfo {
                    reason: inner.reason,
                    reason_message: inner.reason_message,
                    deadline_timestamp_ms: deadline_ms,
                },
            );

            // Autoscaler-side caches: ensure the node is known, then
            // flip is_draining/deadline so `get_node_states` reports
            // DRAINING. Parity with the node-manager side of C++
            // `SetNodeDraining`, which is what downstream readers
            // observe.
            if !self.node_resource_info.contains_key(&node_id) {
                self.on_node_add(&node);
            }
            self.set_node_draining_in_resource_info(&node_id, deadline_ms);

            Ok(Response::new(DrainNodeReply {
                is_accepted: true,
                rejection_reason_message: String::new(),
            }))
        } else {
            info!(
                node_id = hex::encode(&node_id),
                rejection = raylet_reply.rejection_reason_message,
                "Node drain rejected by raylet. The node will not be drained."
            );
            Ok(Response::new(DrainNodeReply {
                is_accepted: false,
                rejection_reason_message: raylet_reply.rejection_reason_message,
            }))
        }
    }

    /// Resize raylet resource instances — forward to the target raylet.
    /// Maps C++ `HandleResizeRayletResourceInstances`
    /// (gcs_autoscaler_state_manager.cc:525-564).
    async fn resize_raylet_resource_instances(
        &self,
        req: Request<ResizeRayletResourceInstancesRequest>,
    ) -> Result<Response<ResizeRayletResourceInstancesReply>, Status> {
        let inner = req.into_inner();

        // Validate node_id size (C++ checks NodeID::Size() == 28).
        if inner.node_id.len() != NODE_ID_SIZE {
            return Err(Status::invalid_argument(format!(
                "Expected node_id to be {} bytes, but got {} bytes.",
                NODE_ID_SIZE,
                inner.node_id.len()
            )));
        }

        // Resolve the raylet address from GcsNodeManager (C++ 540-548).
        let node = self
            .node_manager
            .get_alive_node(&inner.node_id)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Raylet {} is not alive.",
                    hex::encode(&inner.node_id)
                ))
            })?;

        // Forward to the raylet and return updated total_resources (C++ 550-563).
        let pool = self.raylet_client_pool.read().clone();
        let total_resources = pool
            .resize_local_resource_instances(
                &node.node_manager_address,
                node.node_manager_port as u16,
                inner.resources,
            )
            .await?;

        Ok(Response::new(ResizeRayletResourceInstancesReply {
            total_resources,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_proto::ray::rpc::{
        bundle, gcs_node_info, Bundle, NodeDeathInfo, ResourceDemand, ResourceLoad,
    };
    use gcs_pubsub::GcsPublisher;
    use gcs_store::InMemoryStoreClient;
    use gcs_table_storage::GcsTableStorage;
    use parking_lot::Mutex as PlMutex;

    // ─── Fake placement-group load source for tests ────────────────
    struct FakePgLoadSource {
        load: PlMutex<PlacementGroupLoad>,
    }

    impl FakePgLoadSource {
        fn empty() -> Arc<Self> {
            Arc::new(Self {
                load: PlMutex::new(PlacementGroupLoad::default()),
            })
        }
        fn with(pgs: Vec<PlacementGroupTableData>) -> Arc<Self> {
            Arc::new(Self {
                load: PlMutex::new(PlacementGroupLoad {
                    placement_group_data: pgs,
                }),
            })
        }
    }

    impl PlacementGroupLoadSource for FakePgLoadSource {
        fn get_placement_group_load(&self) -> PlacementGroupLoad {
            self.load.lock().clone()
        }
    }

    // ─── Test fixtures ──────────────────────────────────────────────

    struct FakeRayletClientPool {
        calls: Arc<parking_lot::Mutex<Vec<(String, u16, HashMap<String, f64>)>>>,
        response: HashMap<String, f64>,
        fail: bool,
    }

    impl FakeRayletClientPool {
        fn new(response: HashMap<String, f64>) -> Arc<Self> {
            Arc::new(Self {
                calls: Arc::new(parking_lot::Mutex::new(Vec::new())),
                response,
                fail: false,
            })
        }
        fn new_failing() -> Arc<Self> {
            Arc::new(Self {
                calls: Arc::new(parking_lot::Mutex::new(Vec::new())),
                response: HashMap::new(),
                fail: true,
            })
        }
    }

    #[async_trait]
    impl RayletClientPool for FakeRayletClientPool {
        async fn resize_local_resource_instances(
            &self,
            address: &str,
            port: u16,
            resources: HashMap<String, f64>,
        ) -> Result<HashMap<String, f64>, Status> {
            self.calls
                .lock()
                .push((address.to_string(), port, resources));
            if self.fail {
                Err(Status::unavailable("raylet unavailable"))
            } else {
                Ok(self.response.clone())
            }
        }

        async fn drain_raylet(
            &self,
            _address: &str,
            _port: u16,
            _request: NodeDrainRayletRequest,
        ) -> Result<NodeDrainRayletReply, Status> {
            // Existing tests in this module don't exercise drain_raylet;
            // default to "accepted" so any incidental call doesn't fail
            // unrelated assertions. The drain-node-specific tests use
            // a dedicated scripted fake below.
            Ok(NodeDrainRayletReply {
                is_accepted: true,
                rejection_reason_message: String::new(),
            })
        }

        async fn kill_local_actor(
            &self,
            _address: &str,
            _port: u16,
            _request: KillLocalActorRequest,
        ) -> Result<(), Status> {
            // Autoscaler tests don't exercise kill_local_actor — the
            // actor-manager tests use `FakeKillActorPool` in
            // `actor_stub.rs` for assertions. Succeed silently.
            Ok(())
        }
    }

    fn make_node_manager() -> Arc<GcsNodeManager> {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(GcsPublisher::new(64));
        Arc::new(GcsNodeManager::new(
            table_storage,
            publisher,
            b"cluster_1".to_vec(),
        ))
    }

    fn make_kv() -> Arc<dyn InternalKVInterface> {
        let store = Arc::new(InMemoryStoreClient::new());
        Arc::new(gcs_kv::StoreClientInternalKV::new(store))
    }

    fn make_mgr() -> GcsAutoscalerStateManager {
        let nm = make_node_manager();
        let pool = FakeRayletClientPool::new(HashMap::new());
        GcsAutoscalerStateManager::new(
            nm,
            FakePgLoadSource::empty(),
            pool,
            make_kv(),
            "test_session".to_string(),
        )
    }

    fn make_mgr_with(nm: Arc<GcsNodeManager>) -> GcsAutoscalerStateManager {
        let pool = FakeRayletClientPool::new(HashMap::new());
        GcsAutoscalerStateManager::new(
            nm,
            FakePgLoadSource::empty(),
            pool,
            make_kv(),
            "test_session".to_string(),
        )
    }

    fn make_mgr_with_pool(
        nm: Arc<GcsNodeManager>,
        pool: Arc<dyn RayletClientPool>,
    ) -> GcsAutoscalerStateManager {
        GcsAutoscalerStateManager::new(
            nm,
            FakePgLoadSource::empty(),
            pool,
            make_kv(),
            "test_session".to_string(),
        )
    }

    fn make_mgr_with_pg_load(
        nm: Arc<GcsNodeManager>,
        pg_source: Arc<dyn PlacementGroupLoadSource>,
    ) -> GcsAutoscalerStateManager {
        let pool = FakeRayletClientPool::new(HashMap::new());
        GcsAutoscalerStateManager::new(nm, pg_source, pool, make_kv(), "test_session".to_string())
    }

    fn make_node(id: &[u8]) -> GcsNodeInfo {
        let mut total = std::collections::HashMap::new();
        total.insert("CPU".to_string(), 4.0);
        GcsNodeInfo {
            node_id: id.to_vec(),
            state: gcs_node_info::GcsNodeState::Alive as i32,
            node_manager_address: "127.0.0.1".into(),
            node_manager_port: 12345,
            resources_total: total,
            ..Default::default()
        }
    }

    fn make_bundle(pg_id: &[u8], index: i32, cpu: f64) -> Bundle {
        let mut unit = HashMap::new();
        unit.insert("CPU".to_string(), cpu);
        Bundle {
            bundle_id: Some(bundle::BundleIdentifier {
                placement_group_id: pg_id.to_vec(),
                bundle_index: index,
            }),
            unit_resources: unit,
            node_id: Vec::new(),
            label_selector: HashMap::new(),
        }
    }

    // ─── ClusterResourceState tests ──────────────────────────────────

    #[tokio::test]
    async fn test_cluster_resource_state_version_increments() {
        let mgr = make_mgr();

        let r1 = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let r2 = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let v1 = r1.cluster_resource_state.unwrap().cluster_resource_state_version;
        let v2 = r2.cluster_resource_state.unwrap().cluster_resource_state_version;
        assert!(v2 > v1);
    }

    #[tokio::test]
    async fn test_cluster_session_name() {
        let mgr = make_mgr();
        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            reply.cluster_resource_state.unwrap().cluster_session_name,
            "test_session"
        );
    }

    #[tokio::test]
    async fn test_node_states_alive_running() {
        let nm = make_node_manager();
        nm.add_node(make_node(b"n1")).await;
        let mgr = make_mgr_with(nm);

        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let state = reply.cluster_resource_state.unwrap();
        assert_eq!(state.node_states.len(), 1);
        let ns = &state.node_states[0];
        assert_eq!(ns.node_id, b"n1");
        assert_eq!(ns.status, NodeStatus::Running as i32);
        assert_eq!(ns.total_resources.get("CPU"), Some(&4.0));
        assert_eq!(ns.available_resources.get("CPU"), Some(&4.0));
        assert_eq!(ns.node_ip_address, "127.0.0.1");
    }

    #[tokio::test]
    async fn test_node_states_dead() {
        let nm = make_node_manager();
        nm.add_node(make_node(b"n1")).await;
        nm.remove_node(b"n1", NodeDeathInfo::default()).await;
        let mgr = make_mgr_with(nm);

        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let state = reply.cluster_resource_state.unwrap();
        assert_eq!(state.node_states.len(), 1);
        assert_eq!(state.node_states[0].status, NodeStatus::Dead as i32);
        // Dead nodes have no resource data.
        assert!(state.node_states[0].total_resources.is_empty());
    }

    #[tokio::test]
    async fn test_node_states_draining() {
        let nm = make_node_manager();
        nm.add_node(make_node(b"n1")).await;
        let mgr = make_mgr_with(nm.clone());

        // Initialize the resource cache, then drain.
        mgr.on_node_add(&make_node(b"n1"));
        mgr.set_node_draining_in_resource_info(b"n1", 99999);

        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let ns = &reply.cluster_resource_state.unwrap().node_states[0];
        assert_eq!(ns.status, NodeStatus::Draining as i32);
    }

    #[tokio::test]
    async fn test_node_states_idle() {
        let nm = make_node_manager();
        nm.add_node(make_node(b"n1")).await;
        let mgr = make_mgr_with(nm);

        // Set idle_duration_ms on the resource data.
        mgr.on_node_add(&make_node(b"n1"));
        if let Some(mut entry) = mgr.node_resource_info.get_mut(b"n1".as_slice()) {
            entry.1.idle_duration_ms = 5000;
        }

        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let ns = &reply.cluster_resource_state.unwrap().node_states[0];
        assert_eq!(ns.status, NodeStatus::Idle as i32);
        assert!(ns.idle_duration_ms >= 5000); // reported + elapsed
    }

    #[tokio::test]
    async fn test_node_states_alive_and_dead() {
        let nm = make_node_manager();
        nm.add_node(make_node(b"alive")).await;
        nm.add_node(make_node(b"dead")).await;
        nm.remove_node(b"dead", NodeDeathInfo::default()).await;
        let mgr = make_mgr_with(nm);

        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let state = reply.cluster_resource_state.unwrap();
        assert_eq!(state.node_states.len(), 2);
        let alive_count = state
            .node_states
            .iter()
            .filter(|n| n.status == NodeStatus::Running as i32)
            .count();
        let dead_count = state
            .node_states
            .iter()
            .filter(|n| n.status == NodeStatus::Dead as i32)
            .count();
        assert_eq!(alive_count, 1);
        assert_eq!(dead_count, 1);
    }

    // ─── Pending resource requests tests ─────────────────────────────

    #[tokio::test]
    async fn test_pending_resource_requests_aggregation() {
        let nm = make_node_manager();
        nm.add_node(make_node(b"n1")).await;
        let mgr = make_mgr_with(nm);
        mgr.on_node_add(&make_node(b"n1"));

        // Inject resource load data.
        let mut shape = HashMap::new();
        shape.insert("CPU".to_string(), 2.0);
        let demand = ResourceDemand {
            shape,
            num_ready_requests_queued: 3,
            num_infeasible_requests_queued: 1,
            backlog_size: 2,
            ..Default::default()
        };
        if let Some(mut entry) = mgr.node_resource_info.get_mut(b"n1".as_slice()) {
            entry.1.resource_load_by_shape = Some(ResourceLoad {
                resource_demands: vec![demand],
            });
        }

        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let state = reply.cluster_resource_state.unwrap();
        assert_eq!(state.pending_resource_requests.len(), 1);
        assert_eq!(state.pending_resource_requests[0].count, 6); // 3+1+2
        let req = state.pending_resource_requests[0].request.as_ref().unwrap();
        assert_eq!(req.resources_bundle.get("CPU"), Some(&2.0));
    }

    // ─── Pending gang resource request tests ─────────────────────────

    #[tokio::test]
    async fn test_gang_requests_empty_when_no_pgs() {
        let nm = make_node_manager();
        nm.add_node(make_node(b"n1")).await;
        let mgr = make_mgr_with(nm);

        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply
            .cluster_resource_state
            .unwrap()
            .pending_gang_resource_requests
            .is_empty());
    }

    fn make_pg_data(
        pg_id: &[u8],
        strategy: PlacementStrategy,
        state: PlacementGroupState,
        bundles: Vec<Bundle>,
    ) -> PlacementGroupTableData {
        PlacementGroupTableData {
            placement_group_id: pg_id.to_vec(),
            bundles,
            strategy: strategy as i32,
            state: state as i32,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_gang_requests_strict_spread_has_anti_affinity() {
        let nm = make_node_manager();
        let pg_id = b"pg_spread_01234567890123456";
        let source = FakePgLoadSource::with(vec![make_pg_data(
            pg_id,
            PlacementStrategy::StrictSpread,
            PlacementGroupState::Pending,
            vec![make_bundle(pg_id, 0, 1.0), make_bundle(pg_id, 1, 1.0)],
        )]);
        let mgr = make_mgr_with_pg_load(nm, source);
        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let state = reply.cluster_resource_state.unwrap();
        assert_eq!(state.pending_gang_resource_requests.len(), 1);
        let req = &state.pending_gang_resource_requests[0];
        assert_eq!(req.requests.len(), 2);
        assert_eq!(req.bundle_selectors.len(), 1);
        // STRICT_SPREAD → every request must have an anti-affinity constraint
        // with label name "_PG_<pg_id_hex>".
        let expected_label = format!("_PG_{}", hex::encode(pg_id));
        for r in &req.requests {
            assert_eq!(r.placement_constraints.len(), 1);
            let c = &r.placement_constraints[0];
            assert!(c.anti_affinity.is_some());
            assert_eq!(c.anti_affinity.as_ref().unwrap().label_name, expected_label);
            assert!(c.affinity.is_none());
        }
        // Details string format: <hex>:STRICT_SPREAD|PENDING
        let details = &req.details;
        assert!(details.contains(&hex::encode(pg_id)));
        assert!(details.contains("STRICT_SPREAD"));
        assert!(details.contains("PENDING"));
    }

    #[tokio::test]
    async fn test_gang_requests_strict_pack_has_affinity() {
        let nm = make_node_manager();
        let pg_id = b"pg_pack__01234567890123456__";
        let source = FakePgLoadSource::with(vec![make_pg_data(
            pg_id,
            PlacementStrategy::StrictPack,
            PlacementGroupState::Pending,
            vec![make_bundle(pg_id, 0, 1.0)],
        )]);
        let mgr = make_mgr_with_pg_load(nm, source);
        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let state = reply.cluster_resource_state.unwrap();
        assert_eq!(state.pending_gang_resource_requests.len(), 1);
        let req = &state.pending_gang_resource_requests[0];
        let c = &req.requests[0].placement_constraints[0];
        assert!(c.affinity.is_some());
        assert!(c.anti_affinity.is_none());
        let expected_label = format!("_PG_{}", hex::encode(pg_id));
        assert_eq!(c.affinity.as_ref().unwrap().label_name, expected_label);
    }

    #[tokio::test]
    async fn test_gang_requests_pack_no_constraint() {
        let nm = make_node_manager();
        let pg_id = b"pg_pack_plain______________";
        let source = FakePgLoadSource::with(vec![make_pg_data(
            pg_id,
            PlacementStrategy::Pack,
            PlacementGroupState::Pending,
            vec![make_bundle(pg_id, 0, 1.0)],
        )]);
        let mgr = make_mgr_with_pg_load(nm, source);
        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let state = reply.cluster_resource_state.unwrap();
        assert_eq!(state.pending_gang_resource_requests.len(), 1);
        let req = &state.pending_gang_resource_requests[0];
        assert_eq!(req.requests.len(), 1);
        // PACK → no placement constraint.
        assert!(req.requests[0].placement_constraints.is_empty());
        assert!(
            req.bundle_selectors[0].resource_requests[0]
                .placement_constraints
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_gang_requests_rescheduling_skips_placed_bundles() {
        let nm = make_node_manager();
        let pg_id = b"pg_resched_0123456789012345";
        let mut placed = make_bundle(pg_id, 0, 1.0);
        placed.node_id = vec![7u8; NODE_ID_SIZE]; // already placed
        let unplaced = make_bundle(pg_id, 1, 1.0);
        let source = FakePgLoadSource::with(vec![make_pg_data(
            pg_id,
            PlacementStrategy::StrictSpread,
            PlacementGroupState::Rescheduling,
            vec![placed, unplaced],
        )]);
        let mgr = make_mgr_with_pg_load(nm, source);
        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let state = reply.cluster_resource_state.unwrap();
        assert_eq!(state.pending_gang_resource_requests.len(), 1);
        // Only the unplaced bundle should appear as load.
        assert_eq!(state.pending_gang_resource_requests[0].requests.len(), 1);
    }

    #[tokio::test]
    async fn test_gang_requests_skips_created_state() {
        let nm = make_node_manager();
        let pg_id = b"pg_created_0123456789012345";
        let source = FakePgLoadSource::with(vec![make_pg_data(
            pg_id,
            PlacementStrategy::StrictSpread,
            PlacementGroupState::Created,
            vec![make_bundle(pg_id, 0, 1.0)],
        )]);
        let mgr = make_mgr_with_pg_load(nm, source);
        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply
            .cluster_resource_state
            .unwrap()
            .pending_gang_resource_requests
            .is_empty());
    }

    // ─── Constraint replacement tests ────────────────────────────────

    #[tokio::test]
    async fn test_constraint_replaces_not_accumulates() {
        let mgr = make_mgr();

        // Submit first constraint.
        mgr.request_cluster_resource_constraint(Request::new(
            RequestClusterResourceConstraintRequest {
                cluster_resource_constraint: Some(ClusterResourceConstraint::default()),
            },
        ))
        .await
        .unwrap();

        // Submit second constraint — should replace, not accumulate.
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
        // C++ stores only the latest constraint, so at most 1.
        assert_eq!(state.cluster_resource_constraints.len(), 1);
    }

    // ─── Autoscaling state version check tests ───────────────────────

    #[tokio::test]
    async fn test_report_autoscaling_state_version_check() {
        let mgr = make_mgr();

        // Report version 5.
        mgr.report_autoscaling_state(Request::new(ReportAutoscalingStateRequest {
            autoscaling_state: Some(AutoscalingState {
                autoscaler_state_version: 5,
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        // Report older version 3 — should be discarded.
        mgr.report_autoscaling_state(Request::new(ReportAutoscalingStateRequest {
            autoscaling_state: Some(AutoscalingState {
                autoscaler_state_version: 3,
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        // get_cluster_status should still show version 5.
        let reply = mgr
            .get_cluster_status(Request::new(GetClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            reply
                .autoscaling_state
                .unwrap()
                .autoscaler_state_version,
            5
        );
    }

    #[tokio::test]
    async fn test_report_autoscaling_state_newer_accepted() {
        let mgr = make_mgr();

        mgr.report_autoscaling_state(Request::new(ReportAutoscalingStateRequest {
            autoscaling_state: Some(AutoscalingState {
                autoscaler_state_version: 5,
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        // Report newer version 10 — should be accepted.
        mgr.report_autoscaling_state(Request::new(ReportAutoscalingStateRequest {
            autoscaling_state: Some(AutoscalingState {
                autoscaler_state_version: 10,
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

        assert_eq!(
            reply
                .autoscaling_state
                .unwrap()
                .autoscaler_state_version,
            10
        );
    }

    // ─── Cluster config KV persistence tests ─────────────────────────

    #[tokio::test]
    async fn test_report_cluster_config_persists_to_kv() {
        let kv = make_kv();
        let nm = make_node_manager();
        let pool = FakeRayletClientPool::new(HashMap::new());
        let mgr = GcsAutoscalerStateManager::new(
            nm,
            FakePgLoadSource::empty(),
            pool,
            kv.clone(),
            "s".into(),
        );

        mgr.report_cluster_config(Request::new(ReportClusterConfigRequest {
            cluster_config: Some(ClusterConfig::default()),
        }))
        .await
        .unwrap();

        // Verify KV contains the persisted config.
        let stored = kv
            .get(AUTOSCALER_STATE_NAMESPACE, AUTOSCALER_CLUSTER_CONFIG_KEY)
            .await;
        assert!(stored.is_some());
    }

    // ─── Resize validation tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_resize_bad_node_id_size() {
        let mgr = make_mgr();
        let result = mgr
            .resize_raylet_resource_instances(Request::new(
                ResizeRayletResourceInstancesRequest {
                    node_id: b"too_short".to_vec(),
                    resources: HashMap::new(),
                },
            ))
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_resize_dead_node() {
        let mgr = make_mgr();
        let result = mgr
            .resize_raylet_resource_instances(Request::new(
                ResizeRayletResourceInstancesRequest {
                    node_id: vec![0u8; NODE_ID_SIZE], // valid size but not alive
                    resources: HashMap::new(),
                },
            ))
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_resize_alive_node_forwards_and_returns_totals() {
        let nm = make_node_manager();
        let mut node = make_node(&vec![0u8; NODE_ID_SIZE]);
        node.node_id = vec![0u8; NODE_ID_SIZE];
        node.node_manager_address = "10.1.2.3".into();
        node.node_manager_port = 4321;
        nm.add_node(node).await;

        let mut response_totals = HashMap::new();
        response_totals.insert("CPU".to_string(), 8.0);
        response_totals.insert("GPU".to_string(), 1.0);
        let pool = FakeRayletClientPool::new(response_totals.clone());
        let mgr = make_mgr_with_pool(nm, pool.clone());

        let mut request_resources = HashMap::new();
        request_resources.insert("CPU".to_string(), 8.0);

        let reply = mgr
            .resize_raylet_resource_instances(Request::new(
                ResizeRayletResourceInstancesRequest {
                    node_id: vec![0u8; NODE_ID_SIZE],
                    resources: request_resources.clone(),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.total_resources, response_totals);

        // Verify the pool was called with the right raylet address and body.
        let calls = pool.calls.lock();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "10.1.2.3");
        assert_eq!(calls[0].1, 4321);
        assert_eq!(calls[0].2, request_resources);
    }

    #[tokio::test]
    async fn test_resize_raylet_error_propagates() {
        let nm = make_node_manager();
        let mut node = make_node(&vec![0u8; NODE_ID_SIZE]);
        node.node_id = vec![0u8; NODE_ID_SIZE];
        nm.add_node(node).await;

        let pool = FakeRayletClientPool::new_failing();
        let mgr = make_mgr_with_pool(nm, pool);

        let result = mgr
            .resize_raylet_resource_instances(Request::new(
                ResizeRayletResourceInstancesRequest {
                    node_id: vec![0u8; NODE_ID_SIZE],
                    resources: HashMap::new(),
                },
            ))
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }

    // ─── Drain node tests ────────────────────────────────────────────

    #[tokio::test]
    async fn test_drain_node_unknown_accepted() {
        let mgr = make_mgr();
        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                node_id: b"unknown".to_vec(),
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
    async fn test_drain_node_dead_accepted() {
        let nm = make_node_manager();
        nm.add_node(make_node(b"n1")).await;
        nm.remove_node(b"n1", NodeDeathInfo::default()).await;
        let mgr = make_mgr_with(nm);

        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                node_id: b"n1".to_vec(),
                reason: DrainNodeReason::IdleTermination as i32,
                reason_message: "idle".into(),
                deadline_timestamp_ms: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.is_accepted);
    }

    #[tokio::test]
    async fn test_drain_node_alive_accepted_and_shows_draining() {
        let nm = make_node_manager();
        nm.add_node(make_node(b"n1")).await;
        let mgr = make_mgr_with(nm);

        mgr.drain_node(Request::new(DrainNodeRequest {
            node_id: b"n1".to_vec(),
            reason: DrainNodeReason::IdleTermination as i32,
            reason_message: "idle".into(),
            deadline_timestamp_ms: 5000,
        }))
        .await
        .unwrap();

        // After draining, get_cluster_resource_state should show DRAINING.
        let reply = mgr
            .get_cluster_resource_state(Request::new(GetClusterResourceStateRequest {
                last_seen_cluster_resource_state_version: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let ns = &reply.cluster_resource_state.unwrap().node_states[0];
        assert_eq!(ns.status, NodeStatus::Draining as i32);
    }

    #[tokio::test]
    async fn test_drain_node_negative_deadline_rejected() {
        let mgr = make_mgr();
        let result = mgr
            .drain_node(Request::new(DrainNodeRequest {
                node_id: b"n1".to_vec(),
                reason: 0,
                reason_message: "test".into(),
                deadline_timestamp_ms: -1,
            }))
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    // ─── on_node_add / on_node_dead tests ────────────────────────────

    #[tokio::test]
    async fn test_on_node_add_idempotent() {
        let mgr = make_mgr();
        let node = make_node(b"n1");
        mgr.on_node_add(&node);
        mgr.on_node_add(&node); // Should not overwrite.
        assert_eq!(mgr.node_resource_info.len(), 1);
    }

    #[tokio::test]
    async fn test_on_node_dead_removes() {
        let mgr = make_mgr();
        mgr.on_node_add(&make_node(b"n1"));
        assert_eq!(mgr.node_resource_info.len(), 1);
        mgr.on_node_dead(b"n1");
        assert_eq!(mgr.node_resource_info.len(), 0);
    }

    // ─── get_cluster_status tests ────────────────────────────────────

    #[tokio::test]
    async fn test_get_cluster_status_includes_both() {
        let mgr = make_mgr();
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
        assert!(reply.cluster_resource_state.is_some());
        assert!(!reply
            .cluster_resource_state
            .unwrap()
            .cluster_session_name
            .is_empty());
    }

    // ─── gen_placement_constraint_for_pg unit tests ──────────────────

    #[test]
    fn test_gen_constraint_strict_spread() {
        let c = gen_placement_constraint_for_pg(
            "abcd",
            PlacementStrategy::StrictSpread as i32,
        )
        .unwrap();
        assert!(c.anti_affinity.is_some());
        assert_eq!(c.anti_affinity.unwrap().label_name, "_PG_abcd");
    }

    #[test]
    fn test_gen_constraint_strict_pack() {
        let c = gen_placement_constraint_for_pg(
            "abcd",
            PlacementStrategy::StrictPack as i32,
        )
        .unwrap();
        assert!(c.affinity.is_some());
        assert_eq!(c.affinity.unwrap().label_name, "_PG_abcd");
    }

    #[test]
    fn test_gen_constraint_pack_none() {
        assert!(gen_placement_constraint_for_pg(
            "abcd",
            PlacementStrategy::Pack as i32
        )
        .is_none());
    }

    #[test]
    fn test_gen_constraint_spread_none() {
        assert!(gen_placement_constraint_for_pg(
            "abcd",
            PlacementStrategy::Spread as i32
        )
        .is_none());
    }

    #[test]
    fn test_format_pg_details() {
        let pg = PlacementGroupTableData {
            placement_group_id: vec![0xab, 0xcd],
            strategy: PlacementStrategy::StrictSpread as i32,
            state: PlacementGroupState::Pending as i32,
            ..Default::default()
        };
        assert_eq!(format_placement_group_details(&pg), "abcd:STRICT_SPREAD|PENDING");
    }

    // ─── drain_node raylet-forwarding parity tests ────────────────────

    /// A scripted raylet pool that returns pre-programmed
    /// `DrainRaylet` replies and records every call. Separate from
    /// `FakeRayletClientPool` so these tests can assert the exact
    /// `DrainRaylet` arguments the autoscaler forwards.
    struct ScriptedDrainPool {
        drain_reply: parking_lot::Mutex<Result<NodeDrainRayletReply, Status>>,
        drain_calls: parking_lot::Mutex<Vec<(String, u16, NodeDrainRayletRequest)>>,
    }
    impl ScriptedDrainPool {
        fn accepted() -> Arc<Self> {
            Arc::new(Self {
                drain_reply: parking_lot::Mutex::new(Ok(NodeDrainRayletReply {
                    is_accepted: true,
                    rejection_reason_message: String::new(),
                })),
                drain_calls: parking_lot::Mutex::new(Vec::new()),
            })
        }
        fn rejected(reason: &str) -> Arc<Self> {
            Arc::new(Self {
                drain_reply: parking_lot::Mutex::new(Ok(NodeDrainRayletReply {
                    is_accepted: false,
                    rejection_reason_message: reason.into(),
                })),
                drain_calls: parking_lot::Mutex::new(Vec::new()),
            })
        }
        fn unavailable() -> Arc<Self> {
            Arc::new(Self {
                drain_reply: parking_lot::Mutex::new(Err(Status::unavailable("raylet down"))),
                drain_calls: parking_lot::Mutex::new(Vec::new()),
            })
        }
    }
    #[async_trait]
    impl RayletClientPool for ScriptedDrainPool {
        async fn resize_local_resource_instances(
            &self,
            _a: &str,
            _p: u16,
            _r: HashMap<String, f64>,
        ) -> Result<HashMap<String, f64>, Status> {
            Ok(HashMap::new())
        }
        async fn drain_raylet(
            &self,
            address: &str,
            port: u16,
            request: NodeDrainRayletRequest,
        ) -> Result<NodeDrainRayletReply, Status> {
            self.drain_calls
                .lock()
                .push((address.to_string(), port, request));
            match &*self.drain_reply.lock() {
                Ok(r) => Ok(r.clone()),
                Err(s) => Err(Status::new(s.code(), s.message().to_string())),
            }
        }
        async fn kill_local_actor(
            &self,
            _address: &str,
            _port: u16,
            _request: KillLocalActorRequest,
        ) -> Result<(), Status> {
            // Drain-path tests don't care about kill_local_actor.
            Ok(())
        }
    }

    /// Build a node with an explicit address/port so tests can assert
    /// the autoscaler forwarded to the right raylet.
    fn make_node_with_address(id: &[u8], addr: &str, port: i32) -> GcsNodeInfo {
        GcsNodeInfo {
            node_id: id.to_vec(),
            state: gcs_proto::ray::rpc::gcs_node_info::GcsNodeState::Alive as i32,
            node_manager_address: addr.into(),
            node_manager_port: port,
            resources_total: {
                let mut m = HashMap::new();
                m.insert("CPU".to_string(), 4.0);
                m
            },
            ..Default::default()
        }
    }

    /// Parity guard for C++ `gcs_autoscaler_state_manager.cc:498-514`:
    /// an alive-node drain must forward to the raylet and honour the
    /// raylet's acceptance. Also confirms the forwarded
    /// `(address, port, reason, reason_message, deadline)` match the
    /// original request.
    #[tokio::test]
    async fn drain_node_alive_forwards_to_raylet_and_accepts() {
        let nm = make_node_manager();
        nm.add_node(make_node_with_address(b"n1", "10.0.0.1", 7001))
            .await;
        let pool = ScriptedDrainPool::accepted();
        let mgr = make_mgr_with_pool(nm.clone(), pool.clone());

        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                node_id: b"n1".to_vec(),
                reason: DrainNodeReason::Preemption as i32,
                reason_message: "spot reclaim".into(),
                deadline_timestamp_ms: 5_000,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.is_accepted);
        assert!(reply.rejection_reason_message.is_empty());

        let calls = pool.drain_calls.lock();
        assert_eq!(calls.len(), 1, "raylet must be called exactly once");
        let (addr, port, req) = &calls[0];
        assert_eq!(addr, "10.0.0.1");
        assert_eq!(*port, 7001);
        assert_eq!(
            req.reason,
            DrainNodeReason::Preemption as i32
        );
        assert_eq!(req.reason_message, "spot reclaim");
        assert_eq!(req.deadline_timestamp_ms, 5_000);

        // Node manager should have recorded draining state.
        assert!(nm.is_node_draining(b"n1"));
    }

    /// Parity guard for C++ lines 515-520: the raylet may refuse a
    /// drain. The GCS must propagate the rejection unchanged and must
    /// NOT record the node as draining.
    #[tokio::test]
    async fn drain_node_alive_rejection_is_propagated_and_no_state_set() {
        let nm = make_node_manager();
        nm.add_node(make_node_with_address(b"n1", "10.0.0.1", 7001))
            .await;
        let pool = ScriptedDrainPool::rejected("has non-preemptible work");
        let mgr = make_mgr_with_pool(nm.clone(), pool.clone());

        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                node_id: b"n1".to_vec(),
                reason: 0,
                reason_message: "idle".into(),
                deadline_timestamp_ms: 1_000,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!reply.is_accepted, "rejection must propagate");
        assert_eq!(reply.rejection_reason_message, "has non-preemptible work");

        // Critical: the node must NOT be marked draining on rejection.
        assert!(
            !nm.is_node_draining(b"n1"),
            "node_manager must not record a drain the raylet refused"
        );
    }

    /// Parity guard for raylet-transport failures: when `DrainRaylet`
    /// returns a gRPC error, C++ forwards the `Status` via the reply
    /// callback (line 521). We return the same `Status` so the caller
    /// can retry or surface the error.
    #[tokio::test]
    async fn drain_node_raylet_rpc_error_is_propagated() {
        let nm = make_node_manager();
        nm.add_node(make_node_with_address(b"n1", "10.0.0.1", 7001))
            .await;
        let pool = ScriptedDrainPool::unavailable();
        let mgr = make_mgr_with_pool(nm.clone(), pool);

        let err = mgr
            .drain_node(Request::new(DrainNodeRequest {
                node_id: b"n1".to_vec(),
                reason: 0,
                reason_message: "x".into(),
                deadline_timestamp_ms: 0,
            }))
            .await
            .err()
            .expect("raylet unavailable must surface as Err");
        assert_eq!(err.code(), tonic::Code::Unavailable);
        // No draining recorded on transport failure either.
        assert!(!nm.is_node_draining(b"n1"));
    }

    /// Parity guard for C++ lines 482-494: dead / unknown nodes are
    /// accepted without contacting any raylet.
    #[tokio::test]
    async fn drain_node_unknown_does_not_call_raylet() {
        let nm = make_node_manager();
        let pool = ScriptedDrainPool::rejected("should not be called");
        let mgr = make_mgr_with_pool(nm, pool.clone());

        let reply = mgr
            .drain_node(Request::new(DrainNodeRequest {
                node_id: b"gone".to_vec(),
                reason: 0,
                reason_message: "gone".into(),
                deadline_timestamp_ms: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.is_accepted);
        assert!(
            pool.drain_calls.lock().is_empty(),
            "unknown node must not trigger raylet RPC"
        );
    }
}
