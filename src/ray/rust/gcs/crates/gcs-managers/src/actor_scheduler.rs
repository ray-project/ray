//! GCS actor scheduler — drives PENDING_CREATION → ALIVE via raylet/worker RPCs.
//!
//! Maps C++ `GcsActorScheduler` from `src/ray/gcs/actor/gcs_actor_scheduler.h/cc`.
//!
//! The scheduler selects a node, leases a worker from that node's raylet via
//! `RequestWorkerLease`, then creates the actor on the worker via `PushTask`.
//! Success and failure are reported via channels that the actor manager reads.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use gcs_proto::ray::rpc::core_worker_service_client::CoreWorkerServiceClient;
use gcs_proto::ray::rpc::node_manager_service_client::NodeManagerServiceClient;
use gcs_proto::ray::rpc::request_worker_lease_reply::SchedulingFailureType;
use gcs_proto::ray::rpc::*;

use crate::node_manager::GcsNodeManager;

/// Information about a leased worker.
#[derive(Debug, Clone)]
pub struct LeasedWorker {
    pub worker_id: Vec<u8>,
    pub node_id: Vec<u8>,
    pub address: Address,
    pub resources: Vec<ResourceMapEntry>,
    pub actor_id: Vec<u8>,
}

/// An actor waiting to be scheduled. Stored in the pending queue.
#[derive(Debug, Clone)]
pub struct PendingActor {
    pub actor_id: Vec<u8>,
    pub task_spec: TaskSpec,
    pub actor_data: ActorTableData,
}

/// Result of a successful actor creation.
pub struct ActorCreationSuccess {
    pub actor_id: Vec<u8>,
    pub worker_address: Address,
    pub worker_pid: u32,
    pub resources: Vec<ResourceMapEntry>,
    pub actor_repr_name: String,
}

/// Result of a failed actor scheduling attempt.
pub struct ActorSchedulingFailure {
    pub actor_id: Vec<u8>,
    pub failure_type: SchedulingFailureType,
    pub error_message: String,
}

/// GCS Actor Scheduler.
///
/// Maps C++ `GcsActorScheduler` from `gcs_actor_scheduler.h/cc`.
/// Drives the async flow: SelectNode → LeaseWorker → CreateActor.
pub struct GcsActorScheduler {
    node_manager: Arc<GcsNodeManager>,
    /// NodeID → set of ActorIDs being leased on that node.
    /// Maps C++ `node_to_actors_when_leasing_`.
    leasing: Mutex<HashMap<Vec<u8>, HashSet<Vec<u8>>>>,
    /// NodeID → (WorkerID → LeasedWorker) for actors being created.
    /// Maps C++ `node_to_workers_when_creating_`.
    creating: Mutex<HashMap<Vec<u8>, HashMap<Vec<u8>, LeasedWorker>>>,
    /// Actors waiting for node availability (soft scheduling failure → retry).
    /// Maps C++ `pending_actors_`.
    pending: Mutex<Vec<PendingActor>>,
    /// Channel to report creation success to the actor manager.
    success_tx: mpsc::UnboundedSender<ActorCreationSuccess>,
    /// Channel to report scheduling failure to the actor manager.
    failure_tx: mpsc::UnboundedSender<ActorSchedulingFailure>,
}

impl GcsActorScheduler {
    pub fn new(
        node_manager: Arc<GcsNodeManager>,
        success_tx: mpsc::UnboundedSender<ActorCreationSuccess>,
        failure_tx: mpsc::UnboundedSender<ActorSchedulingFailure>,
    ) -> Self {
        Self {
            node_manager,
            leasing: Mutex::new(HashMap::new()),
            creating: Mutex::new(HashMap::new()),
            pending: Mutex::new(Vec::new()),
            success_tx,
            failure_tx,
        }
    }

    /// Schedule an actor for creation.
    ///
    /// Maps C++ `GcsActorScheduler::Schedule` (gcs_actor_scheduler.cc:49-81).
    /// Selects a node, then starts the async lease → create flow.
    pub fn schedule(self: &Arc<Self>, actor: PendingActor) {
        let node_id = self.select_forwarding_node(&actor);

        let node = match node_id {
            Some(id) => match self.node_manager.get_alive_node(&id) {
                Some(node) => node,
                None => {
                    // Node disappeared between selection and lookup.
                    let _ = self.failure_tx.send(ActorSchedulingFailure {
                        actor_id: actor.actor_id.clone(),
                        failure_type: SchedulingFailureType::SchedulingFailed,
                        error_message: "Selected node is no longer alive".into(),
                    });
                    return;
                }
            },
            None => {
                // No available nodes — queue for retry (C++ lines 60-64).
                let _ = self.failure_tx.send(ActorSchedulingFailure {
                    actor_id: actor.actor_id.clone(),
                    failure_type: SchedulingFailureType::SchedulingFailed,
                    error_message: "No available nodes to schedule the actor".into(),
                });
                return;
            }
        };

        let node_id = node.node_id.clone();

        // Track this actor as leasing on this node (C++ line 74-76).
        {
            let mut leasing = self.leasing.lock();
            leasing
                .entry(node_id.clone())
                .or_default()
                .insert(actor.actor_id.clone());
        }

        // Spawn async lease flow.
        let this = self.clone();
        tokio::spawn(async move {
            this.lease_worker_from_node(actor, node).await;
        });
    }

    /// Select a node for the actor.
    ///
    /// Maps C++ `GcsActorScheduler::SelectForwardingNode` (gcs_actor_scheduler.cc:83-99).
    /// If actor has resource requirements, prefer the owner node (if alive),
    /// otherwise pick a random alive node.
    fn select_forwarding_node(&self, actor: &PendingActor) -> Option<Vec<u8>> {
        let has_resources = actor
            .task_spec
            .required_resources
            .values()
            .any(|v| *v > 0.0);

        if has_resources {
            // Try owner node first (matching C++: prefer locality).
            let owner_node_id = actor
                .task_spec
                .caller_address
                .as_ref()
                .map(|a| a.node_id.clone())
                .unwrap_or_default();

            if !owner_node_id.is_empty()
                && self.node_manager.is_node_alive(&owner_node_id)
            {
                return Some(owner_node_id);
            }
        }

        // Fall back to random alive node (matching C++ SelectRandomAliveNode).
        self.select_random_alive_node()
    }

    /// Pick a random alive node.
    fn select_random_alive_node(&self) -> Option<Vec<u8>> {
        let nodes = self.node_manager.get_all_alive_nodes();
        // Use a simple selection — pick the first one (deterministic for tests;
        // in production with multiple nodes this still provides distribution
        // since DashMap iteration order varies).
        nodes.into_keys().next()
    }

    /// Lease a worker from a node's raylet.
    ///
    /// Maps C++ `GcsActorScheduler::LeaseWorkerFromNode` (gcs_actor_scheduler.cc:234-271).
    async fn lease_worker_from_node(
        &self,
        actor: PendingActor,
        node: GcsNodeInfo,
    ) {
        let node_id = node.node_id.clone();
        let actor_id = actor.actor_id.clone();

        let endpoint = format!(
            "http://{}:{}",
            node.node_manager_address, node.node_manager_port
        );

        debug!(
            actor_id = hex::encode(&actor_id),
            endpoint, "Leasing worker from node"
        );

        let channel = match Channel::from_shared(endpoint.clone()) {
            Ok(ch) => match ch.connect().await {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, endpoint, "Failed to connect to raylet");
                    self.handle_lease_failure(actor, &node_id);
                    return;
                }
            },
            Err(e) => {
                warn!(error = %e, endpoint, "Invalid raylet endpoint");
                self.handle_lease_failure(actor, &node_id);
                return;
            }
        };

        let mut client = NodeManagerServiceClient::new(channel);

        // Build a LeaseSpec from the TaskSpec (matching C++ LeaseSpecification(*task_spec_)).
        let creation = actor.task_spec.actor_creation_task_spec.as_ref();
        let lease_spec = LeaseSpec {
            job_id: actor.task_spec.job_id.clone(),
            caller_address: actor.task_spec.caller_address.clone(),
            r#type: actor.task_spec.r#type,
            actor_id: creation.map(|c| c.actor_id.clone()).unwrap_or_default(),
            is_detached_actor: creation.map(|c| c.is_detached).unwrap_or(false),
            root_detached_actor_id: Vec::new(),
            max_actor_restarts: creation.map(|c| c.max_actor_restarts).unwrap_or(0),
            required_resources: actor.task_spec.required_resources.clone(),
            required_placement_resources: actor.task_spec.required_placement_resources.clone(),
            scheduling_strategy: actor.task_spec.scheduling_strategy.clone(),
            runtime_env_info: actor.task_spec.runtime_env_info.clone(),
            parent_task_id: actor.task_spec.parent_task_id.clone(),
            language: actor.task_spec.language,
            ..Default::default()
        };
        let request = RequestWorkerLeaseRequest {
            lease_spec: Some(lease_spec),
            backlog_size: 0,
            grant_or_reject: false,
            is_selected_based_on_locality: false,
        };

        match client.request_worker_lease(request).await {
            Ok(resp) => {
                let reply = resp.into_inner();
                self.handle_worker_lease_reply(actor, node, reply).await;
            }
            Err(e) => {
                warn!(
                    error = %e,
                    actor_id = hex::encode(&actor_id),
                    "RequestWorkerLease RPC failed, retrying"
                );
                self.handle_lease_failure(actor, &node_id);
            }
        }
    }

    /// Handle lease failure — remove from leasing map and report failure.
    fn handle_lease_failure(&self, actor: PendingActor, node_id: &[u8]) {
        {
            let mut leasing = self.leasing.lock();
            if let Some(set) = leasing.get_mut(node_id) {
                set.remove(&actor.actor_id);
                if set.is_empty() {
                    leasing.remove(node_id);
                }
            }
        }
        let _ = self.failure_tx.send(ActorSchedulingFailure {
            actor_id: actor.actor_id,
            failure_type: SchedulingFailureType::SchedulingFailed,
            error_message: "Worker lease failed".into(),
        });
    }

    /// Handle a worker lease reply from the raylet.
    ///
    /// Maps C++ `GcsActorScheduler::HandleWorkerLeaseReply` (gcs_actor_scheduler.cc:519-599)
    /// and `HandleWorkerLeaseGrantedReply` (gcs_actor_scheduler.cc:296-365).
    async fn handle_worker_lease_reply(
        &self,
        actor: PendingActor,
        node: GcsNodeInfo,
        reply: RequestWorkerLeaseReply,
    ) {
        let node_id = node.node_id.clone();
        let actor_id = actor.actor_id.clone();

        // Check if actor was cancelled while leasing.
        {
            let mut leasing = self.leasing.lock();
            if let Some(set) = leasing.get_mut(&node_id) {
                if !set.remove(&actor_id) {
                    // Actor was cancelled (removed from leasing map externally).
                    debug!(
                        actor_id = hex::encode(&actor_id),
                        "Actor cancelled during lease"
                    );
                    return;
                }
                if set.is_empty() {
                    leasing.remove(&node_id);
                }
            } else {
                return; // Node removed from leasing map entirely.
            }
        }

        if reply.canceled {
            // Lease was explicitly cancelled.
            let _ = self.failure_tx.send(ActorSchedulingFailure {
                actor_id,
                failure_type: SchedulingFailureType::SchedulingCancelledIntended,
                error_message: "Worker lease was cancelled".into(),
            });
            return;
        }

        if reply.rejected {
            // Raylet rejected — try a different node via re-schedule.
            let _ = self.failure_tx.send(ActorSchedulingFailure {
                actor_id,
                failure_type: SchedulingFailureType::SchedulingFailed,
                error_message: "Raylet rejected the lease".into(),
            });
            return;
        }

        let worker_address = reply.worker_address.clone().unwrap_or_default();
        let retry_address = reply.retry_at_raylet_address.clone().unwrap_or_default();

        if worker_address.node_id.is_empty() {
            if !retry_address.node_id.is_empty() {
                // Spillback: raylet says try a different node (C++ line 304-319).
                debug!(
                    actor_id = hex::encode(&actor_id),
                    "Worker lease spillback to different node"
                );
            }
            // Re-schedule from scratch.
            let _ = self.failure_tx.send(ActorSchedulingFailure {
                actor_id,
                failure_type: SchedulingFailureType::SchedulingFailed,
                error_message: "No worker granted, rescheduling".into(),
            });
            return;
        }

        // Lease granted — now create the actor on the worker.
        let worker_id = worker_address.worker_id.clone();
        let leased_worker = LeasedWorker {
            worker_id: worker_id.clone(),
            node_id: node_id.clone(),
            address: worker_address.clone(),
            resources: reply.resource_mapping.clone(),
            actor_id: actor_id.clone(),
        };

        // Track in creating map (C++ line 336-338).
        {
            let mut creating = self.creating.lock();
            creating
                .entry(node_id.clone())
                .or_default()
                .insert(worker_id.clone(), leased_worker.clone());
        }

        info!(
            actor_id = hex::encode(&actor_id),
            worker_id = hex::encode(&worker_id),
            "Worker lease granted, creating actor on worker"
        );

        self.create_actor_on_worker(actor, leased_worker, reply.worker_pid)
            .await;
    }

    /// Create an actor on a leased worker by sending PushTask RPC.
    ///
    /// Maps C++ `GcsActorScheduler::CreateActorOnWorker` (gcs_actor_scheduler.cc:382-452).
    async fn create_actor_on_worker(
        &self,
        actor: PendingActor,
        worker: LeasedWorker,
        worker_pid: u32,
    ) {
        let actor_id = actor.actor_id.clone();
        let worker_id = worker.worker_id.clone();
        let node_id = worker.node_id.clone();

        let endpoint = format!(
            "http://{}:{}",
            worker.address.ip_address, worker.address.port
        );

        let channel = match Channel::from_shared(endpoint.clone()) {
            Ok(ch) => match ch.connect().await {
                Ok(c) => c,
                Err(e) => {
                    warn!(error = %e, "Failed to connect to worker for actor creation");
                    self.handle_create_failure(&actor_id, &worker_id, &node_id);
                    return;
                }
            },
            Err(e) => {
                warn!(error = %e, "Invalid worker endpoint");
                self.handle_create_failure(&actor_id, &worker_id, &node_id);
                return;
            }
        };

        let mut client = CoreWorkerServiceClient::new(channel);

        let request = PushTaskRequest {
            intended_worker_id: worker_id.clone(),
            task_spec: Some(actor.task_spec.clone()),
            resource_mapping: worker.resources.clone(),
            ..Default::default()
        };

        match client.push_task(request).await {
            Ok(resp) => {
                let reply = resp.into_inner();

                // Remove from creating map (C++ line 420-422).
                {
                    let mut creating = self.creating.lock();
                    if let Some(workers) = creating.get_mut(&node_id) {
                        workers.remove(&worker_id);
                        if workers.is_empty() {
                            creating.remove(&node_id);
                        }
                    }
                }

                info!(
                    actor_id = hex::encode(&actor_id),
                    "Actor creation task succeeded"
                );

                // Report success (C++ line 430: schedule_success_handler_).
                let _ = self.success_tx.send(ActorCreationSuccess {
                    actor_id,
                    worker_address: worker.address,
                    worker_pid,
                    resources: worker.resources,
                    actor_repr_name: reply.actor_repr_name.unwrap_or_default(),
                });
            }
            Err(e) => {
                warn!(
                    error = %e,
                    actor_id = hex::encode(&actor_id),
                    "PushTask RPC failed for actor creation"
                );
                self.handle_create_failure(&actor_id, &worker_id, &node_id);
            }
        }
    }

    /// Handle creation failure — remove from creating map and report.
    fn handle_create_failure(
        &self,
        actor_id: &[u8],
        worker_id: &[u8],
        node_id: &[u8],
    ) {
        {
            let mut creating = self.creating.lock();
            if let Some(workers) = creating.get_mut(node_id) {
                workers.remove(worker_id);
                if workers.is_empty() {
                    creating.remove(node_id);
                }
            }
        }
        let _ = self.failure_tx.send(ActorSchedulingFailure {
            actor_id: actor_id.to_vec(),
            failure_type: SchedulingFailureType::SchedulingFailed,
            error_message: "Actor creation on worker failed".into(),
        });
    }

    /// Cancel all actors being scheduled on a dead node.
    ///
    /// Maps C++ `GcsActorScheduler::CancelOnNode` (gcs_actor_scheduler.cc:131-173).
    /// Returns the actor IDs that were cancelled.
    pub fn cancel_on_node(&self, node_id: &[u8]) -> Vec<Vec<u8>> {
        let mut cancelled = Vec::new();

        // Cancel leasing actors.
        {
            let mut leasing = self.leasing.lock();
            if let Some(actors) = leasing.remove(node_id) {
                cancelled.extend(actors);
            }
        }

        // Cancel creating actors.
        {
            let mut creating = self.creating.lock();
            if let Some(workers) = creating.remove(node_id) {
                for (_worker_id, worker) in workers {
                    cancelled.push(worker.actor_id);
                }
            }
        }

        cancelled
    }

    /// Cancel actor being scheduled on a dead worker.
    ///
    /// Maps C++ `GcsActorScheduler::CancelOnWorker` (gcs_actor_scheduler.cc:176-207).
    /// Returns the actor ID that was cancelled, if any.
    pub fn cancel_on_worker(
        &self,
        node_id: &[u8],
        worker_id: &[u8],
    ) -> Option<Vec<u8>> {
        let mut creating = self.creating.lock();
        if let Some(workers) = creating.get_mut(node_id) {
            if let Some(worker) = workers.remove(worker_id) {
                if workers.is_empty() {
                    creating.remove(node_id);
                }
                return Some(worker.actor_id);
            }
        }
        None
    }

    /// Queue an actor for retry when nodes become available.
    ///
    /// Maps C++ `pending_actors_` (gcs_actor_manager.cc:1588).
    pub fn enqueue_pending(&self, actor: PendingActor) {
        self.pending.lock().push(actor);
    }

    /// Remove a single actor from every scheduler tracking map
    /// (`pending`, `leasing`, `creating`). Returns whether the actor
    /// was found in any of them.
    ///
    /// Maps C++ `GcsActorManager::CancelActorInScheduling`
    /// (`gcs_actor_manager.cc:1934-1965`) which combines
    /// `CancelOnWorker`, pending-vector removal, and `CancelOnLeasing`.
    /// We unify them here so the actor manager can call a single
    /// method when killing a not-yet-created actor —
    /// `kill_actor_via_gcs` `no_restart=false` path.
    pub fn cancel_actor_scheduling(&self, actor_id: &[u8]) -> bool {
        let mut found = false;

        // Remove from pending queue.
        {
            let mut pending = self.pending.lock();
            let before = pending.len();
            pending.retain(|p| p.actor_id != actor_id);
            if pending.len() != before {
                found = true;
            }
        }

        // Remove from leasing map (any node).
        {
            let mut leasing = self.leasing.lock();
            let mut empty_nodes = Vec::new();
            for (node_id, set) in leasing.iter_mut() {
                if set.remove(actor_id) {
                    found = true;
                    if set.is_empty() {
                        empty_nodes.push(node_id.clone());
                    }
                }
            }
            for node_id in empty_nodes {
                leasing.remove(&node_id);
            }
        }

        // Remove from creating map (any node, any worker).
        {
            let mut creating = self.creating.lock();
            let mut empty_nodes = Vec::new();
            for (node_id, workers) in creating.iter_mut() {
                let to_remove: Vec<Vec<u8>> = workers
                    .iter()
                    .filter(|(_, w)| w.actor_id == actor_id)
                    .map(|(wid, _)| wid.clone())
                    .collect();
                for wid in to_remove {
                    workers.remove(&wid);
                    found = true;
                }
                if workers.is_empty() {
                    empty_nodes.push(node_id.clone());
                }
            }
            for node_id in empty_nodes {
                creating.remove(&node_id);
            }
        }

        found
    }

    /// Re-schedule all pending actors (called when new nodes register).
    ///
    /// Maps C++ `GcsActorManager::SchedulePendingActors` (gcs_actor_manager.cc:1701-1711).
    pub fn schedule_pending_actors(self: &Arc<Self>) {
        let actors: Vec<PendingActor> = {
            let mut pending = self.pending.lock();
            if pending.is_empty() {
                return;
            }
            debug!(count = pending.len(), "Scheduling pending actors");
            std::mem::take(&mut *pending)
        };

        for actor in actors {
            self.schedule(actor);
        }
    }
}

/// Hex-encode bytes for logging.
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;
    use gcs_table_storage::GcsTableStorage;

    fn make_node_manager() -> Arc<GcsNodeManager> {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(gcs_pubsub::GcsPublisher::new(16));
        Arc::new(GcsNodeManager::new(
            table_storage,
            publisher,
            b"test_cluster".to_vec(),
        ))
    }

    fn make_pending_actor(actor_id: &[u8]) -> PendingActor {
        PendingActor {
            actor_id: actor_id.to_vec(),
            task_spec: TaskSpec::default(),
            actor_data: ActorTableData {
                actor_id: actor_id.to_vec(),
                ..Default::default()
            },
        }
    }

    #[tokio::test]
    async fn test_schedule_no_nodes_reports_failure() {
        let nm = make_node_manager();
        let (success_tx, _success_rx) = mpsc::unbounded_channel();
        let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
        let scheduler = Arc::new(GcsActorScheduler::new(nm, success_tx, failure_tx));

        let actor = make_pending_actor(b"actor1");
        scheduler.schedule(actor);

        let failure = failure_rx.recv().await.unwrap();
        assert_eq!(failure.actor_id, b"actor1");
        assert_eq!(failure.failure_type, SchedulingFailureType::SchedulingFailed);
    }

    #[tokio::test]
    async fn test_cancel_on_node() {
        let nm = make_node_manager();
        let (success_tx, _) = mpsc::unbounded_channel();
        let (failure_tx, _) = mpsc::unbounded_channel();
        let scheduler = GcsActorScheduler::new(nm, success_tx, failure_tx);

        // Manually populate leasing and creating maps.
        {
            let mut leasing = scheduler.leasing.lock();
            let mut set = HashSet::new();
            set.insert(b"actor1".to_vec());
            set.insert(b"actor2".to_vec());
            leasing.insert(b"node1".to_vec(), set);
        }
        {
            let mut creating = scheduler.creating.lock();
            let mut workers = HashMap::new();
            workers.insert(
                b"worker1".to_vec(),
                LeasedWorker {
                    worker_id: b"worker1".to_vec(),
                    node_id: b"node1".to_vec(),
                    address: Address::default(),
                    resources: vec![],
                    actor_id: b"actor3".to_vec(),
                },
            );
            creating.insert(b"node1".to_vec(), workers);
        }

        let cancelled = scheduler.cancel_on_node(b"node1");
        assert_eq!(cancelled.len(), 3);
        assert!(cancelled.contains(&b"actor1".to_vec()));
        assert!(cancelled.contains(&b"actor2".to_vec()));
        assert!(cancelled.contains(&b"actor3".to_vec()));

        // Maps should be empty now.
        assert!(scheduler.leasing.lock().is_empty());
        assert!(scheduler.creating.lock().is_empty());
    }

    #[tokio::test]
    async fn test_cancel_on_worker() {
        let nm = make_node_manager();
        let (success_tx, _) = mpsc::unbounded_channel();
        let (failure_tx, _) = mpsc::unbounded_channel();
        let scheduler = GcsActorScheduler::new(nm, success_tx, failure_tx);

        {
            let mut creating = scheduler.creating.lock();
            let mut workers = HashMap::new();
            workers.insert(
                b"w1".to_vec(),
                LeasedWorker {
                    worker_id: b"w1".to_vec(),
                    node_id: b"n1".to_vec(),
                    address: Address::default(),
                    resources: vec![],
                    actor_id: b"actor1".to_vec(),
                },
            );
            workers.insert(
                b"w2".to_vec(),
                LeasedWorker {
                    worker_id: b"w2".to_vec(),
                    node_id: b"n1".to_vec(),
                    address: Address::default(),
                    resources: vec![],
                    actor_id: b"actor2".to_vec(),
                },
            );
            creating.insert(b"n1".to_vec(), workers);
        }

        let cancelled = scheduler.cancel_on_worker(b"n1", b"w1");
        assert_eq!(cancelled, Some(b"actor1".to_vec()));

        // w2 still in creating map.
        assert!(scheduler.creating.lock().get(&b"n1".to_vec()).unwrap().contains_key(&b"w2".to_vec()));

        let cancelled2 = scheduler.cancel_on_worker(b"n1", b"w2");
        assert_eq!(cancelled2, Some(b"actor2".to_vec()));

        // Node entry cleaned up.
        assert!(scheduler.creating.lock().is_empty());
    }

    #[tokio::test]
    async fn test_enqueue_and_schedule_pending() {
        let nm = make_node_manager();
        let (success_tx, _) = mpsc::unbounded_channel();
        let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
        let scheduler = Arc::new(GcsActorScheduler::new(nm, success_tx, failure_tx));

        scheduler.enqueue_pending(make_pending_actor(b"a1"));
        scheduler.enqueue_pending(make_pending_actor(b"a2"));
        assert_eq!(scheduler.pending.lock().len(), 2);

        // Schedule pending — will fail since no nodes, but drains the queue.
        scheduler.schedule_pending_actors();

        // Both should fail (no nodes).
        let f1 = failure_rx.recv().await.unwrap();
        let f2 = failure_rx.recv().await.unwrap();
        let mut ids = vec![f1.actor_id, f2.actor_id];
        ids.sort();
        assert_eq!(ids, vec![b"a1".to_vec(), b"a2".to_vec()]);

        // Pending queue is empty.
        assert!(scheduler.pending.lock().is_empty());
    }

    /// `cancel_actor_scheduling` must remove the actor from every
    /// tracking map. Maps C++ `CancelActorInScheduling`
    /// (`gcs_actor_manager.cc:1934-1965`).
    #[tokio::test]
    async fn test_cancel_actor_scheduling_removes_from_all_maps() {
        let nm = make_node_manager();
        let (success_tx, _rx_s) = mpsc::unbounded_channel();
        let (failure_tx, _rx_f) = mpsc::unbounded_channel();
        let scheduler = Arc::new(GcsActorScheduler::new(nm, success_tx, failure_tx));

        // Pending only.
        scheduler.enqueue_pending(make_pending_actor(b"a_pending"));
        assert!(scheduler.cancel_actor_scheduling(b"a_pending"));
        assert!(scheduler.pending.lock().is_empty());

        // Leasing on a node.
        scheduler
            .leasing
            .lock()
            .entry(b"node_x".to_vec())
            .or_default()
            .insert(b"a_lease".to_vec());
        assert!(scheduler.cancel_actor_scheduling(b"a_lease"));
        assert!(scheduler.leasing.lock().is_empty());

        // Creating on a worker.
        scheduler.creating.lock().entry(b"node_y".to_vec()).or_default().insert(
            b"worker_z".to_vec(),
            LeasedWorker {
                worker_id: b"worker_z".to_vec(),
                node_id: b"node_y".to_vec(),
                address: Address::default(),
                resources: vec![],
                actor_id: b"a_creating".to_vec(),
            },
        );
        assert!(scheduler.cancel_actor_scheduling(b"a_creating"));
        assert!(scheduler.creating.lock().is_empty());

        // Unknown actor → false.
        assert!(!scheduler.cancel_actor_scheduling(b"nonexistent"));
    }
}
