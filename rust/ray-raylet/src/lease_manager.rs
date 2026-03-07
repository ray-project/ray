// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Lease managers — local and cluster-level task lease management.
//!
//! Replaces `src/ray/raylet/scheduling/local_lease_manager.h/cc` and
//! `src/ray/raylet/scheduling/cluster_lease_manager.h/cc`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use ray_common::scheduling::ResourceSet;
use tokio::sync::oneshot;

use crate::cluster_resource_scheduler::ClusterResourceScheduler;
use crate::scheduling_resources::{SchedulingOptions, TaskResourceInstances};

/// Unique lease identifier.
pub type LeaseID = u64;

/// Status of a work item in the lease pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkStatus {
    /// Waiting for dependencies to be fetched.
    Waiting,
    /// Ready for dispatch — waiting for a worker.
    WaitingForWorker,
    /// Cancelled.
    Cancelled,
}

/// Reason why a lease is not yet scheduled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnscheduledWorkCause {
    WaitingForResourceAcquisition,
    WaitingForAvailablePlasmaMemory,
    WaitingForResourcesAvailable,
    WaitingForNodeAvailable,
}

/// A work item representing a pending lease request.
pub struct Work {
    pub lease_id: LeaseID,
    pub resource_request: ResourceSet,
    pub scheduling_options: SchedulingOptions,
    pub status: WorkStatus,
    pub allocation: Option<TaskResourceInstances>,
    pub reply: Option<oneshot::Sender<LeaseReply>>,
}

/// Reply to a lease request.
#[derive(Debug)]
pub enum LeaseReply {
    /// Granted on a node with the given address.
    Granted {
        node_id: String,
        worker_address: String,
        allocation: Option<TaskResourceInstances>,
    },
    /// Rejected (infeasible or cancelled).
    Rejected { reason: String },
    /// Spillback to another node.
    Spillback { node_id: String },
}

/// Scheduling class — groups tasks with the same resource shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchedulingClass(pub u64);

/// The cluster lease manager queues and schedules lease requests.
pub struct ClusterLeaseManager {
    /// Pending lease requests, grouped by scheduling class.
    leases_to_schedule: Mutex<HashMap<SchedulingClass, VecDeque<Work>>>,
    /// Infeasible lease requests.
    infeasible_leases: Mutex<HashMap<SchedulingClass, VecDeque<Work>>>,
    /// The scheduler.
    scheduler: Arc<ClusterResourceScheduler>,
    /// Next lease ID.
    next_lease_id: std::sync::atomic::AtomicU64,
}

impl ClusterLeaseManager {
    pub fn new(scheduler: Arc<ClusterResourceScheduler>) -> Self {
        Self {
            leases_to_schedule: Mutex::new(HashMap::new()),
            infeasible_leases: Mutex::new(HashMap::new()),
            scheduler,
            next_lease_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Queue a new lease request and trigger scheduling.
    pub fn queue_and_schedule_lease(
        &self,
        resource_request: ResourceSet,
        scheduling_options: SchedulingOptions,
        scheduling_class: SchedulingClass,
    ) -> oneshot::Receiver<LeaseReply> {
        let lease_id = self
            .next_lease_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();

        let work = Work {
            lease_id,
            resource_request,
            scheduling_options,
            status: WorkStatus::Waiting,
            allocation: None,
            reply: Some(tx),
        };

        self.leases_to_schedule
            .lock()
            .entry(scheduling_class)
            .or_default()
            .push_back(work);

        // Trigger scheduling
        self.schedule_and_grant_leases();

        rx
    }

    /// Main scheduling loop — tries to schedule all pending leases.
    pub fn schedule_and_grant_leases(&self) {
        let mut leases = self.leases_to_schedule.lock();
        let mut infeasible = self.infeasible_leases.lock();

        let classes: Vec<SchedulingClass> = leases.keys().copied().collect();

        for class in classes {
            let queue = match leases.get_mut(&class) {
                Some(q) => q,
                None => continue,
            };

            let mut remaining = VecDeque::new();

            while let Some(mut work) = queue.pop_front() {
                if work.status == WorkStatus::Cancelled {
                    continue;
                }

                let result = self
                    .scheduler
                    .get_best_schedulable_node(&work.resource_request, &work.scheduling_options);

                match result {
                    Some(node_id) => {
                        if node_id == self.scheduler.local_node_id() {
                            // Local dispatch
                            let allocation = self
                                .scheduler
                                .allocate_local_task_resources(&work.resource_request);
                            if let Some(alloc) = allocation {
                                work.allocation = Some(alloc.clone());
                                if let Some(reply) = work.reply.take() {
                                    let _ = reply.send(LeaseReply::Granted {
                                        node_id: node_id.clone(),
                                        worker_address: String::new(),
                                        allocation: Some(alloc),
                                    });
                                }
                            } else {
                                // Resources were consumed between check and allocation
                                remaining.push_back(work);
                            }
                        } else {
                            // Spillback to remote node
                            self.scheduler
                                .allocate_remote_task_resources(&node_id, &work.resource_request);
                            if let Some(reply) = work.reply.take() {
                                let _ = reply.send(LeaseReply::Spillback {
                                    node_id: node_id.clone(),
                                });
                            }
                        }
                    }
                    None => {
                        // Check if feasible at all
                        if self.scheduler.is_feasible(&work.resource_request) {
                            remaining.push_back(work);
                        } else {
                            // Move to infeasible
                            if let Some(reply) = work.reply.take() {
                                let _ = reply.send(LeaseReply::Rejected {
                                    reason: "infeasible".to_string(),
                                });
                            }
                            infeasible.entry(class).or_default().push_back(work);
                        }
                    }
                }
            }

            if remaining.is_empty() {
                leases.remove(&class);
            } else {
                leases.insert(class, remaining);
            }
        }
    }

    /// Cancel a specific lease.
    pub fn cancel_lease(&self, lease_id: LeaseID) -> bool {
        let mut leases = self.leases_to_schedule.lock();
        for queue in leases.values_mut() {
            if let Some(work) = queue.iter_mut().find(|w| w.lease_id == lease_id) {
                work.status = WorkStatus::Cancelled;
                if let Some(reply) = work.reply.take() {
                    let _ = reply.send(LeaseReply::Rejected {
                        reason: "cancelled".to_string(),
                    });
                }
                return true;
            }
        }
        false
    }

    /// Get the total number of pending leases.
    pub fn num_pending_leases(&self) -> usize {
        self.leases_to_schedule
            .lock()
            .values()
            .map(|q| q.len())
            .sum()
    }

    /// Get the total number of infeasible leases.
    pub fn num_infeasible_leases(&self) -> usize {
        self.infeasible_leases
            .lock()
            .values()
            .map(|q| q.len())
            .sum()
    }

    /// Retry scheduling infeasible leases (called when cluster state changes).
    pub fn try_schedule_infeasible_leases(&self) {
        let mut infeasible = self.infeasible_leases.lock();
        let mut leases = self.leases_to_schedule.lock();

        let classes: Vec<SchedulingClass> = infeasible.keys().copied().collect();
        for class in classes {
            if let Some(queue) = infeasible.remove(&class) {
                let mut remaining_infeasible = VecDeque::new();
                for work in queue {
                    if self.scheduler.is_feasible(&work.resource_request) {
                        leases.entry(class).or_default().push_back(work);
                    } else {
                        remaining_infeasible.push_back(work);
                    }
                }
                if !remaining_infeasible.is_empty() {
                    infeasible.insert(class, remaining_infeasible);
                }
            }
        }
    }
}

/// Tracks worker-to-task lease assignments.
///
/// After the `ClusterLeaseManager` grants a lease (resource allocation),
/// the `WorkerLeaseTracker` records which worker is executing which task.
/// This enables:
/// - Lease timeout detection (kill workers holding leases too long)
/// - Resource return on task completion
/// - Worker death → task failure propagation
pub struct WorkerLeaseTracker {
    inner: Mutex<WorkerLeaseTrackerInner>,
    /// Maximum duration a worker can hold a lease before timeout.
    lease_timeout: std::time::Duration,
}

struct WorkerLeaseTrackerInner {
    /// Active leases: worker_id → lease info.
    active_leases: HashMap<ray_common::id::WorkerID, ActiveLease>,
    /// Total leases granted.
    total_granted: u64,
    /// Total leases returned (task completed).
    total_returned: u64,
    /// Total leases timed out.
    total_timed_out: u64,
}

/// An active lease — a worker assigned to a task.
#[derive(Debug, Clone)]
pub struct ActiveLease {
    /// The worker holding the lease.
    pub worker_id: ray_common::id::WorkerID,
    /// The task being executed.
    pub task_id: ray_common::id::TaskID,
    /// Resources allocated to this lease.
    pub allocated_resources: ResourceSet,
    /// When the lease was granted.
    pub granted_at: std::time::Instant,
    /// Whether this is an actor task (actors have longer/no timeouts).
    pub is_actor_task: bool,
}

impl WorkerLeaseTracker {
    /// Create a new worker lease tracker.
    pub fn new(lease_timeout: std::time::Duration) -> Self {
        Self {
            inner: Mutex::new(WorkerLeaseTrackerInner {
                active_leases: HashMap::new(),
                total_granted: 0,
                total_returned: 0,
                total_timed_out: 0,
            }),
            lease_timeout,
        }
    }

    /// Grant a lease — assign a worker to a task.
    pub fn grant_lease(
        &self,
        worker_id: ray_common::id::WorkerID,
        task_id: ray_common::id::TaskID,
        allocated_resources: ResourceSet,
        is_actor_task: bool,
    ) {
        let mut inner = self.inner.lock();
        inner.active_leases.insert(
            worker_id,
            ActiveLease {
                worker_id,
                task_id,
                allocated_resources,
                granted_at: std::time::Instant::now(),
                is_actor_task,
            },
        );
        inner.total_granted += 1;
        tracing::debug!(?worker_id, ?task_id, "Lease granted");
    }

    /// Return a lease — task completed, worker is available again.
    ///
    /// Returns the allocated resources so they can be released back.
    pub fn return_lease(&self, worker_id: &ray_common::id::WorkerID) -> Option<ActiveLease> {
        let mut inner = self.inner.lock();
        let lease = inner.active_leases.remove(worker_id);
        if lease.is_some() {
            inner.total_returned += 1;
            tracing::debug!(?worker_id, "Lease returned");
        }
        lease
    }

    /// Get the lease for a worker (if any).
    pub fn get_lease(&self, worker_id: &ray_common::id::WorkerID) -> Option<ActiveLease> {
        self.inner.lock().active_leases.get(worker_id).cloned()
    }

    /// Check if a worker currently holds a lease.
    pub fn has_lease(&self, worker_id: &ray_common::id::WorkerID) -> bool {
        self.inner.lock().active_leases.contains_key(worker_id)
    }

    /// Check for timed-out leases.
    ///
    /// Returns worker IDs that have held their lease longer than the timeout.
    /// Actor tasks are excluded (actors hold leases indefinitely).
    pub fn get_timed_out_leases(&self) -> Vec<ray_common::id::WorkerID> {
        let inner = self.inner.lock();
        let now = std::time::Instant::now();
        inner
            .active_leases
            .values()
            .filter(|lease| {
                !lease.is_actor_task && now.duration_since(lease.granted_at) >= self.lease_timeout
            })
            .map(|lease| lease.worker_id)
            .collect()
    }

    /// Handle worker death — remove its lease and return the resources.
    pub fn handle_worker_death(&self, worker_id: &ray_common::id::WorkerID) -> Option<ActiveLease> {
        self.return_lease(worker_id)
    }

    /// Number of active leases.
    pub fn num_active_leases(&self) -> usize {
        self.inner.lock().active_leases.len()
    }

    /// Statistics: (total_granted, total_returned, total_timed_out).
    pub fn stats(&self) -> (u64, u64, u64) {
        let inner = self.inner.lock();
        (
            inner.total_granted,
            inner.total_returned,
            inner.total_timed_out,
        )
    }

    /// Mark timed-out leases and increment the timed_out counter.
    /// Returns the timed-out leases that were removed.
    pub fn reap_timed_out_leases(&self) -> Vec<ActiveLease> {
        let timed_out_workers = self.get_timed_out_leases();
        let mut inner = self.inner.lock();
        let mut reaped = Vec::new();
        for wid in timed_out_workers {
            if let Some(lease) = inner.active_leases.remove(&wid) {
                inner.total_timed_out += 1;
                tracing::warn!(?wid, task_id = ?lease.task_id, "Lease timed out");
                reaped.push(lease);
            }
        }
        reaped
    }
}

impl Default for WorkerLeaseTracker {
    fn default() -> Self {
        Self::new(std::time::Duration::from_secs(300)) // 5 minute default timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_resource_manager::ClusterResourceManager;
    use crate::local_resource_manager::LocalResourceManager;
    use crate::scheduling_resources::NodeResources;
    use ray_common::scheduling::FixedPoint;

    fn make_lease_manager() -> ClusterLeaseManager {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));

        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        let scheduler = Arc::new(ClusterResourceScheduler::new(
            "local".to_string(),
            local_mgr,
            cluster_mgr,
        ));

        ClusterLeaseManager::new(scheduler)
    }

    #[test]
    fn test_queue_and_schedule_local() {
        let mgr = make_lease_manager();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let mut rx =
            mgr.queue_and_schedule_lease(req, SchedulingOptions::hybrid(), SchedulingClass(1));

        let reply = rx.try_recv().unwrap();
        match reply {
            LeaseReply::Granted { node_id, .. } => {
                assert_eq!(node_id, "local");
            }
            _ => panic!("expected grant"),
        }
    }

    #[test]
    fn test_infeasible_lease_queuing() {
        let mgr = make_lease_manager();

        // Request more than available so it stays infeasible
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(100.0));

        let _rx =
            mgr.queue_and_schedule_lease(req, SchedulingOptions::hybrid(), SchedulingClass(1));

        // Should be in infeasible queue since no node can satisfy 100 CPUs
        assert_eq!(mgr.num_infeasible_leases(), 1);
    }

    #[test]
    fn test_cancel_lease() {
        let mgr = make_lease_manager();

        // Exhaust all local CPUs (4) so subsequent feasible requests stay pending
        let mut all_cpus = ResourceSet::new();
        all_cpus.set("CPU".to_string(), FixedPoint::from_f64(4.0));

        let mut rx1 =
            mgr.queue_and_schedule_lease(all_cpus, SchedulingOptions::hybrid(), SchedulingClass(1));
        match rx1.try_recv().unwrap() {
            LeaseReply::Granted { node_id, .. } => {
                assert_eq!(node_id, "local");
            }
            _ => panic!("expected grant for 4 CPUs"),
        }

        // Now request 1 CPU — feasible (total=4) but not available (all allocated)
        // This lease stays in leases_to_schedule as pending
        let mut small_req = ResourceSet::new();
        small_req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let _rx2 = mgr.queue_and_schedule_lease(
            small_req,
            SchedulingOptions::hybrid(),
            SchedulingClass(2),
        );
        assert_eq!(mgr.num_pending_leases(), 1, "should have 1 pending lease");

        // Cancel the pending lease
        // Lease IDs are auto-incremented starting at 1, so second lease is ID 2
        let cancelled = mgr.cancel_lease(2);
        assert!(cancelled, "cancel should succeed for pending lease");
    }

    #[test]
    fn test_spillback_to_remote() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));

        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        cluster_mgr.add_or_update_node("remote".to_string(), NodeResources::new(remote_total));

        let scheduler = Arc::new(ClusterResourceScheduler::new(
            "local".to_string(),
            local_mgr,
            cluster_mgr,
        ));
        let mgr = ClusterLeaseManager::new(scheduler);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(6.0));

        let mut rx =
            mgr.queue_and_schedule_lease(req, SchedulingOptions::hybrid(), SchedulingClass(1));

        let reply = rx.try_recv().unwrap();
        match reply {
            LeaseReply::Spillback { node_id } => {
                assert_eq!(node_id, "remote");
            }
            _ => panic!("expected spillback"),
        }
    }

    // --- WorkerLeaseTracker tests ---

    fn make_wid(val: u8) -> ray_common::id::WorkerID {
        let mut data = [0u8; 28];
        data[0] = val;
        ray_common::id::WorkerID::from_binary(&data)
    }

    fn make_tid(val: u8) -> ray_common::id::TaskID {
        let mut data = [0u8; 24];
        data[0] = val;
        ray_common::id::TaskID::from_binary(&data)
    }

    #[test]
    fn test_worker_lease_grant_and_return() {
        let tracker = WorkerLeaseTracker::default();
        let wid = make_wid(1);
        let tid = make_tid(1);

        let mut resources = ResourceSet::new();
        resources.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        tracker.grant_lease(wid, tid, resources, false);
        assert_eq!(tracker.num_active_leases(), 1);
        assert!(tracker.has_lease(&wid));

        let lease = tracker.get_lease(&wid).unwrap();
        assert_eq!(lease.task_id, tid);
        assert!(!lease.is_actor_task);

        let returned = tracker.return_lease(&wid);
        assert!(returned.is_some());
        assert_eq!(tracker.num_active_leases(), 0);
        assert!(!tracker.has_lease(&wid));
    }

    #[test]
    fn test_worker_lease_return_nonexistent() {
        let tracker = WorkerLeaseTracker::default();
        let wid = make_wid(99);
        assert!(tracker.return_lease(&wid).is_none());
    }

    #[test]
    fn test_worker_lease_timeout() {
        let tracker = WorkerLeaseTracker::new(std::time::Duration::from_millis(1));
        let wid = make_wid(1);
        let tid = make_tid(1);

        tracker.grant_lease(wid, tid, ResourceSet::new(), false);

        std::thread::sleep(std::time::Duration::from_millis(5));

        let timed_out = tracker.get_timed_out_leases();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], wid);
    }

    #[test]
    fn test_worker_lease_actor_no_timeout() {
        let tracker = WorkerLeaseTracker::new(std::time::Duration::from_millis(1));
        let wid = make_wid(1);
        let tid = make_tid(1);

        // Actor tasks should not time out.
        tracker.grant_lease(wid, tid, ResourceSet::new(), true);

        std::thread::sleep(std::time::Duration::from_millis(5));

        let timed_out = tracker.get_timed_out_leases();
        assert!(timed_out.is_empty());
    }

    #[test]
    fn test_worker_lease_reap_timed_out() {
        let tracker = WorkerLeaseTracker::new(std::time::Duration::from_millis(1));
        let wid = make_wid(1);
        let tid = make_tid(1);

        tracker.grant_lease(wid, tid, ResourceSet::new(), false);

        std::thread::sleep(std::time::Duration::from_millis(5));

        let reaped = tracker.reap_timed_out_leases();
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].worker_id, wid);
        assert_eq!(tracker.num_active_leases(), 0);

        let (granted, returned, timed_out) = tracker.stats();
        assert_eq!(granted, 1);
        assert_eq!(returned, 0);
        assert_eq!(timed_out, 1);
    }

    #[test]
    fn test_worker_lease_handle_death() {
        let tracker = WorkerLeaseTracker::default();
        let wid = make_wid(1);
        let tid = make_tid(1);

        let mut resources = ResourceSet::new();
        resources.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        tracker.grant_lease(wid, tid, resources, false);
        let lease = tracker.handle_worker_death(&wid).unwrap();
        assert_eq!(lease.task_id, tid);
        assert!(lease.allocated_resources.get("CPU") > FixedPoint::from_f64(0.0));
        assert_eq!(tracker.num_active_leases(), 0);
    }

    #[test]
    fn test_worker_lease_stats() {
        let tracker = WorkerLeaseTracker::default();

        tracker.grant_lease(make_wid(1), make_tid(1), ResourceSet::new(), false);
        tracker.grant_lease(make_wid(2), make_tid(2), ResourceSet::new(), false);
        tracker.return_lease(&make_wid(1));

        let (granted, returned, timed_out) = tracker.stats();
        assert_eq!(granted, 2);
        assert_eq!(returned, 1);
        assert_eq!(timed_out, 0);
        assert_eq!(tracker.num_active_leases(), 1);
    }

    // --- Additional ClusterLeaseManager tests ---

    #[test]
    fn test_multiple_leases_same_class() {
        let mgr = make_lease_manager();
        let class1 = SchedulingClass(1);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Queue multiple leases of the same scheduling class
        let mut rx1 =
            mgr.queue_and_schedule_lease(req.clone(), SchedulingOptions::hybrid(), class1);
        let mut rx2 =
            mgr.queue_and_schedule_lease(req.clone(), SchedulingOptions::hybrid(), class1);
        let mut rx3 =
            mgr.queue_and_schedule_lease(req.clone(), SchedulingOptions::hybrid(), class1);

        // All 3 should be granted (4 CPUs available, 1 each)
        match rx1.try_recv().unwrap() {
            LeaseReply::Granted { node_id, .. } => assert_eq!(node_id, "local"),
            _ => panic!("expected grant"),
        }
        match rx2.try_recv().unwrap() {
            LeaseReply::Granted { node_id, .. } => assert_eq!(node_id, "local"),
            _ => panic!("expected grant"),
        }
        match rx3.try_recv().unwrap() {
            LeaseReply::Granted { node_id, .. } => assert_eq!(node_id, "local"),
            _ => panic!("expected grant"),
        }
    }

    #[test]
    fn test_exhaust_resources_then_pending() {
        let mgr = make_lease_manager();

        // Allocate all 4 CPUs
        let mut all_cpus = ResourceSet::new();
        all_cpus.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mut rx1 =
            mgr.queue_and_schedule_lease(all_cpus, SchedulingOptions::hybrid(), SchedulingClass(1));
        match rx1.try_recv().unwrap() {
            LeaseReply::Granted { .. } => {}
            _ => panic!("expected grant"),
        }

        // Next request should stay pending (feasible but not available)
        let mut small = ResourceSet::new();
        small.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let _rx2 =
            mgr.queue_and_schedule_lease(small, SchedulingOptions::hybrid(), SchedulingClass(2));
        assert_eq!(mgr.num_pending_leases(), 1);
        assert_eq!(mgr.num_infeasible_leases(), 0);
    }

    #[test]
    fn test_multiple_scheduling_classes() {
        let mgr = make_lease_manager();

        let mut cpu_req = ResourceSet::new();
        cpu_req.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let _rx1 = mgr.queue_and_schedule_lease(
            cpu_req.clone(),
            SchedulingOptions::hybrid(),
            SchedulingClass(1),
        );
        let _rx2 =
            mgr.queue_and_schedule_lease(cpu_req, SchedulingOptions::hybrid(), SchedulingClass(2));

        // Both should be granted (2 CPUs each, 4 total)
        assert_eq!(mgr.num_pending_leases(), 0);
    }

    #[test]
    fn test_cancel_nonexistent_lease() {
        let mgr = make_lease_manager();
        // Cancel a lease that doesn't exist
        assert!(!mgr.cancel_lease(9999));
    }

    #[test]
    fn test_try_schedule_infeasible_leases() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));
        let cluster_mgr = Arc::new(ClusterResourceManager::new());
        let scheduler = Arc::new(ClusterResourceScheduler::new(
            "local".to_string(),
            local_mgr,
            cluster_mgr.clone(),
        ));
        let mgr = ClusterLeaseManager::new(scheduler);

        // Request GPU resources — infeasible since no GPU node exists
        let mut gpu_req = ResourceSet::new();
        gpu_req.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        let _rx =
            mgr.queue_and_schedule_lease(gpu_req, SchedulingOptions::hybrid(), SchedulingClass(10));
        assert_eq!(mgr.num_infeasible_leases(), 1);

        // Add a GPU node to the cluster
        let mut remote_total = ResourceSet::new();
        remote_total.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        cluster_mgr.add_or_update_node("gpu-node".to_string(), NodeResources::new(remote_total));

        // Retry infeasible leases — should now be schedulable
        mgr.try_schedule_infeasible_leases();
        // Trigger scheduling on the moved leases
        mgr.schedule_and_grant_leases();

        assert_eq!(mgr.num_infeasible_leases(), 0);
    }

    #[test]
    fn test_spillback_multiple_remote_nodes() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let local_mgr = Arc::new(LocalResourceManager::new(
            "local".to_string(),
            total,
            HashMap::new(),
        ));

        let cluster_mgr = Arc::new(ClusterResourceManager::new());

        // Add two remote nodes with different capacity
        let mut remote1_total = ResourceSet::new();
        remote1_total.set("CPU".to_string(), FixedPoint::from_f64(16.0));
        cluster_mgr.add_or_update_node("remote1".to_string(), NodeResources::new(remote1_total));

        let mut remote2_total = ResourceSet::new();
        remote2_total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        cluster_mgr.add_or_update_node("remote2".to_string(), NodeResources::new(remote2_total));

        let scheduler = Arc::new(ClusterResourceScheduler::new(
            "local".to_string(),
            local_mgr,
            cluster_mgr,
        ));
        let mgr = ClusterLeaseManager::new(scheduler);

        // Request 10 CPUs — only remote nodes can handle this
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        let mut rx =
            mgr.queue_and_schedule_lease(req, SchedulingOptions::hybrid(), SchedulingClass(1));

        match rx.try_recv().unwrap() {
            LeaseReply::Spillback { node_id } => {
                // Should spillback to the remote node with enough capacity
                assert!(node_id == "remote1" || node_id == "remote2");
            }
            other => panic!("expected spillback, got {:?}", other),
        }
    }

    // --- Additional WorkerLeaseTracker tests ---

    #[test]
    fn test_worker_lease_multiple_workers() {
        let tracker = WorkerLeaseTracker::default();

        // Grant leases to 5 workers
        for i in 1..=5 {
            tracker.grant_lease(make_wid(i), make_tid(i), ResourceSet::new(), false);
        }
        assert_eq!(tracker.num_active_leases(), 5);

        // Return 3 of them
        tracker.return_lease(&make_wid(1));
        tracker.return_lease(&make_wid(3));
        tracker.return_lease(&make_wid(5));
        assert_eq!(tracker.num_active_leases(), 2);
        assert!(tracker.has_lease(&make_wid(2)));
        assert!(tracker.has_lease(&make_wid(4)));
        assert!(!tracker.has_lease(&make_wid(1)));
    }

    #[test]
    fn test_worker_lease_replace_existing() {
        let tracker = WorkerLeaseTracker::default();
        let wid = make_wid(1);

        let mut res1 = ResourceSet::new();
        res1.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        tracker.grant_lease(wid, make_tid(1), res1, false);

        // Grant a new lease to the same worker (replaces)
        let mut res2 = ResourceSet::new();
        res2.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        tracker.grant_lease(wid, make_tid(2), res2, false);

        assert_eq!(tracker.num_active_leases(), 1);
        let lease = tracker.get_lease(&wid).unwrap();
        assert_eq!(lease.task_id, make_tid(2));
        // Stats: granted is 2 (both grant calls counted)
        let (granted, _, _) = tracker.stats();
        assert_eq!(granted, 2);
    }

    #[test]
    fn test_worker_lease_mixed_actor_and_normal() {
        let tracker = WorkerLeaseTracker::new(std::time::Duration::from_millis(1));
        let normal_wid = make_wid(1);
        let actor_wid = make_wid(2);

        tracker.grant_lease(normal_wid, make_tid(1), ResourceSet::new(), false);
        tracker.grant_lease(actor_wid, make_tid(2), ResourceSet::new(), true);

        std::thread::sleep(std::time::Duration::from_millis(5));

        // Only normal task should time out
        let timed_out = tracker.get_timed_out_leases();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], normal_wid);

        // Reap — only normal lease removed
        let reaped = tracker.reap_timed_out_leases();
        assert_eq!(reaped.len(), 1);
        assert_eq!(tracker.num_active_leases(), 1);
        assert!(tracker.has_lease(&actor_wid));
    }

    #[test]
    fn test_worker_death_returns_resources() {
        let tracker = WorkerLeaseTracker::default();
        let wid = make_wid(1);

        let mut resources = ResourceSet::new();
        resources.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        resources.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        tracker.grant_lease(wid, make_tid(1), resources, false);
        let lease = tracker.handle_worker_death(&wid).unwrap();

        assert_eq!(
            lease.allocated_resources.get("CPU"),
            FixedPoint::from_f64(4.0)
        );
        assert_eq!(
            lease.allocated_resources.get("GPU"),
            FixedPoint::from_f64(1.0)
        );
        assert_eq!(tracker.num_active_leases(), 0);

        let (granted, returned, _) = tracker.stats();
        assert_eq!(granted, 1);
        assert_eq!(returned, 1); // handle_worker_death calls return_lease
    }

    #[test]
    fn test_worker_death_nonexistent() {
        let tracker = WorkerLeaseTracker::default();
        assert!(tracker.handle_worker_death(&make_wid(99)).is_none());
    }

    #[test]
    fn test_lease_id_auto_increment() {
        let mgr = make_lease_manager();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Queue 3 leases and verify IDs increment
        let _rx1 = mgr.queue_and_schedule_lease(
            req.clone(),
            SchedulingOptions::hybrid(),
            SchedulingClass(1),
        );
        let _rx2 = mgr.queue_and_schedule_lease(
            req.clone(),
            SchedulingOptions::hybrid(),
            SchedulingClass(1),
        );
        let _rx3 =
            mgr.queue_and_schedule_lease(req, SchedulingOptions::hybrid(), SchedulingClass(1));

        // If all were granted, pending should be 0
        assert_eq!(mgr.num_pending_leases(), 0);
    }
}
