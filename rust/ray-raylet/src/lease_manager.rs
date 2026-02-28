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

        let _rx2 =
            mgr.queue_and_schedule_lease(small_req, SchedulingOptions::hybrid(), SchedulingClass(2));
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
}
