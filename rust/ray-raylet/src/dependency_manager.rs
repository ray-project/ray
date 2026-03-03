// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Dependency manager — tracks task→object dependencies for scheduling.
//!
//! Ports C++ `raylet/dependency_manager.cc`.
//!
//! Responsibilities:
//! - Track which tasks depend on which objects
//! - Block tasks until all their dependencies are locally available
//! - Unblock tasks when objects arrive
//! - Trigger recovery when objects are lost

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;
use ray_common::id::{ObjectID, TaskID};

/// Status of a task's dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyStatus {
    /// All dependencies are satisfied — task is ready to run.
    Ready,
    /// Waiting for some objects to become available.
    Waiting {
        /// Object IDs that are not yet available.
        pending: Vec<ObjectID>,
    },
    /// One or more dependencies are permanently lost.
    Failed {
        /// Object IDs that are unrecoverable.
        lost_objects: Vec<ObjectID>,
    },
}

/// Callback invoked when a task becomes ready (all deps satisfied).
pub type TaskReadyCallback = Box<dyn Fn(&TaskID) + Send + Sync>;

/// Callback invoked when a task's dependency is permanently lost.
pub type TaskFailedCallback = Box<dyn Fn(&TaskID, &[ObjectID]) + Send + Sync>;

/// A task waiting for dependencies.
#[derive(Debug, Clone)]
struct WaitingTask {
    /// Object IDs this task depends on.
    dependencies: HashSet<ObjectID>,
    /// Dependencies that are not yet available.
    pending: HashSet<ObjectID>,
}

/// Manages task→object dependencies for the Raylet scheduler.
pub struct DependencyManager {
    inner: Mutex<DependencyManagerInner>,
}

struct DependencyManagerInner {
    /// Tasks waiting for dependencies.
    waiting_tasks: HashMap<TaskID, WaitingTask>,
    /// Reverse index: object_id → set of tasks waiting for it.
    object_waiters: HashMap<ObjectID, HashSet<TaskID>>,
    /// Objects known to be locally available.
    local_objects: HashSet<ObjectID>,
    /// Objects marked as permanently lost.
    lost_objects: HashSet<ObjectID>,
    /// Callback when a task becomes ready.
    ready_callback: Option<TaskReadyCallback>,
    /// Callback when a task's dependency is lost.
    failed_callback: Option<TaskFailedCallback>,
    /// Statistics.
    total_tasks_unblocked: u64,
    total_tasks_failed: u64,
}

impl DependencyManager {
    /// Create a new dependency manager.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(DependencyManagerInner {
                waiting_tasks: HashMap::new(),
                object_waiters: HashMap::new(),
                local_objects: HashSet::new(),
                lost_objects: HashSet::new(),
                ready_callback: None,
                failed_callback: None,
                total_tasks_unblocked: 0,
                total_tasks_failed: 0,
            }),
        }
    }

    /// Set callback for when a task becomes ready.
    pub fn set_ready_callback(&self, callback: TaskReadyCallback) {
        self.inner.lock().ready_callback = Some(callback);
    }

    /// Set callback for when a task's dependency is lost.
    pub fn set_failed_callback(&self, callback: TaskFailedCallback) {
        self.inner.lock().failed_callback = Some(callback);
    }

    /// Register a task's dependencies. If all deps are already available,
    /// returns `DependencyStatus::Ready` immediately.
    pub fn wait_for_dependencies(
        &self,
        task_id: TaskID,
        dependencies: Vec<ObjectID>,
    ) -> DependencyStatus {
        let mut inner = self.inner.lock();

        // Check which dependencies are not yet local.
        let pending: HashSet<ObjectID> = dependencies
            .iter()
            .filter(|oid| !inner.local_objects.contains(oid))
            .copied()
            .collect();

        // Check if any dependencies are permanently lost.
        let lost: Vec<ObjectID> = dependencies
            .iter()
            .filter(|oid| inner.lost_objects.contains(oid))
            .copied()
            .collect();

        if !lost.is_empty() {
            inner.total_tasks_failed += 1;
            return DependencyStatus::Failed {
                lost_objects: lost,
            };
        }

        if pending.is_empty() {
            // All deps already available.
            inner.total_tasks_unblocked += 1;
            return DependencyStatus::Ready;
        }

        // Register the waiting task.
        let deps_set: HashSet<ObjectID> = dependencies.into_iter().collect();
        let pending_vec: Vec<ObjectID> = pending.iter().copied().collect();

        // Add reverse index entries.
        for oid in &pending {
            inner
                .object_waiters
                .entry(*oid)
                .or_default()
                .insert(task_id);
        }

        inner.waiting_tasks.insert(
            task_id,
            WaitingTask {
                dependencies: deps_set,
                pending,
            },
        );

        DependencyStatus::Waiting {
            pending: pending_vec,
        }
    }

    /// Cancel waiting for a task's dependencies (task was cancelled).
    pub fn cancel_wait(&self, task_id: &TaskID) -> bool {
        let mut inner = self.inner.lock();
        if let Some(task) = inner.waiting_tasks.remove(task_id) {
            // Remove from reverse index.
            for oid in &task.pending {
                if let Some(waiters) = inner.object_waiters.get_mut(oid) {
                    waiters.remove(task_id);
                    if waiters.is_empty() {
                        inner.object_waiters.remove(oid);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// Notify that an object is now locally available.
    ///
    /// Returns the list of task IDs that became ready.
    pub fn object_available(&self, object_id: ObjectID) -> Vec<TaskID> {
        let mut inner = self.inner.lock();
        inner.local_objects.insert(object_id);

        let mut newly_ready = Vec::new();

        if let Some(waiters) = inner.object_waiters.remove(&object_id) {
            for task_id in waiters {
                if let Some(task) = inner.waiting_tasks.get_mut(&task_id) {
                    task.pending.remove(&object_id);
                    if task.pending.is_empty() {
                        newly_ready.push(task_id);
                    }
                }
            }
        }

        // Remove ready tasks from waiting.
        for tid in &newly_ready {
            inner.waiting_tasks.remove(tid);
            inner.total_tasks_unblocked += 1;
        }

        // Fire ready callback.
        if !newly_ready.is_empty() {
            if let Some(ref cb) = inner.ready_callback {
                for tid in &newly_ready {
                    cb(tid);
                }
            }
        }

        newly_ready
    }

    /// Notify that an object has been lost.
    ///
    /// Returns the list of task IDs that are now failed.
    pub fn object_lost(&self, object_id: ObjectID) -> Vec<TaskID> {
        let mut inner = self.inner.lock();
        inner.local_objects.remove(&object_id);
        inner.lost_objects.insert(object_id);

        let mut failed_tasks = Vec::new();

        if let Some(waiters) = inner.object_waiters.remove(&object_id) {
            for task_id in waiters {
                failed_tasks.push(task_id);
            }
        }

        // Remove failed tasks and fire callback.
        for tid in &failed_tasks {
            if let Some(task) = inner.waiting_tasks.remove(tid) {
                inner.total_tasks_failed += 1;
                let lost_objs: Vec<ObjectID> = task
                    .dependencies
                    .iter()
                    .filter(|oid| inner.lost_objects.contains(oid))
                    .copied()
                    .collect();

                if let Some(ref cb) = inner.failed_callback {
                    cb(tid, &lost_objs);
                }

                // Clean up remaining reverse index entries.
                for oid in &task.pending {
                    if let Some(waiters) = inner.object_waiters.get_mut(oid) {
                        waiters.remove(tid);
                        if waiters.is_empty() {
                            inner.object_waiters.remove(oid);
                        }
                    }
                }
            }
        }

        failed_tasks
    }

    /// Mark an object as locally available (e.g., after Put).
    pub fn mark_object_local(&self, object_id: ObjectID) {
        self.inner.lock().local_objects.insert(object_id);
    }

    /// Check if an object is locally available.
    pub fn is_object_local(&self, object_id: &ObjectID) -> bool {
        self.inner.lock().local_objects.contains(object_id)
    }

    /// Number of tasks currently waiting for dependencies.
    pub fn num_waiting_tasks(&self) -> usize {
        self.inner.lock().waiting_tasks.len()
    }

    /// Number of objects being waited on.
    pub fn num_waited_objects(&self) -> usize {
        self.inner.lock().object_waiters.len()
    }

    /// Get the dependency status for a specific task.
    pub fn get_task_status(&self, task_id: &TaskID) -> Option<DependencyStatus> {
        let inner = self.inner.lock();
        inner.waiting_tasks.get(task_id).map(|task| {
            if task.pending.is_empty() {
                DependencyStatus::Ready
            } else {
                DependencyStatus::Waiting {
                    pending: task.pending.iter().copied().collect(),
                }
            }
        })
    }

    /// Statistics: (total_tasks_unblocked, total_tasks_failed).
    pub fn stats(&self) -> (u64, u64) {
        let inner = self.inner.lock();
        (inner.total_tasks_unblocked, inner.total_tasks_failed)
    }
}

impl Default for DependencyManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    fn make_tid(val: u8) -> TaskID {
        let mut data = [0u8; 24];
        data[0] = val;
        TaskID::from_binary(&data)
    }

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    #[test]
    fn test_all_deps_already_available() {
        let mgr = DependencyManager::new();
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        mgr.mark_object_local(oid1);
        mgr.mark_object_local(oid2);

        let status = mgr.wait_for_dependencies(make_tid(1), vec![oid1, oid2]);
        assert_eq!(status, DependencyStatus::Ready);
        assert_eq!(mgr.num_waiting_tasks(), 0);
    }

    #[test]
    fn test_wait_then_satisfy() {
        let mgr = DependencyManager::new();
        let tid = make_tid(1);
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        let status = mgr.wait_for_dependencies(tid, vec![oid1, oid2]);
        assert!(matches!(status, DependencyStatus::Waiting { .. }));
        assert_eq!(mgr.num_waiting_tasks(), 1);

        // Satisfy first dep — not ready yet.
        let ready = mgr.object_available(oid1);
        assert!(ready.is_empty());
        assert_eq!(mgr.num_waiting_tasks(), 1);

        // Satisfy second dep — now ready.
        let ready = mgr.object_available(oid2);
        assert_eq!(ready, vec![tid]);
        assert_eq!(mgr.num_waiting_tasks(), 0);
    }

    #[test]
    fn test_multiple_tasks_same_dep() {
        let mgr = DependencyManager::new();
        let oid = make_oid(1);
        let tid1 = make_tid(1);
        let tid2 = make_tid(2);

        mgr.wait_for_dependencies(tid1, vec![oid]);
        mgr.wait_for_dependencies(tid2, vec![oid]);
        assert_eq!(mgr.num_waiting_tasks(), 2);

        let mut ready = mgr.object_available(oid);
        ready.sort_by_key(|t| t.binary());
        assert_eq!(ready.len(), 2);
        assert_eq!(mgr.num_waiting_tasks(), 0);
    }

    #[test]
    fn test_cancel_wait() {
        let mgr = DependencyManager::new();
        let tid = make_tid(1);
        let oid = make_oid(1);

        mgr.wait_for_dependencies(tid, vec![oid]);
        assert_eq!(mgr.num_waiting_tasks(), 1);

        assert!(mgr.cancel_wait(&tid));
        assert_eq!(mgr.num_waiting_tasks(), 0);
        assert_eq!(mgr.num_waited_objects(), 0);
    }

    #[test]
    fn test_cancel_nonexistent_task() {
        let mgr = DependencyManager::new();
        assert!(!mgr.cancel_wait(&make_tid(99)));
    }

    #[test]
    fn test_object_lost_fails_tasks() {
        let mgr = DependencyManager::new();
        let tid = make_tid(1);
        let oid = make_oid(1);

        mgr.wait_for_dependencies(tid, vec![oid]);
        let failed = mgr.object_lost(oid);
        assert_eq!(failed, vec![tid]);
        assert_eq!(mgr.num_waiting_tasks(), 0);
    }

    #[test]
    fn test_lost_dep_in_new_registration() {
        let mgr = DependencyManager::new();
        let oid = make_oid(1);

        // Mark lost first.
        mgr.object_lost(oid);

        // New task depending on lost object fails immediately.
        let status = mgr.wait_for_dependencies(make_tid(1), vec![oid]);
        assert!(matches!(status, DependencyStatus::Failed { .. }));
    }

    #[test]
    fn test_ready_callback() {
        let mgr = DependencyManager::new();
        let ready_count = Arc::new(AtomicU32::new(0));
        let rc = ready_count.clone();
        mgr.set_ready_callback(Box::new(move |_tid| {
            rc.fetch_add(1, Ordering::Relaxed);
        }));

        let tid = make_tid(1);
        let oid = make_oid(1);
        mgr.wait_for_dependencies(tid, vec![oid]);
        mgr.object_available(oid);

        assert_eq!(ready_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_failed_callback() {
        let mgr = DependencyManager::new();
        let fail_count = Arc::new(AtomicU32::new(0));
        let fc = fail_count.clone();
        mgr.set_failed_callback(Box::new(move |_tid, _objs| {
            fc.fetch_add(1, Ordering::Relaxed);
        }));

        let tid = make_tid(1);
        let oid = make_oid(1);
        mgr.wait_for_dependencies(tid, vec![oid]);
        mgr.object_lost(oid);

        assert_eq!(fail_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_is_object_local() {
        let mgr = DependencyManager::new();
        let oid = make_oid(1);

        assert!(!mgr.is_object_local(&oid));
        mgr.mark_object_local(oid);
        assert!(mgr.is_object_local(&oid));
    }

    #[test]
    fn test_get_task_status() {
        let mgr = DependencyManager::new();
        let tid = make_tid(1);
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        assert!(mgr.get_task_status(&tid).is_none());

        mgr.wait_for_dependencies(tid, vec![oid1, oid2]);
        let status = mgr.get_task_status(&tid).unwrap();
        assert!(matches!(status, DependencyStatus::Waiting { .. }));

        mgr.object_available(oid1);
        mgr.object_available(oid2);
        // Task removed after becoming ready.
        assert!(mgr.get_task_status(&tid).is_none());
    }

    #[test]
    fn test_stats() {
        let mgr = DependencyManager::new();
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        // Task 1: wait then become ready.
        mgr.wait_for_dependencies(make_tid(1), vec![oid1]);
        mgr.object_available(oid1);

        // Task 2: wait then lose dependency.
        mgr.wait_for_dependencies(make_tid(2), vec![oid2]);
        mgr.object_lost(oid2);

        let (unblocked, failed) = mgr.stats();
        assert_eq!(unblocked, 1);
        assert_eq!(failed, 1);
    }

    #[test]
    fn test_empty_dependencies() {
        let mgr = DependencyManager::new();
        let status = mgr.wait_for_dependencies(make_tid(1), vec![]);
        assert_eq!(status, DependencyStatus::Ready);
    }
}
