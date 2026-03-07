// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Task lifecycle management for the core worker.
//!
//! Ports `src/ray/core_worker/task_manager.cc`.
//! Tracks task state transitions, retry logic, and return object ownership.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use ray_common::id::{ObjectID, TaskID};
use ray_proto::ray::rpc::{Address, TaskSpec};

use crate::error::{CoreWorkerError, CoreWorkerResult};
use crate::memory_store::{CoreWorkerMemoryStore, RayObject};
use crate::reference_counter::ReferenceCounter;
use crate::task_event_buffer::TaskEventBuffer;

/// Task status matching the C++ `rpc::TaskStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskState {
    /// Waiting for argument dependencies to be resolved.
    PendingArgsAvail,
    /// Arguments ready, waiting for node assignment.
    PendingNodeAssignment,
    /// Submitted to a worker for execution.
    SubmittedToWorker,
    /// Task completed successfully.
    Finished,
    /// Task failed (may trigger retry).
    Failed,
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::PendingArgsAvail => write!(f, "PENDING_ARGS_AVAIL"),
            TaskState::PendingNodeAssignment => write!(f, "PENDING_NODE_ASSIGNMENT"),
            TaskState::SubmittedToWorker => write!(f, "SUBMITTED_TO_WORKER"),
            TaskState::Finished => write!(f, "FINISHED"),
            TaskState::Failed => write!(f, "FAILED"),
        }
    }
}

/// Callback for retrying a task after a delay.
pub type RetryTaskCallback = Box<dyn Fn(&TaskSpec, u32) -> CoreWorkerResult<()> + Send + Sync>;

/// A single tracked task entry.
struct TaskEntry {
    /// The full task specification.
    spec: TaskSpec,
    /// Current task state.
    status: TaskState,
    /// Number of general retries remaining. -1 = infinite.
    num_retries_left: i32,
    /// Number of OOM-specific retries remaining. -1 = infinite.
    num_oom_retries_left: i32,
    /// Whether the task has been cancelled.
    is_canceled: bool,
    /// The attempt number (incremented on each retry).
    attempt_number: i32,
    /// Number of successful executions.
    num_successful_executions: i32,
    /// Return object IDs for this task.
    return_ids: Vec<ObjectID>,
    /// The node where the task is being executed.
    execution_node_id: Vec<u8>,
    /// The worker executing this task.
    execution_worker_id: Vec<u8>,
}

impl TaskEntry {
    fn new(spec: TaskSpec, max_retries: i32) -> Self {
        let return_ids = Self::extract_return_ids(&spec);
        Self {
            spec,
            status: TaskState::PendingArgsAvail,
            num_retries_left: max_retries,
            num_oom_retries_left: 0,
            is_canceled: false,
            attempt_number: 0,
            num_successful_executions: 0,
            return_ids,
            execution_node_id: Vec::new(),
            execution_worker_id: Vec::new(),
        }
    }

    fn extract_return_ids(spec: &TaskSpec) -> Vec<ObjectID> {
        let task_id = TaskID::from_binary(&spec.task_id);
        let num_returns = spec.num_returns as usize;
        (0..num_returns)
            .map(|i| ObjectID::from_index(&task_id, (i + 1) as u32))
            .collect()
    }

    fn is_pending(&self) -> bool {
        !matches!(self.status, TaskState::Finished | TaskState::Failed)
    }

    /// Set the task state, returning the previous state for counter updates.
    fn set_status(&mut self, new_state: TaskState) -> TaskState {
        let old = self.status;
        self.status = new_state;
        old
    }
}

/// Manages the lifecycle of submitted tasks.
///
/// Tracks task state transitions (PENDING → RUNNING → FINISHED/FAILED),
/// retry logic, and return object ownership via the ReferenceCounter.
pub struct TaskManager {
    inner: Mutex<TaskManagerInner>,
    memory_store: Arc<CoreWorkerMemoryStore>,
    reference_counter: Arc<ReferenceCounter>,
    task_event_buffer: Arc<TaskEventBuffer>,
}

struct TaskManagerInner {
    /// All tasks that may be (re)submitted, keyed by TaskID bytes.
    submissible_tasks: HashMap<Vec<u8>, TaskEntry>,
    /// Count of currently pending tasks.
    num_pending_tasks: usize,
    /// Optional retry callback.
    retry_task_callback: Option<RetryTaskCallback>,
    /// Shutdown hook called when pending tasks reach zero.
    shutdown_hook: Option<Box<dyn FnOnce() + Send>>,
    /// Task state counters for metrics.
    state_counts: HashMap<TaskState, usize>,
}

impl TaskManager {
    /// Create a new TaskManager.
    pub fn new(
        memory_store: Arc<CoreWorkerMemoryStore>,
        reference_counter: Arc<ReferenceCounter>,
        task_event_buffer: Arc<TaskEventBuffer>,
    ) -> Self {
        let mut state_counts = HashMap::new();
        for state in [
            TaskState::PendingArgsAvail,
            TaskState::PendingNodeAssignment,
            TaskState::SubmittedToWorker,
            TaskState::Finished,
            TaskState::Failed,
        ] {
            state_counts.insert(state, 0);
        }

        Self {
            inner: Mutex::new(TaskManagerInner {
                submissible_tasks: HashMap::new(),
                num_pending_tasks: 0,
                retry_task_callback: None,
                shutdown_hook: None,
                state_counts,
            }),
            memory_store,
            reference_counter,
            task_event_buffer,
        }
    }

    /// Set the retry callback used to resubmit failed tasks.
    pub fn set_retry_task_callback(&self, callback: RetryTaskCallback) {
        self.inner.lock().retry_task_callback = Some(callback);
    }

    /// Register a new pending task.
    ///
    /// Creates a TaskEntry, registers return objects as owned with the
    /// ReferenceCounter, and records the initial task event.
    pub fn add_pending_task(
        &self,
        caller_address: Address,
        spec: TaskSpec,
        max_retries: i32,
    ) -> Vec<ObjectID> {
        let entry = TaskEntry::new(spec.clone(), max_retries);
        let return_ids = entry.return_ids.clone();
        let task_id = spec.task_id.clone();

        // Register return objects as owned in the reference counter.
        for oid in &return_ids {
            self.reference_counter
                .add_owned_object(*oid, caller_address.clone(), vec![]);
        }

        // Track argument references.
        let arg_ids: Vec<_> = spec
            .args
            .iter()
            .filter_map(|arg| {
                arg.object_ref
                    .as_ref()
                    .map(|r| ObjectID::from_binary(&r.object_id))
            })
            .collect();
        self.reference_counter
            .update_submitted_task_references(&arg_ids);

        // Record task event.
        self.task_event_buffer
            .record_task_status_event(&task_id, TaskState::PendingArgsAvail, 0);

        let mut inner = self.inner.lock();
        *inner
            .state_counts
            .entry(TaskState::PendingArgsAvail)
            .or_insert(0) += 1;
        inner.num_pending_tasks += 1;
        inner.submissible_tasks.insert(task_id, entry);

        return_ids
    }

    /// Mark that all dependencies for a task have been resolved.
    ///
    /// Transitions: PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT
    pub fn mark_dependencies_resolved(&self, task_id: &[u8]) {
        let mut inner = self.inner.lock();
        let transition = {
            if let Some(entry) = inner.submissible_tasks.get_mut(task_id) {
                if entry.status == TaskState::PendingArgsAvail {
                    let old = entry.set_status(TaskState::PendingNodeAssignment);
                    Some((old, entry.attempt_number))
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some((old, attempt)) = transition {
            Self::update_state_counts(
                &mut inner.state_counts,
                old,
                TaskState::PendingNodeAssignment,
            );
            self.task_event_buffer.record_task_status_event(
                task_id,
                TaskState::PendingNodeAssignment,
                attempt,
            );
        }
    }

    /// Mark that a task has been submitted to a worker for execution.
    ///
    /// Transitions: PENDING_NODE_ASSIGNMENT → SUBMITTED_TO_WORKER
    pub fn mark_task_waiting_for_execution(
        &self,
        task_id: &[u8],
        node_id: Vec<u8>,
        worker_id: Vec<u8>,
    ) {
        let mut inner = self.inner.lock();
        let transition = {
            if let Some(entry) = inner.submissible_tasks.get_mut(task_id) {
                if entry.status == TaskState::PendingNodeAssignment {
                    entry.execution_node_id = node_id;
                    entry.execution_worker_id = worker_id;
                    let old = entry.set_status(TaskState::SubmittedToWorker);
                    Some((old, entry.attempt_number))
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some((old, attempt)) = transition {
            Self::update_state_counts(&mut inner.state_counts, old, TaskState::SubmittedToWorker);
            self.task_event_buffer.record_task_status_event(
                task_id,
                TaskState::SubmittedToWorker,
                attempt,
            );
        }
    }

    /// Complete a pending task successfully.
    ///
    /// Stores return objects in the memory store and updates reference counts.
    pub fn complete_pending_task(
        &self,
        task_id: &[u8],
        return_objects: Vec<(ObjectID, RayObject)>,
    ) -> CoreWorkerResult<()> {
        // Store return objects.
        for (oid, obj) in &return_objects {
            // Ignore duplicate errors — the object may already exist from a previous attempt.
            let _ = self.memory_store.put(*oid, obj.clone());
        }

        let arg_ids;
        {
            let mut inner = self.inner.lock();
            let (old_state, attempt, args) = {
                let entry = inner.submissible_tasks.get_mut(task_id).ok_or_else(|| {
                    CoreWorkerError::Internal(format!(
                        "complete_pending_task: task {} not found",
                        hex::encode(task_id)
                    ))
                })?;

                entry.num_successful_executions += 1;
                let old = entry.set_status(TaskState::Finished);
                let attempt = entry.attempt_number;
                let args: Vec<_> = entry
                    .spec
                    .args
                    .iter()
                    .filter_map(|arg| {
                        arg.object_ref
                            .as_ref()
                            .map(|r| ObjectID::from_binary(&r.object_id))
                    })
                    .collect();
                (old, attempt, args)
            };

            Self::update_state_counts(&mut inner.state_counts, old_state, TaskState::Finished);
            self.task_event_buffer
                .record_task_status_event(task_id, TaskState::Finished, attempt);
            arg_ids = args;

            inner.num_pending_tasks = inner.num_pending_tasks.saturating_sub(1);
        }

        // Release submitted task references (outside lock).
        self.reference_counter
            .update_finished_task_references(&arg_ids);

        self.maybe_fire_shutdown_hook();
        Ok(())
    }

    /// Permanently fail a pending task.
    ///
    /// Marks return objects as failed in the memory store and removes the task.
    pub fn fail_pending_task(&self, task_id: &[u8], error_message: &str) -> CoreWorkerResult<()> {
        let (return_ids, arg_ids);
        {
            let mut inner = self.inner.lock();
            let (old_state, attempt, rets, args) = {
                let entry = inner.submissible_tasks.get_mut(task_id).ok_or_else(|| {
                    CoreWorkerError::Internal(format!(
                        "fail_pending_task: task {} not found",
                        hex::encode(task_id)
                    ))
                })?;

                let rets = entry.return_ids.clone();
                let args: Vec<_> = entry
                    .spec
                    .args
                    .iter()
                    .filter_map(|arg| {
                        arg.object_ref
                            .as_ref()
                            .map(|r| ObjectID::from_binary(&r.object_id))
                    })
                    .collect();

                let old = entry.set_status(TaskState::Failed);
                let attempt = entry.attempt_number;
                (old, attempt, rets, args)
            };

            Self::update_state_counts(&mut inner.state_counts, old_state, TaskState::Failed);
            self.task_event_buffer
                .record_task_status_event(task_id, TaskState::Failed, attempt);
            return_ids = rets;
            arg_ids = args;

            inner.num_pending_tasks = inner.num_pending_tasks.saturating_sub(1);
        }

        // Store error objects for return IDs so that callers waiting on `get`
        // receive an error instead of hanging forever.
        let error_data = Bytes::from(error_message.to_string());
        let error_metadata = Bytes::from_static(b"ERROR");
        for oid in &return_ids {
            let obj = RayObject::new(error_data.clone(), error_metadata.clone(), Vec::new());
            let _ = self.memory_store.put(*oid, obj);
        }

        // Release submitted task references (outside lock).
        self.reference_counter
            .update_finished_task_references(&arg_ids);

        self.maybe_fire_shutdown_hook();
        Ok(())
    }

    /// Attempt to retry a task, or fail it permanently if no retries remain.
    ///
    /// Returns `true` if the task will be retried.
    pub fn fail_or_retry_pending_task(
        &self,
        task_id: &[u8],
        error_message: &str,
        is_oom: bool,
    ) -> CoreWorkerResult<bool> {
        let retry_info;
        {
            let mut inner = self.inner.lock();

            // First pass: check retry eligibility and extract needed data.
            let retry_data = {
                let entry = match inner.submissible_tasks.get_mut(task_id) {
                    Some(e) => e,
                    None => return Ok(false),
                };

                if entry.is_canceled {
                    drop(inner);
                    self.fail_pending_task(task_id, "task was cancelled")?;
                    return Ok(false);
                }

                let retries_left = if is_oom {
                    entry.num_oom_retries_left
                } else {
                    entry.num_retries_left
                };

                if retries_left == 0 {
                    drop(inner);
                    self.fail_pending_task(task_id, error_message)?;
                    return Ok(false);
                }

                // Decrement retry counter.
                if is_oom {
                    if entry.num_oom_retries_left > 0 {
                        entry.num_oom_retries_left -= 1;
                    }
                } else if entry.num_retries_left > 0 {
                    entry.num_retries_left -= 1;
                }

                // Record FAILED for this attempt, then reset for retry.
                let old = entry.set_status(TaskState::Failed);
                let fail_attempt = entry.attempt_number;

                entry.attempt_number += 1;
                let old2 = entry.set_status(TaskState::PendingArgsAvail);
                let retry_attempt = entry.attempt_number;

                let delay_ms = if is_oom {
                    1000u32.saturating_mul(2u32.saturating_pow(retry_attempt as u32 - 1))
                } else {
                    0
                };

                let spec = entry.spec.clone();
                (old, fail_attempt, old2, retry_attempt, spec, delay_ms)
            };

            // Second pass: update state counts (entry borrow is dropped).
            let (old, fail_attempt, old2, retry_attempt, spec, delay_ms) = retry_data;
            Self::update_state_counts(&mut inner.state_counts, old, TaskState::Failed);
            self.task_event_buffer.record_task_status_event(
                task_id,
                TaskState::Failed,
                fail_attempt,
            );
            Self::update_state_counts(&mut inner.state_counts, old2, TaskState::PendingArgsAvail);
            self.task_event_buffer.record_task_status_event(
                task_id,
                TaskState::PendingArgsAvail,
                retry_attempt,
            );

            retry_info = Some((spec, delay_ms));
        }

        if let Some((spec, delay_ms)) = retry_info {
            let inner = self.inner.lock();
            if let Some(ref callback) = inner.retry_task_callback {
                let _ = callback(&spec, delay_ms);
            }
        }

        tracing::info!(
            task_id = %hex::encode(task_id),
            "Retrying task"
        );
        Ok(true)
    }

    /// Mark a task as cancelled. It will not be retried on failure.
    pub fn mark_task_canceled(&self, task_id: &[u8]) {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.submissible_tasks.get_mut(task_id) {
            entry.is_canceled = true;
            entry.num_retries_left = 0;
            entry.num_oom_retries_left = 0;
        }
    }

    /// Check whether a task has been cancelled.
    pub fn is_task_canceled(&self, task_id: &[u8]) -> bool {
        self.inner
            .lock()
            .submissible_tasks
            .get(task_id)
            .is_some_and(|e| e.is_canceled)
    }

    /// Get the current state of a task.
    pub fn get_task_state(&self, task_id: &[u8]) -> Option<TaskState> {
        self.inner
            .lock()
            .submissible_tasks
            .get(task_id)
            .map(|e| e.status)
    }

    /// Get the task spec for a task.
    pub fn get_task_spec(&self, task_id: &[u8]) -> Option<TaskSpec> {
        self.inner
            .lock()
            .submissible_tasks
            .get(task_id)
            .map(|e| e.spec.clone())
    }

    /// Check whether a task is still pending (not finished or failed).
    pub fn is_task_pending(&self, task_id: &[u8]) -> bool {
        self.inner
            .lock()
            .submissible_tasks
            .get(task_id)
            .is_some_and(|e| e.is_pending())
    }

    /// Check if a task is in SUBMITTED_TO_WORKER state.
    pub fn is_task_waiting_for_execution(&self, task_id: &[u8]) -> bool {
        self.inner
            .lock()
            .submissible_tasks
            .get(task_id)
            .is_some_and(|e| e.status == TaskState::SubmittedToWorker)
    }

    /// Number of tasks currently tracked.
    pub fn num_submissible_tasks(&self) -> usize {
        self.inner.lock().submissible_tasks.len()
    }

    /// Number of pending tasks.
    pub fn num_pending_tasks(&self) -> usize {
        self.inner.lock().num_pending_tasks
    }

    /// Get the return object IDs for a task.
    pub fn get_return_ids(&self, task_id: &[u8]) -> Vec<ObjectID> {
        self.inner
            .lock()
            .submissible_tasks
            .get(task_id)
            .map(|e| e.return_ids.clone())
            .unwrap_or_default()
    }

    /// Get state counts for metrics.
    pub fn state_counts(&self) -> HashMap<TaskState, usize> {
        self.inner.lock().state_counts.clone()
    }

    /// Wait for all pending tasks to finish, then call the shutdown hook.
    pub fn drain_and_shutdown(&self, hook: Box<dyn FnOnce() + Send>) {
        let mut inner = self.inner.lock();
        if inner.num_pending_tasks == 0 {
            drop(inner);
            hook();
        } else {
            inner.shutdown_hook = Some(hook);
        }
    }

    /// Get the attempt number for a task.
    pub fn get_attempt_number(&self, task_id: &[u8]) -> i32 {
        self.inner
            .lock()
            .submissible_tasks
            .get(task_id)
            .map(|e| e.attempt_number)
            .unwrap_or(0)
    }

    /// Get all pending tasks that have a given parent task ID.
    pub fn get_pending_children_tasks(&self, parent_task_id: &[u8]) -> Vec<Vec<u8>> {
        self.inner
            .lock()
            .submissible_tasks
            .iter()
            .filter(|(_, e)| e.is_pending() && e.spec.parent_task_id == parent_task_id)
            .map(|(id, _)| id.clone())
            .collect()
    }

    // ── Private helpers ──────────────────────────────────────────────

    /// Update state count bookkeeping after a task transition.
    fn update_state_counts(
        state_counts: &mut HashMap<TaskState, usize>,
        old_state: TaskState,
        new_state: TaskState,
    ) {
        if old_state == new_state {
            return;
        }

        // Decrement old state count for non-terminal states.
        if !matches!(old_state, TaskState::Finished | TaskState::Failed) {
            if let Some(count) = state_counts.get_mut(&old_state) {
                *count = count.saturating_sub(1);
            }
        }

        // Increment new state count.
        *state_counts.entry(new_state).or_insert(0) += 1;
    }

    fn maybe_fire_shutdown_hook(&self) {
        let hook;
        {
            let mut inner = self.inner.lock();
            if inner.num_pending_tasks > 0 || inner.shutdown_hook.is_none() {
                return;
            }
            hook = inner.shutdown_hook.take();
        }
        if let Some(h) = hook {
            h();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_store::CoreWorkerMemoryStore;
    use ray_proto::ray::rpc;
    use std::sync::atomic::Ordering;

    fn make_address() -> Address {
        Address {
            node_id: vec![0u8; 28],
            ip_address: "127.0.0.1".to_string(),
            port: 1234,
            worker_id: vec![0u8; 28],
        }
    }

    fn make_task_manager() -> TaskManager {
        TaskManager::new(
            Arc::new(CoreWorkerMemoryStore::new()),
            Arc::new(ReferenceCounter::new()),
            Arc::new(TaskEventBuffer::new(1000)),
        )
    }

    fn make_task_spec(name: &str) -> TaskSpec {
        rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            name: name.to_string(),
            num_returns: 1,
            ..Default::default()
        }
    }

    fn make_task_spec_with_id(task_id: &TaskID) -> TaskSpec {
        rpc::TaskSpec {
            task_id: task_id.binary(),
            name: "test_task".to_string(),
            num_returns: 1,
            ..Default::default()
        }
    }

    #[test]
    fn test_add_pending_task() {
        let tm = make_task_manager();
        let spec = make_task_spec("task1");
        let task_id = spec.task_id.clone();

        let return_ids = tm.add_pending_task(make_address(), spec, 0);
        assert_eq!(return_ids.len(), 1);
        assert_eq!(tm.num_pending_tasks(), 1);
        assert_eq!(tm.num_submissible_tasks(), 1);
        assert_eq!(
            tm.get_task_state(&task_id),
            Some(TaskState::PendingArgsAvail)
        );
        assert!(tm.is_task_pending(&task_id));
    }

    #[test]
    fn test_task_state_transitions() {
        let tm = make_task_manager();
        let spec = make_task_spec("task2");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 0);

        // PENDING_ARGS_AVAIL → PENDING_NODE_ASSIGNMENT
        tm.mark_dependencies_resolved(&task_id);
        assert_eq!(
            tm.get_task_state(&task_id),
            Some(TaskState::PendingNodeAssignment)
        );

        // PENDING_NODE_ASSIGNMENT → SUBMITTED_TO_WORKER
        tm.mark_task_waiting_for_execution(&task_id, vec![1; 28], vec![2; 28]);
        assert_eq!(
            tm.get_task_state(&task_id),
            Some(TaskState::SubmittedToWorker)
        );
        assert!(tm.is_task_waiting_for_execution(&task_id));
    }

    #[test]
    fn test_complete_pending_task() {
        let tm = make_task_manager();
        let tid = TaskID::from_random();
        let spec = make_task_spec_with_id(&tid);
        let task_id_bytes = spec.task_id.clone();

        let return_ids = tm.add_pending_task(make_address(), spec, 0);
        let return_obj = RayObject::from_data(Bytes::from("result"));
        tm.complete_pending_task(&task_id_bytes, vec![(return_ids[0], return_obj)])
            .unwrap();

        assert_eq!(tm.get_task_state(&task_id_bytes), Some(TaskState::Finished));
        assert_eq!(tm.num_pending_tasks(), 0);
        assert!(!tm.is_task_pending(&task_id_bytes));
    }

    #[test]
    fn test_fail_pending_task() {
        let tm = make_task_manager();
        let spec = make_task_spec("fail_task");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 0);
        tm.fail_pending_task(&task_id, "something went wrong")
            .unwrap();

        assert_eq!(tm.get_task_state(&task_id), Some(TaskState::Failed));
        assert_eq!(tm.num_pending_tasks(), 0);
    }

    #[test]
    fn test_retry_on_failure() {
        let tm = make_task_manager();
        let spec = make_task_spec("retry_task");
        let task_id = spec.task_id.clone();

        // Allow 2 retries.
        tm.add_pending_task(make_address(), spec, 2);
        assert_eq!(tm.num_pending_tasks(), 1);

        // First failure: should retry.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "error 1", false)
            .unwrap();
        assert!(retried);
        assert_eq!(tm.num_pending_tasks(), 1);
        assert_eq!(
            tm.get_task_state(&task_id),
            Some(TaskState::PendingArgsAvail)
        );
        assert_eq!(tm.get_attempt_number(&task_id), 1);

        // Second failure: should retry.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "error 2", false)
            .unwrap();
        assert!(retried);
        assert_eq!(tm.get_attempt_number(&task_id), 2);

        // Third failure: no retries left, should fail permanently.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "error 3", false)
            .unwrap();
        assert!(!retried);
        assert_eq!(tm.get_task_state(&task_id), Some(TaskState::Failed));
        assert_eq!(tm.num_pending_tasks(), 0);
    }

    #[test]
    fn test_infinite_retries() {
        let tm = make_task_manager();
        let spec = make_task_spec("infinite_retry");
        let task_id = spec.task_id.clone();

        // -1 means infinite retries.
        tm.add_pending_task(make_address(), spec, -1);

        for i in 0..100 {
            let retried = tm
                .fail_or_retry_pending_task(&task_id, &format!("error {}", i), false)
                .unwrap();
            assert!(retried, "should always retry with -1 retries");
        }
        assert_eq!(tm.get_attempt_number(&task_id), 100);
    }

    #[test]
    fn test_cancel_prevents_retry() {
        let tm = make_task_manager();
        let spec = make_task_spec("cancel_task");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 10);
        tm.mark_task_canceled(&task_id);
        assert!(tm.is_task_canceled(&task_id));

        // Should not retry — cancelled.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "died", false)
            .unwrap();
        assert!(!retried);
        assert_eq!(tm.num_pending_tasks(), 0);
    }

    #[test]
    fn test_multiple_return_objects() {
        let tm = make_task_manager();
        let spec = rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            name: "multi_return".to_string(),
            num_returns: 3,
            ..Default::default()
        };
        let task_id = spec.task_id.clone();

        let return_ids = tm.add_pending_task(make_address(), spec, 0);
        assert_eq!(return_ids.len(), 3);
        assert_eq!(tm.get_return_ids(&task_id).len(), 3);
    }

    #[test]
    fn test_drain_and_shutdown_immediate() {
        let tm = make_task_manager();
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = called.clone();

        // No pending tasks, hook fires immediately.
        tm.drain_and_shutdown(Box::new(move || {
            called_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        }));
        assert!(called.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_drain_and_shutdown_deferred() {
        let tm = make_task_manager();
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = called.clone();

        let spec = make_task_spec("drain_task");
        let task_id = spec.task_id.clone();
        tm.add_pending_task(make_address(), spec, 0);

        // Set hook — should NOT fire yet.
        tm.drain_and_shutdown(Box::new(move || {
            called_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        }));
        assert!(!called.load(std::sync::atomic::Ordering::Relaxed));

        // Complete the task — hook should fire.
        tm.complete_pending_task(&task_id, vec![]).unwrap();
        assert!(called.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_state_counts() {
        let tm = make_task_manager();
        let spec1 = make_task_spec("s1");
        let spec2 = make_task_spec("s2");
        let id1 = spec1.task_id.clone();
        let id2 = spec2.task_id.clone();

        tm.add_pending_task(make_address(), spec1, 0);
        tm.add_pending_task(make_address(), spec2, 0);

        let counts = tm.state_counts();
        assert_eq!(counts[&TaskState::PendingArgsAvail], 2);

        tm.mark_dependencies_resolved(&id1);
        let counts = tm.state_counts();
        assert_eq!(counts[&TaskState::PendingArgsAvail], 1);
        assert_eq!(counts[&TaskState::PendingNodeAssignment], 1);

        tm.mark_dependencies_resolved(&id2);
        tm.mark_task_waiting_for_execution(&id2, vec![], vec![]);
        let counts = tm.state_counts();
        assert_eq!(counts[&TaskState::PendingNodeAssignment], 1);
        assert_eq!(counts[&TaskState::SubmittedToWorker], 1);
    }

    #[test]
    fn test_get_pending_children_tasks() {
        let tm = make_task_manager();
        let parent_id = TaskID::from_random();

        let spec1 = rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            parent_task_id: parent_id.binary(),
            num_returns: 1,
            ..Default::default()
        };
        let spec2 = rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            parent_task_id: parent_id.binary(),
            num_returns: 1,
            ..Default::default()
        };
        let spec3 = rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            parent_task_id: TaskID::from_random().binary(), // different parent
            num_returns: 1,
            ..Default::default()
        };

        tm.add_pending_task(make_address(), spec1, 0);
        tm.add_pending_task(make_address(), spec2, 0);
        tm.add_pending_task(make_address(), spec3, 0);

        let children = tm.get_pending_children_tasks(&parent_id.binary());
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_oom_retry() {
        let tm = make_task_manager();
        let spec = make_task_spec("oom_task");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 0);
        // Set OOM retries via internal access.
        {
            let mut inner = tm.inner.lock();
            if let Some(entry) = inner.submissible_tasks.get_mut(&task_id) {
                entry.num_oom_retries_left = 2;
            }
        }

        // First OOM failure: should retry.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "OOM", true)
            .unwrap();
        assert!(retried);

        // Second OOM failure: should retry.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "OOM", true)
            .unwrap();
        assert!(retried);

        // Third OOM failure: no retries left.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "OOM", true)
            .unwrap();
        assert!(!retried);
    }

    #[test]
    fn test_nonexistent_task_operations() {
        let tm = make_task_manager();
        let fake_id = TaskID::from_random().binary();

        // These should not panic.
        tm.mark_dependencies_resolved(&fake_id);
        tm.mark_task_waiting_for_execution(&fake_id, vec![], vec![]);
        tm.mark_task_canceled(&fake_id);
        assert!(!tm.is_task_canceled(&fake_id));
        assert!(!tm.is_task_pending(&fake_id));
        assert!(tm.get_task_state(&fake_id).is_none());
        assert!(tm.get_task_spec(&fake_id).is_none());
    }

    // ── Additional tests ported from task_manager_test.cc ───────────

    /// Port of TestRecordMetrics: verify state counts are accurate
    /// through multiple transitions.
    #[test]
    fn test_record_metrics_through_transitions() {
        let tm = make_task_manager();
        let spec = make_task_spec("metrics_task");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 0);
        assert_eq!(tm.state_counts()[&TaskState::PendingArgsAvail], 1);

        tm.mark_dependencies_resolved(&task_id);
        assert_eq!(tm.state_counts()[&TaskState::PendingArgsAvail], 0);
        assert_eq!(tm.state_counts()[&TaskState::PendingNodeAssignment], 1);

        tm.mark_task_waiting_for_execution(&task_id, vec![1; 28], vec![2; 28]);
        assert_eq!(tm.state_counts()[&TaskState::PendingNodeAssignment], 0);
        assert_eq!(tm.state_counts()[&TaskState::SubmittedToWorker], 1);

        tm.complete_pending_task(&task_id, vec![]).unwrap();
        assert_eq!(tm.state_counts()[&TaskState::SubmittedToWorker], 0);
        assert_eq!(tm.state_counts()[&TaskState::Finished], 1);
    }

    /// Port of TestTaskSuccess: complete task stores return objects
    /// and transitions to Finished.
    #[test]
    fn test_task_success_stores_return_objects() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let tm = TaskManager::new(
            store.clone(),
            Arc::new(ReferenceCounter::new()),
            Arc::new(TaskEventBuffer::new(1000)),
        );
        let tid = TaskID::from_random();
        let spec = make_task_spec_with_id(&tid);
        let task_id = spec.task_id.clone();

        let return_ids = tm.add_pending_task(make_address(), spec, 0);
        assert_eq!(return_ids.len(), 1);

        let return_obj = RayObject::from_data(Bytes::from("success_result"));
        tm.complete_pending_task(&task_id, vec![(return_ids[0], return_obj)])
            .unwrap();

        // Verify return object is in the store.
        let obj = store.get(&return_ids[0]).unwrap();
        assert_eq!(obj.data.as_ref(), b"success_result");
        assert_eq!(tm.get_task_state(&task_id), Some(TaskState::Finished));
    }

    /// Port of TestTaskFailure: failed task stores error objects for
    /// return IDs.
    #[test]
    fn test_task_failure_stores_error_objects() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let tm = TaskManager::new(
            store.clone(),
            Arc::new(ReferenceCounter::new()),
            Arc::new(TaskEventBuffer::new(1000)),
        );
        let spec = make_task_spec("fail_store");
        let task_id = spec.task_id.clone();

        let return_ids = tm.add_pending_task(make_address(), spec, 0);
        tm.fail_pending_task(&task_id, "task execution failed")
            .unwrap();

        // Verify error object is in the store.
        let obj = store.get(&return_ids[0]).unwrap();
        assert_eq!(obj.data.as_ref(), b"task execution failed");
        assert_eq!(obj.metadata.as_ref(), b"ERROR");
    }

    /// Port of TestTaskReconstruction: retrying a task increments
    /// the attempt number and resets state.
    #[test]
    fn test_task_reconstruction_increments_attempt() {
        let tm = make_task_manager();
        let spec = make_task_spec("reconstruct");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 3);
        assert_eq!(tm.get_attempt_number(&task_id), 0);

        // Fail and retry.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "error", false)
            .unwrap();
        assert!(retried);
        assert_eq!(tm.get_attempt_number(&task_id), 1);

        let retried = tm
            .fail_or_retry_pending_task(&task_id, "error", false)
            .unwrap();
        assert!(retried);
        assert_eq!(tm.get_attempt_number(&task_id), 2);
    }

    /// Port of TestTaskKill: cancel prevents retry and fails the task.
    #[test]
    fn test_task_kill_prevents_retry_on_failure() {
        let tm = make_task_manager();
        let spec = make_task_spec("kill_task");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 5);
        tm.mark_task_canceled(&task_id);

        // Even though retries are available, cancellation prevents them.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "killed", false)
            .unwrap();
        assert!(!retried);
        assert_eq!(tm.get_task_state(&task_id), Some(TaskState::Failed));
    }

    /// Port of TestResubmitCanceledTask: verifying that marking a
    /// task as no-retry and then failing succeeds without panic.
    #[test]
    fn test_cancel_then_fail_succeeds() {
        let tm = make_task_manager();
        let spec = make_task_spec("cancel_fail");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 1);
        tm.mark_task_canceled(&task_id);
        assert!(tm.is_task_canceled(&task_id));

        // Fail the task.
        let retried = tm
            .fail_or_retry_pending_task(&task_id, "cancelled", false)
            .unwrap();
        assert!(!retried);
        assert_eq!(tm.num_pending_tasks(), 0);
    }

    /// Port of TestFailsImmediatelyOverridesRetry: fail_or_retry with
    /// fail_immediately should not retry even if retries remain.
    #[test]
    fn test_zero_retries_fails_immediately() {
        let tm = make_task_manager();
        let spec = make_task_spec("zero_retry");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 0);

        let retried = tm
            .fail_or_retry_pending_task(&task_id, "error", false)
            .unwrap();
        assert!(!retried);
        assert_eq!(tm.get_task_state(&task_id), Some(TaskState::Failed));
    }

    /// Port of TestTaskOomInfiniteRetry: OOM retries with -1 should
    /// retry indefinitely.
    #[test]
    fn test_oom_infinite_retry() {
        let tm = make_task_manager();
        let spec = make_task_spec("oom_infinite");
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 0);
        // Set OOM retries to infinite.
        {
            let mut inner = tm.inner.lock();
            if let Some(entry) = inner.submissible_tasks.get_mut(&task_id) {
                entry.num_oom_retries_left = -1;
            }
        }

        for i in 0..50 {
            let retried = tm
                .fail_or_retry_pending_task(&task_id, &format!("OOM {}", i), true)
                .unwrap();
            assert!(retried, "should always retry with OOM -1");
        }
        assert_eq!(tm.get_attempt_number(&task_id), 50);
    }

    /// Port of TestGetTaskSpec: verify that task spec is retrievable.
    #[test]
    fn test_get_task_spec_returns_correct_spec() {
        let tm = make_task_manager();
        let spec = rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            name: "my_special_task".to_string(),
            num_returns: 2,
            ..Default::default()
        };
        let task_id = spec.task_id.clone();

        tm.add_pending_task(make_address(), spec, 0);
        let got = tm.get_task_spec(&task_id).unwrap();
        assert_eq!(got.name, "my_special_task");
        assert_eq!(got.num_returns, 2);
    }

    /// Port of TestRetryCallback: verify the retry callback is invoked
    /// when a task is retried.
    #[test]
    fn test_retry_callback_invoked() {
        let tm = make_task_manager();

        let retry_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let retry_count_clone = retry_count.clone();
        tm.set_retry_task_callback(Box::new(move |_spec, _delay| {
            retry_count_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));

        let spec = make_task_spec("retry_cb");
        let task_id = spec.task_id.clone();
        tm.add_pending_task(make_address(), spec, 3);

        tm.fail_or_retry_pending_task(&task_id, "error", false)
            .unwrap();
        assert_eq!(retry_count.load(Ordering::Relaxed), 1);

        tm.fail_or_retry_pending_task(&task_id, "error", false)
            .unwrap();
        assert_eq!(retry_count.load(Ordering::Relaxed), 2);
    }

    /// Port of TestFailPendingTaskAfterCancellation: completing a
    /// non-existent task should fail gracefully.
    #[test]
    fn test_complete_nonexistent_task_fails() {
        let tm = make_task_manager();
        let fake_id = TaskID::from_random().binary();
        let result = tm.complete_pending_task(&fake_id, vec![]);
        assert!(result.is_err());
    }

    /// Port of TestLineageEvicted concept: verify that return IDs
    /// are correctly derived from task ID.
    #[test]
    fn test_return_ids_derived_from_task_id() {
        let tm = make_task_manager();
        let tid = TaskID::from_random();
        let spec = rpc::TaskSpec {
            task_id: tid.binary(),
            num_returns: 3,
            ..Default::default()
        };
        let task_id = spec.task_id.clone();

        let return_ids = tm.add_pending_task(make_address(), spec, 0);
        assert_eq!(return_ids.len(), 3);

        // Verify each return ID is derived from the task ID.
        for (i, oid) in return_ids.iter().enumerate() {
            let expected = ObjectID::from_index(&tid, (i + 1) as u32);
            assert_eq!(*oid, expected);
        }

        // Also verify via get_return_ids.
        assert_eq!(tm.get_return_ids(&task_id), return_ids);
    }
}
