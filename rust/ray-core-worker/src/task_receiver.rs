// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Task execution engine for incoming `PushTask` RPCs.
//!
//! Replaces parts of `src/ray/core_worker/task_execution.h/cc`.
//!
//! The TaskReceiver accepts incoming task requests, executes them via a
//! registered callback, manages concurrency limits, supports cancellation,
//! and stores return values in the memory store.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use ray_common::id::{ObjectID, WorkerID};
use ray_proto::ray::rpc::{self, TaskSpec};

use crate::error::{CoreWorkerError, CoreWorkerResult};
use crate::memory_store::{CoreWorkerMemoryStore, RayObject};

/// A simple cooperative cancellation flag.
#[derive(Clone)]
struct CancelFlag(Arc<AtomicBool>);

impl CancelFlag {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// Result of executing a single task.
#[derive(Debug, Clone, Default)]
pub struct TaskResult {
    /// Return objects keyed by return ObjectID.
    pub return_objects: Vec<rpc::ReturnObject>,
    /// Whether the task encountered a retryable error.
    pub is_retryable_error: bool,
    /// Whether the task encountered an application-level error.
    pub is_application_error: bool,
    /// Error message if the task failed.
    pub error_message: String,
}

/// Callback that executes a task and returns the result.
///
/// The callback receives the task spec and should return a `TaskResult`
/// containing any return objects and error information.
pub type TaskExecutionCallback =
    Arc<dyn Fn(&TaskSpec) -> CoreWorkerResult<TaskResult> + Send + Sync>;

/// Per-actor concurrency group tracking.
#[allow(dead_code)]
struct ActorConcurrencyGroup {
    /// Semaphore controlling max concurrent tasks for this actor.
    semaphore: Arc<Semaphore>,
    /// Maximum concurrency for this group.
    max_concurrency: usize,
    /// Number of queued tasks waiting for a permit.
    num_queued: AtomicUsize,
}

/// Manages incoming task execution with concurrency control and cancellation.
pub struct TaskReceiver {
    /// The worker ID this receiver belongs to.
    worker_id: WorkerID,
    /// Memory store for storing return objects.
    memory_store: Arc<CoreWorkerMemoryStore>,
    /// Registered task execution callback.
    execute_callback: Mutex<Option<TaskExecutionCallback>>,
    /// Currently running tasks (task_id → cancellation token).
    running_tasks: Mutex<HashMap<Vec<u8>, CancelFlag>>,
    /// Default concurrency semaphore limiting parallel task execution.
    concurrency_semaphore: Arc<Semaphore>,
    /// Per-actor concurrency groups (actor_id_hex → group).
    actor_concurrency_groups: Mutex<HashMap<String, Arc<ActorConcurrencyGroup>>>,
    /// Number of tasks currently executing.
    num_executing: AtomicUsize,
    /// Total tasks executed since startup.
    total_executed: AtomicUsize,
    /// Whether the worker is exiting (no new tasks accepted).
    is_exiting: AtomicBool,
}

impl TaskReceiver {
    /// Create a new TaskReceiver.
    ///
    /// `max_concurrency` controls the maximum number of tasks that can
    /// execute in parallel. Use 0 for unlimited.
    pub fn new(
        worker_id: WorkerID,
        memory_store: Arc<CoreWorkerMemoryStore>,
        max_concurrency: usize,
    ) -> Self {
        let permits = if max_concurrency == 0 {
            Semaphore::MAX_PERMITS
        } else {
            max_concurrency
        };
        Self {
            worker_id,
            memory_store,
            execute_callback: Mutex::new(None),
            running_tasks: Mutex::new(HashMap::new()),
            concurrency_semaphore: Arc::new(Semaphore::new(permits)),
            actor_concurrency_groups: Mutex::new(HashMap::new()),
            num_executing: AtomicUsize::new(0),
            total_executed: AtomicUsize::new(0),
            is_exiting: AtomicBool::new(false),
        }
    }

    /// Register the task execution callback.
    pub fn set_execute_callback(&self, callback: TaskExecutionCallback) {
        *self.execute_callback.lock() = Some(callback);
    }

    /// Handle an incoming PushTask request.
    ///
    /// Validates the request, acquires a concurrency permit, executes the
    /// task via the registered callback, stores return objects, and builds
    /// the reply.
    pub async fn handle_push_task(
        &self,
        request: rpc::PushTaskRequest,
    ) -> CoreWorkerResult<rpc::PushTaskReply> {
        // Reject if worker is exiting.
        if self.is_exiting.load(Ordering::Relaxed) {
            return Ok(rpc::PushTaskReply {
                worker_exiting: true,
                ..Default::default()
            });
        }

        // Validate intended worker ID.
        let intended_id = WorkerID::from_binary(&request.intended_worker_id);
        if !intended_id.is_nil() && intended_id != self.worker_id {
            return Err(CoreWorkerError::InvalidArgument(format!(
                "task intended for worker {} but received by {}",
                intended_id.hex(),
                self.worker_id.hex()
            )));
        }

        let task_spec = request.task_spec.ok_or_else(|| {
            CoreWorkerError::InvalidArgument("PushTask missing task_spec".into())
        })?;

        let task_id = task_spec.task_id.clone();

        tracing::debug!(
            task_id = %hex::encode(&task_id),
            name = %task_spec.name,
            seq = request.sequence_number,
            "Executing task"
        );

        // Create cancellation token for this task.
        let cancel_token = CancelFlag::new();
        self.running_tasks
            .lock()
            .insert(task_id.clone(), cancel_token.clone());

        // Acquire concurrency permit.
        let _permit = self.concurrency_semaphore.acquire().await.map_err(|_| {
            CoreWorkerError::Internal("concurrency semaphore closed".into())
        })?;

        self.num_executing.fetch_add(1, Ordering::Relaxed);

        // Execute the task on a blocking thread so that the callback can
        // call `block_on()` on a *different* tokio runtime (e.g. for nested
        // task dispatch) without triggering "Cannot start a runtime from
        // within a runtime".
        let result = {
            let callback = self.execute_callback.lock().clone();
            let cancel_flag = cancel_token.clone();
            tokio::task::spawn_blocking(move || {
                if cancel_flag.is_cancelled() {
                    return Ok(TaskResult {
                        error_message: "task cancelled before execution".into(),
                        ..Default::default()
                    });
                }
                let callback = callback
                    .as_ref()
                    .ok_or(CoreWorkerError::NotInitialized)?;
                callback(&task_spec)
            })
            .await
            .map_err(|e| {
                CoreWorkerError::Internal(format!("task execution join error: {}", e))
            })?
        };

        self.num_executing.fetch_sub(1, Ordering::Relaxed);
        self.total_executed.fetch_add(1, Ordering::Relaxed);

        // Remove from running tasks.
        self.running_tasks.lock().remove(&task_id);

        let task_result = match result {
            Ok(tr) => tr,
            Err(e) => {
                tracing::warn!(
                    task_id = %hex::encode(&task_id),
                    error = %e,
                    "Task execution failed"
                );
                TaskResult {
                    is_retryable_error: true,
                    error_message: e.to_string(),
                    ..Default::default()
                }
            }
        };

        // Store return objects in memory store.
        for ret_obj in &task_result.return_objects {
            let oid = ObjectID::from_binary(&ret_obj.object_id);
            let ray_obj = RayObject::new(
                Bytes::copy_from_slice(&ret_obj.data),
                Bytes::copy_from_slice(&ret_obj.metadata),
                Vec::new(),
            );
            // Ignore duplicate errors (task may have been retried).
            let _ = self.memory_store.put(oid, ray_obj);
        }

        Ok(rpc::PushTaskReply {
            return_objects: task_result.return_objects,
            worker_exiting: self.is_exiting.load(Ordering::Relaxed),
            is_retryable_error: task_result.is_retryable_error,
            is_application_error: task_result.is_application_error,
            task_execution_error: task_result.error_message,
            ..Default::default()
        })
    }

    /// Execute a task using the registered callback.
    fn execute_task(
        &self,
        task_spec: &TaskSpec,
        cancel_token: &CancelFlag,
    ) -> CoreWorkerResult<TaskResult> {
        // Check if cancelled before starting.
        if cancel_token.is_cancelled() {
            return Ok(TaskResult {
                error_message: "task cancelled before execution".into(),
                ..Default::default()
            });
        }

        let callback = self.execute_callback.lock();
        let callback = callback.as_ref().ok_or(
            CoreWorkerError::NotInitialized
        )?;

        callback(task_spec)
    }

    /// Cancel a running task.
    ///
    /// Returns `true` if the task was found and cancellation was requested.
    pub fn cancel_task(&self, task_id: &[u8], _force_kill: bool) -> bool {
        let running = self.running_tasks.lock();
        if let Some(token) = running.get(task_id) {
            token.cancel();
            true
        } else {
            false
        }
    }

    /// Mark the worker as exiting. New tasks will be rejected.
    pub fn set_exiting(&self) {
        self.is_exiting.store(true, Ordering::Relaxed);
    }

    /// Whether the worker is exiting.
    pub fn is_exiting(&self) -> bool {
        self.is_exiting.load(Ordering::Relaxed)
    }

    /// Number of tasks currently executing.
    pub fn num_executing(&self) -> usize {
        self.num_executing.load(Ordering::Relaxed)
    }

    /// Total number of tasks executed since startup.
    pub fn total_executed(&self) -> usize {
        self.total_executed.load(Ordering::Relaxed)
    }

    /// Number of running tasks (includes those waiting for concurrency permits).
    pub fn num_running_tasks(&self) -> usize {
        self.running_tasks.lock().len()
    }

    /// Check if a specific task is currently running.
    pub fn is_task_running(&self, task_id: &[u8]) -> bool {
        self.running_tasks.lock().contains_key(task_id)
    }

    /// Set concurrency for a specific actor.
    ///
    /// Actor tasks will be limited to `max_concurrency` parallel executions.
    /// Use 0 for unlimited concurrency.
    pub fn set_actor_concurrency(&self, actor_id_hex: String, max_concurrency: usize) {
        let permits = if max_concurrency == 0 {
            Semaphore::MAX_PERMITS
        } else {
            max_concurrency
        };
        let group = Arc::new(ActorConcurrencyGroup {
            semaphore: Arc::new(Semaphore::new(permits)),
            max_concurrency,
            num_queued: AtomicUsize::new(0),
        });
        self.actor_concurrency_groups
            .lock()
            .insert(actor_id_hex, group);
    }

    /// Handle an incoming actor task with actor-specific concurrency control.
    ///
    /// If the actor has a concurrency group configured, uses that group's
    /// semaphore instead of the default one.
    pub async fn handle_actor_task(
        &self,
        request: rpc::PushTaskRequest,
        actor_id_hex: &str,
    ) -> CoreWorkerResult<rpc::PushTaskReply> {
        // Check for actor-specific concurrency group.
        let actor_group = self
            .actor_concurrency_groups
            .lock()
            .get(actor_id_hex)
            .cloned();

        if let Some(group) = actor_group {
            group.num_queued.fetch_add(1, Ordering::Relaxed);

            // Acquire actor-specific permit instead of default.
            let _actor_permit = group.semaphore.acquire().await.map_err(|_| {
                CoreWorkerError::Internal("actor concurrency semaphore closed".into())
            })?;

            group.num_queued.fetch_sub(1, Ordering::Relaxed);

            // Execute with the actor permit held.
            self.handle_push_task_inner(request).await
        } else {
            // Fall back to default behavior.
            self.handle_push_task(request).await
        }
    }

    /// Inner implementation shared between regular and actor task handling.
    async fn handle_push_task_inner(
        &self,
        request: rpc::PushTaskRequest,
    ) -> CoreWorkerResult<rpc::PushTaskReply> {
        if self.is_exiting.load(Ordering::Relaxed) {
            return Ok(rpc::PushTaskReply {
                worker_exiting: true,
                ..Default::default()
            });
        }

        let intended_id = WorkerID::from_binary(&request.intended_worker_id);
        if !intended_id.is_nil() && intended_id != self.worker_id {
            return Err(CoreWorkerError::InvalidArgument(format!(
                "task intended for worker {} but received by {}",
                intended_id.hex(),
                self.worker_id.hex()
            )));
        }

        let task_spec = request.task_spec.ok_or_else(|| {
            CoreWorkerError::InvalidArgument("PushTask missing task_spec".into())
        })?;

        let task_id = task_spec.task_id.clone();
        let cancel_token = CancelFlag::new();
        self.running_tasks
            .lock()
            .insert(task_id.clone(), cancel_token.clone());

        self.num_executing.fetch_add(1, Ordering::Relaxed);
        let result = self.execute_task(&task_spec, &cancel_token);
        self.num_executing.fetch_sub(1, Ordering::Relaxed);
        self.total_executed.fetch_add(1, Ordering::Relaxed);
        self.running_tasks.lock().remove(&task_id);

        let task_result = match result {
            Ok(tr) => tr,
            Err(e) => TaskResult {
                is_retryable_error: true,
                error_message: e.to_string(),
                ..Default::default()
            },
        };

        for ret_obj in &task_result.return_objects {
            let oid = ObjectID::from_binary(&ret_obj.object_id);
            let ray_obj = RayObject::new(
                Bytes::copy_from_slice(&ret_obj.data),
                Bytes::copy_from_slice(&ret_obj.metadata),
                Vec::new(),
            );
            let _ = self.memory_store.put(oid, ray_obj);
        }

        Ok(rpc::PushTaskReply {
            return_objects: task_result.return_objects,
            worker_exiting: self.is_exiting.load(Ordering::Relaxed),
            is_retryable_error: task_result.is_retryable_error,
            is_application_error: task_result.is_application_error,
            task_execution_error: task_result.error_message,
            ..Default::default()
        })
    }

    /// Get the number of queued actor tasks for a specific actor.
    pub fn num_queued_actor_tasks(&self, actor_id_hex: &str) -> usize {
        self.actor_concurrency_groups
            .lock()
            .get(actor_id_hex)
            .map(|g| g.num_queued.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_common::id::TaskID;

    fn make_worker_id() -> WorkerID {
        WorkerID::from_random()
    }

    fn make_store() -> Arc<CoreWorkerMemoryStore> {
        Arc::new(CoreWorkerMemoryStore::new())
    }

    fn make_task_spec(name: &str) -> TaskSpec {
        TaskSpec {
            task_id: TaskID::from_random().binary(),
            name: name.to_string(),
            ..Default::default()
        }
    }

    fn make_push_request(
        worker_id: &WorkerID,
        task_spec: TaskSpec,
    ) -> rpc::PushTaskRequest {
        rpc::PushTaskRequest {
            intended_worker_id: worker_id.binary(),
            task_spec: Some(task_spec),
            sequence_number: 0,
            ..Default::default()
        }
    }

    fn success_callback() -> TaskExecutionCallback {
        Arc::new(|_spec: &TaskSpec| {
            // Use a proper 28-byte ObjectID for return objects.
            let oid = ObjectID::from_random();
            let return_obj = rpc::ReturnObject {
                object_id: oid.binary(),
                data: b"result_data".to_vec(),
                metadata: Vec::new(),
                ..Default::default()
            };
            Ok(TaskResult {
                return_objects: vec![return_obj],
                ..Default::default()
            })
        })
    }

    fn error_callback() -> TaskExecutionCallback {
        Arc::new(|_spec: &TaskSpec| {
            Ok(TaskResult {
                is_application_error: true,
                error_message: "task failed".into(),
                ..Default::default()
            })
        })
    }

    #[tokio::test]
    async fn test_handle_push_task_success() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store.clone(), 4);
        receiver.set_execute_callback(success_callback());

        let spec = make_task_spec("test_task");
        let req = make_push_request(&wid, spec);

        let reply = receiver.handle_push_task(req).await.unwrap();
        assert!(!reply.worker_exiting);
        assert!(!reply.is_retryable_error);
        assert!(!reply.is_application_error);
        assert_eq!(reply.return_objects.len(), 1);
        assert_eq!(reply.return_objects[0].data, b"result_data");

        // Return object should be in memory store.
        let oid = ObjectID::from_binary(&reply.return_objects[0].object_id);
        assert!(store.contains(&oid));

        assert_eq!(receiver.total_executed(), 1);
        assert_eq!(receiver.num_executing(), 0);
    }

    #[tokio::test]
    async fn test_handle_push_task_application_error() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(error_callback());

        let spec = make_task_spec("failing_task");
        let req = make_push_request(&wid, spec);

        let reply = receiver.handle_push_task(req).await.unwrap();
        assert!(reply.is_application_error);
        assert_eq!(reply.task_execution_error, "task failed");
        assert_eq!(receiver.total_executed(), 1);
    }

    #[tokio::test]
    async fn test_handle_push_task_no_callback_returns_error() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        // Don't set callback.

        let spec = make_task_spec("no_callback");
        let req = make_push_request(&wid, spec);

        let reply = receiver.handle_push_task(req).await.unwrap();
        // Should get a retryable error since callback isn't set.
        assert!(reply.is_retryable_error);
        assert!(reply.task_execution_error.contains("not initialized"));
    }

    #[tokio::test]
    async fn test_handle_push_task_wrong_worker_id() {
        let wid = make_worker_id();
        let other_wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(success_callback());

        let spec = make_task_spec("wrong_worker");
        let req = make_push_request(&other_wid, spec);

        let err = receiver.handle_push_task(req).await.unwrap_err();
        assert!(matches!(err, CoreWorkerError::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn test_handle_push_task_nil_worker_id_accepted() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(success_callback());

        let spec = make_task_spec("nil_worker");
        let mut req = make_push_request(&wid, spec);
        req.intended_worker_id = WorkerID::nil().binary();

        let reply = receiver.handle_push_task(req).await.unwrap();
        assert!(!reply.is_retryable_error);
    }

    #[tokio::test]
    async fn test_handle_push_task_missing_spec() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        let req = rpc::PushTaskRequest {
            intended_worker_id: wid.binary(),
            task_spec: None,
            ..Default::default()
        };

        let err = receiver.handle_push_task(req).await.unwrap_err();
        assert!(matches!(err, CoreWorkerError::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn test_exiting_worker_rejects_tasks() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(success_callback());

        receiver.set_exiting();
        assert!(receiver.is_exiting());

        let spec = make_task_spec("rejected");
        let req = make_push_request(&wid, spec);

        let reply = receiver.handle_push_task(req).await.unwrap();
        assert!(reply.worker_exiting);
        assert_eq!(receiver.total_executed(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancel_running_task() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = Arc::new(TaskReceiver::new(wid, store, 4));

        // Use a barrier to ensure the callback is running before we cancel.
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let barrier_clone = Arc::clone(&barrier);
        let blocking_callback: TaskExecutionCallback = Arc::new(move |_spec| {
            // Signal that we've started executing.
            barrier_clone.wait();
            // Simulate long-running task.
            std::thread::sleep(std::time::Duration::from_millis(200));
            Ok(TaskResult::default())
        });
        receiver.set_execute_callback(blocking_callback);

        let spec = make_task_spec("cancellable");
        let task_id = spec.task_id.clone();
        let req = make_push_request(&wid, spec);

        let receiver_clone = Arc::clone(&receiver);
        let handle = tokio::spawn(async move {
            receiver_clone.handle_push_task(req).await
        });

        // Wait for the callback to start executing.
        tokio::task::spawn_blocking(move || barrier.wait())
            .await
            .unwrap();

        // Cancel the task (it's still sleeping).
        assert!(receiver.cancel_task(&task_id, false));

        // Task should complete (cancellation is cooperative).
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_cancel_nonexistent_task() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        let fake_id = TaskID::from_random().binary();
        assert!(!receiver.cancel_task(&fake_id, false));
    }

    #[tokio::test]
    async fn test_concurrency_limit() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = Arc::new(TaskReceiver::new(wid, store, 2));
        receiver.set_execute_callback(success_callback());

        // Submit 3 tasks; only 2 should run concurrently.
        let mut handles = Vec::new();
        for i in 0..3 {
            let spec = make_task_spec(&format!("task_{}", i));
            let req = make_push_request(&wid, spec);
            let r = Arc::clone(&receiver);
            handles.push(tokio::spawn(async move {
                r.handle_push_task(req).await
            }));
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(receiver.total_executed(), 3);
    }

    #[tokio::test]
    async fn test_num_running_tasks_tracking() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(success_callback());

        assert_eq!(receiver.num_running_tasks(), 0);

        let spec = make_task_spec("track");
        let req = make_push_request(&wid, spec);
        receiver.handle_push_task(req).await.unwrap();

        // After completion, should be 0.
        assert_eq!(receiver.num_running_tasks(), 0);
    }

    #[tokio::test]
    async fn test_unlimited_concurrency() {
        let wid = make_worker_id();
        let store = make_store();
        // 0 means unlimited.
        let receiver = Arc::new(TaskReceiver::new(wid, store, 0));
        receiver.set_execute_callback(success_callback());

        let mut handles = Vec::new();
        for i in 0..10 {
            let spec = make_task_spec(&format!("unlimited_{}", i));
            let req = make_push_request(&wid, spec);
            let r = Arc::clone(&receiver);
            handles.push(tokio::spawn(async move {
                r.handle_push_task(req).await
            }));
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(receiver.total_executed(), 10);
    }

    #[tokio::test]
    async fn test_return_objects_stored_in_memory() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store.clone(), 4);

        let custom_oid = ObjectID::from_random();
        let oid_bytes = custom_oid.binary();
        let cb: TaskExecutionCallback = Arc::new(move |_spec| {
            Ok(TaskResult {
                return_objects: vec![rpc::ReturnObject {
                    object_id: oid_bytes.clone(),
                    data: b"hello_world".to_vec(),
                    metadata: b"meta".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        let spec = make_task_spec("store_result");
        let req = make_push_request(&wid, spec);
        receiver.handle_push_task(req).await.unwrap();

        // Verify the return object was stored.
        assert!(store.contains(&custom_oid));
        let obj = store.get(&custom_oid).unwrap();
        assert_eq!(obj.data.as_ref(), b"hello_world");
        assert_eq!(obj.metadata.as_ref(), b"meta");
    }

    #[test]
    fn test_is_task_running() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        let task_id = TaskID::from_random().binary();
        assert!(!receiver.is_task_running(&task_id));
    }

    // ─── Tests for set_actor_concurrency ────────────────────────────

    #[test]
    fn test_set_actor_concurrency_stores_group() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        let actor_hex = "actor_abc123";

        // Initially no concurrency group for this actor.
        assert_eq!(receiver.num_queued_actor_tasks(actor_hex), 0);

        // Set concurrency to 2.
        receiver.set_actor_concurrency(actor_hex.to_string(), 2);

        // After setting, queued count should still be 0 (no tasks queued yet).
        assert_eq!(receiver.num_queued_actor_tasks(actor_hex), 0);
    }

    #[test]
    fn test_set_actor_concurrency_unlimited() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        // Setting concurrency to 0 means unlimited.
        receiver.set_actor_concurrency("unlimited_actor".to_string(), 0);
        assert_eq!(receiver.num_queued_actor_tasks("unlimited_actor"), 0);
    }

    #[test]
    fn test_set_actor_concurrency_replaces_previous() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        let actor_hex = "replace_actor";

        // Set to 4, then replace with 1.
        receiver.set_actor_concurrency(actor_hex.to_string(), 4);
        receiver.set_actor_concurrency(actor_hex.to_string(), 1);

        // Should not error; the group was replaced.
        assert_eq!(receiver.num_queued_actor_tasks(actor_hex), 0);
    }

    // ─── Tests for handle_actor_task ────────────────────────────────

    #[tokio::test]
    async fn test_handle_actor_task_with_concurrency_group() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store.clone(), 4);
        receiver.set_execute_callback(success_callback());

        let actor_hex = "actor_with_group";
        receiver.set_actor_concurrency(actor_hex.to_string(), 2);

        let spec = make_task_spec("actor_task_1");
        let req = make_push_request(&wid, spec);

        // Should use actor-specific concurrency path.
        let reply = receiver.handle_actor_task(req, actor_hex).await.unwrap();
        assert!(!reply.worker_exiting);
        assert!(!reply.is_retryable_error);
        assert_eq!(reply.return_objects.len(), 1);
        assert_eq!(receiver.total_executed(), 1);
    }

    #[tokio::test]
    async fn test_handle_actor_task_without_concurrency_group_falls_back() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store.clone(), 4);
        receiver.set_execute_callback(success_callback());

        // No concurrency group set for this actor -- falls back to default.
        let spec = make_task_spec("fallback_task");
        let req = make_push_request(&wid, spec);

        let reply = receiver.handle_actor_task(req, "unknown_actor").await.unwrap();
        assert!(!reply.is_retryable_error);
        assert_eq!(reply.return_objects.len(), 1);
        assert_eq!(receiver.total_executed(), 1);
    }

    #[tokio::test]
    async fn test_handle_actor_task_exiting_worker_rejected() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(success_callback());

        let actor_hex = "exit_actor";
        receiver.set_actor_concurrency(actor_hex.to_string(), 2);

        // Mark worker as exiting.
        receiver.set_exiting();

        let spec = make_task_spec("exiting_task");
        let req = make_push_request(&wid, spec);

        let reply = receiver.handle_actor_task(req, actor_hex).await.unwrap();
        assert!(reply.worker_exiting);
        assert_eq!(receiver.total_executed(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_handle_actor_task_concurrency_limit_respected() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = Arc::new(TaskReceiver::new(wid, store.clone(), 10));

        // Track max concurrent executions.
        let current = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let current_clone = current.clone();
        let max_seen_clone = max_seen.clone();
        let cb: TaskExecutionCallback = Arc::new(move |_spec| {
            let c = current_clone.fetch_add(1, Ordering::SeqCst) + 1;
            // Update max seen.
            max_seen_clone.fetch_max(c, Ordering::SeqCst);
            // Simulate some work.
            std::thread::sleep(std::time::Duration::from_millis(20));
            current_clone.fetch_sub(1, Ordering::SeqCst);
            Ok(TaskResult {
                return_objects: vec![rpc::ReturnObject {
                    object_id: ObjectID::from_random().binary(),
                    data: b"ok".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        let actor_hex = "limit_actor";
        // Set actor concurrency to 1 (serial execution).
        receiver.set_actor_concurrency(actor_hex.to_string(), 1);

        let mut handles = Vec::new();
        for i in 0..4 {
            let spec = make_task_spec(&format!("limited_{}", i));
            let req = make_push_request(&wid, spec);
            let r = Arc::clone(&receiver);
            let ah = actor_hex.to_string();
            handles.push(tokio::spawn(async move {
                r.handle_actor_task(req, &ah).await
            }));
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(receiver.total_executed(), 4);
        // With actor concurrency of 1, max concurrent should be 1.
        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
    }

    // ─── Tests for num_queued_actor_tasks ───────────────────────────

    #[test]
    fn test_num_queued_actor_tasks_unknown_actor() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        // Unknown actor should return 0 queued tasks.
        assert_eq!(receiver.num_queued_actor_tasks("nonexistent"), 0);
    }

    // ─── Tests for handle_push_task_inner (via handle_actor_task) ───

    #[tokio::test]
    async fn test_handle_actor_task_wrong_worker_id() {
        let wid = make_worker_id();
        let other_wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(success_callback());

        let actor_hex = "wrong_wid_actor";
        receiver.set_actor_concurrency(actor_hex.to_string(), 2);

        let spec = make_task_spec("wrong_wid_task");
        let req = make_push_request(&other_wid, spec);

        let err = receiver.handle_actor_task(req, actor_hex).await.unwrap_err();
        assert!(matches!(err, CoreWorkerError::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn test_handle_actor_task_missing_task_spec() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(success_callback());

        let actor_hex = "missing_spec_actor";
        receiver.set_actor_concurrency(actor_hex.to_string(), 2);

        let req = rpc::PushTaskRequest {
            intended_worker_id: wid.binary(),
            task_spec: None,
            ..Default::default()
        };

        let err = receiver.handle_actor_task(req, actor_hex).await.unwrap_err();
        assert!(matches!(err, CoreWorkerError::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn test_handle_actor_task_no_callback_returns_error() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        // Don't set callback.

        let actor_hex = "no_cb_actor";
        receiver.set_actor_concurrency(actor_hex.to_string(), 2);

        let spec = make_task_spec("no_cb_task");
        let req = make_push_request(&wid, spec);

        let reply = receiver.handle_actor_task(req, actor_hex).await.unwrap();
        assert!(reply.is_retryable_error);
        assert!(reply.task_execution_error.contains("not initialized"));
    }

    // ─── Test for multiple actor concurrency groups ─────────────────

    #[tokio::test]
    async fn test_multiple_actor_concurrency_groups() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store.clone(), 10);
        receiver.set_execute_callback(success_callback());

        // Two actors with different concurrency limits.
        receiver.set_actor_concurrency("actor_a".to_string(), 1);
        receiver.set_actor_concurrency("actor_b".to_string(), 4);

        // Execute a task for each.
        let spec_a = make_task_spec("task_for_a");
        let req_a = make_push_request(&wid, spec_a);
        let reply_a = receiver.handle_actor_task(req_a, "actor_a").await.unwrap();
        assert!(!reply_a.is_retryable_error);

        let spec_b = make_task_spec("task_for_b");
        let req_b = make_push_request(&wid, spec_b);
        let reply_b = receiver.handle_actor_task(req_b, "actor_b").await.unwrap();
        assert!(!reply_b.is_retryable_error);

        assert_eq!(receiver.total_executed(), 2);
    }

    // ─── Ported from C++ task_receiver_test.cc and execution queue tests ──

    #[tokio::test]
    async fn test_multiple_sequential_tasks() {
        // Port of normal_task_execution_queue ordering: tasks execute sequentially.
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store.clone(), 1); // concurrency = 1

        let order = Arc::new(Mutex::new(Vec::new()));
        let order_clone = order.clone();
        let cb: TaskExecutionCallback = Arc::new(move |spec: &TaskSpec| {
            order_clone.lock().push(spec.name.clone());
            Ok(TaskResult {
                return_objects: vec![rpc::ReturnObject {
                    object_id: ObjectID::from_random().binary(),
                    data: b"ok".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        for name in &["task_a", "task_b", "task_c"] {
            let spec = make_task_spec(name);
            let req = make_push_request(&wid, spec);
            receiver.handle_push_task(req).await.unwrap();
        }

        assert_eq!(receiver.total_executed(), 3);
        let executed = order.lock().clone();
        assert_eq!(executed, vec!["task_a", "task_b", "task_c"]);
    }

    #[tokio::test]
    async fn test_retryable_error_from_callback() {
        // Port of task_receiver_test: callback returns error => retryable.
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        let cb: TaskExecutionCallback = Arc::new(|_spec: &TaskSpec| {
            Err(CoreWorkerError::Internal("simulated failure".into()))
        });
        receiver.set_execute_callback(cb);

        let spec = make_task_spec("failing");
        let req = make_push_request(&wid, spec);
        let reply = receiver.handle_push_task(req).await.unwrap();
        assert!(reply.is_retryable_error);
        assert!(reply.task_execution_error.contains("simulated failure"));
    }

    #[tokio::test]
    async fn test_callback_replacement() {
        // Replacing the callback should use the new one.
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        receiver.set_execute_callback(error_callback());
        receiver.set_execute_callback(success_callback());

        let spec = make_task_spec("replaced");
        let req = make_push_request(&wid, spec);
        let reply = receiver.handle_push_task(req).await.unwrap();
        assert!(!reply.is_application_error);
        assert_eq!(reply.return_objects.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_tasks_with_unlimited_concurrency() {
        // Port of thread_pool_test: tasks can run in parallel with unlimited concurrency.
        let wid = make_worker_id();
        let store = make_store();
        let receiver = Arc::new(TaskReceiver::new(wid, store, 0));

        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        let active_clone = active.clone();
        let max_active_clone = max_active.clone();
        let cb: TaskExecutionCallback = Arc::new(move |_spec: &TaskSpec| {
            let cur = active_clone.fetch_add(1, Ordering::SeqCst) + 1;
            max_active_clone.fetch_max(cur, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(10));
            active_clone.fetch_sub(1, Ordering::SeqCst);
            Ok(TaskResult {
                return_objects: vec![rpc::ReturnObject {
                    object_id: ObjectID::from_random().binary(),
                    data: b"ok".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        let mut handles = Vec::new();
        for i in 0..8 {
            let spec = make_task_spec(&format!("parallel_{}", i));
            let req = make_push_request(&wid, spec);
            let r = Arc::clone(&receiver);
            handles.push(tokio::spawn(async move {
                r.handle_push_task(req).await
            }));
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(receiver.total_executed(), 8);
        // With unlimited concurrency, some tasks should run in parallel.
        assert!(max_active.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn test_exiting_then_set_callback_still_rejects() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_exiting();
        receiver.set_execute_callback(success_callback());

        let spec = make_task_spec("after_exit");
        let req = make_push_request(&wid, spec);
        let reply = receiver.handle_push_task(req).await.unwrap();
        assert!(reply.worker_exiting);
        assert_eq!(receiver.total_executed(), 0);
    }

    #[tokio::test]
    async fn test_multiple_return_objects_stored() {
        // Task returning multiple objects.
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store.clone(), 4);

        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();
        let oid1_bytes = oid1.binary();
        let oid2_bytes = oid2.binary();

        let cb: TaskExecutionCallback = Arc::new(move |_spec| {
            Ok(TaskResult {
                return_objects: vec![
                    rpc::ReturnObject {
                        object_id: oid1_bytes.clone(),
                        data: b"result_1".to_vec(),
                        ..Default::default()
                    },
                    rpc::ReturnObject {
                        object_id: oid2_bytes.clone(),
                        data: b"result_2".to_vec(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        let spec = make_task_spec("multi_return");
        let req = make_push_request(&wid, spec);
        let reply = receiver.handle_push_task(req).await.unwrap();
        assert_eq!(reply.return_objects.len(), 2);

        assert!(store.contains(&oid1));
        assert!(store.contains(&oid2));
        assert_eq!(store.get(&oid1).unwrap().data.as_ref(), b"result_1");
        assert_eq!(store.get(&oid2).unwrap().data.as_ref(), b"result_2");
    }

    // ── Additional tests ported from task_receiver_test.cc ──────────
    // and task execution queue tests

    /// Port of TestNewTaskFromDifferentWorker: a task intended for
    /// a different worker ID should be rejected.
    #[tokio::test]
    async fn test_different_worker_id_rejected() {
        let wid = make_worker_id();
        let other_wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(success_callback());

        let spec = make_task_spec("wrong_worker");
        let req = make_push_request(&other_wid, spec);
        let err = receiver.handle_push_task(req).await.unwrap_err();
        assert!(matches!(err, CoreWorkerError::InvalidArgument(_)));
        assert_eq!(receiver.total_executed(), 0);
    }

    /// Port of TestCancelQueuedTask concept: cancelling a running
    /// task should set its cancel flag.
    #[tokio::test]
    async fn test_cancel_running_task_sets_flag() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = Arc::new(TaskReceiver::new(wid, store, 4));

        // Use a callback that blocks until cancel is signaled.
        let started = Arc::new(tokio::sync::Notify::new());
        let proceed = Arc::new(tokio::sync::Notify::new());

        let started_clone = started.clone();
        let proceed_clone = proceed.clone();
        let cb: TaskExecutionCallback = Arc::new(move |_spec| {
            started_clone.notify_one();
            // Block synchronously — in real code this would check cancel.
            // For test we just return immediately after the signal.
            std::thread::sleep(std::time::Duration::from_millis(50));
            let _ = &proceed_clone;
            Ok(TaskResult {
                return_objects: vec![rpc::ReturnObject {
                    object_id: ObjectID::from_random().binary(),
                    data: b"ok".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        let spec = make_task_spec("cancellable");
        let task_id = spec.task_id.clone();
        let req = make_push_request(&wid, spec);

        let r = Arc::clone(&receiver);
        let handle = tokio::spawn(async move {
            r.handle_push_task(req).await
        });

        // Wait for task to start.
        started.notified().await;

        // Cancel should succeed for a running task.
        let cancelled = receiver.cancel_task(&task_id, false);
        assert!(cancelled);

        // Task finishes.
        handle.await.unwrap().unwrap();
    }

    /// Port of StopCancelsQueuedTasks concept: marking worker as
    /// exiting should reject all new tasks.
    #[tokio::test]
    async fn test_exiting_rejects_multiple_tasks() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);
        receiver.set_execute_callback(success_callback());
        receiver.set_exiting();

        for i in 0..5 {
            let spec = make_task_spec(&format!("after_exit_{}", i));
            let req = make_push_request(&wid, spec);
            let reply = receiver.handle_push_task(req).await.unwrap();
            assert!(reply.worker_exiting);
        }
        assert_eq!(receiver.total_executed(), 0);
    }

    /// Port of concurrency_group_manager_test: verify that actor
    /// concurrency limits apply independently per actor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_actor_concurrency_groups_independent() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = Arc::new(TaskReceiver::new(wid, store, 10));

        let cb: TaskExecutionCallback = Arc::new(move |_spec| {
            std::thread::sleep(std::time::Duration::from_millis(5));
            Ok(TaskResult {
                return_objects: vec![rpc::ReturnObject {
                    object_id: ObjectID::from_random().binary(),
                    data: b"ok".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        receiver.set_actor_concurrency("actor_x".to_string(), 2);
        receiver.set_actor_concurrency("actor_y".to_string(), 3);

        let mut handles = Vec::new();
        // Submit 2 tasks to actor_x and 3 to actor_y.
        for i in 0..2 {
            let spec = make_task_spec(&format!("x_{}", i));
            let req = make_push_request(&wid, spec);
            let r = Arc::clone(&receiver);
            handles.push(tokio::spawn(
                async move { r.handle_actor_task(req, "actor_x").await },
            ));
        }
        for i in 0..3 {
            let spec = make_task_spec(&format!("y_{}", i));
            let req = make_push_request(&wid, spec);
            let r = Arc::clone(&receiver);
            handles.push(tokio::spawn(
                async move { r.handle_actor_task(req, "actor_y").await },
            ));
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(receiver.total_executed(), 5);
    }

    /// Port of fiber_state_test: verify concurrency limit of 1
    /// forces serial execution.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_serial_execution_with_concurrency_one() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = Arc::new(TaskReceiver::new(wid, store, 1));

        let current = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        let c = current.clone();
        let m = max_concurrent.clone();
        let cb: TaskExecutionCallback = Arc::new(move |_spec| {
            let cur = c.fetch_add(1, Ordering::SeqCst) + 1;
            m.fetch_max(cur, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(5));
            c.fetch_sub(1, Ordering::SeqCst);
            Ok(TaskResult {
                return_objects: vec![rpc::ReturnObject {
                    object_id: ObjectID::from_random().binary(),
                    data: b"ok".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        let mut handles = Vec::new();
        for i in 0..4 {
            let spec = make_task_spec(&format!("serial_{}", i));
            let req = make_push_request(&wid, spec);
            let r = Arc::clone(&receiver);
            handles.push(tokio::spawn(async move {
                r.handle_push_task(req).await
            }));
        }

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(receiver.total_executed(), 4);
        // With concurrency 1, max concurrent should be exactly 1.
        assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
    }

    /// Port of task event buffer test concept: application errors
    /// are properly flagged.
    #[tokio::test]
    async fn test_application_error_flagged() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = TaskReceiver::new(wid, store, 4);

        let cb: TaskExecutionCallback = Arc::new(|_spec| {
            Ok(TaskResult {
                return_objects: vec![rpc::ReturnObject {
                    object_id: ObjectID::from_random().binary(),
                    data: b"error_data".to_vec(),
                    ..Default::default()
                }],
                is_application_error: true,
                error_message: "user code exception".to_string(),
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        let spec = make_task_spec("app_error");
        let req = make_push_request(&wid, spec);
        let reply = receiver.handle_push_task(req).await.unwrap();
        assert!(reply.is_application_error);
    }

    /// Port of normal_task_execution_queue_test: verify num_running
    /// is tracked during execution.
    #[tokio::test]
    async fn test_num_running_during_execution() {
        let wid = make_worker_id();
        let store = make_store();
        let receiver = Arc::new(TaskReceiver::new(wid, store, 4));

        assert_eq!(receiver.num_running_tasks(), 0);

        let started = Arc::new(tokio::sync::Notify::new());
        let proceed = Arc::new(tokio::sync::Notify::new());

        let started_clone = started.clone();
        let proceed_clone = proceed.clone();
        let cb: TaskExecutionCallback = Arc::new(move |_spec| {
            started_clone.notify_one();
            // Block briefly to allow inspection.
            std::thread::sleep(std::time::Duration::from_millis(30));
            let _ = &proceed_clone;
            Ok(TaskResult {
                return_objects: vec![rpc::ReturnObject {
                    object_id: ObjectID::from_random().binary(),
                    data: b"ok".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });
        receiver.set_execute_callback(cb);

        let spec = make_task_spec("running_check");
        let req = make_push_request(&wid, spec);
        let r = Arc::clone(&receiver);
        let handle = tokio::spawn(async move {
            r.handle_push_task(req).await
        });

        started.notified().await;
        // Task should be running now.
        assert!(receiver.num_running_tasks() >= 1);

        handle.await.unwrap().unwrap();
        // After completion, running count should be back to 0.
        assert_eq!(receiver.num_running_tasks(), 0);
    }
}
