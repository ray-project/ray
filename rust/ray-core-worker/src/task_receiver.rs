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
    /// Concurrency semaphore limiting parallel task execution.
    concurrency_semaphore: Arc<Semaphore>,
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

        // Execute the task.
        let result = self.execute_task(&task_spec, &cancel_token);

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
}
