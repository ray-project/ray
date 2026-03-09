// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! The main CoreWorker struct that ties all components together.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use ray_common::id::{ActorID, JobID, ObjectID, TaskID, WorkerID};
use ray_proto::ray::rpc::{Address, TaskSpec};

use crate::actor_handle::ActorHandle;
use crate::actor_manager::ActorManager;
use crate::actor_task_submitter::ActorTaskSubmitter;
use crate::back_pressure::{BackPressureConfig, BackPressureController};
use crate::context::WorkerContext;
use crate::dependency_resolver::DependencyResolver;
use crate::direct_transport::{DirectActorTransport, DirectTransportConfig};
use crate::error::{CoreWorkerError, CoreWorkerResult};
use crate::future_resolver::FutureResolver;
use crate::memory_store::{CoreWorkerMemoryStore, RayObject};
use crate::normal_task_submitter::NormalTaskSubmitter;
use crate::object_recovery_manager::ObjectRecoveryManager;
use crate::options::CoreWorkerOptions;
use crate::ownership_directory::OwnershipDirectory;
use crate::reference_counter::ReferenceCounter;
use crate::task_event_buffer::TaskEventBuffer;
use crate::task_manager::TaskManager;
use crate::task_receiver::TaskReceiver;
use ray_object_manager::spill_manager::{SpillManager, SpillManagerConfig};
use ray_pubsub::Publisher;

/// The core worker orchestrating task submission, object management,
/// actor management, and reference counting.
pub struct CoreWorker {
    context: WorkerContext,
    memory_store: Arc<CoreWorkerMemoryStore>,
    reference_counter: Arc<ReferenceCounter>,
    future_resolver: Arc<FutureResolver>,
    ownership_directory: Arc<OwnershipDirectory>,
    object_recovery_manager: Arc<ObjectRecoveryManager>,
    back_pressure: Arc<BackPressureController>,
    direct_transport: Arc<DirectActorTransport>,
    actor_manager: Arc<ActorManager>,
    normal_task_submitter: NormalTaskSubmitter,
    actor_task_submitter: ActorTaskSubmitter,
    dependency_resolver: DependencyResolver,
    task_receiver: TaskReceiver,
    task_manager: Arc<TaskManager>,
    task_event_buffer: Arc<TaskEventBuffer>,
    spill_manager: Arc<SpillManager>,
    publisher: Arc<Publisher>,
    worker_address: Address,
}

impl CoreWorker {
    /// Create a new CoreWorker from options.
    pub fn new(options: CoreWorkerOptions) -> Self {
        let context = WorkerContext::new(options.worker_type, options.worker_id, options.job_id);
        let reference_counter = Arc::new(ReferenceCounter::new());
        let normal_task_submitter = NormalTaskSubmitter::new(reference_counter.clone());

        let worker_address = Address {
            node_id: options.node_id.binary(),
            ip_address: options.node_ip_address.clone(),
            port: 0,
            worker_id: options.worker_id.binary(),
        };

        let memory_store = Arc::new(CoreWorkerMemoryStore::new());

        let task_receiver = TaskReceiver::new(
            options.worker_id,
            memory_store.clone(),
            options.max_concurrency,
        );

        let task_event_buffer = Arc::new(TaskEventBuffer::new(100_000));
        let task_manager = Arc::new(TaskManager::new(
            memory_store.clone(),
            reference_counter.clone(),
            task_event_buffer.clone(),
        ));

        let future_resolver = Arc::new(FutureResolver::new(
            memory_store.clone(),
            reference_counter.clone(),
        ));
        let ownership_directory = Arc::new(OwnershipDirectory::new());
        let object_recovery_manager = Arc::new(ObjectRecoveryManager::new(
            reference_counter.clone(),
            3, // max recovery attempts
        ));

        let back_pressure = Arc::new(BackPressureController::new(BackPressureConfig::default()));

        let direct_transport =
            Arc::new(DirectActorTransport::new(DirectTransportConfig::default()));

        let spill_manager = Arc::new(SpillManager::new(SpillManagerConfig::default()));

        let publisher = Arc::new(Publisher::new(ray_pubsub::PublisherConfig::default()));
        // Register standard channels.
        publisher.register_channel(0, true); // WorkerObjectEviction (droppable)
        publisher.register_channel(1, false); // WorkerRefRemovedChannel
        publisher.register_channel(2, true); // WorkerObjectLocationsChannel (droppable)

        Self {
            context,
            memory_store,
            reference_counter,
            future_resolver,
            ownership_directory,
            object_recovery_manager,
            back_pressure,
            direct_transport,
            actor_manager: Arc::new(ActorManager::new()),
            normal_task_submitter,
            actor_task_submitter: ActorTaskSubmitter::new(),
            dependency_resolver: DependencyResolver::new(),
            task_receiver,
            task_manager,
            task_event_buffer,
            spill_manager,
            publisher,
            worker_address,
        }
    }

    // ─── Object API ──────────────────────────────────────────────────

    /// Put an object into the in-process memory store.
    pub fn put_object(
        &self,
        object_id: ObjectID,
        data: Bytes,
        metadata: Bytes,
    ) -> CoreWorkerResult<()> {
        let obj = RayObject::new(data, metadata, Vec::new());
        self.memory_store.put(object_id, obj)?;
        // Track as owned in both reference counter and ownership directory.
        self.reference_counter
            .add_owned_object(object_id, self.worker_address.clone(), vec![]);
        self.ownership_directory
            .add_owned_object(object_id, self.worker_address.clone());
        self.ownership_directory.mark_object_created(&object_id);
        self.dependency_resolver.object_available(&object_id);
        Ok(())
    }

    /// Get objects from the memory store, waiting up to `timeout` for each.
    pub async fn get_objects(
        &self,
        object_ids: &[ObjectID],
        timeout: Duration,
    ) -> CoreWorkerResult<Vec<Option<RayObject>>> {
        let mut results = Vec::with_capacity(object_ids.len());
        for oid in object_ids {
            match self.memory_store.get_or_wait(oid, timeout).await {
                Ok(obj) => results.push(Some(obj)),
                Err(CoreWorkerError::TimedOut(_)) => results.push(None),
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    /// Wait for at least `num_objects` to be ready, up to `timeout`.
    /// Returns a vector of booleans indicating which objects are ready.
    pub async fn wait(
        &self,
        object_ids: &[ObjectID],
        num_objects: usize,
        timeout: Duration,
    ) -> CoreWorkerResult<Vec<bool>> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut ready = vec![false; object_ids.len()];
        let mut num_ready = 0;

        // Initial check.
        for (i, oid) in object_ids.iter().enumerate() {
            if self.memory_store.contains(oid) {
                ready[i] = true;
                num_ready += 1;
            }
        }

        while num_ready < num_objects {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1).min(remaining)).await;
            for (i, oid) in object_ids.iter().enumerate() {
                if !ready[i] && self.memory_store.contains(oid) {
                    ready[i] = true;
                    num_ready += 1;
                }
            }
        }

        Ok(ready)
    }

    /// Delete objects from the memory store.
    pub fn delete_objects(&self, object_ids: &[ObjectID]) {
        for oid in object_ids {
            self.memory_store.delete(oid);
        }
    }

    /// Check if an object exists in the memory store.
    pub fn contains_object(&self, object_id: &ObjectID) -> bool {
        self.memory_store.contains(object_id)
    }

    // ─── Task API ────────────────────────────────────────────────────

    /// Submit a normal (non-actor) task.
    pub async fn submit_task(&self, task_spec: &TaskSpec) -> CoreWorkerResult<()> {
        self.normal_task_submitter.submit_task(task_spec).await
    }

    /// Submit an actor task.
    pub async fn submit_actor_task(
        &self,
        actor_id: &ActorID,
        task_spec: TaskSpec,
    ) -> CoreWorkerResult<()> {
        self.actor_task_submitter
            .submit_task(actor_id, task_spec)
            .await
    }

    /// Create an actor. Registers the handle and queues the creation task.
    pub fn create_actor(&self, actor_id: ActorID, handle: ActorHandle) -> CoreWorkerResult<()> {
        self.actor_task_submitter.add_actor(actor_id);
        self.actor_manager
            .register_actor_handle(actor_id, Arc::new(handle));
        Ok(())
    }

    /// Kill an actor by ID.
    pub fn kill_actor(
        &self,
        actor_id: &ActorID,
        _force_kill: bool,
        no_restart: bool,
    ) -> CoreWorkerResult<()> {
        if no_restart {
            // Permanently dead: submits get "actor is dead" error.
            self.actor_task_submitter.mark_actor_dead(actor_id);
        } else {
            self.actor_task_submitter.disconnect_actor(actor_id);
        }
        self.actor_manager.remove_actor_handle(actor_id);
        Ok(())
    }

    // ─── Ref Counting ────────────────────────────────────────────────

    /// Add a local reference to an object.
    pub fn add_local_reference(&self, object_id: ObjectID) {
        self.reference_counter.add_local_reference(object_id);
    }

    /// Remove a local reference. Returns freed object IDs.
    pub fn remove_local_reference(&self, object_id: &ObjectID) -> Vec<ObjectID> {
        self.reference_counter.remove_local_reference(object_id)
    }

    // ─── Context ─────────────────────────────────────────────────────

    pub fn current_task_id(&self) -> TaskID {
        self.context.current_task_id()
    }

    pub fn current_job_id(&self) -> JobID {
        self.context.current_job_id()
    }

    pub fn worker_id(&self) -> WorkerID {
        self.context.worker_id()
    }

    pub fn worker_context(&self) -> &WorkerContext {
        &self.context
    }

    pub fn memory_store(&self) -> &Arc<CoreWorkerMemoryStore> {
        &self.memory_store
    }

    pub fn reference_counter(&self) -> &Arc<ReferenceCounter> {
        &self.reference_counter
    }

    pub fn actor_manager(&self) -> &Arc<ActorManager> {
        &self.actor_manager
    }

    pub fn worker_address(&self) -> &Address {
        &self.worker_address
    }

    pub fn dependency_resolver(&self) -> &DependencyResolver {
        &self.dependency_resolver
    }

    pub fn future_resolver(&self) -> &Arc<FutureResolver> {
        &self.future_resolver
    }

    pub fn ownership_directory(&self) -> &Arc<OwnershipDirectory> {
        &self.ownership_directory
    }

    pub fn object_recovery_manager(&self) -> &Arc<ObjectRecoveryManager> {
        &self.object_recovery_manager
    }

    pub fn back_pressure(&self) -> &Arc<BackPressureController> {
        &self.back_pressure
    }

    pub fn direct_transport(&self) -> &Arc<DirectActorTransport> {
        &self.direct_transport
    }

    pub fn spill_manager(&self) -> &Arc<SpillManager> {
        &self.spill_manager
    }

    pub fn publisher(&self) -> &Arc<Publisher> {
        &self.publisher
    }

    /// Access the normal task submitter.
    pub fn normal_task_submitter(&self) -> &NormalTaskSubmitter {
        &self.normal_task_submitter
    }

    /// Number of normal pending tasks.
    pub fn num_pending_normal_tasks(&self) -> usize {
        self.normal_task_submitter.num_pending_tasks()
    }

    /// Access the task receiver for handling incoming PushTask RPCs.
    pub fn task_receiver(&self) -> &TaskReceiver {
        &self.task_receiver
    }

    /// Number of tasks currently being executed by this worker.
    pub fn num_executing_tasks(&self) -> usize {
        self.task_receiver.num_executing()
    }

    /// Access the actor task submitter.
    pub fn actor_task_submitter(&self) -> &ActorTaskSubmitter {
        &self.actor_task_submitter
    }

    /// Connect an actor to a worker address, enabling task delivery.
    ///
    /// After connection, any pending tasks for this actor will be flushed.
    pub fn connect_actor(&self, actor_id: &ActorID, address: Address) {
        self.actor_task_submitter.connect_actor(actor_id, address);
        self.actor_task_submitter.flush_actor_tasks(actor_id);
    }

    /// Set the callback used to send actor tasks to workers.
    ///
    /// This must be called before actor tasks can be delivered.
    pub fn set_actor_task_send_callback(
        &self,
        callback: crate::actor_task_submitter::ActorTaskSendCallback,
    ) {
        self.actor_task_submitter.set_send_callback(callback);
    }

    /// Register a callback for executing incoming tasks.
    ///
    /// This must be called before the worker can handle PushTask RPCs.
    pub fn set_task_execution_callback(
        &self,
        callback: crate::task_receiver::TaskExecutionCallback,
    ) {
        self.task_receiver.set_execute_callback(callback);
    }

    // ─── Task Manager ────────────────────────────────────────────────

    /// Access the task manager.
    pub fn task_manager(&self) -> &Arc<TaskManager> {
        &self.task_manager
    }

    /// Access the task event buffer.
    pub fn task_event_buffer(&self) -> &Arc<TaskEventBuffer> {
        &self.task_event_buffer
    }

    /// Submit a task through the task manager.
    ///
    /// Registers the task with the TaskManager (which tracks state,
    /// retries, and return object ownership) and then submits it
    /// via the NormalTaskSubmitter.
    pub async fn submit_task_with_manager(
        &self,
        task_spec: TaskSpec,
        max_retries: i32,
    ) -> CoreWorkerResult<Vec<ObjectID>> {
        let return_ids = self.task_manager.add_pending_task(
            self.worker_address.clone(),
            task_spec.clone(),
            max_retries,
        );
        self.normal_task_submitter.submit_task(&task_spec).await?;
        Ok(return_ids)
    }

    /// Complete a task through the task manager.
    pub fn complete_task(
        &self,
        task_id: &[u8],
        return_objects: Vec<(ObjectID, RayObject)>,
    ) -> CoreWorkerResult<()> {
        self.task_manager
            .complete_pending_task(task_id, return_objects)
    }

    /// Fail a task through the task manager, retrying if possible.
    pub fn fail_task(
        &self,
        task_id: &[u8],
        error_message: &str,
        is_oom: bool,
    ) -> CoreWorkerResult<bool> {
        self.task_manager
            .fail_or_retry_pending_task(task_id, error_message, is_oom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor_handle::ActorHandle;
    use ray_proto::ray::rpc;

    fn make_worker() -> CoreWorker {
        CoreWorker::new(CoreWorkerOptions {
            job_id: JobID::from_int(1),
            ..CoreWorkerOptions::default()
        })
    }

    #[test]
    fn test_put_and_contains() {
        let worker = make_worker();
        let oid = ObjectID::from_random();
        assert!(!worker.contains_object(&oid));
        worker
            .put_object(oid, Bytes::from("data"), Bytes::new())
            .unwrap();
        assert!(worker.contains_object(&oid));
    }

    #[test]
    fn test_put_registers_ownership() {
        let worker = make_worker();
        let oid = ObjectID::from_random();
        worker
            .put_object(oid, Bytes::from("owned"), Bytes::new())
            .unwrap();
        // put_object must register the object as owned in the reference counter.
        assert!(worker.reference_counter().owned_by_us(&oid));
        let owner = worker.reference_counter().get_owner(&oid).unwrap();
        assert_eq!(owner.ip_address, worker.worker_address().ip_address);
    }

    #[test]
    fn test_put_duplicate_errors() {
        let worker = make_worker();
        let oid = ObjectID::from_random();
        worker
            .put_object(oid, Bytes::from("first"), Bytes::new())
            .unwrap();
        let err = worker
            .put_object(oid, Bytes::from("second"), Bytes::new())
            .unwrap_err();
        assert!(matches!(err, CoreWorkerError::ObjectAlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_get_objects() {
        let worker = make_worker();
        let oid = ObjectID::from_random();
        worker
            .put_object(oid, Bytes::from("hello"), Bytes::new())
            .unwrap();
        let results = worker
            .get_objects(&[oid], Duration::from_millis(100))
            .await
            .unwrap();
        assert!(results[0].is_some());
        assert_eq!(results[0].as_ref().unwrap().data.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn test_get_objects_timeout_returns_none() {
        let worker = make_worker();
        let oid = ObjectID::from_random();
        let results = worker
            .get_objects(&[oid], Duration::from_millis(10))
            .await
            .unwrap();
        assert!(results[0].is_none());
    }

    #[tokio::test]
    async fn test_get_objects_multiple() {
        let worker = make_worker();
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();
        worker
            .put_object(oid1, Bytes::from("aaa"), Bytes::new())
            .unwrap();
        worker
            .put_object(oid2, Bytes::from("bbb"), Bytes::new())
            .unwrap();
        let results = worker
            .get_objects(&[oid1, oid2], Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(results[0].as_ref().unwrap().data.as_ref(), b"aaa");
        assert_eq!(results[1].as_ref().unwrap().data.as_ref(), b"bbb");
    }

    #[test]
    fn test_delete_objects() {
        let worker = make_worker();
        let oid = ObjectID::from_random();
        worker
            .put_object(oid, Bytes::from("del"), Bytes::new())
            .unwrap();
        assert!(worker.contains_object(&oid));
        worker.delete_objects(&[oid]);
        assert!(!worker.contains_object(&oid));
    }

    #[tokio::test]
    async fn test_wait_objects() {
        let worker = make_worker();
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();
        worker
            .put_object(oid1, Bytes::from("w"), Bytes::new())
            .unwrap();
        // Wait for 1 of 2 objects. oid1 is ready, oid2 is not.
        let ready = worker
            .wait(&[oid1, oid2], 1, Duration::from_millis(50))
            .await
            .unwrap();
        assert!(ready[0]); // oid1 is ready
        assert!(!ready[1]); // oid2 is not
    }

    #[test]
    fn test_create_and_kill_actor() {
        let worker = make_worker();
        let aid = ActorID::from_random();
        let handle = ActorHandle::from_proto(rpc::ActorHandle {
            actor_id: aid.binary(),
            name: "test_actor".to_string(),
            ..Default::default()
        });
        worker.create_actor(aid, handle).unwrap();
        // Actor should be registered.
        assert!(worker.actor_manager().get_actor_handle(&aid).is_some());
        assert_eq!(
            worker
                .actor_manager()
                .get_actor_handle(&aid)
                .unwrap()
                .name(),
            "test_actor"
        );

        worker.kill_actor(&aid, false, false).unwrap();
        // Actor should be removed.
        assert!(worker.actor_manager().get_actor_handle(&aid).is_none());
    }

    #[test]
    fn test_add_remove_local_reference() {
        let worker = make_worker();
        let oid = ObjectID::from_random();
        worker.add_local_reference(oid);
        assert!(worker.reference_counter().has_reference(&oid));
        let deleted = worker.remove_local_reference(&oid);
        assert_eq!(deleted, vec![oid]);
        assert!(!worker.reference_counter().has_reference(&oid));
    }

    #[test]
    fn test_context_accessors() {
        let opts = CoreWorkerOptions {
            job_id: JobID::from_int(42),
            ..CoreWorkerOptions::default()
        };
        let wid = opts.worker_id;
        let worker = CoreWorker::new(opts);
        assert_eq!(worker.current_job_id(), JobID::from_int(42));
        assert!(worker.current_task_id().is_nil());
        assert_eq!(worker.worker_id(), wid);
    }

    #[tokio::test]
    async fn test_submit_normal_task() {
        let worker = make_worker();
        assert_eq!(worker.num_pending_normal_tasks(), 0);
        let spec = rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            name: "test_task".to_string(),
            ..Default::default()
        };
        worker.submit_task(&spec).await.unwrap();
        assert_eq!(worker.num_pending_normal_tasks(), 1);
    }

    #[tokio::test]
    async fn test_submit_actor_task_requires_registration() {
        let worker = make_worker();
        let aid = ActorID::from_random();
        let spec = rpc::TaskSpec::default();
        // Submitting to unregistered actor should fail.
        let err = worker.submit_actor_task(&aid, spec).await.unwrap_err();
        assert!(matches!(err, CoreWorkerError::ActorNotFound(_)));
    }

    #[test]
    fn test_set_actor_task_send_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let worker = make_worker();
        let aid = ActorID::from_random();

        // Register the actor first.
        let handle = ActorHandle::from_proto(rpc::ActorHandle {
            actor_id: aid.binary(),
            name: "callback_actor".to_string(),
            ..Default::default()
        });
        worker.create_actor(aid, handle).unwrap();

        // Connect the actor to a fake address so tasks can be flushed.
        let addr = rpc::Address {
            ip_address: "127.0.0.1".to_string(),
            port: 9999,
            worker_id: worker.worker_id().binary(),
            ..Default::default()
        };
        worker.connect_actor(&aid, addr);

        // Set a callback that records invocation.
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        worker.set_actor_task_send_callback(Arc::new(move |_spec, _addr| {
            called_clone.store(true, Ordering::Relaxed);
            Ok(())
        }));

        // Submit an actor task. The callback should be invoked during flush.
        let spec = rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            name: "callback_test_task".to_string(),
            ..Default::default()
        };
        // Use a runtime to drive the async submission.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            worker.submit_actor_task(&aid, spec).await.unwrap();
        });

        // The callback should have been invoked.
        assert!(
            called.load(Ordering::Relaxed),
            "actor task send callback was not invoked"
        );
    }

    #[tokio::test]
    async fn test_set_task_execution_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let worker = make_worker();

        // Set a task execution callback that records invocation.
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        worker.set_task_execution_callback(Arc::new(move |_spec| {
            called_clone.store(true, Ordering::Relaxed);
            Ok(crate::task_receiver::TaskResult::default())
        }));

        // Push a task through the task receiver.
        let spec = rpc::TaskSpec {
            task_id: TaskID::from_random().binary(),
            name: "exec_callback_test".to_string(),
            ..Default::default()
        };
        let req = ray_proto::ray::rpc::PushTaskRequest {
            intended_worker_id: worker.worker_id().binary(),
            task_spec: Some(spec),
            sequence_number: 0,
            ..Default::default()
        };
        let reply = worker.task_receiver().handle_push_task(req).await.unwrap();
        assert!(!reply.is_retryable_error);

        // The callback should have been invoked.
        assert!(
            called.load(Ordering::Relaxed),
            "task execution callback was not invoked"
        );
    }

    #[test]
    fn test_set_task_execution_callback_replaces_previous() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let worker = make_worker();

        let counter = Arc::new(AtomicUsize::new(0));

        // Set first callback (increments by 1).
        let c1 = counter.clone();
        worker.set_task_execution_callback(Arc::new(move |_spec| {
            c1.fetch_add(1, Ordering::Relaxed);
            Ok(crate::task_receiver::TaskResult::default())
        }));

        // Set second callback (increments by 10), replacing the first.
        let c2 = counter.clone();
        worker.set_task_execution_callback(Arc::new(move |_spec| {
            c2.fetch_add(10, Ordering::Relaxed);
            Ok(crate::task_receiver::TaskResult::default())
        }));

        // Execute a task -- should use the second callback.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let spec = rpc::TaskSpec {
                task_id: TaskID::from_random().binary(),
                name: "replace_test".to_string(),
                ..Default::default()
            };
            let req = ray_proto::ray::rpc::PushTaskRequest {
                intended_worker_id: worker.worker_id().binary(),
                task_spec: Some(spec),
                sequence_number: 0,
                ..Default::default()
            };
            worker.task_receiver().handle_push_task(req).await.unwrap();
        });

        // Counter should be 10 (second callback), not 1 (first callback).
        assert_eq!(counter.load(Ordering::Relaxed), 10);
    }
}
