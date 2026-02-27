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
use crate::context::WorkerContext;
use crate::dependency_resolver::DependencyResolver;
use crate::error::{CoreWorkerError, CoreWorkerResult};
use crate::memory_store::{CoreWorkerMemoryStore, RayObject};
use crate::normal_task_submitter::NormalTaskSubmitter;
use crate::options::CoreWorkerOptions;
use crate::reference_counter::ReferenceCounter;

/// The core worker orchestrating task submission, object management,
/// actor management, and reference counting.
pub struct CoreWorker {
    context: WorkerContext,
    memory_store: Arc<CoreWorkerMemoryStore>,
    reference_counter: Arc<ReferenceCounter>,
    actor_manager: Arc<ActorManager>,
    normal_task_submitter: NormalTaskSubmitter,
    actor_task_submitter: ActorTaskSubmitter,
    dependency_resolver: DependencyResolver,
    worker_address: Address,
}

impl CoreWorker {
    /// Create a new CoreWorker from options.
    pub fn new(options: CoreWorkerOptions) -> Self {
        let context = WorkerContext::new(
            options.worker_type,
            options.worker_id,
            options.job_id,
        );
        let reference_counter = Arc::new(ReferenceCounter::new());
        let normal_task_submitter =
            NormalTaskSubmitter::new(reference_counter.clone());

        let worker_address = Address {
            node_id: options.node_id.binary(),
            ip_address: options.node_ip_address.clone(),
            port: 0,
            worker_id: options.worker_id.binary(),
        };

        Self {
            context,
            memory_store: Arc::new(CoreWorkerMemoryStore::new()),
            reference_counter,
            actor_manager: Arc::new(ActorManager::new()),
            normal_task_submitter,
            actor_task_submitter: ActorTaskSubmitter::new(),
            dependency_resolver: DependencyResolver::new(),
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
        // Track as owned.
        self.reference_counter.add_owned_object(
            object_id,
            self.worker_address.clone(),
            vec![],
        );
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
    pub fn create_actor(
        &self,
        actor_id: ActorID,
        handle: ActorHandle,
    ) -> CoreWorkerResult<()> {
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
        _no_restart: bool,
    ) -> CoreWorkerResult<()> {
        self.actor_task_submitter.disconnect_actor(actor_id);
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

    /// Number of normal pending tasks.
    pub fn num_pending_normal_tasks(&self) -> usize {
        self.normal_task_submitter.num_pending_tasks()
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
        assert_eq!(worker.actor_manager().get_actor_handle(&aid).unwrap().name(), "test_actor");

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
}
