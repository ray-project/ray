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
}
