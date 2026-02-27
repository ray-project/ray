// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Python-facing CoreWorker wrapper.
//!
//! Owns an `Arc<CoreWorker>` and a `tokio::runtime::Runtime`, bridging
//! sync Python calls into async Rust via `runtime.block_on()`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use ray_common::id::{ActorID, JobID, ObjectID, TaskID, WorkerID};
use ray_core_worker::error::CoreWorkerResult;
use ray_core_worker::memory_store::RayObject;
use ray_core_worker::options::CoreWorkerOptions;
use ray_core_worker::CoreWorker;

/// Python-facing wrapper around `CoreWorker`.
pub struct PyCoreWorker {
    inner: Arc<CoreWorker>,
    runtime: tokio::runtime::Runtime,
}

impl PyCoreWorker {
    /// Create a new PyCoreWorker.
    pub fn new(options: CoreWorkerOptions) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        let inner = Arc::new(CoreWorker::new(options));
        Self { inner, runtime }
    }

    /// Put an object.
    pub fn put_object(
        &self,
        object_id: ObjectID,
        data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> CoreWorkerResult<()> {
        self.inner
            .put_object(object_id, Bytes::from(data), Bytes::from(metadata))
    }

    /// Get objects, blocking until available or timeout.
    pub fn get_objects(
        &self,
        object_ids: &[ObjectID],
        timeout_ms: u64,
    ) -> CoreWorkerResult<Vec<Option<RayObject>>> {
        let timeout = Duration::from_millis(timeout_ms);
        self.runtime
            .block_on(self.inner.get_objects(object_ids, timeout))
    }

    /// Wait for objects.
    pub fn wait(
        &self,
        object_ids: &[ObjectID],
        num_objects: usize,
        timeout_ms: u64,
    ) -> CoreWorkerResult<Vec<bool>> {
        let timeout = Duration::from_millis(timeout_ms);
        self.runtime
            .block_on(self.inner.wait(object_ids, num_objects, timeout))
    }

    /// Free (delete) objects.
    pub fn free_objects(&self, object_ids: &[ObjectID]) {
        self.inner.delete_objects(object_ids);
    }

    /// Check if an object exists.
    pub fn contains_object(&self, object_id: &ObjectID) -> bool {
        self.inner.contains_object(object_id)
    }

    /// Submit a normal task.
    pub fn submit_task(
        &self,
        task_spec: &ray_proto::ray::rpc::TaskSpec,
    ) -> CoreWorkerResult<()> {
        self.runtime.block_on(self.inner.submit_task(task_spec))
    }

    /// Submit an actor task.
    pub fn submit_actor_task(
        &self,
        actor_id: &ActorID,
        task_spec: ray_proto::ray::rpc::TaskSpec,
    ) -> CoreWorkerResult<()> {
        self.runtime
            .block_on(self.inner.submit_actor_task(actor_id, task_spec))
    }

    /// Create an actor.
    pub fn create_actor(
        &self,
        actor_id: ActorID,
        handle: ray_core_worker::actor_handle::ActorHandle,
    ) -> CoreWorkerResult<()> {
        self.inner.create_actor(actor_id, handle)
    }

    /// Kill an actor.
    pub fn kill_actor(
        &self,
        actor_id: &ActorID,
        force_kill: bool,
        no_restart: bool,
    ) -> CoreWorkerResult<()> {
        self.inner.kill_actor(actor_id, force_kill, no_restart)
    }

    /// Get the current task ID.
    pub fn get_current_task_id(&self) -> TaskID {
        self.inner.current_task_id()
    }

    /// Get the current job ID.
    pub fn get_current_job_id(&self) -> JobID {
        self.inner.current_job_id()
    }

    /// Get the worker ID.
    pub fn get_worker_id(&self) -> WorkerID {
        self.inner.worker_id()
    }

    /// Access the underlying CoreWorker.
    pub fn inner(&self) -> &Arc<CoreWorker> {
        &self.inner
    }
}
