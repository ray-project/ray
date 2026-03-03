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
#[cfg_attr(feature = "python", pyo3::pyclass(module = "_raylet"))]
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

// ─── PyO3 methods (only when "python" feature is enabled) ────────────

#[cfg(feature = "python")]
#[pyo3::pymethods]
impl PyCoreWorker {
    /// Initialize a core worker from Python.
    ///
    /// Arguments:
    ///   worker_type: 0=WORKER, 1=DRIVER
    ///   node_ip_address: e.g. "127.0.0.1"
    ///   gcs_address: "host:port" of the GCS server
    ///   job_id_int: integer job ID
    #[new]
    fn py_new(
        worker_type: i32,
        node_ip_address: String,
        gcs_address: String,
        job_id_int: u32,
    ) -> pyo3::PyResult<Self> {
        use crate::common::PyWorkerType;
        let wt = PyWorkerType::from_i32(worker_type)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(
                format!("invalid worker_type: {}", worker_type),
            ))?;
        let options = CoreWorkerOptions {
            worker_type: wt.to_core(),
            node_ip_address,
            gcs_address,
            job_id: JobID::from_int(job_id_int),
            ..CoreWorkerOptions::default()
        };
        Ok(Self::new(options))
    }

    /// Put an object, returning the object ID.
    ///
    /// Arguments:
    ///   data: the serialized object bytes
    ///   metadata: optional metadata bytes
    ///   object_id: optional binary object ID (generated if None)
    #[pyo3(name = "put", signature = (data, metadata, object_id=None))]
    fn py_put(
        &self,
        data: &[u8],
        metadata: &[u8],
        object_id: Option<&[u8]>,
    ) -> pyo3::PyResult<crate::ids::PyObjectID> {
        let oid = match object_id {
            Some(bytes) => ObjectID::from_binary(bytes),
            None => ObjectID::from_random(),
        };
        self.put_object(oid, data.to_vec(), metadata.to_vec())
            .map_err(crate::common::to_py_err)?;
        Ok(crate::ids::PyObjectID::from_inner(oid))
    }

    /// Get objects by their binary IDs.
    ///
    /// Returns a list of (data_bytes, metadata_bytes) or None for each object.
    #[pyo3(name = "get")]
    fn py_get(
        &self,
        object_ids: Vec<Vec<u8>>,
        timeout_ms: u64,
    ) -> pyo3::PyResult<Vec<Option<(Vec<u8>, Vec<u8>)>>> {
        let oids: Vec<ObjectID> = object_ids
            .iter()
            .map(|b| ObjectID::from_binary(b))
            .collect();
        let results = self
            .get_objects(&oids, timeout_ms)
            .map_err(crate::common::to_py_err)?;
        Ok(results
            .into_iter()
            .map(|opt| {
                opt.map(|obj| (obj.data.to_vec(), obj.metadata.to_vec()))
            })
            .collect())
    }

    /// Wait for at least num_objects to be ready.
    ///
    /// Returns a list of booleans indicating which objects are ready.
    #[pyo3(name = "wait")]
    fn py_wait(
        &self,
        object_ids: Vec<Vec<u8>>,
        num_objects: usize,
        timeout_ms: u64,
    ) -> pyo3::PyResult<Vec<bool>> {
        let oids: Vec<ObjectID> = object_ids
            .iter()
            .map(|b| ObjectID::from_binary(b))
            .collect();
        self.wait(&oids, num_objects, timeout_ms)
            .map_err(crate::common::to_py_err)
    }

    /// Delete (free) objects by their binary IDs.
    #[pyo3(name = "free")]
    fn py_free(&self, object_ids: Vec<Vec<u8>>) {
        let oids: Vec<ObjectID> = object_ids
            .iter()
            .map(|b| ObjectID::from_binary(b))
            .collect();
        self.free_objects(&oids);
    }

    /// Check if an object exists in the local store.
    #[pyo3(name = "contains")]
    fn py_contains(&self, object_id: &[u8]) -> bool {
        let oid = ObjectID::from_binary(object_id);
        self.contains_object(&oid)
    }

    /// Get the current task ID as a hex string.
    #[pyo3(name = "current_task_id")]
    fn py_current_task_id(&self) -> crate::ids::PyTaskID {
        crate::ids::PyTaskID::from_inner(self.get_current_task_id())
    }

    /// Get the current job ID.
    #[pyo3(name = "current_job_id")]
    fn py_current_job_id(&self) -> crate::ids::PyJobID {
        crate::ids::PyJobID::from_inner(self.get_current_job_id())
    }

    /// Get the worker ID.
    #[pyo3(name = "worker_id")]
    fn py_worker_id(&self) -> crate::ids::PyWorkerID {
        crate::ids::PyWorkerID::from_inner(self.get_worker_id())
    }

    /// Kill an actor by binary actor ID.
    #[pyo3(name = "kill_actor")]
    fn py_kill_actor(
        &self,
        actor_id: &[u8],
        force_kill: bool,
        no_restart: bool,
    ) -> pyo3::PyResult<()> {
        let aid = ActorID::from_binary(actor_id);
        self.kill_actor(&aid, force_kill, no_restart)
            .map_err(crate::common::to_py_err)
    }

    /// Add a local reference to an object.
    #[pyo3(name = "add_local_reference")]
    fn py_add_local_reference(&self, object_id: &[u8]) {
        let oid = ObjectID::from_binary(object_id);
        self.inner.add_local_reference(oid);
    }

    /// Remove a local reference to an object.
    #[pyo3(name = "remove_local_reference")]
    fn py_remove_local_reference(&self, object_id: &[u8]) -> Vec<Vec<u8>> {
        let oid = ObjectID::from_binary(object_id);
        self.inner
            .remove_local_reference(&oid)
            .into_iter()
            .map(|id| id.binary())
            .collect()
    }

    /// Get the number of pending normal tasks.
    #[pyo3(name = "num_pending_tasks")]
    fn py_num_pending_tasks(&self) -> usize {
        self.inner.num_pending_normal_tasks()
    }

    /// Get the number of currently executing tasks.
    #[pyo3(name = "num_executing_tasks")]
    fn py_num_executing_tasks(&self) -> usize {
        self.inner.num_executing_tasks()
    }
}
