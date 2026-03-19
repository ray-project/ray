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
    pub fn submit_task(&self, task_spec: &ray_proto::ray::rpc::TaskSpec) -> CoreWorkerResult<()> {
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
    ///   worker_id: optional PyWorkerID (random if None)
    ///   node_id: optional PyNodeID (nil if None)
    #[new]
    #[pyo3(signature = (worker_type, node_ip_address, gcs_address, job_id_int, worker_id=None, node_id=None, max_concurrency=0))]
    fn py_new(
        worker_type: i32,
        node_ip_address: String,
        gcs_address: String,
        job_id_int: u32,
        worker_id: Option<&crate::ids::PyWorkerID>,
        node_id: Option<&crate::ids::PyNodeID>,
        max_concurrency: usize,
    ) -> pyo3::PyResult<Self> {
        use crate::common::PyWorkerType;
        use ray_common::id::NodeID;
        let wt = PyWorkerType::from_i32(worker_type).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid worker_type: {}", worker_type))
        })?;
        let wid = worker_id
            .map(|w| *w.inner())
            .unwrap_or_else(WorkerID::from_random);
        let nid = node_id.map(|n| *n.inner()).unwrap_or_else(NodeID::nil);
        let options = CoreWorkerOptions {
            worker_type: wt.to_core(),
            node_ip_address,
            gcs_address,
            job_id: JobID::from_int(job_id_int),
            worker_id: wid,
            node_id: nid,
            max_concurrency,
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
        py: pyo3::Python<'_>,
        object_ids: Vec<Vec<u8>>,
        timeout_ms: u64,
    ) -> pyo3::PyResult<Vec<Option<(pyo3::PyObject, pyo3::PyObject)>>> {
        let oids: Vec<ObjectID> = object_ids
            .iter()
            .map(|b| ObjectID::from_binary(b))
            .collect();
        // Release the GIL while waiting for objects.
        let results = py
            .allow_threads(|| self.get_objects(&oids, timeout_ms))
            .map_err(crate::common::to_py_err)?;
        let mut out = Vec::with_capacity(results.len());
        for opt in results {
            match opt {
                Some(obj) if obj.metadata.as_ref() == b"ERROR" => {
                    let msg = String::from_utf8_lossy(&obj.data);
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "RayTaskError: {}",
                        msg
                    )));
                }
                Some(obj) => {
                    let data = pyo3::types::PyBytes::new_bound(py, &obj.data).into();
                    let meta = pyo3::types::PyBytes::new_bound(py, &obj.metadata).into();
                    out.push(Some((data, meta)));
                }
                None => out.push(None),
            }
        }
        Ok(out)
    }

    /// Wait for at least num_objects to be ready.
    ///
    /// Returns a list of booleans indicating which objects are ready.
    #[pyo3(name = "wait")]
    fn py_wait(
        &self,
        py: pyo3::Python<'_>,
        object_ids: Vec<Vec<u8>>,
        num_objects: usize,
        timeout_ms: u64,
    ) -> pyo3::PyResult<Vec<bool>> {
        let oids: Vec<ObjectID> = object_ids
            .iter()
            .map(|b| ObjectID::from_binary(b))
            .collect();
        // Release the GIL while waiting.
        py.allow_threads(|| self.wait(&oids, num_objects, timeout_ms))
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

    /// Set a Python callable as the task execution callback.
    ///
    /// The callback receives (method_name: str, args: list[bytes]) and must
    /// return bytes (the result data).
    #[pyo3(name = "set_task_callback")]
    fn py_set_task_callback(&self, callback: pyo3::PyObject) -> pyo3::PyResult<()> {
        use ray_core_worker::error::CoreWorkerError;
        use ray_core_worker::task_receiver::{TaskExecutionCallback, TaskResult};
        use ray_proto::ray::rpc as task_rpc;

        let cb: TaskExecutionCallback = Arc::new(move |spec: &task_rpc::TaskSpec| {
            let name = spec.name.clone();
            let num_returns = std::cmp::max(spec.num_returns, 1) as usize;
            let args: Vec<Vec<u8>> = spec.args.iter().map(|a| a.data.clone()).collect();

            let result_data =
                pyo3::Python::with_gil(|py| -> Result<Vec<Vec<u8>>, CoreWorkerError> {
                    // Convert args to Python list of bytes objects.
                    let py_args: Vec<pyo3::PyObject> = args
                        .iter()
                        .map(|a| pyo3::types::PyBytes::new_bound(py, a).into())
                        .collect();
                    let py_args_list = pyo3::types::PyList::new_bound(py, &py_args);
                    let result = callback
                        .call1(py, (&name, py_args_list, num_returns))
                        .map_err(|e| {
                            CoreWorkerError::Internal(format!("Python callback error: {}", e))
                        })?;
                    if num_returns <= 1 {
                        // Single return: callback returns bytes.
                        let bytes: Vec<u8> = result.extract(py).map_err(|e| {
                            CoreWorkerError::Internal(format!("callback must return bytes: {}", e))
                        })?;
                        Ok(vec![bytes])
                    } else {
                        // Multi-return: callback returns list[bytes].
                        let list: Vec<Vec<u8>> = result.extract(py).map_err(|e| {
                            CoreWorkerError::Internal(format!(
                                "callback must return list[bytes] for multi-return: {}",
                                e
                            ))
                        })?;
                        Ok(list)
                    }
                })?;

            let task_id = TaskID::from_binary(&spec.task_id);
            let return_objects: Vec<task_rpc::ReturnObject> = result_data
                .into_iter()
                .enumerate()
                .map(|(i, data)| task_rpc::ReturnObject {
                    object_id: ObjectID::from_index(&task_id, (i + 1) as u32).binary(),
                    data,
                    metadata: b"python".to_vec(),
                    ..Default::default()
                })
                .collect();
            Ok(TaskResult {
                return_objects,
                ..Default::default()
            })
        });
        self.inner.set_task_execution_callback(cb);
        Ok(())
    }

    /// Start a gRPC server for this CoreWorker and return the bound port.
    ///
    /// Arguments:
    ///   bind_ip: IP address to bind to (default "0.0.0.0")
    ///   bind_port: port to bind to (default 0 = random)
    #[pyo3(name = "start_grpc_server", signature = (bind_ip="0.0.0.0", bind_port=0))]
    fn py_start_grpc_server(&self, bind_ip: &str, bind_port: u16) -> pyo3::PyResult<u16> {
        use ray_core_worker::grpc_service::CoreWorkerServiceImpl;
        use ray_proto::ray::rpc as grpc_rpc;

        let bind_addr = format!("{}:{}", bind_ip, bind_port);
        let core_worker = Arc::clone(&self.inner);
        let port = self.runtime.block_on(async {
            let svc = CoreWorkerServiceImpl { core_worker };
            let listener = tokio::net::TcpListener::bind(&bind_addr)
                .await
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("bind failed: {}", e))
                })?;
            let addr = listener.local_addr().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("local_addr failed: {}", e))
            })?;
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(
                        grpc_rpc::core_worker_service_server::CoreWorkerServiceServer::new(svc)
                            .max_decoding_message_size(512 * 1024 * 1024)
                            .max_encoding_message_size(512 * 1024 * 1024),
                    )
                    .serve_with_incoming(incoming)
                    .await
                    .ok();
            });
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok::<u16, pyo3::PyErr>(addr.port())
        })?;
        Ok(port)
    }

    /// Set up an actor on this (driver) CoreWorker.
    ///
    /// Creates the actor handle, sets the gRPC task send callback, and connects
    /// the actor to the given worker address.
    #[pyo3(name = "setup_actor")]
    #[allow(clippy::too_many_arguments)]
    fn py_setup_actor(
        &self,
        actor_id: &crate::ids::PyActorID,
        name: &str,
        namespace: &str,
        worker_ip: &str,
        worker_port: u16,
        node_id: &crate::ids::PyNodeID,
        worker_id: &crate::ids::PyWorkerID,
    ) -> pyo3::PyResult<()> {
        use ray_proto::ray::rpc as actor_rpc;

        let aid = *actor_id.inner();
        let nid = *node_id.inner();
        let wid = *worker_id.inner();

        // Create and register actor handle.
        let handle =
            ray_core_worker::actor_handle::ActorHandle::from_proto(actor_rpc::ActorHandle {
                actor_id: aid.binary(),
                name: name.to_string(),
                ray_namespace: namespace.to_string(),
                ..Default::default()
            });
        self.inner
            .create_actor(aid, handle)
            .map_err(crate::common::to_py_err)?;

        // Set actor task send callback: gRPC PushTask with return capture.
        // Uses channel + tokio::spawn to avoid nested block_on panic.
        // The spawned task runs on a tokio worker thread, while rx.recv()
        // blocks the calling thread (which is inside runtime.block_on).
        let driver_store = Arc::clone(self.inner.memory_store());
        self.inner
            .set_actor_task_send_callback(Arc::new(move |spec, addr| {
                let endpoint = format!("http://{}:{}", addr.ip_address, addr.port);
                let spec_clone = spec.clone();
                let wid_bytes = addr.worker_id.clone();
                let store = Arc::clone(&driver_store);
                let (tx, rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
                tokio::spawn(async move {
                    let result = async {
                        let channel = tonic::transport::Endpoint::from_shared(endpoint)
                            .map_err(|e| format!("invalid endpoint: {}", e))?
                            .connect()
                            .await
                            .map_err(|e| format!("connect failed: {}", e))?;
                        let mut client =
                            actor_rpc::core_worker_service_client::CoreWorkerServiceClient::new(
                                channel,
                            )
                            .max_decoding_message_size(512 * 1024 * 1024)
                            .max_encoding_message_size(512 * 1024 * 1024);
                        let response = client
                            .push_task(actor_rpc::PushTaskRequest {
                                intended_worker_id: wid_bytes,
                                task_spec: Some(spec_clone),
                                ..Default::default()
                            })
                            .await
                            .map_err(|e| format!("push_task failed: {}", e))?;
                        let reply = response.into_inner();
                        // Check for task execution error (actor callback crashed).
                        if !reply.task_execution_error.is_empty() && reply.return_objects.is_empty()
                        {
                            return Err(format!("ACTOR_TASK_ERROR:{}", reply.task_execution_error));
                        }
                        for ret_obj in &reply.return_objects {
                            let oid = ObjectID::from_binary(&ret_obj.object_id);
                            let ray_obj = RayObject::new(
                                Bytes::copy_from_slice(&ret_obj.data),
                                Bytes::copy_from_slice(&ret_obj.metadata),
                                Vec::new(),
                            );
                            let _ = store.put(oid, ray_obj);
                        }
                        Ok(())
                    }
                    .await;
                    let _ = tx.send(result);
                });
                rx.recv()
                    .map_err(|_| {
                        ray_core_worker::error::CoreWorkerError::Internal(
                            "actor task send channel closed".into(),
                        )
                    })?
                    .map_err(|e| ray_core_worker::error::CoreWorkerError::Internal(e))
            }));

        // Connect actor to worker address.
        let address = actor_rpc::Address {
            node_id: nid.binary(),
            ip_address: worker_ip.to_string(),
            port: worker_port as i32,
            worker_id: wid.binary(),
        };
        self.inner.connect_actor(&aid, address);

        Ok(())
    }

    /// Submit an actor method call.
    ///
    /// Arguments:
    ///   actor_id: the actor to call
    ///   method_name: the method name (e.g. "increment")
    ///   args: list of byte arrays (each is a serialized argument)
    ///
    /// Returns the ObjectID of the return value.
    #[pyo3(name = "submit_actor_method")]
    fn py_submit_actor_method(
        &self,
        py: pyo3::Python<'_>,
        actor_id: &crate::ids::PyActorID,
        method_name: &str,
        args: Vec<Vec<u8>>,
    ) -> pyo3::PyResult<crate::ids::PyObjectID> {
        use ray_proto::ray::rpc as submit_rpc;

        let aid = *actor_id.inner();
        let task_id = TaskID::from_random();
        let return_oid = ObjectID::from_index(&task_id, 1);
        let task_args: Vec<submit_rpc::TaskArg> = args
            .into_iter()
            .map(|data| submit_rpc::TaskArg {
                data,
                ..Default::default()
            })
            .collect();
        let spec = submit_rpc::TaskSpec {
            task_id: task_id.binary(),
            name: method_name.to_string(),
            num_returns: 1,
            args: task_args,
            ..Default::default()
        };
        // Release the GIL during block_on: the actor task send callback
        // blocks on rx.recv() while the worker's Python callback needs the GIL.
        py.allow_threads(|| {
            self.runtime
                .block_on(self.inner.submit_actor_task(&aid, spec))
        })
        .map_err(crate::common::to_py_err)?;
        Ok(crate::ids::PyObjectID::from_inner(return_oid))
    }

    /// Configure non-actor task dispatch to a specific worker.
    ///
    /// Sets up the NormalTaskSubmitter with a direct-dispatch raylet client
    /// that always grants a lease to the given worker, and a dispatch callback
    /// that sends PushTask via gRPC and stores return objects in the driver's
    /// memory store.
    ///
    /// Arguments:
    ///   worker_ip: IP address of the task worker
    ///   worker_port: gRPC port of the task worker
    ///   worker_id: binary worker ID of the task worker
    #[pyo3(name = "setup_task_dispatch")]
    fn py_setup_task_dispatch(
        &self,
        worker_ip: &str,
        worker_port: u16,
        worker_id: &crate::ids::PyWorkerID,
    ) -> pyo3::PyResult<()> {
        use ray_proto::ray::rpc as dispatch_rpc;

        let wid = *worker_id.inner();
        let ip = worker_ip.to_string();
        let port = worker_port;

        // Configure the raylet client to always grant a lease to our worker.
        let raylet = Arc::new(DirectDispatchRayletClient {
            worker_address: dispatch_rpc::Address {
                node_id: vec![],
                ip_address: ip.clone(),
                port: port as i32,
                worker_id: wid.binary(),
            },
        });
        let submitter = self.inner.normal_task_submitter();
        submitter.set_raylet_client(raylet);

        // Set the dispatch callback: PushTask + capture return objects.
        let driver_store = Arc::clone(self.inner.memory_store());
        submitter.set_dispatch_callback(Box::new(move |spec, addr| {
            let endpoint = format!("http://{}:{}", addr.ip_address, addr.port);
            let spec_clone = spec.clone();
            let wid_bytes = addr.worker_id.clone();
            let store = Arc::clone(&driver_store);
            let (tx, rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
            tokio::spawn(async move {
                let result = async {
                    let channel = tonic::transport::Endpoint::from_shared(endpoint)
                        .map_err(|e| format!("invalid endpoint: {}", e))?
                        .connect()
                        .await
                        .map_err(|e| format!("connect failed: {}", e))?;
                    let mut client =
                        dispatch_rpc::core_worker_service_client::CoreWorkerServiceClient::new(
                            channel,
                        )
                        .max_decoding_message_size(512 * 1024 * 1024)
                        .max_encoding_message_size(512 * 1024 * 1024);
                    let response = client
                        .push_task(dispatch_rpc::PushTaskRequest {
                            intended_worker_id: wid_bytes,
                            task_spec: Some(spec_clone),
                            ..Default::default()
                        })
                        .await
                        .map_err(|e| format!("push_task failed: {}", e))?;
                    let reply = response.into_inner();
                    // Check for task execution error (e.g. Python callback raised).
                    if !reply.task_execution_error.is_empty() && reply.return_objects.is_empty() {
                        return Err(format!("TASK_ERROR:{}", reply.task_execution_error));
                    }
                    for ret_obj in &reply.return_objects {
                        let oid = ObjectID::from_binary(&ret_obj.object_id);
                        let ray_obj = RayObject::new(
                            Bytes::copy_from_slice(&ret_obj.data),
                            Bytes::copy_from_slice(&ret_obj.metadata),
                            Vec::new(),
                        );
                        let _ = store.put(oid, ray_obj);
                    }
                    Ok(())
                }
                .await;
                let _ = tx.send(result);
            });
            rx.recv()
                .map_err(|_| {
                    ray_core_worker::error::CoreWorkerError::Internal(
                        "task dispatch channel closed".into(),
                    )
                })?
                .map_err(ray_core_worker::error::CoreWorkerError::Internal)
        }));

        Ok(())
    }

    /// Set up multi-worker round-robin task dispatch for multi-node execution.
    ///
    /// Instead of dispatching to a single worker, this distributes tasks
    /// across all provided workers in round-robin order. Each worker is
    /// identified by its IP address, gRPC port, and worker ID.
    ///
    /// Arguments:
    ///   workers: list of (ip, port, worker_id_hex) tuples
    #[pyo3(name = "setup_multi_worker_dispatch")]
    fn py_setup_multi_worker_dispatch(
        &self,
        workers: Vec<(String, u16, String)>,
    ) -> pyo3::PyResult<()> {
        use ray_proto::ray::rpc as dispatch_rpc;

        if workers.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "at least one worker required",
            ));
        }

        let worker_addresses: Vec<dispatch_rpc::Address> = workers
            .iter()
            .map(|(ip, port, wid_hex)| {
                let wid_bytes = hex::decode(wid_hex).unwrap_or_default();
                dispatch_rpc::Address {
                    node_id: vec![],
                    ip_address: ip.clone(),
                    port: *port as i32,
                    worker_id: wid_bytes,
                }
            })
            .collect();

        let client = Arc::new(RoundRobinDispatchClient {
            workers: worker_addresses,
            next_index: std::sync::atomic::AtomicUsize::new(0),
        });

        let submitter = self.inner.normal_task_submitter();
        submitter.set_raylet_client(client);

        // Set the same dispatch callback as setup_task_dispatch.
        let driver_store = Arc::clone(self.inner.memory_store());
        submitter.set_dispatch_callback(Box::new(move |spec, addr| {
            let endpoint = format!("http://{}:{}", addr.ip_address, addr.port);
            let spec_clone = spec.clone();
            let wid_bytes = addr.worker_id.clone();
            let store = Arc::clone(&driver_store);
            let (tx, rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);
            tokio::spawn(async move {
                let result = async {
                    let channel = tonic::transport::Endpoint::from_shared(endpoint)
                        .map_err(|e| format!("invalid endpoint: {}", e))?
                        .connect()
                        .await
                        .map_err(|e| format!("connect failed: {}", e))?;
                    let mut client =
                        dispatch_rpc::core_worker_service_client::CoreWorkerServiceClient::new(
                            channel,
                        )
                        .max_decoding_message_size(512 * 1024 * 1024)
                        .max_encoding_message_size(512 * 1024 * 1024);
                    let response = client
                        .push_task(dispatch_rpc::PushTaskRequest {
                            intended_worker_id: wid_bytes,
                            task_spec: Some(spec_clone),
                            ..Default::default()
                        })
                        .await
                        .map_err(|e| format!("push_task failed: {}", e))?;
                    let reply = response.into_inner();
                    if !reply.task_execution_error.is_empty() && reply.return_objects.is_empty() {
                        return Err(format!("TASK_ERROR:{}", reply.task_execution_error));
                    }
                    for ret_obj in &reply.return_objects {
                        let oid = ObjectID::from_binary(&ret_obj.object_id);
                        let ray_obj = RayObject::new(
                            Bytes::copy_from_slice(&ret_obj.data),
                            Bytes::copy_from_slice(&ret_obj.metadata),
                            Vec::new(),
                        );
                        let _ = store.put(oid, ray_obj);
                    }
                    Ok(())
                }
                .await;
                let _ = tx.send(result);
            });
            rx.recv()
                .map_err(|_| {
                    ray_core_worker::error::CoreWorkerError::Internal(
                        "task dispatch channel closed".into(),
                    )
                })?
                .map_err(ray_core_worker::error::CoreWorkerError::Internal)
        }));

        Ok(())
    }

    /// Submit a non-actor remote task.
    ///
    /// Arguments:
    ///   name: the function name (e.g. "square")
    ///   args: list of byte arrays (each is a serialized argument)
    ///   num_returns: number of return values (default 1)
    ///   max_retries: max retry attempts on task failure (default 0)
    ///   placement_group_id: optional PG ID bytes for PG scheduling
    ///   placement_group_bundle_index: bundle index within the PG (default -1 = any)
    ///   placement_group_capture_child_tasks: inherit PG in child tasks (default false)
    ///
    /// Returns a list of ObjectIDs for the return values.
    #[pyo3(name = "submit_task", signature = (
        name, args, num_returns=1, max_retries=0,
        placement_group_id=None, placement_group_bundle_index=-1,
        placement_group_capture_child_tasks=false
    ))]
    fn py_submit_task(
        &self,
        py: pyo3::Python<'_>,
        name: &str,
        args: Vec<Vec<u8>>,
        num_returns: u64,
        max_retries: i32,
        placement_group_id: Option<Vec<u8>>,
        placement_group_bundle_index: i64,
        placement_group_capture_child_tasks: bool,
    ) -> pyo3::PyResult<Vec<crate::ids::PyObjectID>> {
        use ray_proto::ray::rpc as task_rpc;

        let task_id = TaskID::from_random();
        let return_oids: Vec<ObjectID> = (1..=num_returns as u32)
            .map(|i| ObjectID::from_index(&task_id, i))
            .collect();

        let task_args: Vec<task_rpc::TaskArg> = args
            .into_iter()
            .map(|data| task_rpc::TaskArg {
                data,
                ..Default::default()
            })
            .collect();

        let scheduling_strategy = placement_group_id.as_ref().map(|pg_bytes| {
            task_rpc::SchedulingStrategy {
                scheduling_strategy: Some(
                    task_rpc::scheduling_strategy::SchedulingStrategy::PlacementGroupSchedulingStrategy(
                        task_rpc::PlacementGroupSchedulingStrategy {
                            placement_group_id: pg_bytes.clone(),
                            placement_group_bundle_index,
                            placement_group_capture_child_tasks,
                        },
                    ),
                ),
            }
        });

        let spec = task_rpc::TaskSpec {
            task_id: task_id.binary(),
            name: name.to_string(),
            num_returns: num_returns,
            args: task_args,
            scheduling_strategy,
            ..Default::default()
        };

        // Release the GIL during block_on: the dispatch callback blocks on
        // rx.recv() while the worker's Python callback needs the GIL.
        let mut retries_left = max_retries;
        loop {
            let submit_result =
                py.allow_threads(|| self.runtime.block_on(self.inner.submit_task(&spec)));
            match submit_result {
                Ok(()) => break,
                Err(ref e) if retries_left > 0 => {
                    let msg = format!("{}", e);
                    if msg.contains("TASK_ERROR:") {
                        retries_left -= 1;
                        tracing::debug!(retries_left, "Task failed, retrying");
                        continue;
                    }
                    // Non-task errors (e.g. connection) — don't retry.
                    return Err(crate::common::to_py_err(submit_result.unwrap_err()));
                }
                Err(e) => {
                    // Final failure — store error objects so py_get returns error.
                    let msg = format!("{}", e);
                    let error_msg = msg.strip_prefix("TASK_ERROR:").unwrap_or(&msg);
                    let store = self.inner.memory_store();
                    for oid in &return_oids {
                        let error_obj = ray_core_worker::memory_store::RayObject::new(
                            bytes::Bytes::from(error_msg.to_string()),
                            bytes::Bytes::from_static(b"ERROR"),
                            Vec::new(),
                        );
                        let _ = store.put(*oid, error_obj);
                    }
                    break;
                }
            }
        }

        Ok(return_oids
            .into_iter()
            .map(crate::ids::PyObjectID::from_inner)
            .collect())
    }
}

// ─── DirectDispatchRayletClient ──────────────────────────────────────

/// A mock `RayletClient` that always immediately grants a worker lease
/// to a pre-configured worker address. Used for direct task dispatch
/// when the driver knows exactly which worker should execute the task.
#[cfg_attr(not(feature = "python"), allow(dead_code))]
struct DirectDispatchRayletClient {
    worker_address: ray_proto::ray::rpc::Address,
}

// ─── RoundRobinDispatchClient ───────────────────────────────────────

/// A `RayletClient` that distributes tasks across multiple workers
/// in round-robin order. Used for multi-node dispatch when the driver
/// knows all worker addresses across the cluster.
#[cfg_attr(not(feature = "python"), allow(dead_code))]
struct RoundRobinDispatchClient {
    workers: Vec<ray_proto::ray::rpc::Address>,
    next_index: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl ray_raylet_rpc_client::RayletClient for RoundRobinDispatchClient {
    async fn request_worker_lease(
        &self,
        _req: ray_proto::ray::rpc::RequestWorkerLeaseRequest,
    ) -> Result<ray_proto::ray::rpc::RequestWorkerLeaseReply, tonic::Status> {
        if self.workers.is_empty() {
            return Err(tonic::Status::unavailable("no workers registered"));
        }
        let idx = self
            .next_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.workers.len();
        Ok(ray_proto::ray::rpc::RequestWorkerLeaseReply {
            worker_address: Some(self.workers[idx].clone()),
            ..Default::default()
        })
    }

    async fn return_worker_lease(
        &self,
        _req: ray_proto::ray::rpc::ReturnWorkerLeaseRequest,
    ) -> Result<ray_proto::ray::rpc::ReturnWorkerLeaseReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn cancel_worker_lease(
        &self,
        _req: ray_proto::ray::rpc::CancelWorkerLeaseRequest,
    ) -> Result<ray_proto::ray::rpc::CancelWorkerLeaseReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn report_worker_backlog(
        &self,
        _req: ray_proto::ray::rpc::ReportWorkerBacklogRequest,
    ) -> Result<ray_proto::ray::rpc::ReportWorkerBacklogReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn prestart_workers(
        &self,
        _req: ray_proto::ray::rpc::PrestartWorkersRequest,
    ) -> Result<ray_proto::ray::rpc::PrestartWorkersReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn prepare_bundle_resources(
        &self,
        _req: ray_proto::ray::rpc::PrepareBundleResourcesRequest,
    ) -> Result<ray_proto::ray::rpc::PrepareBundleResourcesReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn commit_bundle_resources(
        &self,
        _req: ray_proto::ray::rpc::CommitBundleResourcesRequest,
    ) -> Result<ray_proto::ray::rpc::CommitBundleResourcesReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn cancel_resource_reserve(
        &self,
        _req: ray_proto::ray::rpc::CancelResourceReserveRequest,
    ) -> Result<ray_proto::ray::rpc::CancelResourceReserveReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn pin_object_ids(
        &self,
        _req: ray_proto::ray::rpc::PinObjectIDsRequest,
    ) -> Result<ray_proto::ray::rpc::PinObjectIDsReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn get_resource_load(
        &self,
        _req: ray_proto::ray::rpc::GetResourceLoadRequest,
    ) -> Result<ray_proto::ray::rpc::GetResourceLoadReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn shutdown_raylet(
        &self,
        _req: ray_proto::ray::rpc::ShutdownRayletRequest,
    ) -> Result<ray_proto::ray::rpc::ShutdownRayletReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn drain_raylet(
        &self,
        _req: ray_proto::ray::rpc::DrainRayletRequest,
    ) -> Result<ray_proto::ray::rpc::DrainRayletReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn notify_gcs_restart(
        &self,
        _req: ray_proto::ray::rpc::NotifyGcsRestartRequest,
    ) -> Result<ray_proto::ray::rpc::NotifyGcsRestartReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn get_node_stats(
        &self,
        _req: ray_proto::ray::rpc::GetNodeStatsRequest,
    ) -> Result<ray_proto::ray::rpc::GetNodeStatsReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn get_system_config(
        &self,
        _req: ray_proto::ray::rpc::GetSystemConfigRequest,
    ) -> Result<ray_proto::ray::rpc::GetSystemConfigReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn kill_local_actor(
        &self,
        _req: ray_proto::ray::rpc::KillLocalActorRequest,
    ) -> Result<ray_proto::ray::rpc::KillLocalActorReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn cancel_local_task(
        &self,
        _req: ray_proto::ray::rpc::CancelLocalTaskRequest,
    ) -> Result<ray_proto::ray::rpc::CancelLocalTaskReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn global_gc(
        &self,
        _req: ray_proto::ray::rpc::GlobalGcRequest,
    ) -> Result<ray_proto::ray::rpc::GlobalGcReply, tonic::Status> {
        Ok(Default::default())
    }
}

#[async_trait::async_trait]
impl ray_raylet_rpc_client::RayletClient for DirectDispatchRayletClient {
    async fn request_worker_lease(
        &self,
        _req: ray_proto::ray::rpc::RequestWorkerLeaseRequest,
    ) -> Result<ray_proto::ray::rpc::RequestWorkerLeaseReply, tonic::Status> {
        Ok(ray_proto::ray::rpc::RequestWorkerLeaseReply {
            worker_address: Some(self.worker_address.clone()),
            ..Default::default()
        })
    }

    async fn return_worker_lease(
        &self,
        _req: ray_proto::ray::rpc::ReturnWorkerLeaseRequest,
    ) -> Result<ray_proto::ray::rpc::ReturnWorkerLeaseReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn cancel_worker_lease(
        &self,
        _req: ray_proto::ray::rpc::CancelWorkerLeaseRequest,
    ) -> Result<ray_proto::ray::rpc::CancelWorkerLeaseReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn report_worker_backlog(
        &self,
        _req: ray_proto::ray::rpc::ReportWorkerBacklogRequest,
    ) -> Result<ray_proto::ray::rpc::ReportWorkerBacklogReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn prestart_workers(
        &self,
        _req: ray_proto::ray::rpc::PrestartWorkersRequest,
    ) -> Result<ray_proto::ray::rpc::PrestartWorkersReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn prepare_bundle_resources(
        &self,
        _req: ray_proto::ray::rpc::PrepareBundleResourcesRequest,
    ) -> Result<ray_proto::ray::rpc::PrepareBundleResourcesReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn commit_bundle_resources(
        &self,
        _req: ray_proto::ray::rpc::CommitBundleResourcesRequest,
    ) -> Result<ray_proto::ray::rpc::CommitBundleResourcesReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn cancel_resource_reserve(
        &self,
        _req: ray_proto::ray::rpc::CancelResourceReserveRequest,
    ) -> Result<ray_proto::ray::rpc::CancelResourceReserveReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn pin_object_ids(
        &self,
        _req: ray_proto::ray::rpc::PinObjectIDsRequest,
    ) -> Result<ray_proto::ray::rpc::PinObjectIDsReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn get_resource_load(
        &self,
        _req: ray_proto::ray::rpc::GetResourceLoadRequest,
    ) -> Result<ray_proto::ray::rpc::GetResourceLoadReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn shutdown_raylet(
        &self,
        _req: ray_proto::ray::rpc::ShutdownRayletRequest,
    ) -> Result<ray_proto::ray::rpc::ShutdownRayletReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn drain_raylet(
        &self,
        _req: ray_proto::ray::rpc::DrainRayletRequest,
    ) -> Result<ray_proto::ray::rpc::DrainRayletReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn notify_gcs_restart(
        &self,
        _req: ray_proto::ray::rpc::NotifyGcsRestartRequest,
    ) -> Result<ray_proto::ray::rpc::NotifyGcsRestartReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn get_node_stats(
        &self,
        _req: ray_proto::ray::rpc::GetNodeStatsRequest,
    ) -> Result<ray_proto::ray::rpc::GetNodeStatsReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn get_system_config(
        &self,
        _req: ray_proto::ray::rpc::GetSystemConfigRequest,
    ) -> Result<ray_proto::ray::rpc::GetSystemConfigReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn kill_local_actor(
        &self,
        _req: ray_proto::ray::rpc::KillLocalActorRequest,
    ) -> Result<ray_proto::ray::rpc::KillLocalActorReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn cancel_local_task(
        &self,
        _req: ray_proto::ray::rpc::CancelLocalTaskRequest,
    ) -> Result<ray_proto::ray::rpc::CancelLocalTaskReply, tonic::Status> {
        Ok(Default::default())
    }

    async fn global_gc(
        &self,
        _req: ray_proto::ray::rpc::GlobalGcRequest,
    ) -> Result<ray_proto::ray::rpc::GlobalGcReply, tonic::Status> {
        Ok(Default::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_common::id::{ActorID, JobID, ObjectID};
    use ray_core_worker::options::CoreWorkerOptions;

    fn make_py_worker() -> PyCoreWorker {
        PyCoreWorker::new(CoreWorkerOptions {
            job_id: JobID::from_int(1),
            ..CoreWorkerOptions::default()
        })
    }

    #[test]
    fn test_py_core_worker_creation() {
        let w = make_py_worker();
        assert_eq!(w.get_current_job_id(), JobID::from_int(1));
    }

    #[test]
    fn test_py_core_worker_worker_id() {
        let w = make_py_worker();
        // Worker ID is random but should not be nil.
        assert!(!w.get_worker_id().is_nil());
    }

    #[test]
    fn test_py_core_worker_task_id() {
        let w = make_py_worker();
        // Initial task ID is nil for a driver.
        let tid = w.get_current_task_id();
        assert!(tid.is_nil());
    }

    #[test]
    fn test_py_core_worker_inner() {
        let w = make_py_worker();
        let inner = w.inner();
        assert_eq!(inner.current_job_id(), JobID::from_int(1));
    }

    #[test]
    fn test_py_core_worker_put_and_contains() {
        let w = make_py_worker();
        let oid = ObjectID::from_random();
        assert!(!w.contains_object(&oid));
        w.put_object(oid, b"hello".to_vec(), b"meta".to_vec())
            .unwrap();
        assert!(w.contains_object(&oid));
    }

    #[test]
    fn test_py_core_worker_put_duplicate_errors() {
        let w = make_py_worker();
        let oid = ObjectID::from_random();
        w.put_object(oid, b"data".to_vec(), vec![]).unwrap();
        let result = w.put_object(oid, b"data2".to_vec(), vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_py_core_worker_get_objects() {
        // PyCoreWorker owns its own tokio runtime, so use #[test] not #[tokio::test]
        // to avoid nested runtime panic.
        let w = make_py_worker();
        let oid = ObjectID::from_random();
        w.put_object(oid, b"value".to_vec(), b"m".to_vec()).unwrap();
        let results = w.get_objects(&[oid], 1000).unwrap();
        assert_eq!(results.len(), 1);
        let obj = results[0].as_ref().unwrap();
        assert_eq!(obj.data.as_ref(), b"value");
        assert_eq!(obj.metadata.as_ref(), b"m");
    }

    #[test]
    fn test_py_core_worker_get_objects_timeout() {
        let w = make_py_worker();
        let oid = ObjectID::from_random();
        // Object not put — should timeout and return None.
        let results = w.get_objects(&[oid], 50).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_none());
    }

    #[test]
    fn test_py_core_worker_wait() {
        let w = make_py_worker();
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();
        w.put_object(oid1, b"d".to_vec(), vec![]).unwrap();
        // Wait for at least 1 of 2 objects.
        let ready = w.wait(&[oid1, oid2], 1, 100).unwrap();
        assert_eq!(ready.len(), 2);
        assert!(ready[0]); // oid1 is ready
    }

    #[test]
    fn test_py_core_worker_free_objects() {
        let w = make_py_worker();
        let oid = ObjectID::from_random();
        w.put_object(oid, b"data".to_vec(), vec![]).unwrap();
        assert!(w.contains_object(&oid));
        w.free_objects(&[oid]);
        assert!(!w.contains_object(&oid));
    }

    #[test]
    fn test_py_core_worker_create_and_kill_actor() {
        let w = make_py_worker();
        let aid = ActorID::from_random();
        let handle = ray_core_worker::actor_handle::ActorHandle::from_proto(
            ray_proto::ray::rpc::ActorHandle {
                actor_id: aid.binary(),
                name: "test_actor".to_string(),
                ..Default::default()
            },
        );
        w.create_actor(aid, handle).unwrap();
        w.kill_actor(&aid, false, true).unwrap();
    }

    #[test]
    fn test_py_core_worker_kill_unregistered_actor() {
        let w = make_py_worker();
        let aid = ActorID::from_random();
        // kill_actor on an unregistered actor is a no-op (no error).
        let result = w.kill_actor(&aid, false, true);
        assert!(result.is_ok());
    }

    #[test]
    #[ignore = "Parity gap vs Cython/C++ CoreWorker: Python-facing contract should accept ObjectRef-level semantics, not only raw IDs"]
    fn test_parity_get_contract_is_not_binary_id_only() {
        let w = make_py_worker();
        let oid = ObjectID::from_random();
        w.put_object(oid, b"value".to_vec(), b"meta".to_vec()).unwrap();

        let results = w.get_objects(&[oid], 1000).unwrap();
        let obj = results[0].as_ref().unwrap();

        // This test documents that the local Rust-side get path only traffics in
        // raw RayObject payloads. The Cython/Python contract is richer and is
        // expected to preserve ObjectRef-based semantics through the boundary.
        assert_eq!(obj.data.as_ref(), b"value");
        assert_eq!(obj.metadata.as_ref(), b"meta");
        panic!("parity gap: this API path remains raw-ID/raw-bytes centric");
    }

    // ── DirectDispatchRayletClient tests ─────────────────────────────

    fn make_raylet_client() -> DirectDispatchRayletClient {
        DirectDispatchRayletClient {
            worker_address: ray_proto::ray::rpc::Address {
                node_id: vec![1, 2, 3],
                ip_address: "10.0.0.1".to_string(),
                port: 9999,
                worker_id: vec![4, 5, 6],
            },
        }
    }

    #[tokio::test]
    async fn test_direct_dispatch_request_worker_lease() {
        use ray_raylet_rpc_client::RayletClient;
        let client = make_raylet_client();
        let reply = client
            .request_worker_lease(ray_proto::ray::rpc::RequestWorkerLeaseRequest::default())
            .await
            .unwrap();
        let addr = reply.worker_address.unwrap();
        assert_eq!(addr.ip_address, "10.0.0.1");
        assert_eq!(addr.port, 9999);
        assert_eq!(addr.worker_id, vec![4, 5, 6]);
        assert_eq!(addr.node_id, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_direct_dispatch_return_worker_lease() {
        use ray_raylet_rpc_client::RayletClient;
        let client = make_raylet_client();
        let reply = client
            .return_worker_lease(ray_proto::ray::rpc::ReturnWorkerLeaseRequest::default())
            .await
            .unwrap();
        // Returns default (empty) reply.
        assert_eq!(reply, Default::default());
    }

    #[tokio::test]
    async fn test_direct_dispatch_cancel_worker_lease() {
        use ray_raylet_rpc_client::RayletClient;
        let client = make_raylet_client();
        let reply = client
            .cancel_worker_lease(ray_proto::ray::rpc::CancelWorkerLeaseRequest::default())
            .await
            .unwrap();
        assert_eq!(reply, Default::default());
    }

    #[tokio::test]
    async fn test_direct_dispatch_all_methods_return_ok() {
        use ray_raylet_rpc_client::RayletClient;
        let c = make_raylet_client();
        // Verify all 18 trait methods return Ok.
        assert!(c.report_worker_backlog(Default::default()).await.is_ok());
        assert!(c.prestart_workers(Default::default()).await.is_ok());
        assert!(c.prepare_bundle_resources(Default::default()).await.is_ok());
        assert!(c.commit_bundle_resources(Default::default()).await.is_ok());
        assert!(c.cancel_resource_reserve(Default::default()).await.is_ok());
        assert!(c.pin_object_ids(Default::default()).await.is_ok());
        assert!(c.get_resource_load(Default::default()).await.is_ok());
        assert!(c.shutdown_raylet(Default::default()).await.is_ok());
        assert!(c.drain_raylet(Default::default()).await.is_ok());
        assert!(c.notify_gcs_restart(Default::default()).await.is_ok());
        assert!(c.get_node_stats(Default::default()).await.is_ok());
        assert!(c.get_system_config(Default::default()).await.is_ok());
        assert!(c.kill_local_actor(Default::default()).await.is_ok());
        assert!(c.cancel_local_task(Default::default()).await.is_ok());
        assert!(c.global_gc(Default::default()).await.is_ok());
    }
}
