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
    ///   worker_id: optional PyWorkerID (random if None)
    ///   node_id: optional PyNodeID (nil if None)
    #[new]
    #[pyo3(signature = (worker_type, node_ip_address, gcs_address, job_id_int, worker_id=None, node_id=None))]
    fn py_new(
        worker_type: i32,
        node_ip_address: String,
        gcs_address: String,
        job_id_int: u32,
        worker_id: Option<&crate::ids::PyWorkerID>,
        node_id: Option<&crate::ids::PyNodeID>,
    ) -> pyo3::PyResult<Self> {
        use crate::common::PyWorkerType;
        use ray_common::id::NodeID;
        let wt = PyWorkerType::from_i32(worker_type)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(
                format!("invalid worker_type: {}", worker_type),
            ))?;
        let wid = worker_id
            .map(|w| *w.inner())
            .unwrap_or_else(WorkerID::from_random);
        let nid = node_id
            .map(|n| *n.inner())
            .unwrap_or_else(NodeID::nil);
        let options = CoreWorkerOptions {
            worker_type: wt.to_core(),
            node_ip_address,
            gcs_address,
            job_id: JobID::from_int(job_id_int),
            worker_id: wid,
            node_id: nid,
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
        let results = py.allow_threads(|| {
            self.get_objects(&oids, timeout_ms)
        })
        .map_err(crate::common::to_py_err)?;
        Ok(results
            .into_iter()
            .map(|opt| {
                opt.map(|obj| {
                    let data = pyo3::types::PyBytes::new_bound(py, &obj.data).into();
                    let meta = pyo3::types::PyBytes::new_bound(py, &obj.metadata).into();
                    (data, meta)
                })
            })
            .collect())
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
        py.allow_threads(|| {
            self.wait(&oids, num_objects, timeout_ms)
        })
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
            let args: Vec<Vec<u8>> = spec.args.iter().map(|a| a.data.clone()).collect();

            let result_bytes = pyo3::Python::with_gil(|py| -> Result<Vec<u8>, CoreWorkerError> {
                // Convert args to Python list of bytes objects.
                let py_args: Vec<pyo3::PyObject> = args
                    .iter()
                    .map(|a| {
                        pyo3::types::PyBytes::new_bound(py, a).into()
                    })
                    .collect();
                let py_args_list = pyo3::types::PyList::new_bound(py, &py_args);
                let result = callback
                    .call1(py, (&name, py_args_list))
                    .map_err(|e| CoreWorkerError::Internal(format!("Python callback error: {}", e)))?;
                // Extract bytes from the result.
                let bytes: Vec<u8> = result
                    .extract(py)
                    .map_err(|e| CoreWorkerError::Internal(format!("callback must return bytes: {}", e)))?;
                Ok(bytes)
            })?;

            let task_id = TaskID::from_binary(&spec.task_id);
            let return_oid = ObjectID::from_index(&task_id, 1);
            Ok(TaskResult {
                return_objects: vec![task_rpc::ReturnObject {
                    object_id: return_oid.binary(),
                    data: result_bytes,
                    metadata: b"python".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })
        });
        self.inner.set_task_execution_callback(cb);
        Ok(())
    }

    /// Start a gRPC server for this CoreWorker and return the bound port.
    #[pyo3(name = "start_grpc_server")]
    fn py_start_grpc_server(&self) -> pyo3::PyResult<u16> {
        use ray_core_worker::grpc_service::CoreWorkerServiceImpl;
        use ray_proto::ray::rpc as grpc_rpc;

        let core_worker = Arc::clone(&self.inner);
        let port = self.runtime.block_on(async {
            let svc = CoreWorkerServiceImpl { core_worker };
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
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
                        grpc_rpc::core_worker_service_server::CoreWorkerServiceServer::new(svc),
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
        let handle = ray_core_worker::actor_handle::ActorHandle::from_proto(actor_rpc::ActorHandle {
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
            .set_actor_task_send_callback(Box::new(move |spec, addr| {
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
                            );
                        let response = client
                            .push_task(actor_rpc::PushTaskRequest {
                                intended_worker_id: wid_bytes,
                                task_spec: Some(spec_clone),
                                ..Default::default()
                            })
                            .await
                            .map_err(|e| format!("push_task failed: {}", e))?;
                        let reply = response.into_inner();
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
                    .map_err(|e| {
                        ray_core_worker::error::CoreWorkerError::Internal(e)
                    })
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
                        );
                    let response = client
                        .push_task(dispatch_rpc::PushTaskRequest {
                            intended_worker_id: wid_bytes,
                            task_spec: Some(spec_clone),
                            ..Default::default()
                        })
                        .await
                        .map_err(|e| format!("push_task failed: {}", e))?;
                    let reply = response.into_inner();
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
    ///
    /// Returns a list of ObjectIDs for the return values.
    #[pyo3(name = "submit_task", signature = (name, args, num_returns=1))]
    fn py_submit_task(
        &self,
        py: pyo3::Python<'_>,
        name: &str,
        args: Vec<Vec<u8>>,
        num_returns: u64,
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

        let spec = task_rpc::TaskSpec {
            task_id: task_id.binary(),
            name: name.to_string(),
            num_returns: num_returns,
            args: task_args,
            ..Default::default()
        };

        // Release the GIL during block_on: the dispatch callback blocks on
        // rx.recv() while the worker's Python callback needs the GIL.
        py.allow_threads(|| {
            self.runtime
                .block_on(self.inner.submit_task(&spec))
        })
        .map_err(crate::common::to_py_err)?;

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
