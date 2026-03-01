// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Normal (non-actor) task submission with raylet lease integration.
//!
//! Replaces `src/ray/core_worker/transport/normal_task_submitter.h/cc`.
//!
//! The submitter requests a worker lease from the local raylet, then the
//! task is dispatched to the granted worker. If the raylet spills back to
//! a remote node, we re-submit to that node's raylet.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use ray_common::id::TaskID;
use ray_proto::ray::rpc::{self, TaskSpec};
use ray_raylet_rpc_client::RayletClient;

use crate::error::{CoreWorkerError, CoreWorkerResult};
use crate::reference_counter::ReferenceCounter;

/// Callback invoked when a task has been granted a worker and is ready for dispatch.
pub type TaskDispatchCallback =
    Box<dyn Fn(&TaskSpec, &rpc::Address) -> CoreWorkerResult<()> + Send + Sync>;

/// Status of a submitted task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// Waiting for a worker lease.
    WaitingForLease,
    /// Lease granted, dispatching.
    Dispatching,
    /// Task completed or cancelled.
    Finished,
    /// Lease was rejected (infeasible).
    Rejected,
    /// Spilled back to another node.
    SpilledBack,
}

/// Submitter for normal (non-actor) tasks.
///
/// Integrates with the RayletClient trait for worker lease requests.
/// When a lease is granted, the task is dispatched via the dispatch callback.
/// Spillback replies cause the task to be marked for re-submission.
pub struct NormalTaskSubmitter {
    reference_counter: Arc<ReferenceCounter>,
    pending_tasks: AtomicUsize,
    /// Optional raylet client for lease requests.
    raylet_client: Mutex<Option<Arc<dyn RayletClient>>>,
    /// Tracks status of in-flight tasks.
    task_status: Mutex<HashMap<Vec<u8>, TaskStatus>>,
    /// Optional callback for dispatching granted tasks.
    dispatch_callback: Mutex<Option<TaskDispatchCallback>>,
    /// Caller address for lease requests.
    caller_address: Mutex<Option<rpc::Address>>,
}

impl NormalTaskSubmitter {
    pub fn new(reference_counter: Arc<ReferenceCounter>) -> Self {
        Self {
            reference_counter,
            pending_tasks: AtomicUsize::new(0),
            raylet_client: Mutex::new(None),
            task_status: Mutex::new(HashMap::new()),
            dispatch_callback: Mutex::new(None),
            caller_address: Mutex::new(None),
        }
    }

    /// Set the raylet client for lease requests.
    pub fn set_raylet_client(&self, client: Arc<dyn RayletClient>) {
        *self.raylet_client.lock() = Some(client);
    }

    /// Set the dispatch callback invoked when a task is granted a worker.
    pub fn set_dispatch_callback(&self, callback: TaskDispatchCallback) {
        *self.dispatch_callback.lock() = Some(callback);
    }

    /// Set the caller address used in lease requests.
    pub fn set_caller_address(&self, address: rpc::Address) {
        *self.caller_address.lock() = Some(address);
    }

    /// Submit a normal task for execution.
    ///
    /// If a raylet client is set, requests a worker lease from the raylet.
    /// Otherwise, falls back to local-only tracking (stub mode).
    pub async fn submit_task(&self, task_spec: &TaskSpec) -> CoreWorkerResult<()> {
        // Track references for task arguments.
        let arg_ids: Vec<_> = task_spec
            .args
            .iter()
            .filter_map(|arg| {
                arg.object_ref
                    .as_ref()
                    .map(|r| ray_common::id::ObjectID::from_binary(&r.object_id))
            })
            .collect();
        self.reference_counter
            .update_submitted_task_references(&arg_ids);

        self.pending_tasks.fetch_add(1, Ordering::Relaxed);

        let task_id = task_spec.task_id.clone();

        // Check if we have a raylet client
        let client = self.raylet_client.lock().clone();
        if let Some(client) = client {
            self.task_status
                .lock()
                .insert(task_id.clone(), TaskStatus::WaitingForLease);

            let caller_addr = self.caller_address.lock().clone();

            // Build the lease request
            let lease_spec = rpc::LeaseSpec {
                lease_id: task_id.clone(),
                required_resources: extract_required_resources(task_spec),
                caller_address: caller_addr,
                ..Default::default()
            };

            let req = rpc::RequestWorkerLeaseRequest {
                lease_spec: Some(lease_spec),
                ..Default::default()
            };

            tracing::debug!(
                task_id = %hex::encode(&task_id),
                "Requesting worker lease from raylet"
            );

            match client.request_worker_lease(req).await {
                Ok(reply) => {
                    self.handle_lease_reply(&task_id, task_spec, &reply)?;
                }
                Err(e) => {
                    self.task_finished(&task_id);
                    return Err(CoreWorkerError::TaskSubmissionFailed(format!(
                        "lease request failed: {}",
                        e
                    )));
                }
            }
        } else {
            // No raylet client — stub mode
            tracing::debug!(
                task_id = %hex::encode(&task_id),
                "Normal task submitted (no raylet client)"
            );
        }

        Ok(())
    }

    /// Handle the reply from a worker lease request.
    fn handle_lease_reply(
        &self,
        task_id: &[u8],
        task_spec: &TaskSpec,
        reply: &rpc::RequestWorkerLeaseReply,
    ) -> CoreWorkerResult<()> {
        if reply.rejected {
            self.task_status
                .lock()
                .insert(task_id.to_vec(), TaskStatus::Rejected);
            self.task_finished(task_id);
            tracing::warn!(
                task_id = %hex::encode(task_id),
                reason = %reply.scheduling_failure_message,
                "Task lease rejected"
            );
            return Err(CoreWorkerError::TaskSubmissionFailed(format!(
                "scheduling rejected: {}",
                reply.scheduling_failure_message
            )));
        }

        if reply.canceled {
            self.task_finished(task_id);
            return Ok(());
        }

        if let Some(ref retry_addr) = reply.retry_at_raylet_address {
            // Spillback — the task should be re-submitted to the remote raylet.
            self.task_status
                .lock()
                .insert(task_id.to_vec(), TaskStatus::SpilledBack);
            tracing::info!(
                task_id = %hex::encode(task_id),
                remote_node = %hex::encode(&retry_addr.node_id),
                "Task spilled back to remote node"
            );
            // In a full implementation, we'd create a new raylet client connection
            // to the remote node and re-submit. For now, mark as spilled back.
            self.task_finished(task_id);
            return Ok(());
        }

        if let Some(ref worker_addr) = reply.worker_address {
            // Lease granted — dispatch the task
            self.task_status
                .lock()
                .insert(task_id.to_vec(), TaskStatus::Dispatching);

            tracing::debug!(
                task_id = %hex::encode(task_id),
                worker_ip = %worker_addr.ip_address,
                worker_port = worker_addr.port,
                "Worker lease granted, dispatching task"
            );

            let callback = self.dispatch_callback.lock();
            if let Some(ref cb) = *callback {
                let result = cb(task_spec, worker_addr);
                self.task_finished(task_id);
                return result;
            }

            self.task_finished(task_id);
            return Ok(());
        }

        // Unexpected reply with no worker address and no rejection
        self.task_finished(task_id);
        Ok(())
    }

    /// Mark a task as finished and decrement the pending counter.
    ///
    /// If the task already has a terminal status (Rejected, SpilledBack), it is preserved.
    fn task_finished(&self, task_id: &[u8]) {
        let mut status_map = self.task_status.lock();
        let current = status_map.get(task_id).copied();
        match current {
            Some(TaskStatus::Rejected) | Some(TaskStatus::SpilledBack) => {
                // Keep the existing terminal status
            }
            _ => {
                status_map.insert(task_id.to_vec(), TaskStatus::Finished);
            }
        }
        drop(status_map);
        self.pending_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    /// Cancel a pending task.
    pub async fn cancel_task(
        &self,
        task_id: &TaskID,
        _force_kill: bool,
    ) -> CoreWorkerResult<()> {
        let client = self.raylet_client.lock().clone();
        if let Some(client) = client {
            let req = rpc::CancelWorkerLeaseRequest {
                lease_id: task_id.binary(),
            };
            match client.cancel_worker_lease(req).await {
                Ok(reply) => {
                    if reply.success {
                        self.task_finished(&task_id.binary());
                        Ok(())
                    } else {
                        Err(CoreWorkerError::Internal(
                            "cancel lease failed: task already dispatched".into(),
                        ))
                    }
                }
                Err(e) => Err(CoreWorkerError::Internal(format!(
                    "cancel lease RPC failed: {}",
                    e
                ))),
            }
        } else {
            Err(CoreWorkerError::Internal(
                "normal task cancellation requires raylet client".into(),
            ))
        }
    }

    /// Get the status of a task.
    pub fn task_status(&self, task_id: &[u8]) -> Option<TaskStatus> {
        self.task_status.lock().get(task_id).copied()
    }

    /// Number of pending tasks.
    pub fn num_pending_tasks(&self) -> usize {
        self.pending_tasks.load(Ordering::Relaxed)
    }

    /// Reference to the reference counter.
    pub fn reference_counter(&self) -> &Arc<ReferenceCounter> {
        &self.reference_counter
    }
}

/// Extract required resources from a TaskSpec for the lease request.
fn extract_required_resources(task_spec: &TaskSpec) -> HashMap<String, f64> {
    task_spec
        .required_resources
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    fn make_submitter() -> NormalTaskSubmitter {
        let rc = Arc::new(ReferenceCounter::new());
        NormalTaskSubmitter::new(rc)
    }

    // ── Mock RayletClient ────────────────────────────────────────────

    /// A configurable mock raylet client for testing.
    struct MockRayletClient {
        reply: Mutex<rpc::RequestWorkerLeaseReply>,
        cancel_reply: Mutex<rpc::CancelWorkerLeaseReply>,
        request_count: AtomicU32,
    }

    impl MockRayletClient {
        fn new(reply: rpc::RequestWorkerLeaseReply) -> Self {
            Self {
                reply: Mutex::new(reply),
                cancel_reply: Mutex::new(rpc::CancelWorkerLeaseReply { success: true }),
                request_count: AtomicU32::new(0),
            }
        }

        fn with_cancel_reply(
            reply: rpc::RequestWorkerLeaseReply,
            cancel: rpc::CancelWorkerLeaseReply,
        ) -> Self {
            Self {
                reply: Mutex::new(reply),
                cancel_reply: Mutex::new(cancel),
                request_count: AtomicU32::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl RayletClient for MockRayletClient {
        async fn request_worker_lease(
            &self,
            _req: rpc::RequestWorkerLeaseRequest,
        ) -> Result<rpc::RequestWorkerLeaseReply, tonic::Status> {
            self.request_count.fetch_add(1, Ordering::Relaxed);
            Ok(self.reply.lock().clone())
        }

        async fn return_worker_lease(
            &self,
            _req: rpc::ReturnWorkerLeaseRequest,
        ) -> Result<rpc::ReturnWorkerLeaseReply, tonic::Status> {
            Ok(rpc::ReturnWorkerLeaseReply::default())
        }

        async fn cancel_worker_lease(
            &self,
            _req: rpc::CancelWorkerLeaseRequest,
        ) -> Result<rpc::CancelWorkerLeaseReply, tonic::Status> {
            Ok(self.cancel_reply.lock().clone())
        }

        async fn report_worker_backlog(
            &self,
            _req: rpc::ReportWorkerBacklogRequest,
        ) -> Result<rpc::ReportWorkerBacklogReply, tonic::Status> {
            Ok(rpc::ReportWorkerBacklogReply::default())
        }

        async fn prestart_workers(
            &self,
            _req: rpc::PrestartWorkersRequest,
        ) -> Result<rpc::PrestartWorkersReply, tonic::Status> {
            Ok(rpc::PrestartWorkersReply::default())
        }

        async fn prepare_bundle_resources(
            &self,
            _req: rpc::PrepareBundleResourcesRequest,
        ) -> Result<rpc::PrepareBundleResourcesReply, tonic::Status> {
            Ok(rpc::PrepareBundleResourcesReply::default())
        }

        async fn commit_bundle_resources(
            &self,
            _req: rpc::CommitBundleResourcesRequest,
        ) -> Result<rpc::CommitBundleResourcesReply, tonic::Status> {
            Ok(rpc::CommitBundleResourcesReply::default())
        }

        async fn cancel_resource_reserve(
            &self,
            _req: rpc::CancelResourceReserveRequest,
        ) -> Result<rpc::CancelResourceReserveReply, tonic::Status> {
            Ok(rpc::CancelResourceReserveReply::default())
        }

        async fn pin_object_ids(
            &self,
            _req: rpc::PinObjectIDsRequest,
        ) -> Result<rpc::PinObjectIDsReply, tonic::Status> {
            Ok(rpc::PinObjectIDsReply::default())
        }

        async fn get_resource_load(
            &self,
            _req: rpc::GetResourceLoadRequest,
        ) -> Result<rpc::GetResourceLoadReply, tonic::Status> {
            Ok(rpc::GetResourceLoadReply::default())
        }

        async fn shutdown_raylet(
            &self,
            _req: rpc::ShutdownRayletRequest,
        ) -> Result<rpc::ShutdownRayletReply, tonic::Status> {
            Ok(rpc::ShutdownRayletReply::default())
        }

        async fn drain_raylet(
            &self,
            _req: rpc::DrainRayletRequest,
        ) -> Result<rpc::DrainRayletReply, tonic::Status> {
            Ok(rpc::DrainRayletReply::default())
        }

        async fn notify_gcs_restart(
            &self,
            _req: rpc::NotifyGcsRestartRequest,
        ) -> Result<rpc::NotifyGcsRestartReply, tonic::Status> {
            Ok(rpc::NotifyGcsRestartReply::default())
        }

        async fn get_node_stats(
            &self,
            _req: rpc::GetNodeStatsRequest,
        ) -> Result<rpc::GetNodeStatsReply, tonic::Status> {
            Ok(rpc::GetNodeStatsReply::default())
        }

        async fn get_system_config(
            &self,
            _req: rpc::GetSystemConfigRequest,
        ) -> Result<rpc::GetSystemConfigReply, tonic::Status> {
            Ok(rpc::GetSystemConfigReply::default())
        }

        async fn kill_local_actor(
            &self,
            _req: rpc::KillLocalActorRequest,
        ) -> Result<rpc::KillLocalActorReply, tonic::Status> {
            Ok(rpc::KillLocalActorReply::default())
        }

        async fn cancel_local_task(
            &self,
            _req: rpc::CancelLocalTaskRequest,
        ) -> Result<rpc::CancelLocalTaskReply, tonic::Status> {
            Ok(rpc::CancelLocalTaskReply::default())
        }

        async fn global_gc(
            &self,
            _req: rpc::GlobalGcRequest,
        ) -> Result<rpc::GlobalGcReply, tonic::Status> {
            Ok(rpc::GlobalGcReply::default())
        }
    }

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_submit_increments_pending() {
        let submitter = make_submitter();
        assert_eq!(submitter.num_pending_tasks(), 0);

        let task_spec = TaskSpec::default();
        submitter.submit_task(&task_spec).await.unwrap();
        assert_eq!(submitter.num_pending_tasks(), 1);

        submitter.submit_task(&task_spec).await.unwrap();
        assert_eq!(submitter.num_pending_tasks(), 2);
    }

    #[tokio::test]
    async fn test_submit_empty_args() {
        let submitter = make_submitter();
        let task_spec = TaskSpec {
            args: vec![],
            ..Default::default()
        };
        submitter.submit_task(&task_spec).await.unwrap();
        assert_eq!(submitter.num_pending_tasks(), 1);
    }

    #[tokio::test]
    async fn test_cancel_without_client_returns_error() {
        let submitter = make_submitter();
        let tid = TaskID::nil();
        let result = submitter.cancel_task(&tid, false).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_reference_counter_accessible() {
        let submitter = make_submitter();
        let _rc = submitter.reference_counter();
    }

    #[tokio::test]
    async fn test_submit_with_lease_granted() {
        let submitter = make_submitter();

        let reply = rpc::RequestWorkerLeaseReply {
            worker_address: Some(rpc::Address {
                node_id: vec![1; 28],
                ip_address: "10.0.0.1".to_string(),
                port: 5000,
                worker_id: vec![2; 28],
            }),
            ..Default::default()
        };
        let client = Arc::new(MockRayletClient::new(reply));
        submitter.set_raylet_client(client.clone());

        let task_spec = TaskSpec {
            task_id: vec![1, 2, 3],
            ..Default::default()
        };
        submitter.submit_task(&task_spec).await.unwrap();

        // Task should be finished (dispatched and completed)
        assert_eq!(
            submitter.task_status(&[1, 2, 3]),
            Some(TaskStatus::Finished)
        );
        assert_eq!(client.request_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_submit_with_lease_rejected() {
        let submitter = make_submitter();

        let reply = rpc::RequestWorkerLeaseReply {
            rejected: true,
            scheduling_failure_message: "infeasible".to_string(),
            ..Default::default()
        };
        let client = Arc::new(MockRayletClient::new(reply));
        submitter.set_raylet_client(client);

        let task_spec = TaskSpec {
            task_id: vec![4, 5, 6],
            ..Default::default()
        };
        let result = submitter.submit_task(&task_spec).await;
        assert!(result.is_err());
        assert_eq!(
            submitter.task_status(&[4, 5, 6]),
            Some(TaskStatus::Rejected)
        );
    }

    #[tokio::test]
    async fn test_submit_with_spillback() {
        let submitter = make_submitter();

        let reply = rpc::RequestWorkerLeaseReply {
            retry_at_raylet_address: Some(rpc::Address {
                node_id: vec![9; 28],
                ip_address: "10.0.0.9".to_string(),
                port: 6000,
                worker_id: vec![],
            }),
            ..Default::default()
        };
        let client = Arc::new(MockRayletClient::new(reply));
        submitter.set_raylet_client(client);

        let task_spec = TaskSpec {
            task_id: vec![7, 8, 9],
            ..Default::default()
        };
        submitter.submit_task(&task_spec).await.unwrap();

        assert_eq!(
            submitter.task_status(&[7, 8, 9]),
            Some(TaskStatus::SpilledBack)
        );
    }

    #[tokio::test]
    async fn test_submit_with_dispatch_callback() {
        let submitter = make_submitter();

        let dispatched = Arc::new(AtomicU32::new(0));
        let dispatched_clone = dispatched.clone();
        submitter.set_dispatch_callback(Box::new(move |_spec, addr| {
            assert_eq!(addr.ip_address, "10.0.0.1");
            dispatched_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));

        let reply = rpc::RequestWorkerLeaseReply {
            worker_address: Some(rpc::Address {
                node_id: vec![1; 28],
                ip_address: "10.0.0.1".to_string(),
                port: 5000,
                worker_id: vec![2; 28],
            }),
            ..Default::default()
        };
        let client = Arc::new(MockRayletClient::new(reply));
        submitter.set_raylet_client(client);

        let task_spec = TaskSpec::default();
        submitter.submit_task(&task_spec).await.unwrap();

        assert_eq!(dispatched.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_cancel_with_client_success() {
        let submitter = make_submitter();

        let reply = rpc::RequestWorkerLeaseReply::default();
        let cancel_reply = rpc::CancelWorkerLeaseReply { success: true };
        let client = Arc::new(MockRayletClient::with_cancel_reply(reply, cancel_reply));
        submitter.set_raylet_client(client);

        let tid = TaskID::nil();
        let result = submitter.cancel_task(&tid, false).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cancel_with_client_failure() {
        let submitter = make_submitter();

        let reply = rpc::RequestWorkerLeaseReply::default();
        let cancel_reply = rpc::CancelWorkerLeaseReply { success: false };
        let client = Arc::new(MockRayletClient::with_cancel_reply(reply, cancel_reply));
        submitter.set_raylet_client(client);

        let tid = TaskID::nil();
        let result = submitter.cancel_task(&tid, false).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_required_resources() {
        let spec = TaskSpec {
            required_resources: HashMap::from([
                ("CPU".to_string(), 2.0),
                ("GPU".to_string(), 1.0),
            ]),
            ..Default::default()
        };
        let resources = extract_required_resources(&spec);
        assert_eq!(resources.get("CPU"), Some(&2.0));
        assert_eq!(resources.get("GPU"), Some(&1.0));
    }
}
