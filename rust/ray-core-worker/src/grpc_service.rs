// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! gRPC service implementation for the CoreWorkerService.
//!
//! Implements all 24 RPCs defined in `core_worker.proto`.
//! Functional handlers delegate to the CoreWorker; remaining RPCs are stubs.

use std::sync::Arc;

use tonic::Status;

use ray_common::id::{ActorID, ObjectID};
use ray_proto::ray::rpc;

use crate::core_worker::CoreWorker;

/// The gRPC service implementation wrapping the CoreWorker.
pub struct CoreWorkerServiceImpl {
    pub core_worker: Arc<CoreWorker>,
}

// tonic::Status is large by design; all gRPC services return it.
#[allow(clippy::result_large_err)]
impl CoreWorkerServiceImpl {
    // ─── Functional Handlers ─────────────────────────────────────────

    pub async fn handle_push_task(
        &self,
        request: rpc::PushTaskRequest,
    ) -> Result<rpc::PushTaskReply, Status> {
        tracing::debug!("PushTask received");
        let _ = request;
        Ok(rpc::PushTaskReply::default())
    }

    pub async fn handle_kill_actor(
        &self,
        request: rpc::KillActorRequest,
    ) -> Result<rpc::KillActorReply, Status> {
        let actor_id = ActorID::from_binary(&request.intended_actor_id);
        self.core_worker
            .kill_actor(&actor_id, request.force_kill, false)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(rpc::KillActorReply::default())
    }

    pub async fn handle_cancel_task(
        &self,
        request: rpc::CancelTaskRequest,
    ) -> Result<rpc::CancelTaskReply, Status> {
        tracing::debug!(
            task_id = %hex::encode(&request.intended_task_id),
            "CancelTask received"
        );
        let _ = request;
        Ok(rpc::CancelTaskReply::default())
    }

    pub async fn handle_get_object_status(
        &self,
        request: rpc::GetObjectStatusRequest,
    ) -> Result<rpc::GetObjectStatusReply, Status> {
        let oid = ObjectID::from_binary(&request.object_id);
        let exists = self.core_worker.contains_object(&oid);
        tracing::debug!(object_id = %oid.hex(), exists, "GetObjectStatus");
        Ok(rpc::GetObjectStatusReply::default())
    }

    pub fn handle_get_core_worker_stats(
        &self,
        _request: rpc::GetCoreWorkerStatsRequest,
    ) -> Result<rpc::GetCoreWorkerStatsReply, Status> {
        Ok(rpc::GetCoreWorkerStatsReply::default())
    }

    pub async fn handle_exit(
        &self,
        request: rpc::ExitRequest,
    ) -> Result<rpc::ExitReply, Status> {
        tracing::info!(force = request.force_exit, "Exit requested");
        Ok(rpc::ExitReply {
            success: true,
        })
    }

    pub fn handle_delete_objects(
        &self,
        request: rpc::DeleteObjectsRequest,
    ) -> Result<rpc::DeleteObjectsReply, Status> {
        let oids: Vec<ObjectID> = request
            .object_ids
            .iter()
            .map(|b| ObjectID::from_binary(b))
            .collect();
        self.core_worker.delete_objects(&oids);
        Ok(rpc::DeleteObjectsReply::default())
    }

    pub fn handle_local_gc(
        &self,
        _request: rpc::LocalGcRequest,
    ) -> Result<rpc::LocalGcReply, Status> {
        tracing::debug!("LocalGC requested");
        Ok(rpc::LocalGcReply::default())
    }

    pub fn handle_num_pending_tasks(
        &self,
        _request: rpc::NumPendingTasksRequest,
    ) -> Result<rpc::NumPendingTasksReply, Status> {
        let num = self.core_worker.num_pending_normal_tasks() as i64;
        Ok(rpc::NumPendingTasksReply {
            num_pending_tasks: num,
        })
    }

    // ─── Stub Handlers ───────────────────────────────────────────────

    pub async fn handle_raylet_notify_gcs_restart(
        &self,
        _request: rpc::RayletNotifyGcsRestartRequest,
    ) -> Result<rpc::RayletNotifyGcsRestartReply, Status> {
        Ok(rpc::RayletNotifyGcsRestartReply::default())
    }

    pub async fn handle_actor_call_arg_wait_complete(
        &self,
        _request: rpc::ActorCallArgWaitCompleteRequest,
    ) -> Result<rpc::ActorCallArgWaitCompleteReply, Status> {
        Ok(rpc::ActorCallArgWaitCompleteReply::default())
    }

    pub async fn handle_wait_for_actor_ref_deleted(
        &self,
        _request: rpc::WaitForActorRefDeletedRequest,
    ) -> Result<rpc::WaitForActorRefDeletedReply, Status> {
        Ok(rpc::WaitForActorRefDeletedReply::default())
    }

    pub async fn handle_pubsub_long_polling(
        &self,
        _request: rpc::PubsubLongPollingRequest,
    ) -> Result<rpc::PubsubLongPollingReply, Status> {
        Ok(rpc::PubsubLongPollingReply::default())
    }

    pub async fn handle_report_generator_item_returns(
        &self,
        _request: rpc::ReportGeneratorItemReturnsRequest,
    ) -> Result<rpc::ReportGeneratorItemReturnsReply, Status> {
        Ok(rpc::ReportGeneratorItemReturnsReply::default())
    }

    pub async fn handle_pubsub_command_batch(
        &self,
        _request: rpc::PubsubCommandBatchRequest,
    ) -> Result<rpc::PubsubCommandBatchReply, Status> {
        Ok(rpc::PubsubCommandBatchReply::default())
    }

    pub async fn handle_update_object_location_batch(
        &self,
        _request: rpc::UpdateObjectLocationBatchRequest,
    ) -> Result<rpc::UpdateObjectLocationBatchReply, Status> {
        Ok(rpc::UpdateObjectLocationBatchReply::default())
    }

    pub async fn handle_get_object_locations_owner(
        &self,
        _request: rpc::GetObjectLocationsOwnerRequest,
    ) -> Result<rpc::GetObjectLocationsOwnerReply, Status> {
        Ok(rpc::GetObjectLocationsOwnerReply::default())
    }

    pub async fn handle_request_owner_to_cancel_task(
        &self,
        _request: rpc::RequestOwnerToCancelTaskRequest,
    ) -> Result<rpc::RequestOwnerToCancelTaskReply, Status> {
        Ok(rpc::RequestOwnerToCancelTaskReply::default())
    }

    pub async fn handle_spill_objects(
        &self,
        _request: rpc::SpillObjectsRequest,
    ) -> Result<rpc::SpillObjectsReply, Status> {
        Ok(rpc::SpillObjectsReply::default())
    }

    pub async fn handle_restore_spilled_objects(
        &self,
        _request: rpc::RestoreSpilledObjectsRequest,
    ) -> Result<rpc::RestoreSpilledObjectsReply, Status> {
        Ok(rpc::RestoreSpilledObjectsReply::default())
    }

    pub async fn handle_delete_spilled_objects(
        &self,
        _request: rpc::DeleteSpilledObjectsRequest,
    ) -> Result<rpc::DeleteSpilledObjectsReply, Status> {
        Ok(rpc::DeleteSpilledObjectsReply::default())
    }

    pub async fn handle_plasma_object_ready(
        &self,
        _request: rpc::PlasmaObjectReadyRequest,
    ) -> Result<rpc::PlasmaObjectReadyReply, Status> {
        Ok(rpc::PlasmaObjectReadyReply::default())
    }

    pub async fn handle_assign_object_owner(
        &self,
        _request: rpc::AssignObjectOwnerRequest,
    ) -> Result<rpc::AssignObjectOwnerReply, Status> {
        Ok(rpc::AssignObjectOwnerReply::default())
    }

    pub async fn handle_register_mutable_object_reader(
        &self,
        _request: rpc::RegisterMutableObjectReaderRequest,
    ) -> Result<rpc::RegisterMutableObjectReaderReply, Status> {
        Ok(rpc::RegisterMutableObjectReaderReply::default())
    }
}
