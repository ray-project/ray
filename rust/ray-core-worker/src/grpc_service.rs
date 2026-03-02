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

use tonic::{Request, Response, Status};

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

    /// Handle an incoming PushTask request by delegating to the TaskReceiver.
    pub async fn handle_push_task(
        &self,
        request: rpc::PushTaskRequest,
    ) -> Result<rpc::PushTaskReply, Status> {
        tracing::debug!(
            seq = request.sequence_number,
            "PushTask received"
        );
        self.core_worker
            .task_receiver()
            .handle_push_task(request)
            .await
            .map_err(|e| Status::internal(e.to_string()))
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

    /// Handle task cancellation by delegating to the TaskReceiver.
    pub async fn handle_cancel_task(
        &self,
        request: rpc::CancelTaskRequest,
    ) -> Result<rpc::CancelTaskReply, Status> {
        let task_id_hex = hex::encode(&request.intended_task_id);
        tracing::debug!(task_id = %task_id_hex, "CancelTask received");

        let running = self.core_worker.task_receiver().is_task_running(
            &request.intended_task_id,
        );

        if running {
            let cancelled = self.core_worker.task_receiver().cancel_task(
                &request.intended_task_id,
                request.force_kill,
            );
            Ok(rpc::CancelTaskReply {
                requested_task_running: true,
                attempt_succeeded: cancelled,
            })
        } else {
            Ok(rpc::CancelTaskReply {
                requested_task_running: false,
                attempt_succeeded: false,
            })
        }
    }

    /// Handle GetObjectStatus by checking the memory store and reference counter.
    pub async fn handle_get_object_status(
        &self,
        request: rpc::GetObjectStatusRequest,
    ) -> Result<rpc::GetObjectStatusReply, Status> {
        let oid = ObjectID::from_binary(&request.object_id);
        let exists = self.core_worker.contains_object(&oid);
        tracing::debug!(object_id = %oid.hex(), exists, "GetObjectStatus");

        if exists {
            // Object is available locally.
            let obj = self.core_worker.memory_store().get(&oid);
            let (ray_obj, obj_size) = match obj {
                Some(o) => {
                    let size = o.data.len() as u64;
                    let proto_obj = rpc::RayObject {
                        data: o.data.to_vec(),
                        metadata: o.metadata.to_vec(),
                        nested_inlined_refs: o
                            .nested_refs
                            .iter()
                            .map(|r| rpc::ObjectReference {
                                object_id: r.binary(),
                                ..Default::default()
                            })
                            .collect(),
                    };
                    (Some(proto_obj), size)
                }
                None => (None, 0),
            };
            Ok(rpc::GetObjectStatusReply {
                status: rpc::get_object_status_reply::ObjectStatus::Created as i32,
                object: ray_obj,
                object_size: obj_size,
                ..Default::default()
            })
        } else {
            // Check if object was ever tracked by reference counter.
            if self.core_worker.reference_counter().has_reference(&oid) {
                Ok(rpc::GetObjectStatusReply {
                    status: rpc::get_object_status_reply::ObjectStatus::Created as i32,
                    ..Default::default()
                })
            } else {
                Ok(rpc::GetObjectStatusReply {
                    status: rpc::get_object_status_reply::ObjectStatus::OutOfScope as i32,
                    ..Default::default()
                })
            }
        }
    }

    /// Return core worker statistics.
    pub fn handle_get_core_worker_stats(
        &self,
        _request: rpc::GetCoreWorkerStatsRequest,
    ) -> Result<rpc::GetCoreWorkerStatsReply, Status> {
        let num_pending = self.core_worker.num_pending_normal_tasks();
        let num_executing = self.core_worker.num_executing_tasks();
        Ok(rpc::GetCoreWorkerStatsReply {
            core_worker_stats: Some(rpc::CoreWorkerStats {
                num_pending_tasks: num_pending as i32,
                num_running_tasks: num_executing as i64,
                ..Default::default()
            }),
            tasks_total: (num_pending + num_executing) as i64,
            ..Default::default()
        })
    }

    /// Handle worker exit request.
    pub async fn handle_exit(
        &self,
        request: rpc::ExitRequest,
    ) -> Result<rpc::ExitReply, Status> {
        tracing::info!(force = request.force_exit, "Exit requested");
        // Mark the task receiver as exiting so no new tasks are accepted.
        self.core_worker.task_receiver().set_exiting();
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

    /// Handle local GC by deleting zero-ref objects from the memory store.
    pub fn handle_local_gc(
        &self,
        _request: rpc::LocalGcRequest,
    ) -> Result<rpc::LocalGcReply, Status> {
        tracing::debug!("LocalGC requested");
        // In a full implementation, this would trigger garbage collection
        // of objects with zero reference count. For now, acknowledged.
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
        request: rpc::UpdateObjectLocationBatchRequest,
    ) -> Result<rpc::UpdateObjectLocationBatchReply, Status> {
        let node_id_hex = hex::encode(&request.node_id);
        let ref_counter = self.core_worker.reference_counter();

        for update in &request.object_location_updates {
            let oid = ObjectID::from_binary(&update.object_id);
            if let Some(plasma_update) = update.plasma_location_update {
                if plasma_update == rpc::ObjectPlasmaLocationUpdate::Added as i32 {
                    ref_counter.add_object_location(&oid, node_id_hex.clone());
                } else {
                    ref_counter.remove_object_location(&oid, &node_id_hex);
                }
            }
        }
        Ok(rpc::UpdateObjectLocationBatchReply::default())
    }

    pub async fn handle_get_object_locations_owner(
        &self,
        request: rpc::GetObjectLocationsOwnerRequest,
    ) -> Result<rpc::GetObjectLocationsOwnerReply, Status> {
        let ref_counter = self.core_worker.reference_counter();
        let object_location_infos = request
            .object_ids
            .iter()
            .map(|oid_bytes| {
                let oid = ObjectID::from_binary(oid_bytes);
                let locations = ref_counter.get_object_locations(&oid);
                rpc::WorkerObjectLocationsPubMessage {
                    node_ids: locations
                        .iter()
                        .filter_map(|loc| hex::decode(loc).ok())
                        .collect(),
                    ..Default::default()
                }
            })
            .collect();
        Ok(rpc::GetObjectLocationsOwnerReply {
            object_location_infos,
        })
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
        request: rpc::PlasmaObjectReadyRequest,
    ) -> Result<rpc::PlasmaObjectReadyReply, Status> {
        let oid = ObjectID::from_binary(&request.object_id);
        self.core_worker.dependency_resolver().object_available(&oid);
        Ok(rpc::PlasmaObjectReadyReply::default())
    }

    pub async fn handle_assign_object_owner(
        &self,
        request: rpc::AssignObjectOwnerRequest,
    ) -> Result<rpc::AssignObjectOwnerReply, Status> {
        let oid = ObjectID::from_binary(&request.object_id);
        let owner_address = request
            .borrower_address
            .unwrap_or_else(|| self.core_worker.worker_address().clone());
        let contained: Vec<ObjectID> = request
            .contained_object_ids
            .iter()
            .map(|b| ObjectID::from_binary(b))
            .collect();
        self.core_worker
            .reference_counter()
            .add_owned_object(oid, owner_address, contained);
        Ok(rpc::AssignObjectOwnerReply::default())
    }

    pub async fn handle_register_mutable_object_reader(
        &self,
        _request: rpc::RegisterMutableObjectReaderRequest,
    ) -> Result<rpc::RegisterMutableObjectReaderReply, Status> {
        Ok(rpc::RegisterMutableObjectReaderReply::default())
    }
}

// ─── Tonic trait impl ───────────────────────────────────────────────────

#[tonic::async_trait]
impl rpc::core_worker_service_server::CoreWorkerService for CoreWorkerServiceImpl {
    async fn push_task(
        &self,
        req: Request<rpc::PushTaskRequest>,
    ) -> Result<Response<rpc::PushTaskReply>, Status> {
        self.handle_push_task(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn raylet_notify_gcs_restart(
        &self,
        req: Request<rpc::RayletNotifyGcsRestartRequest>,
    ) -> Result<Response<rpc::RayletNotifyGcsRestartReply>, Status> {
        self.handle_raylet_notify_gcs_restart(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn actor_call_arg_wait_complete(
        &self,
        req: Request<rpc::ActorCallArgWaitCompleteRequest>,
    ) -> Result<Response<rpc::ActorCallArgWaitCompleteReply>, Status> {
        self.handle_actor_call_arg_wait_complete(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_object_status(
        &self,
        req: Request<rpc::GetObjectStatusRequest>,
    ) -> Result<Response<rpc::GetObjectStatusReply>, Status> {
        self.handle_get_object_status(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn wait_for_actor_ref_deleted(
        &self,
        req: Request<rpc::WaitForActorRefDeletedRequest>,
    ) -> Result<Response<rpc::WaitForActorRefDeletedReply>, Status> {
        self.handle_wait_for_actor_ref_deleted(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn pubsub_long_polling(
        &self,
        req: Request<rpc::PubsubLongPollingRequest>,
    ) -> Result<Response<rpc::PubsubLongPollingReply>, Status> {
        self.handle_pubsub_long_polling(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn report_generator_item_returns(
        &self,
        req: Request<rpc::ReportGeneratorItemReturnsRequest>,
    ) -> Result<Response<rpc::ReportGeneratorItemReturnsReply>, Status> {
        self.handle_report_generator_item_returns(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn pubsub_command_batch(
        &self,
        req: Request<rpc::PubsubCommandBatchRequest>,
    ) -> Result<Response<rpc::PubsubCommandBatchReply>, Status> {
        self.handle_pubsub_command_batch(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn update_object_location_batch(
        &self,
        req: Request<rpc::UpdateObjectLocationBatchRequest>,
    ) -> Result<Response<rpc::UpdateObjectLocationBatchReply>, Status> {
        self.handle_update_object_location_batch(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_object_locations_owner(
        &self,
        req: Request<rpc::GetObjectLocationsOwnerRequest>,
    ) -> Result<Response<rpc::GetObjectLocationsOwnerReply>, Status> {
        self.handle_get_object_locations_owner(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn kill_actor(
        &self,
        req: Request<rpc::KillActorRequest>,
    ) -> Result<Response<rpc::KillActorReply>, Status> {
        self.handle_kill_actor(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn cancel_task(
        &self,
        req: Request<rpc::CancelTaskRequest>,
    ) -> Result<Response<rpc::CancelTaskReply>, Status> {
        self.handle_cancel_task(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn request_owner_to_cancel_task(
        &self,
        req: Request<rpc::RequestOwnerToCancelTaskRequest>,
    ) -> Result<Response<rpc::RequestOwnerToCancelTaskReply>, Status> {
        self.handle_request_owner_to_cancel_task(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_core_worker_stats(
        &self,
        req: Request<rpc::GetCoreWorkerStatsRequest>,
    ) -> Result<Response<rpc::GetCoreWorkerStatsReply>, Status> {
        self.handle_get_core_worker_stats(req.into_inner())
            .map(Response::new)
    }

    async fn local_gc(
        &self,
        req: Request<rpc::LocalGcRequest>,
    ) -> Result<Response<rpc::LocalGcReply>, Status> {
        self.handle_local_gc(req.into_inner()).map(Response::new)
    }

    async fn delete_objects(
        &self,
        req: Request<rpc::DeleteObjectsRequest>,
    ) -> Result<Response<rpc::DeleteObjectsReply>, Status> {
        self.handle_delete_objects(req.into_inner())
            .map(Response::new)
    }

    async fn spill_objects(
        &self,
        req: Request<rpc::SpillObjectsRequest>,
    ) -> Result<Response<rpc::SpillObjectsReply>, Status> {
        self.handle_spill_objects(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn restore_spilled_objects(
        &self,
        req: Request<rpc::RestoreSpilledObjectsRequest>,
    ) -> Result<Response<rpc::RestoreSpilledObjectsReply>, Status> {
        self.handle_restore_spilled_objects(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn delete_spilled_objects(
        &self,
        req: Request<rpc::DeleteSpilledObjectsRequest>,
    ) -> Result<Response<rpc::DeleteSpilledObjectsReply>, Status> {
        self.handle_delete_spilled_objects(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn plasma_object_ready(
        &self,
        req: Request<rpc::PlasmaObjectReadyRequest>,
    ) -> Result<Response<rpc::PlasmaObjectReadyReply>, Status> {
        self.handle_plasma_object_ready(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn exit(
        &self,
        req: Request<rpc::ExitRequest>,
    ) -> Result<Response<rpc::ExitReply>, Status> {
        self.handle_exit(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn assign_object_owner(
        &self,
        req: Request<rpc::AssignObjectOwnerRequest>,
    ) -> Result<Response<rpc::AssignObjectOwnerReply>, Status> {
        self.handle_assign_object_owner(req.into_inner())
            .await
            .map(Response::new)
    }

    async fn num_pending_tasks(
        &self,
        req: Request<rpc::NumPendingTasksRequest>,
    ) -> Result<Response<rpc::NumPendingTasksReply>, Status> {
        self.handle_num_pending_tasks(req.into_inner())
            .map(Response::new)
    }

    async fn register_mutable_object_reader(
        &self,
        req: Request<rpc::RegisterMutableObjectReaderRequest>,
    ) -> Result<Response<rpc::RegisterMutableObjectReaderReply>, Status> {
        self.handle_register_mutable_object_reader(req.into_inner())
            .await
            .map(Response::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_common::id::{JobID, ObjectID, WorkerID};
    use crate::options::CoreWorkerOptions;

    fn make_service() -> CoreWorkerServiceImpl {
        let cw = CoreWorker::new(CoreWorkerOptions {
            job_id: JobID::from_int(1),
            ..CoreWorkerOptions::default()
        });
        CoreWorkerServiceImpl {
            core_worker: Arc::new(cw),
        }
    }

    #[tokio::test]
    async fn test_update_object_location_batch_add_and_remove() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // First, register the object so the ref counter tracks it.
        svc.core_worker.reference_counter().add_local_reference(oid);

        let node_id = vec![1u8; 28];
        let node_id_hex = hex::encode(&node_id);

        // Add a location.
        let request = rpc::UpdateObjectLocationBatchRequest {
            node_id: node_id.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Added as i32),
                ..Default::default()
            }],
            ..Default::default()
        };
        let reply = svc.handle_update_object_location_batch(request).await.unwrap();
        assert_eq!(reply, rpc::UpdateObjectLocationBatchReply::default());

        // Verify location was added.
        let locs = svc.core_worker.reference_counter().get_object_locations(&oid);
        assert_eq!(locs, vec![node_id_hex.clone()]);

        // Remove the location.
        let request = rpc::UpdateObjectLocationBatchRequest {
            node_id: node_id.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Removed as i32),
                ..Default::default()
            }],
            ..Default::default()
        };
        svc.handle_update_object_location_batch(request).await.unwrap();
        let locs = svc.core_worker.reference_counter().get_object_locations(&oid);
        assert!(locs.is_empty());
    }

    #[tokio::test]
    async fn test_get_object_locations_owner() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        let node_id = vec![2u8; 28];
        let node_id_hex = hex::encode(&node_id);

        // Register and add a location.
        svc.core_worker.reference_counter().add_local_reference(oid);
        svc.core_worker.reference_counter().add_object_location(&oid, node_id_hex.clone());

        let request = rpc::GetObjectLocationsOwnerRequest {
            object_ids: vec![oid.binary()],
            ..Default::default()
        };
        let reply = svc.handle_get_object_locations_owner(request).await.unwrap();
        assert_eq!(reply.object_location_infos.len(), 1);
        assert_eq!(reply.object_location_infos[0].node_ids, vec![node_id]);
    }

    #[tokio::test]
    async fn test_get_object_locations_owner_empty() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Object not tracked — should return empty locations.
        let request = rpc::GetObjectLocationsOwnerRequest {
            object_ids: vec![oid.binary()],
            ..Default::default()
        };
        let reply = svc.handle_get_object_locations_owner(request).await.unwrap();
        assert_eq!(reply.object_location_infos.len(), 1);
        assert!(reply.object_location_infos[0].node_ids.is_empty());
    }

    #[tokio::test]
    async fn test_plasma_object_ready() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Should succeed without error (just notifies the dependency resolver).
        let request = rpc::PlasmaObjectReadyRequest {
            object_id: oid.binary(),
        };
        let reply = svc.handle_plasma_object_ready(request).await.unwrap();
        assert_eq!(reply, rpc::PlasmaObjectReadyReply::default());
    }

    #[tokio::test]
    async fn test_assign_object_owner() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        let owner_address = rpc::Address {
            ip_address: "10.0.0.1".to_string(),
            port: 5000,
            worker_id: WorkerID::from_random().binary(),
            ..Default::default()
        };

        let request = rpc::AssignObjectOwnerRequest {
            object_id: oid.binary(),
            object_size: 1024,
            contained_object_ids: vec![],
            borrower_address: Some(owner_address.clone()),
            ..Default::default()
        };
        let reply = svc.handle_assign_object_owner(request).await.unwrap();
        assert_eq!(reply, rpc::AssignObjectOwnerReply::default());

        // Verify the object is now owned.
        assert!(svc.core_worker.reference_counter().owned_by_us(&oid));
        let owner = svc.core_worker.reference_counter().get_owner(&oid).unwrap();
        assert_eq!(owner.ip_address, "10.0.0.1");
    }

    #[tokio::test]
    async fn test_assign_object_owner_defaults_to_worker_address() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // No borrower_address — should default to worker's own address.
        let request = rpc::AssignObjectOwnerRequest {
            object_id: oid.binary(),
            object_size: 512,
            contained_object_ids: vec![],
            borrower_address: None,
            ..Default::default()
        };
        svc.handle_assign_object_owner(request).await.unwrap();

        assert!(svc.core_worker.reference_counter().owned_by_us(&oid));
        let owner = svc.core_worker.reference_counter().get_owner(&oid).unwrap();
        assert_eq!(owner.ip_address, svc.core_worker.worker_address().ip_address);
    }
}
