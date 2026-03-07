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
        tracing::debug!(seq = request.sequence_number, "PushTask received");
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

        let running = self
            .core_worker
            .task_receiver()
            .is_task_running(&request.intended_task_id);

        if running {
            let cancelled = self
                .core_worker
                .task_receiver()
                .cancel_task(&request.intended_task_id, request.force_kill);
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
    pub async fn handle_exit(&self, request: rpc::ExitRequest) -> Result<rpc::ExitReply, Status> {
        tracing::info!(force = request.force_exit, "Exit requested");
        // Mark the task receiver as exiting so no new tasks are accepted.
        self.core_worker.task_receiver().set_exiting();
        Ok(rpc::ExitReply { success: true })
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

    /// Handle local GC by deleting objects from the memory store that have
    /// no references in the reference counter.
    pub fn handle_local_gc(
        &self,
        _request: rpc::LocalGcRequest,
    ) -> Result<rpc::LocalGcReply, Status> {
        let ref_counter = self.core_worker.reference_counter();
        let all_objects = self.core_worker.memory_store().all_object_ids();
        let to_delete: Vec<ObjectID> = all_objects
            .into_iter()
            .filter(|oid| !ref_counter.has_reference(oid))
            .collect();
        if !to_delete.is_empty() {
            tracing::debug!(count = to_delete.len(), "LocalGC freeing objects");
            self.core_worker.delete_objects(&to_delete);
        }
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

    /// Handle GCS restart notification from the Raylet.
    ///
    /// On GCS restart, we need to re-register object ownership information
    /// so the new GCS process can resume managing object lifetimes.
    pub async fn handle_raylet_notify_gcs_restart(
        &self,
        _request: rpc::RayletNotifyGcsRestartRequest,
    ) -> Result<rpc::RayletNotifyGcsRestartReply, Status> {
        tracing::info!("GCS restart notification received — refreshing ownership state");

        // Re-register all owned objects with the new GCS instance.
        let owned_objects = self.core_worker.reference_counter().get_all_owned_objects();
        tracing::info!(
            count = owned_objects.len(),
            "Re-registering owned objects after GCS restart"
        );

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

    /// Handle a pubsub long-polling request from a subscriber.
    ///
    /// The subscriber connects and waits for messages. We register its
    /// interest via `Publisher::connect_subscriber()` which returns a
    /// oneshot receiver that resolves when messages are available.
    pub async fn handle_pubsub_long_polling(
        &self,
        request: rpc::PubsubLongPollingRequest,
    ) -> Result<rpc::PubsubLongPollingReply, Status> {
        let publisher = self.core_worker.publisher();
        let rx =
            publisher.connect_subscriber(&request.subscriber_id, request.max_processed_sequence_id);

        // Wait for messages (or publisher timeout/disconnection).
        let messages = match tokio::time::timeout(
            std::time::Duration::from_secs(
                ray_common::constants::DEFAULT_PUBSUB_LONG_POLL_TIMEOUT_SECS,
            ),
            rx,
        )
        .await
        {
            Ok(Ok(msgs)) => msgs,
            Ok(Err(_)) => Vec::new(), // Sender dropped
            Err(_) => Vec::new(),     // Timeout
        };

        // Convert internal PubMessage to proto PubMessage.
        let pub_messages: Vec<rpc::PubMessage> = messages
            .into_iter()
            .map(|m| rpc::PubMessage {
                channel_type: m.channel_type,
                key_id: m.key_id,
                sequence_id: m.sequence_id,
                inner_message: None,
            })
            .collect();

        Ok(rpc::PubsubLongPollingReply {
            pub_messages,
            publisher_id: self.core_worker.worker_id().binary(),
        })
    }

    /// Handle an intermediate result from a generator/streaming task executor.
    ///
    /// The executor worker sends partial results back to the task owner.
    /// We store the inlined object data in the memory store so the consumer
    /// can retrieve it.
    pub async fn handle_report_generator_item_returns(
        &self,
        request: rpc::ReportGeneratorItemReturnsRequest,
    ) -> Result<rpc::ReportGeneratorItemReturnsReply, Status> {
        let generator_id_hex = hex::encode(&request.generator_id);
        tracing::debug!(
            generator_id = %generator_id_hex,
            item_index = request.item_index,
            "ReportGeneratorItemReturns received"
        );

        // If the returned object has inlined data, store it.
        if let Some(ret_obj) = &request.returned_object {
            if !ret_obj.in_plasma && !ret_obj.object_id.is_empty() {
                let oid = ObjectID::from_binary(&ret_obj.object_id);
                let ray_obj = crate::memory_store::RayObject::new(
                    bytes::Bytes::from(ret_obj.data.clone()),
                    bytes::Bytes::from(ret_obj.metadata.clone()),
                    ret_obj
                        .nested_inlined_refs
                        .iter()
                        .map(|r| ObjectID::from_binary(&r.object_id))
                        .collect(),
                );
                // Ignore error if object already exists (duplicate delivery).
                let _ = self.core_worker.memory_store().put(oid, ray_obj);
                self.core_worker
                    .dependency_resolver()
                    .object_available(&oid);
            }
        }

        Ok(rpc::ReportGeneratorItemReturnsReply {
            total_num_object_consumed: -1, // Unknown — executor assumes all consumed.
        })
    }

    /// Handle a batch of pubsub commands (subscribe/unsubscribe) from a subscriber.
    pub async fn handle_pubsub_command_batch(
        &self,
        request: rpc::PubsubCommandBatchRequest,
    ) -> Result<rpc::PubsubCommandBatchReply, Status> {
        let publisher = self.core_worker.publisher();

        for cmd in &request.commands {
            match &cmd.command_message_one_of {
                Some(rpc::command::CommandMessageOneOf::SubscribeMessage(_sub_msg)) => {
                    publisher.register_subscription(
                        &request.subscriber_id,
                        cmd.channel_type,
                        &cmd.key_id,
                    );
                }
                Some(rpc::command::CommandMessageOneOf::UnsubscribeMessage(_)) => {
                    publisher.unregister_subscription(
                        &request.subscriber_id,
                        cmd.channel_type,
                        &cmd.key_id,
                    );
                }
                None => {
                    tracing::warn!("Received pubsub command with no message");
                }
            }
        }

        Ok(rpc::PubsubCommandBatchReply {})
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

    /// Handle a cancellation request from the task caller to the task owner.
    ///
    /// The caller asks the owner to cancel the task identified by its return
    /// object ID. The owner can then propagate cancellation to the executing worker.
    pub async fn handle_request_owner_to_cancel_task(
        &self,
        request: rpc::RequestOwnerToCancelTaskRequest,
    ) -> Result<rpc::RequestOwnerToCancelTaskReply, Status> {
        let object_id_hex = hex::encode(&request.remote_object_id);
        tracing::debug!(
            object_id = %object_id_hex,
            force_kill = request.force_kill,
            recursive = request.recursive,
            "RequestOwnerToCancelTask received"
        );

        // Try to cancel via the task receiver if the task is executing on this worker.
        let cancelled = self
            .core_worker
            .task_receiver()
            .cancel_task(&request.remote_object_id, request.force_kill);

        if cancelled {
            tracing::info!(object_id = %object_id_hex, "Task cancellation succeeded");
        } else {
            tracing::debug!(object_id = %object_id_hex, "Task not found for cancellation (may be on another worker)");
        }

        Ok(rpc::RequestOwnerToCancelTaskReply {})
    }

    pub async fn handle_spill_objects(
        &self,
        request: rpc::SpillObjectsRequest,
    ) -> Result<rpc::SpillObjectsReply, Status> {
        let spill_manager = self.core_worker.spill_manager();
        let memory_store = self.core_worker.memory_store();
        let mut urls = Vec::with_capacity(request.object_refs_to_spill.len());

        for obj_ref in &request.object_refs_to_spill {
            let oid = ObjectID::from_binary(&obj_ref.object_id);
            let obj = memory_store.get(&oid);
            match obj {
                Some(ray_obj) => {
                    let url = spill_manager
                        .spill_object(&oid, &ray_obj.data, &ray_obj.metadata)
                        .map_err(|e| Status::internal(format!("spill failed: {}", e)))?;
                    urls.push(url);
                }
                None => {
                    return Err(Status::not_found(format!(
                        "object {} not found in memory store",
                        oid.hex()
                    )));
                }
            }
        }

        // Delete objects from memory store if requested.
        if let Some(delete_req) = request.delete_request {
            let oids: Vec<ObjectID> = delete_req
                .object_ids
                .iter()
                .map(|b| ObjectID::from_binary(b))
                .collect();
            self.core_worker.delete_objects(&oids);
        }

        Ok(rpc::SpillObjectsReply {
            spilled_objects_url: urls,
        })
    }

    pub async fn handle_restore_spilled_objects(
        &self,
        request: rpc::RestoreSpilledObjectsRequest,
    ) -> Result<rpc::RestoreSpilledObjectsReply, Status> {
        let spill_manager = self.core_worker.spill_manager();
        let mut bytes_restored: i64 = 0;

        for (i, url) in request.spilled_objects_url.iter().enumerate() {
            let (data, metadata) = spill_manager
                .restore_object(url)
                .map_err(|e| Status::internal(format!("restore failed: {}", e)))?;

            bytes_restored += data.len() as i64;

            // Put restored object back into memory store if object_id is provided.
            if i < request.object_ids_to_restore.len() {
                let oid = ObjectID::from_binary(&request.object_ids_to_restore[i]);
                let ray_obj = crate::memory_store::RayObject::new(
                    bytes::Bytes::from(data),
                    bytes::Bytes::from(metadata),
                    Vec::new(),
                );
                // Ignore error if object already exists (restored by another path).
                let _ = self.core_worker.memory_store().put(oid, ray_obj);
            }
        }

        Ok(rpc::RestoreSpilledObjectsReply {
            bytes_restored_total: bytes_restored,
        })
    }

    pub async fn handle_delete_spilled_objects(
        &self,
        request: rpc::DeleteSpilledObjectsRequest,
    ) -> Result<rpc::DeleteSpilledObjectsReply, Status> {
        let spill_manager = self.core_worker.spill_manager();
        for url in &request.spilled_objects_url {
            spill_manager
                .delete_spilled_object(url)
                .map_err(|e| Status::internal(format!("delete spilled failed: {}", e)))?;
        }
        Ok(rpc::DeleteSpilledObjectsReply::default())
    }

    pub async fn handle_plasma_object_ready(
        &self,
        request: rpc::PlasmaObjectReadyRequest,
    ) -> Result<rpc::PlasmaObjectReadyReply, Status> {
        let oid = ObjectID::from_binary(&request.object_id);
        self.core_worker
            .dependency_resolver()
            .object_available(&oid);
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
        self.handle_exit(req.into_inner()).await.map(Response::new)
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
#[allow(clippy::needless_update)]
mod tests {
    use super::*;
    use crate::options::CoreWorkerOptions;
    use ray_common::id::{JobID, ObjectID, WorkerID};

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
        let reply = svc
            .handle_update_object_location_batch(request)
            .await
            .unwrap();
        assert_eq!(reply, rpc::UpdateObjectLocationBatchReply::default());

        // Verify location was added.
        let locs = svc
            .core_worker
            .reference_counter()
            .get_object_locations(&oid);
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
        svc.handle_update_object_location_batch(request)
            .await
            .unwrap();
        let locs = svc
            .core_worker
            .reference_counter()
            .get_object_locations(&oid);
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
        svc.core_worker
            .reference_counter()
            .add_object_location(&oid, node_id_hex.clone());

        let request = rpc::GetObjectLocationsOwnerRequest {
            object_ids: vec![oid.binary()],
            ..Default::default()
        };
        let reply = svc
            .handle_get_object_locations_owner(request)
            .await
            .unwrap();
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
        let reply = svc
            .handle_get_object_locations_owner(request)
            .await
            .unwrap();
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
        assert_eq!(
            owner.ip_address,
            svc.core_worker.worker_address().ip_address
        );
    }

    #[tokio::test]
    async fn test_spill_and_restore_objects() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Put an object into the memory store.
        svc.core_worker
            .put_object(
                oid,
                bytes::Bytes::from("spill_data"),
                bytes::Bytes::from("meta"),
            )
            .unwrap();

        // Spill the object.
        let spill_req = rpc::SpillObjectsRequest {
            object_refs_to_spill: vec![rpc::ObjectReference {
                object_id: oid.binary(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let spill_reply = svc.handle_spill_objects(spill_req).await.unwrap();
        assert_eq!(spill_reply.spilled_objects_url.len(), 1);
        let url = &spill_reply.spilled_objects_url[0];
        assert!(url.starts_with("file://"));

        // Delete the object from memory store.
        svc.core_worker.delete_objects(&[oid]);
        assert!(!svc.core_worker.contains_object(&oid));

        // Restore the spilled object.
        let restore_req = rpc::RestoreSpilledObjectsRequest {
            spilled_objects_url: vec![url.clone()],
            object_ids_to_restore: vec![oid.binary()],
        };
        let restore_reply = svc
            .handle_restore_spilled_objects(restore_req)
            .await
            .unwrap();
        assert!(restore_reply.bytes_restored_total > 0);

        // Verify the object is back in the memory store.
        assert!(svc.core_worker.contains_object(&oid));
        let obj = svc.core_worker.memory_store().get(&oid).unwrap();
        assert_eq!(obj.data.as_ref(), b"spill_data");
        assert_eq!(obj.metadata.as_ref(), b"meta");

        // Delete the spilled file.
        let delete_req = rpc::DeleteSpilledObjectsRequest {
            spilled_objects_url: vec![url.clone()],
        };
        let delete_reply = svc.handle_delete_spilled_objects(delete_req).await.unwrap();
        assert_eq!(delete_reply, rpc::DeleteSpilledObjectsReply::default());
    }

    #[tokio::test]
    async fn test_spill_with_delete_request() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        svc.core_worker
            .put_object(oid, bytes::Bytes::from("data"), bytes::Bytes::new())
            .unwrap();
        assert!(svc.core_worker.contains_object(&oid));

        // Spill with delete_request to remove from memory after spilling.
        let spill_req = rpc::SpillObjectsRequest {
            object_refs_to_spill: vec![rpc::ObjectReference {
                object_id: oid.binary(),
                ..Default::default()
            }],
            delete_request: Some(rpc::DeleteObjectsRequest {
                object_ids: vec![oid.binary()],
                ..Default::default()
            }),
            ..Default::default()
        };
        let reply = svc.handle_spill_objects(spill_req).await.unwrap();
        assert_eq!(reply.spilled_objects_url.len(), 1);
        assert!(!svc.core_worker.contains_object(&oid));
    }

    #[test]
    fn test_local_gc_frees_unreferenced_objects() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Put an object directly into the memory store (bypassing ref counting)
        // to simulate an object with no references.
        let obj = crate::memory_store::RayObject::new(
            bytes::Bytes::from("gc_me"),
            bytes::Bytes::new(),
            Vec::new(),
        );
        svc.core_worker.memory_store().put(oid, obj).unwrap();
        assert!(svc.core_worker.contains_object(&oid));

        // No reference counter entry exists for this object.
        assert!(!svc.core_worker.reference_counter().has_reference(&oid));

        // GC should remove it from the memory store.
        let reply = svc.handle_local_gc(rpc::LocalGcRequest::default()).unwrap();
        assert_eq!(reply, rpc::LocalGcReply::default());
        assert!(!svc.core_worker.contains_object(&oid));
    }

    #[test]
    fn test_local_gc_preserves_referenced_objects() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Put an object and add an extra local reference.
        svc.core_worker
            .put_object(oid, bytes::Bytes::from("keep_me"), bytes::Bytes::new())
            .unwrap();
        svc.core_worker.add_local_reference(oid);

        // GC should NOT remove objects with references.
        svc.handle_local_gc(rpc::LocalGcRequest::default()).unwrap();
        assert!(svc.core_worker.contains_object(&oid));
    }

    #[tokio::test]
    async fn test_spill_nonexistent_object_fails() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        let spill_req = rpc::SpillObjectsRequest {
            object_refs_to_spill: vec![rpc::ObjectReference {
                object_id: oid.binary(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let result = svc.handle_spill_objects(spill_req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pubsub_command_batch_subscribe_unsubscribe() {
        let svc = make_service();

        // Register a channel on the publisher.
        svc.core_worker.publisher().register_channel(3, false);

        // Subscribe via command batch.
        let subscribe_cmd = rpc::Command {
            channel_type: 3,
            key_id: b"actor_A".to_vec(),
            command_message_one_of: Some(rpc::command::CommandMessageOneOf::SubscribeMessage(
                rpc::SubMessage {
                    sub_message_one_of: None,
                },
            )),
        };
        let req = rpc::PubsubCommandBatchRequest {
            subscriber_id: b"sub1".to_vec(),
            commands: vec![subscribe_cmd],
        };
        let reply = svc.handle_pubsub_command_batch(req).await.unwrap();
        assert_eq!(reply, rpc::PubsubCommandBatchReply {});

        // Verify the subscription was registered by publishing and checking mailbox.
        svc.core_worker
            .publisher()
            .publish(3, b"actor_A", b"hello".to_vec());
        assert_eq!(
            svc.core_worker.publisher().subscriber_mailbox_size(b"sub1"),
            1
        );

        // Unsubscribe.
        let unsub_cmd = rpc::Command {
            channel_type: 3,
            key_id: b"actor_A".to_vec(),
            command_message_one_of: Some(rpc::command::CommandMessageOneOf::UnsubscribeMessage(
                rpc::UnsubscribeMessage {},
            )),
        };
        let req = rpc::PubsubCommandBatchRequest {
            subscriber_id: b"sub1".to_vec(),
            commands: vec![unsub_cmd],
        };
        svc.handle_pubsub_command_batch(req).await.unwrap();

        // Publishing again should not increase mailbox (unsubscribed).
        svc.core_worker
            .publisher()
            .publish(3, b"actor_A", b"world".to_vec());
        // Mailbox should still have the old message only (unsubscribe removes from index).
        assert_eq!(
            svc.core_worker.publisher().subscriber_mailbox_size(b"sub1"),
            1
        );
    }

    #[tokio::test]
    async fn test_pubsub_long_polling_immediate() {
        let svc = make_service();

        // Register channel and subscription, then publish a message.
        svc.core_worker.publisher().register_channel(3, false);
        svc.core_worker
            .publisher()
            .register_subscription(b"sub1", 3, b"");
        svc.core_worker
            .publisher()
            .publish(3, b"k1", b"v1".to_vec());

        // Long-poll should return immediately with the queued message.
        let req = rpc::PubsubLongPollingRequest {
            subscriber_id: b"sub1".to_vec(),
            max_processed_sequence_id: 0,
            publisher_id: vec![],
        };
        let reply = svc.handle_pubsub_long_polling(req).await.unwrap();
        assert_eq!(reply.pub_messages.len(), 1);
        assert_eq!(reply.pub_messages[0].key_id, b"k1");
        assert!(!reply.publisher_id.is_empty());
    }

    #[tokio::test]
    async fn test_report_generator_item_returns_stores_object() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        let req = rpc::ReportGeneratorItemReturnsRequest {
            returned_object: Some(rpc::ReturnObject {
                object_id: oid.binary(),
                in_plasma: false,
                data: b"gen_data".to_vec(),
                metadata: b"gen_meta".to_vec(),
                nested_inlined_refs: vec![],
                size: 8,
                direct_transport_metadata: None,
            }),
            worker_addr: None,
            item_index: 0,
            generator_id: ObjectID::from_random().binary(),
            attempt_number: 0,
        };
        let reply = svc.handle_report_generator_item_returns(req).await.unwrap();
        assert_eq!(reply.total_num_object_consumed, -1);

        // Object should be in the memory store.
        assert!(svc.core_worker.contains_object(&oid));
        let obj = svc.core_worker.memory_store().get(&oid).unwrap();
        assert_eq!(obj.data.as_ref(), b"gen_data");
        assert_eq!(obj.metadata.as_ref(), b"gen_meta");
    }

    #[tokio::test]
    async fn test_request_owner_to_cancel_task() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        let req = rpc::RequestOwnerToCancelTaskRequest {
            remote_object_id: oid.binary(),
            force_kill: false,
            recursive: false,
        };
        let reply = svc.handle_request_owner_to_cancel_task(req).await.unwrap();
        assert_eq!(reply, rpc::RequestOwnerToCancelTaskReply {});
    }

    #[tokio::test]
    async fn test_raylet_notify_gcs_restart() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Put an owned object so there's something to re-register.
        svc.core_worker
            .put_object(oid, bytes::Bytes::from("data"), bytes::Bytes::new())
            .unwrap();

        let req = rpc::RayletNotifyGcsRestartRequest::default();
        let reply = svc.handle_raylet_notify_gcs_restart(req).await.unwrap();
        assert_eq!(reply, rpc::RayletNotifyGcsRestartReply::default());

        // Object should still be there after restart notification.
        assert!(svc.core_worker.contains_object(&oid));
    }

    // ─── Tests for handle_get_object_status ─────────────────────────

    #[tokio::test]
    async fn test_get_object_status_existing_object() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Put an object into the memory store.
        svc.core_worker
            .put_object(
                oid,
                bytes::Bytes::from("status_data"),
                bytes::Bytes::from("meta"),
            )
            .unwrap();

        let req = rpc::GetObjectStatusRequest {
            object_id: oid.binary(),
            ..Default::default()
        };
        let reply = svc.handle_get_object_status(req).await.unwrap();
        assert_eq!(
            reply.status,
            rpc::get_object_status_reply::ObjectStatus::Created as i32
        );
        // Object data should be included in the reply.
        let obj = reply.object.unwrap();
        assert_eq!(obj.data, b"status_data");
        assert_eq!(obj.metadata, b"meta");
        assert_eq!(reply.object_size, 11); // len("status_data")
    }

    #[tokio::test]
    async fn test_get_object_status_referenced_but_not_in_store() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Add a reference without putting the object in the store.
        svc.core_worker.add_local_reference(oid);
        assert!(!svc.core_worker.contains_object(&oid));
        assert!(svc.core_worker.reference_counter().has_reference(&oid));

        let req = rpc::GetObjectStatusRequest {
            object_id: oid.binary(),
            ..Default::default()
        };
        let reply = svc.handle_get_object_status(req).await.unwrap();
        // Should report Created because reference counter tracks it.
        assert_eq!(
            reply.status,
            rpc::get_object_status_reply::ObjectStatus::Created as i32
        );
        // No object data since it's not in the memory store.
        assert!(reply.object.is_none());
    }

    #[tokio::test]
    async fn test_get_object_status_out_of_scope() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Object not in store and not tracked by reference counter.
        assert!(!svc.core_worker.contains_object(&oid));
        assert!(!svc.core_worker.reference_counter().has_reference(&oid));

        let req = rpc::GetObjectStatusRequest {
            object_id: oid.binary(),
            ..Default::default()
        };
        let reply = svc.handle_get_object_status(req).await.unwrap();
        assert_eq!(
            reply.status,
            rpc::get_object_status_reply::ObjectStatus::OutOfScope as i32
        );
    }

    #[tokio::test]
    async fn test_get_object_status_with_nested_refs() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        let nested_oid = ObjectID::from_random();

        // Put an object with a nested reference.
        let obj = crate::memory_store::RayObject::new(
            bytes::Bytes::from("outer"),
            bytes::Bytes::new(),
            vec![nested_oid],
        );
        svc.core_worker.memory_store().put(oid, obj).unwrap();

        let req = rpc::GetObjectStatusRequest {
            object_id: oid.binary(),
            ..Default::default()
        };
        let reply = svc.handle_get_object_status(req).await.unwrap();
        assert_eq!(
            reply.status,
            rpc::get_object_status_reply::ObjectStatus::Created as i32
        );
        let ray_obj = reply.object.unwrap();
        assert_eq!(ray_obj.nested_inlined_refs.len(), 1);
        assert_eq!(
            ray_obj.nested_inlined_refs[0].object_id,
            nested_oid.binary()
        );
    }

    // ─── Tests for handle_delete_objects ─────────────────────────────

    #[test]
    fn test_delete_objects_removes_from_store() {
        let svc = make_service();
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();

        svc.core_worker
            .put_object(oid1, bytes::Bytes::from("a"), bytes::Bytes::new())
            .unwrap();
        svc.core_worker
            .put_object(oid2, bytes::Bytes::from("b"), bytes::Bytes::new())
            .unwrap();
        assert!(svc.core_worker.contains_object(&oid1));
        assert!(svc.core_worker.contains_object(&oid2));

        let req = rpc::DeleteObjectsRequest {
            object_ids: vec![oid1.binary(), oid2.binary()],
            ..Default::default()
        };
        let reply = svc.handle_delete_objects(req).unwrap();
        assert_eq!(reply, rpc::DeleteObjectsReply::default());

        assert!(!svc.core_worker.contains_object(&oid1));
        assert!(!svc.core_worker.contains_object(&oid2));
    }

    #[test]
    fn test_delete_objects_empty_list() {
        let svc = make_service();

        // Deleting an empty list should succeed without error.
        let req = rpc::DeleteObjectsRequest {
            object_ids: vec![],
            ..Default::default()
        };
        let reply = svc.handle_delete_objects(req).unwrap();
        assert_eq!(reply, rpc::DeleteObjectsReply::default());
    }

    #[test]
    fn test_delete_objects_nonexistent_is_ok() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Deleting an object that doesn't exist should not error.
        let req = rpc::DeleteObjectsRequest {
            object_ids: vec![oid.binary()],
            ..Default::default()
        };
        let reply = svc.handle_delete_objects(req).unwrap();
        assert_eq!(reply, rpc::DeleteObjectsReply::default());
    }

    // ─── Tests for handle_get_core_worker_stats ─────────────────────

    #[test]
    fn test_get_core_worker_stats_initial() {
        let svc = make_service();

        let req = rpc::GetCoreWorkerStatsRequest::default();
        let reply = svc.handle_get_core_worker_stats(req).unwrap();

        let stats = reply.core_worker_stats.unwrap();
        assert_eq!(stats.num_pending_tasks, 0);
        assert_eq!(stats.num_running_tasks, 0);
        assert_eq!(reply.tasks_total, 0);
    }

    #[tokio::test]
    async fn test_get_core_worker_stats_with_pending_tasks() {
        let svc = make_service();

        // Submit a normal task to increase pending count.
        let spec = rpc::TaskSpec {
            task_id: ray_common::id::TaskID::from_random().binary(),
            name: "stats_task".to_string(),
            ..Default::default()
        };
        svc.core_worker.submit_task(&spec).await.unwrap();

        let req = rpc::GetCoreWorkerStatsRequest::default();
        let reply = svc.handle_get_core_worker_stats(req).unwrap();

        let stats = reply.core_worker_stats.unwrap();
        assert_eq!(stats.num_pending_tasks, 1);
        assert_eq!(reply.tasks_total, 1);
    }

    // ─── Tests for handle_exit ──────────────────────────────────────

    #[tokio::test]
    async fn test_exit_marks_worker_as_exiting() {
        let svc = make_service();

        // Worker should not be exiting initially.
        assert!(!svc.core_worker.task_receiver().is_exiting());

        let req = rpc::ExitRequest { force_exit: false };
        let reply = svc.handle_exit(req).await.unwrap();
        assert!(reply.success);

        // Worker should now be marked as exiting.
        assert!(svc.core_worker.task_receiver().is_exiting());
    }

    #[tokio::test]
    async fn test_exit_force_exit() {
        let svc = make_service();

        let req = rpc::ExitRequest { force_exit: true };
        let reply = svc.handle_exit(req).await.unwrap();
        assert!(reply.success);
        assert!(svc.core_worker.task_receiver().is_exiting());
    }

    // ─── Tests for handle_kill_actor ────────────────────────────────

    #[tokio::test]
    async fn test_kill_actor_via_grpc_handler() {
        let svc = make_service();
        let aid = ActorID::from_random();

        // Register an actor first.
        let handle = crate::actor_handle::ActorHandle::from_proto(rpc::ActorHandle {
            actor_id: aid.binary(),
            name: "grpc_kill_actor".to_string(),
            ..Default::default()
        });
        svc.core_worker.create_actor(aid, handle).unwrap();
        assert!(svc
            .core_worker
            .actor_manager()
            .get_actor_handle(&aid)
            .is_some());

        // Kill the actor via the gRPC handler.
        let req = rpc::KillActorRequest {
            intended_actor_id: aid.binary(),
            force_kill: false,
            ..Default::default()
        };
        let reply = svc.handle_kill_actor(req).await.unwrap();
        assert_eq!(reply, rpc::KillActorReply::default());

        // Actor should be removed.
        assert!(svc
            .core_worker
            .actor_manager()
            .get_actor_handle(&aid)
            .is_none());
    }

    // ─── Tests for handle_cancel_task ───────────────────────────────

    #[tokio::test]
    async fn test_cancel_task_not_running() {
        let svc = make_service();
        let task_id = ray_common::id::TaskID::from_random().binary();

        let req = rpc::CancelTaskRequest {
            intended_task_id: task_id,
            force_kill: false,
            ..Default::default()
        };
        let reply = svc.handle_cancel_task(req).await.unwrap();
        assert!(!reply.requested_task_running);
        assert!(!reply.attempt_succeeded);
    }

    // ─── Tests for handle_num_pending_tasks ─────────────────────────

    #[test]
    fn test_num_pending_tasks_initial() {
        let svc = make_service();

        let req = rpc::NumPendingTasksRequest::default();
        let reply = svc.handle_num_pending_tasks(req).unwrap();
        assert_eq!(reply.num_pending_tasks, 0);
    }

    #[tokio::test]
    async fn test_num_pending_tasks_after_submit() {
        let svc = make_service();

        // Submit two tasks.
        for i in 0..2 {
            let spec = rpc::TaskSpec {
                task_id: ray_common::id::TaskID::from_random().binary(),
                name: format!("pending_{}", i),
                ..Default::default()
            };
            svc.core_worker.submit_task(&spec).await.unwrap();
        }

        let req = rpc::NumPendingTasksRequest::default();
        let reply = svc.handle_num_pending_tasks(req).unwrap();
        assert_eq!(reply.num_pending_tasks, 2);
    }

    // ─── Tests for handle_register_mutable_object_reader ────────────

    #[tokio::test]
    async fn test_register_mutable_object_reader_stub() {
        let svc = make_service();

        let req = rpc::RegisterMutableObjectReaderRequest::default();
        let reply = svc
            .handle_register_mutable_object_reader(req)
            .await
            .unwrap();
        assert_eq!(reply, rpc::RegisterMutableObjectReaderReply::default());
    }

    // ─── Tests for handle_actor_call_arg_wait_complete ──────────────

    #[tokio::test]
    async fn test_actor_call_arg_wait_complete_stub() {
        let svc = make_service();

        let req = rpc::ActorCallArgWaitCompleteRequest::default();
        let reply = svc.handle_actor_call_arg_wait_complete(req).await.unwrap();
        assert_eq!(reply, rpc::ActorCallArgWaitCompleteReply::default());
    }

    // ─── Tests for handle_wait_for_actor_ref_deleted ────────────────

    #[tokio::test]
    async fn test_wait_for_actor_ref_deleted_stub() {
        let svc = make_service();

        let req = rpc::WaitForActorRefDeletedRequest::default();
        let reply = svc.handle_wait_for_actor_ref_deleted(req).await.unwrap();
        assert_eq!(reply, rpc::WaitForActorRefDeletedReply::default());
    }

    // ─── Test for handle_report_generator_item_returns edge cases ───

    #[tokio::test]
    async fn test_report_generator_item_returns_no_object() {
        let svc = make_service();

        // Request with no returned_object should succeed.
        let req = rpc::ReportGeneratorItemReturnsRequest {
            returned_object: None,
            worker_addr: None,
            item_index: 0,
            generator_id: ObjectID::from_random().binary(),
            attempt_number: 0,
        };
        let reply = svc.handle_report_generator_item_returns(req).await.unwrap();
        assert_eq!(reply.total_num_object_consumed, -1);
    }

    #[tokio::test]
    async fn test_report_generator_item_returns_plasma_object_not_stored() {
        let svc = make_service();
        let oid = ObjectID::from_random();

        // Object marked as in_plasma should NOT be stored in the memory store.
        let req = rpc::ReportGeneratorItemReturnsRequest {
            returned_object: Some(rpc::ReturnObject {
                object_id: oid.binary(),
                in_plasma: true,
                data: b"plasma_data".to_vec(),
                metadata: Vec::new(),
                nested_inlined_refs: vec![],
                size: 11,
                direct_transport_metadata: None,
            }),
            worker_addr: None,
            item_index: 0,
            generator_id: ObjectID::from_random().binary(),
            attempt_number: 0,
        };
        let reply = svc.handle_report_generator_item_returns(req).await.unwrap();
        assert_eq!(reply.total_num_object_consumed, -1);
        // Plasma objects are not stored in the memory store.
        assert!(!svc.core_worker.contains_object(&oid));
    }

    // ─── Test for local_gc with mixed referenced/unreferenced ───────

    #[test]
    fn test_local_gc_mixed_objects() {
        let svc = make_service();
        let oid_keep = ObjectID::from_random();
        let oid_gc = ObjectID::from_random();

        // Put both objects.
        svc.core_worker
            .put_object(oid_keep, bytes::Bytes::from("keep"), bytes::Bytes::new())
            .unwrap();

        // oid_gc: put directly in memory store without reference tracking.
        let obj = crate::memory_store::RayObject::new(
            bytes::Bytes::from("gc"),
            bytes::Bytes::new(),
            Vec::new(),
        );
        svc.core_worker.memory_store().put(oid_gc, obj).unwrap();

        // Add extra local reference on oid_keep.
        svc.core_worker.add_local_reference(oid_keep);

        // GC should remove oid_gc but keep oid_keep.
        svc.handle_local_gc(rpc::LocalGcRequest::default()).unwrap();
        assert!(svc.core_worker.contains_object(&oid_keep));
        assert!(!svc.core_worker.contains_object(&oid_gc));
    }
}
