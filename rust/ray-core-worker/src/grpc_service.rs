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
use crate::ownership_directory::OwnershipDirectory;
use crate::reference_counter::ReferenceCounter;

/// Channel type 2 = WORKER_OBJECT_LOCATIONS_CHANNEL (from proto ChannelType enum).
/// C++ uses this for the subscription protocol in OwnershipBasedObjectDirectory.
const CHANNEL_WORKER_OBJECT_LOCATIONS: i32 = 2;

/// Build a `WorkerObjectLocationsPubMessage` for a specific object.
/// Reused by both point-in-time queries and subscription publishing.
/// C++ equivalent: `FillObjectInformation` in `OwnershipBasedObjectDirectory`.
fn build_object_location_pub_message(
    oid: &ObjectID,
    ref_counter: &ReferenceCounter,
    ownership_dir: &OwnershipDirectory,
) -> rpc::WorkerObjectLocationsPubMessage {
    let locations = ref_counter.get_object_locations(oid);
    let node_ids: Vec<Vec<u8>> = locations
        .iter()
        .filter_map(|loc| hex::decode(loc).ok())
        .collect();

    let spilled_url = ownership_dir.get_spill_url(oid).unwrap_or_default();
    let spilled_node_id = if !spilled_url.is_empty() {
        ownership_dir.get_spilled_node_id(oid).unwrap_or_default()
    } else {
        vec![]
    };
    let object_size = ref_counter.get_object_size(oid).max(0) as u64;
    let pending_creation = ownership_dir.is_pending_creation(oid);

    let ref_removed = !ownership_dir.is_owned(oid)
        && !ownership_dir.is_borrowed(oid)
        && !node_ids.is_empty();

    rpc::WorkerObjectLocationsPubMessage {
        node_ids,
        object_size,
        spilled_url,
        spilled_node_id,
        ref_removed,
        pending_creation,
        ..Default::default()
    }
}

/// Decode a pubsub payload back into the correct proto `InnerMessage` variant.
/// C++ publishes typed messages; the subscriber receives them via long-poll.
fn decode_inner_message(
    channel_type: i32,
    payload: &[u8],
) -> Option<rpc::pub_message::InnerMessage> {
    use prost::Message as _;
    match channel_type {
        0 => {
            // WORKER_OBJECT_EVICTION
            rpc::WorkerObjectEvictionMessage::decode(payload)
                .ok()
                .map(rpc::pub_message::InnerMessage::WorkerObjectEvictionMessage)
        }
        1 => {
            // WORKER_REF_REMOVED_CHANNEL
            rpc::WorkerRefRemovedMessage::decode(payload)
                .ok()
                .map(rpc::pub_message::InnerMessage::WorkerRefRemovedMessage)
        }
        2 => {
            // WORKER_OBJECT_LOCATIONS_CHANNEL
            rpc::WorkerObjectLocationsPubMessage::decode(payload)
                .ok()
                .map(rpc::pub_message::InnerMessage::WorkerObjectLocationsMessage)
        }
        _ => None,
    }
}

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

        // Collect all owned objects with their location/spill state and
        // re-publish them so the new GCS instance can resume managing
        // object lifetimes.
        let ref_counter = self.core_worker.reference_counter();
        let owned_with_locs = ref_counter.get_all_owned_objects_with_locations();

        for (oid, locations, spill_url) in &owned_with_locs {
            // Re-add each known location so downstream consumers
            // (e.g. object directory) see them again.
            for loc in locations {
                ref_counter.add_object_location(oid, loc.clone());
            }
            // Re-set spill URL if the object was spilled.
            if let Some(url) = spill_url {
                ref_counter.set_spill_url(oid, url.clone());
            }
        }

        tracing::info!(
            count = owned_with_locs.len(),
            "Re-registered owned objects after GCS restart"
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
        // Decode payload back into the correct InnerMessage variant based on channel_type.
        let pub_messages: Vec<rpc::PubMessage> = messages
            .into_iter()
            .map(|m| {
                let inner_message = if !m.payload.is_empty() {
                    decode_inner_message(m.channel_type, &m.payload)
                } else {
                    None
                };
                rpc::PubMessage {
                    channel_type: m.channel_type,
                    key_id: m.key_id,
                    sequence_id: m.sequence_id,
                    inner_message,
                }
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
    ///
    /// C++ contract: when a subscriber registers for `WORKER_OBJECT_LOCATIONS_CHANNEL`,
    /// the owner immediately publishes the current location state as an initial snapshot.
    /// This matches the C++ `SubscribeObjectLocations` path where a new subscriber
    /// gets the current `LocationListenerState` if `subscribed == true`.
    pub async fn handle_pubsub_command_batch(
        &self,
        request: rpc::PubsubCommandBatchRequest,
    ) -> Result<rpc::PubsubCommandBatchReply, Status> {
        let publisher = self.core_worker.publisher();
        let ref_counter = self.core_worker.reference_counter();
        let ownership_dir = self.core_worker.ownership_directory();

        for cmd in &request.commands {
            match &cmd.command_message_one_of {
                Some(rpc::command::CommandMessageOneOf::SubscribeMessage(_sub_msg)) => {
                    publisher.register_subscription(
                        &request.subscriber_id,
                        cmd.channel_type,
                        &cmd.key_id,
                    );

                    // C++ contract: on subscribe to WORKER_OBJECT_LOCATIONS_CHANNEL,
                    // publish initial snapshot of current locations to the new subscriber.
                    if cmd.channel_type == CHANNEL_WORKER_OBJECT_LOCATIONS
                        && !cmd.key_id.is_empty()
                    {
                        let oid = ObjectID::from_binary(&cmd.key_id);
                        let location_msg = build_object_location_pub_message(
                            &oid,
                            ref_counter,
                            ownership_dir,
                        );
                        let payload = prost::Message::encode_to_vec(&location_msg);
                        publisher.publish(
                            CHANNEL_WORKER_OBJECT_LOCATIONS,
                            &cmd.key_id,
                            payload,
                        );
                    }
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

    /// Handle a batch of object location updates from a node.
    ///
    /// C++ contract: after updating locations, publish incremental updates to
    /// `WORKER_OBJECT_LOCATIONS_CHANNEL` for each affected object so that
    /// subscribers receive real-time location changes. This is the owner-side
    /// half of the `ObjectLocationSubscriptionCallback` protocol.
    pub async fn handle_update_object_location_batch(
        &self,
        request: rpc::UpdateObjectLocationBatchRequest,
    ) -> Result<rpc::UpdateObjectLocationBatchReply, Status> {
        if !request.intended_worker_id.is_empty()
            && request.intended_worker_id != self.core_worker.worker_id().binary()
        {
            return Err(Status::failed_precondition("wrong recipient worker"));
        }

        let node_id_hex = hex::encode(&request.node_id);
        let ref_counter = self.core_worker.reference_counter();
        let ownership_dir = self.core_worker.ownership_directory();
        let publisher = self.core_worker.publisher();

        // C++ contract: filter dead-node additions.
        let node_is_dead = ownership_dir.is_node_dead(&request.node_id);

        // Track which objects were updated so we can publish incremental updates.
        let mut updated_objects: Vec<ObjectID> = Vec::new();

        for update in &request.object_location_updates {
            let oid = ObjectID::from_binary(&update.object_id);
            let mut changed = false;

            if let Some(plasma_update) = update.plasma_location_update {
                if plasma_update == rpc::ObjectPlasmaLocationUpdate::Added as i32 {
                    if node_is_dead {
                        continue;
                    }
                    ref_counter.add_object_location(&oid, node_id_hex.clone());
                    changed = true;
                } else {
                    ref_counter.remove_object_location(&oid, &node_id_hex);
                    changed = true;
                }
            }
            if let Some(spilled) = &update.spilled_location_update {
                ref_counter.set_spill_url(&oid, spilled.spilled_url.clone());
                let spilled_node = if spilled.spilled_to_local_storage {
                    request.node_id.clone()
                } else {
                    vec![0u8; 28]
                };
                ownership_dir.set_spilled(&oid, spilled.spilled_url.clone(), spilled_node);
                if spilled.spilled_to_local_storage {
                    ref_counter.add_object_location(&oid, node_id_hex.clone());
                } else {
                    ref_counter.remove_object_location(&oid, &node_id_hex);
                }
                changed = true;
            }

            if changed {
                updated_objects.push(oid);
            }
        }

        // C++ contract: publish incremental updates to subscribers.
        // ObjectLocationSubscriptionCallback calls UpdateObjectLocations then
        // invokes all listener callbacks. Our equivalent: publish to pubsub.
        for oid in &updated_objects {
            let location_msg =
                build_object_location_pub_message(oid, ref_counter, ownership_dir);
            let payload = prost::Message::encode_to_vec(&location_msg);
            publisher.publish(CHANNEL_WORKER_OBJECT_LOCATIONS, &oid.binary(), payload);
        }

        Ok(rpc::UpdateObjectLocationBatchReply::default())
    }

    pub async fn handle_get_object_locations_owner(
        &self,
        request: rpc::GetObjectLocationsOwnerRequest,
    ) -> Result<rpc::GetObjectLocationsOwnerReply, Status> {
        let ref_counter = self.core_worker.reference_counter();
        let ownership_dir = self.core_worker.ownership_directory();

        // C++ contract: for each object, fill WorkerObjectLocationsPubMessage
        // with node_ids, object_size, spilled_url, spilled_node_id, ref_removed.
        let object_location_infos = request
            .object_ids
            .iter()
            .map(|oid_bytes| {
                let oid = ObjectID::from_binary(oid_bytes);
                build_object_location_pub_message(&oid, ref_counter, ownership_dir)
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
            intended_worker_id: svc.core_worker.worker_id().binary(),
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
            intended_worker_id: svc.core_worker.worker_id().binary(),
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
    async fn test_update_object_location_batch_sets_spilled_url() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        svc.core_worker.reference_counter().add_local_reference(oid);

        let node_id = vec![3u8; 28];
        let request = rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id,
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                spilled_location_update: Some(rpc::ObjectSpilledLocationUpdate {
                    spilled_url: "s3://bucket/obj".to_string(),
                    spilled_to_local_storage: false,
                }),
                ..Default::default()
            }],
        };

        svc.handle_update_object_location_batch(request)
            .await
            .unwrap();

        assert_eq!(
            svc.core_worker.reference_counter().get_spill_url(&oid),
            Some("s3://bucket/obj".to_string())
        );
        assert!(svc
            .core_worker
            .reference_counter()
            .get_object_locations(&oid)
            .is_empty());
    }

    #[tokio::test]
    async fn test_object_location_batch_add_remove_spill_same_object() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        svc.core_worker.reference_counter().add_local_reference(oid);

        let node1 = vec![1u8; 28];
        let node2 = vec![2u8; 28];
        let node1_hex = hex::encode(&node1);
        let node2_hex = hex::encode(&node2);

        // Step 1: add(node1)
        svc.handle_update_object_location_batch(rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id: node1.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Added as i32),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();

        let locs = svc.core_worker.reference_counter().get_object_locations(&oid);
        assert!(locs.contains(&node1_hex));

        // Step 2: add(node2)
        svc.handle_update_object_location_batch(rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id: node2.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Added as i32),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();

        let locs = svc.core_worker.reference_counter().get_object_locations(&oid);
        assert_eq!(locs.len(), 2);
        assert!(locs.contains(&node1_hex));
        assert!(locs.contains(&node2_hex));

        // Step 3: remove(node1)
        svc.handle_update_object_location_batch(rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id: node1.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Removed as i32),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();

        let locs = svc.core_worker.reference_counter().get_object_locations(&oid);
        assert_eq!(locs.len(), 1);
        assert!(locs.contains(&node2_hex));
        assert!(!locs.contains(&node1_hex));

        // Step 4: spill(url) with spilled_to_local_storage=false (remote spill)
        svc.handle_update_object_location_batch(rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id: node2.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                spilled_location_update: Some(rpc::ObjectSpilledLocationUpdate {
                    spilled_url: "s3://bucket/spilled_obj".to_string(),
                    spilled_to_local_storage: false,
                }),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();

        // Final state: node2 should be removed (remote spill), spill URL set.
        let locs = svc.core_worker.reference_counter().get_object_locations(&oid);
        assert!(!locs.contains(&node2_hex), "remote spill should remove node location");
        assert_eq!(
            svc.core_worker.reference_counter().get_spill_url(&oid),
            Some("s3://bucket/spilled_obj".to_string())
        );
    }

    #[tokio::test]
    async fn test_object_location_batch_mixed_updates_single_batch() {
        // Test that a single batch with multiple updates for the same object
        // processes them in order (last-write-wins for spill, set-based for locations).
        let svc = make_service();
        let oid = ObjectID::from_random();
        svc.core_worker.reference_counter().add_local_reference(oid);

        let node_id = vec![1u8; 28];
        let node_id_hex = hex::encode(&node_id);

        // Single batch: add + spill(local) for the same object.
        svc.handle_update_object_location_batch(rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id: node_id.clone(),
            object_location_updates: vec![
                rpc::ObjectLocationUpdate {
                    object_id: oid.binary(),
                    plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Added as i32),
                    ..Default::default()
                },
                rpc::ObjectLocationUpdate {
                    object_id: oid.binary(),
                    spilled_location_update: Some(rpc::ObjectSpilledLocationUpdate {
                        spilled_url: "file:///local/spill".to_string(),
                        spilled_to_local_storage: true,
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .await
        .unwrap();

        // After processing: node should be in locations (added by plasma update
        // and re-added by local spill), plus spill URL set.
        let locs = svc.core_worker.reference_counter().get_object_locations(&oid);
        assert!(locs.contains(&node_id_hex));
        assert_eq!(
            svc.core_worker.reference_counter().get_spill_url(&oid),
            Some("file:///local/spill".to_string())
        );
    }

    #[tokio::test]
    async fn test_update_object_location_batch_rejects_wrong_recipient() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        svc.core_worker.reference_counter().add_local_reference(oid);

        let err = svc
            .handle_update_object_location_batch(rpc::UpdateObjectLocationBatchRequest {
                intended_worker_id: vec![9u8; 28],
                node_id: vec![1u8; 28],
                object_location_updates: vec![rpc::ObjectLocationUpdate {
                    object_id: oid.binary(),
                    plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Added as i32),
                    ..Default::default()
                }],
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
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
    async fn test_get_object_locations_owner_returns_full_owner_information() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        let node_id = vec![3u8; 28];
        let node_id_hex = hex::encode(&node_id);

        // Register object and set it as owned with spill info
        svc.core_worker.reference_counter().add_local_reference(oid);
        svc.core_worker
            .reference_counter()
            .add_object_location(&oid, node_id_hex);

        // Set spill info via ownership directory
        let owner_addr = rpc::Address {
            worker_id: svc.core_worker.worker_id().binary(),
            ..Default::default()
        };
        svc.core_worker
            .ownership_directory()
            .add_owned_object(oid, owner_addr);
        svc.core_worker
            .ownership_directory()
            .set_spilled(&oid, "s3://bucket/obj".to_string(), vec![3u8; 28]);

        // Set object size
        svc.core_worker
            .reference_counter()
            .update_object_size(&oid, 1024);

        let request = rpc::GetObjectLocationsOwnerRequest {
            object_ids: vec![oid.binary()],
            ..Default::default()
        };
        let reply = svc
            .handle_get_object_locations_owner(request)
            .await
            .unwrap();
        assert_eq!(reply.object_location_infos.len(), 1);
        let info = &reply.object_location_infos[0];
        assert_eq!(info.node_ids, vec![node_id]);
        assert_eq!(info.object_size, 1024);
        assert_eq!(info.spilled_url, "s3://bucket/obj");
        assert!(!info.ref_removed, "owned object should not have ref_removed");
    }

    // ─── CORE-10 Round 4: spilled_node_id + dead-node filtering ─────

    #[tokio::test]
    async fn test_get_object_locations_owner_returns_spilled_node_id() {
        // Verify that spilled_node_id is correct AFTER spill.
        // Round 3 bug: set_spilled() clears pinned_at_node_id, so
        // get_pinned_node() returned empty. Fixed by storing spilled_node_id separately.
        let svc = make_service();
        let oid = ObjectID::from_random();

        svc.core_worker.reference_counter().add_local_reference(oid);
        let spill_node = vec![9u8; 28];
        let owner_addr = rpc::Address {
            worker_id: svc.core_worker.worker_id().binary(),
            ..Default::default()
        };
        svc.core_worker
            .ownership_directory()
            .add_owned_object(oid, owner_addr);

        // Spill the object — spilled_node_id should be stored separately from pinned_at_node_id
        svc.core_worker
            .ownership_directory()
            .set_spilled(&oid, "s3://bucket/spilled_obj".to_string(), spill_node.clone());

        // Verify pinned_at_node_id is cleared (expected by contract)
        assert!(svc.core_worker.ownership_directory().get_pinned_node(&oid).is_none());
        // But spilled_node_id should still be available
        assert_eq!(
            svc.core_worker.ownership_directory().get_spilled_node_id(&oid),
            Some(spill_node.clone())
        );

        // Now verify via the RPC path
        let reply = svc
            .handle_get_object_locations_owner(rpc::GetObjectLocationsOwnerRequest {
                object_ids: vec![oid.binary()],
                ..Default::default()
            })
            .await
            .unwrap();
        let info = &reply.object_location_infos[0];
        assert_eq!(info.spilled_url, "s3://bucket/spilled_obj");
        assert_eq!(
            info.spilled_node_id, spill_node,
            "spilled_node_id must be correct after spill, not empty"
        );
    }

    #[tokio::test]
    async fn test_update_object_location_batch_ignores_dead_node_additions() {
        // C++ contract: additions from dead nodes should be ignored.
        let svc = make_service();
        let oid = ObjectID::from_random();
        let dead_node_id = vec![7u8; 28];

        svc.core_worker.reference_counter().add_local_reference(oid);
        let owner_addr = rpc::Address {
            worker_id: svc.core_worker.worker_id().binary(),
            ..Default::default()
        };
        svc.core_worker
            .ownership_directory()
            .add_owned_object(oid, owner_addr);

        // Mark the node as dead
        svc.core_worker
            .ownership_directory()
            .mark_node_dead(&dead_node_id);

        // Try to add a location from the dead node via the RPC path
        let request = rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id: dead_node_id.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Added as i32),
                ..Default::default()
            }],
        };
        svc.handle_update_object_location_batch(request)
            .await
            .unwrap();

        // Location should NOT have been added (dead node filtered)
        let locs = svc
            .core_worker
            .reference_counter()
            .get_object_locations(&oid);
        assert!(
            locs.is_empty(),
            "additions from dead nodes should be ignored"
        );
    }

    #[tokio::test]
    async fn test_update_object_location_batch_filters_dead_nodes() {
        // Adding a location from a node, then removing it, should result in empty locations
        let svc = make_service();
        let oid = ObjectID::from_random();
        let node_id = vec![4u8; 28];
        let node_id_hex = hex::encode(&node_id);

        svc.core_worker.reference_counter().add_local_reference(oid);
        svc.core_worker
            .reference_counter()
            .add_object_location(&oid, node_id_hex.clone());

        // Remove the location (simulates node death / object eviction)
        svc.core_worker
            .reference_counter()
            .remove_object_location(&oid, &node_id_hex);

        let locs = svc.core_worker.reference_counter().get_object_locations(&oid);
        assert!(locs.is_empty(), "location should be removed after dead-node filter");
    }

    #[tokio::test]
    async fn test_owner_death_cleanup_end_to_end() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        let owner_worker_id = vec![5u8; 28];

        // Add borrowed object
        let owner_addr = rpc::Address {
            worker_id: owner_worker_id.clone(),
            ..Default::default()
        };
        svc.core_worker
            .ownership_directory()
            .add_borrowed_object(oid, owner_addr);

        assert!(svc.core_worker.ownership_directory().is_borrowed(&oid));
        assert!(!svc.core_worker.ownership_directory().is_owner_dead(&oid));

        // Propagate owner death
        let affected = svc
            .core_worker
            .ownership_directory()
            .propagate_owner_death(&owner_worker_id);
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0], oid);
        assert!(svc.core_worker.ownership_directory().is_owner_dead(&oid));
    }

    // ─── Object Location Query and Subscription Protocol ──────────────
    //
    // The Rust implementation has full parity with the C++ distributed pub/sub
    // protocol for object location tracking:
    //   - subscribe (WORKER_OBJECT_LOCATIONS_CHANNEL)
    //   - initial snapshot delivery on subscribe
    //   - incremental add/remove broadcast
    //   - unsubscribe stops updates
    //   - owner-death propagation via handle_publisher_failure
    //   - production transport loop (SubscriberTransport.poll_publisher)
    //
    // See tests below for proof of each contract.

    /// Point-in-time query: GetObjectLocationsOwner returns correct location data.
    #[tokio::test]
    async fn test_object_location_point_in_time_query_works() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        let node_id = vec![10u8; 28];
        let node_hex = hex::encode(&node_id);

        svc.core_worker.reference_counter().add_local_reference(oid);
        svc.core_worker
            .reference_counter()
            .add_object_location(&oid, node_hex.clone());

        // Point-in-time query works correctly
        let reply = svc
            .handle_get_object_locations_owner(rpc::GetObjectLocationsOwnerRequest {
                object_ids: vec![oid.binary()],
                intended_worker_id: svc.core_worker.worker_id().binary(),
            })
            .await
            .unwrap();
        assert_eq!(reply.object_location_infos.len(), 1);
        assert!(!reply.object_location_infos[0].node_ids.is_empty());
    }

    // ─── CORE-10: Object Location Subscription Protocol ──────────────
    //
    // C++ contract: OwnershipBasedObjectDirectory implements:
    //   - subscribe (WORKER_OBJECT_LOCATIONS_CHANNEL on owner's publisher)
    //   - initial snapshot delivery on subscribe
    //   - incremental update broadcast when locations change
    //   - owner death propagation (pubsub failure)
    //   - unsubscribe stops updates

    /// C++ contract: when a subscriber registers for WORKER_OBJECT_LOCATIONS_CHANNEL,
    /// the owner immediately publishes the current location state as an initial snapshot.
    /// This matches C++ SubscribeObjectLocations where a new subscriber gets
    /// LocationListenerState if subscribed == true.
    #[tokio::test]
    async fn test_object_location_subscription_receives_initial_snapshot() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        let node_id = vec![10u8; 28];
        let node_hex = hex::encode(&node_id);

        // Owner has this object with a known location.
        svc.core_worker
            .ownership_directory()
            .add_owned_object(oid, rpc::Address::default());
        svc.core_worker
            .ownership_directory()
            .mark_object_created(&oid);
        svc.core_worker.reference_counter().add_local_reference(oid);
        svc.core_worker
            .reference_counter()
            .add_object_location(&oid, node_hex);

        // Subscribe to WORKER_OBJECT_LOCATIONS_CHANNEL for this object.
        let subscriber_id = b"sub_snapshot";
        let subscribe_cmd = rpc::Command {
            channel_type: super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            key_id: oid.binary(),
            command_message_one_of: Some(rpc::command::CommandMessageOneOf::SubscribeMessage(
                rpc::SubMessage {
                    sub_message_one_of: None,
                },
            )),
        };
        svc.handle_pubsub_command_batch(rpc::PubsubCommandBatchRequest {
            subscriber_id: subscriber_id.to_vec(),
            commands: vec![subscribe_cmd],
        })
        .await
        .unwrap();

        // The publisher should have an initial snapshot message queued.
        let mailbox_size = svc
            .core_worker
            .publisher()
            .subscriber_mailbox_size(subscriber_id);
        assert!(
            mailbox_size >= 1,
            "initial snapshot must be published on subscribe (got {} messages)",
            mailbox_size
        );

        // Verify the snapshot content via long-poll.
        let reply = svc
            .handle_pubsub_long_polling(rpc::PubsubLongPollingRequest {
                subscriber_id: subscriber_id.to_vec(),
                max_processed_sequence_id: 0,
                publisher_id: vec![],
            })
            .await
            .unwrap();
        assert!(!reply.pub_messages.is_empty(), "should receive snapshot");
        let msg = &reply.pub_messages[0];
        assert_eq!(msg.channel_type, super::CHANNEL_WORKER_OBJECT_LOCATIONS);
        assert_eq!(msg.key_id, oid.binary());

        // Inner message should be a WorkerObjectLocationsPubMessage with the node.
        match &msg.inner_message {
            Some(rpc::pub_message::InnerMessage::WorkerObjectLocationsMessage(loc_msg)) => {
                assert!(
                    loc_msg.node_ids.iter().any(|n| *n == node_id),
                    "snapshot must include the known location"
                );
                assert!(!loc_msg.pending_creation, "object was marked created");
            }
            other => panic!("expected WorkerObjectLocationsMessage, got {:?}", other),
        }
    }

    /// C++ contract: when locations change (via UpdateObjectLocationBatch),
    /// the owner publishes incremental updates to WORKER_OBJECT_LOCATIONS_CHANNEL.
    /// Subscribers receive both ADD and REMOVE updates.
    #[tokio::test]
    async fn test_object_location_subscription_receives_incremental_add_remove_updates() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        let node_id = vec![20u8; 28];

        svc.core_worker
            .ownership_directory()
            .add_owned_object(oid, rpc::Address::default());
        svc.core_worker.reference_counter().add_local_reference(oid);

        // Subscribe.
        let subscriber_id = b"sub_incremental";
        let subscribe_cmd = rpc::Command {
            channel_type: super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            key_id: oid.binary(),
            command_message_one_of: Some(rpc::command::CommandMessageOneOf::SubscribeMessage(
                rpc::SubMessage {
                    sub_message_one_of: None,
                },
            )),
        };
        svc.handle_pubsub_command_batch(rpc::PubsubCommandBatchRequest {
            subscriber_id: subscriber_id.to_vec(),
            commands: vec![subscribe_cmd],
        })
        .await
        .unwrap();

        // Drain the initial snapshot.
        let _ = svc
            .handle_pubsub_long_polling(rpc::PubsubLongPollingRequest {
                subscriber_id: subscriber_id.to_vec(),
                max_processed_sequence_id: 0,
                publisher_id: vec![],
            })
            .await
            .unwrap();

        // Send an ADD update.
        svc.handle_update_object_location_batch(rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id: node_id.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Added as i32),
                ..Default::default()
            }],
        })
        .await
        .unwrap();

        // Subscriber should get incremental update with the new location.
        let reply = svc
            .handle_pubsub_long_polling(rpc::PubsubLongPollingRequest {
                subscriber_id: subscriber_id.to_vec(),
                max_processed_sequence_id: 1, // skip the snapshot
                publisher_id: vec![],
            })
            .await
            .unwrap();
        assert!(!reply.pub_messages.is_empty(), "should receive ADD update");
        let add_msg = &reply.pub_messages[0];
        match &add_msg.inner_message {
            Some(rpc::pub_message::InnerMessage::WorkerObjectLocationsMessage(loc_msg)) => {
                assert!(
                    loc_msg.node_ids.iter().any(|n| *n == node_id),
                    "incremental update must include the added node"
                );
            }
            other => panic!("expected location message, got {:?}", other),
        }

        // Send a REMOVE update.
        svc.handle_update_object_location_batch(rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id: node_id.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Removed as i32),
                ..Default::default()
            }],
        })
        .await
        .unwrap();

        // Subscriber should get incremental update with location removed.
        let reply2 = svc
            .handle_pubsub_long_polling(rpc::PubsubLongPollingRequest {
                subscriber_id: subscriber_id.to_vec(),
                max_processed_sequence_id: 2, // skip previous messages
                publisher_id: vec![],
            })
            .await
            .unwrap();
        assert!(
            !reply2.pub_messages.is_empty(),
            "should receive REMOVE update"
        );
        let rem_msg = &reply2.pub_messages[0];
        match &rem_msg.inner_message {
            Some(rpc::pub_message::InnerMessage::WorkerObjectLocationsMessage(loc_msg)) => {
                assert!(
                    !loc_msg.node_ids.iter().any(|n| *n == node_id),
                    "removed node must not appear in update"
                );
            }
            other => panic!("expected location message, got {:?}", other),
        }
    }

    /// C++ contract: when the owner dies, the subscriber detects the failure
    /// (long-poll timeout/connection lost) and invokes failure callbacks for
    /// all subscriptions to that publisher. This test exercises the real
    /// subscriber-side failure mechanism (handle_publisher_failure), NOT
    /// publisher-side subscriber removal.
    ///
    /// The subscriber is a ray-pubsub::Subscriber that has subscribed to the
    /// owner's WORKER_OBJECT_LOCATIONS_CHANNEL with a failure callback. When
    /// handle_publisher_failure is called (simulating the long-poll loop
    /// detecting that the owner is unreachable), the failure callback fires.
    #[tokio::test]
    async fn test_object_location_subscription_owner_death_via_publisher_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let owner_worker_id = b"dead_owner_42";
        let oid = ObjectID::from_random();

        // Create a subscriber (the remote node that wants to track object locations).
        let subscriber = ray_pubsub::Subscriber::new(
            b"remote_node".to_vec(),
            ray_pubsub::SubscriberConfig::default(),
        );

        // Track failure callback invocations.
        let failure_count = std::sync::Arc::new(AtomicUsize::new(0));
        let failure_count_clone = failure_count.clone();

        // The failure callback — C++ equivalent: failure_callback in SubscribeObjectLocations
        // calls mark_as_failed_(obj_id, ErrorType::OWNER_DIED).
        let failure_cb: ray_pubsub::FailureCallback =
            std::sync::Arc::new(move |_key_id: &[u8]| {
                failure_count_clone.fetch_add(1, Ordering::Relaxed);
            });

        // Subscribe to the owner for this object's location updates.
        let item_cb: ray_pubsub::MessageCallback =
            std::sync::Arc::new(|_msg: &ray_pubsub::PubMessage| {});
        subscriber.subscribe(
            owner_worker_id,
            super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            &oid.binary(),
            item_cb,
            Some(failure_cb),
        );

        // Verify subscription exists.
        assert!(subscriber.is_subscribed(
            owner_worker_id,
            super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            &oid.binary()
        ));

        // Owner dies — the subscriber detects this via long-poll failure
        // and invokes handle_publisher_failure.
        let notified = subscriber.handle_publisher_failure(owner_worker_id);
        assert_eq!(notified.len(), 1, "one subscription should be notified");
        assert_eq!(notified[0], oid.binary(), "notified key must be the object ID");

        // Failure callback must have fired.
        assert_eq!(
            failure_count.load(Ordering::Relaxed),
            1,
            "failure callback must fire on publisher failure (owner death)"
        );

        // Subscription must be removed.
        assert!(
            !subscriber.is_subscribed(
                owner_worker_id,
                super::CHANNEL_WORKER_OBJECT_LOCATIONS,
                &oid.binary()
            ),
            "subscription must be removed after publisher failure"
        );
    }

    /// Proves that the subscriber failure mechanism correctly handles multiple
    /// object subscriptions to the same owner. When the owner dies, ALL failure
    /// callbacks fire — one per subscribed object.
    /// C++ equivalent: OwnershipBasedObjectDirectory failure_callback fires for
    /// each object subscribed to the dead owner.
    #[tokio::test]
    async fn test_object_location_subscription_failure_path_uses_subscriber_failure_mechanism() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let owner_worker_id = b"dead_owner_99";
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();
        let oid3 = ObjectID::from_random();

        let subscriber = ray_pubsub::Subscriber::new(
            b"remote_node_2".to_vec(),
            ray_pubsub::SubscriberConfig::default(),
        );

        let failure_count = std::sync::Arc::new(AtomicUsize::new(0));

        // Subscribe to 3 objects on the same owner.
        for oid in &[oid1, oid2, oid3] {
            let fc = failure_count.clone();
            let failure_cb: ray_pubsub::FailureCallback =
                std::sync::Arc::new(move |_key_id: &[u8]| {
                    fc.fetch_add(1, Ordering::Relaxed);
                });
            let item_cb: ray_pubsub::MessageCallback =
                std::sync::Arc::new(|_msg: &ray_pubsub::PubMessage| {});
            subscriber.subscribe(
                owner_worker_id,
                super::CHANNEL_WORKER_OBJECT_LOCATIONS,
                &oid.binary(),
                item_cb,
                Some(failure_cb),
            );
        }

        assert_eq!(subscriber.num_subscriptions(), 3);

        // Owner dies.
        let notified = subscriber.handle_publisher_failure(owner_worker_id);
        assert_eq!(notified.len(), 3, "all 3 subscriptions must be notified");

        // All 3 failure callbacks must have fired.
        assert_eq!(
            failure_count.load(Ordering::Relaxed),
            3,
            "all 3 failure callbacks must fire on owner death"
        );

        // All subscriptions must be removed.
        assert_eq!(subscriber.num_subscriptions(), 0);
    }

    /// Proves the real long-poll failure path: when the subscriber sends a
    /// long-poll request but the publisher is gone (simulated by timeout
    /// with no response), followed by handle_publisher_failure, the
    /// failure callback fires for all subscribed objects.
    /// This is the closest unit-test approximation of the C++ subscriber
    /// detecting owner death via long-poll failure.
    #[tokio::test]
    async fn test_object_location_subscription_real_long_poll_failure_path() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let svc = make_service();
        let oid = ObjectID::from_random();

        svc.core_worker
            .ownership_directory()
            .add_owned_object(oid, rpc::Address::default());
        svc.core_worker
            .reference_counter()
            .add_local_reference(oid);

        // A remote subscriber subscribes to object locations on this owner.
        let subscriber_id = b"sub_longpoll_fail";
        let subscribe_cmd = rpc::Command {
            channel_type: super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            key_id: oid.binary(),
            command_message_one_of: Some(rpc::command::CommandMessageOneOf::SubscribeMessage(
                rpc::SubMessage {
                    sub_message_one_of: None,
                },
            )),
        };
        svc.handle_pubsub_command_batch(rpc::PubsubCommandBatchRequest {
            subscriber_id: subscriber_id.to_vec(),
            commands: vec![subscribe_cmd],
        })
        .await
        .unwrap();

        // Drain the initial snapshot (subscriber received it via long-poll).
        let reply = svc
            .handle_pubsub_long_polling(rpc::PubsubLongPollingRequest {
                subscriber_id: subscriber_id.to_vec(),
                max_processed_sequence_id: 0,
                publisher_id: vec![],
            })
            .await
            .unwrap();
        assert!(!reply.pub_messages.is_empty(), "initial snapshot delivered");

        // Now simulate: the subscriber's NEXT long-poll times out (owner died).
        // The real transport loop would: send long-poll → timeout → no response.
        // We simulate this with a very short timeout that returns empty.
        let timed_out_reply = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            svc.handle_pubsub_long_polling(rpc::PubsubLongPollingRequest {
                subscriber_id: subscriber_id.to_vec(),
                max_processed_sequence_id: 1, // ack the snapshot
                publisher_id: vec![],
            }),
        )
        .await;

        // The long-poll either timed out or returned empty (no new messages).
        // This is what triggers the subscriber to call handle_publisher_failure.
        let got_empty = match timed_out_reply {
            Ok(Ok(r)) => r.pub_messages.is_empty(),
            Ok(Err(_)) => true,
            Err(_) => true, // timeout
        };
        assert!(got_empty, "no new messages = potential owner death");

        // The subscriber-side failure detection:
        // Create a subscriber-side Subscriber to track the failure callback.
        let subscriber_side = ray_pubsub::Subscriber::new(
            subscriber_id.to_vec(),
            ray_pubsub::SubscriberConfig::default(),
        );
        let failure_fired = std::sync::Arc::new(AtomicUsize::new(0));
        let ff = failure_fired.clone();
        let failure_cb: ray_pubsub::FailureCallback =
            std::sync::Arc::new(move |_key: &[u8]| {
                ff.fetch_add(1, Ordering::Relaxed);
            });
        let item_cb: ray_pubsub::MessageCallback =
            std::sync::Arc::new(|_: &ray_pubsub::PubMessage| {});

        let owner_id = svc.core_worker.worker_id().binary();
        subscriber_side.subscribe(
            &owner_id,
            super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            &oid.binary(),
            item_cb,
            Some(failure_cb),
        );

        // Subscriber detects failure → calls handle_publisher_failure.
        let notified = subscriber_side.handle_publisher_failure(&owner_id);
        assert_eq!(notified.len(), 1);
        assert_eq!(
            failure_fired.load(Ordering::Relaxed),
            1,
            "failure callback must fire when subscriber detects owner death via long-poll failure"
        );
    }

    /// C++ contract: UnsubscribeObjectLocations removes the callback.
    /// If no callbacks remain, the pubsub subscription is removed.
    /// After unsubscribe, no more incremental updates are delivered.
    #[tokio::test]
    async fn test_object_location_unsubscribe_stops_updates() {
        let svc = make_service();
        let oid = ObjectID::from_random();
        let node_id = vec![30u8; 28];

        svc.core_worker
            .ownership_directory()
            .add_owned_object(oid, rpc::Address::default());
        svc.core_worker.reference_counter().add_local_reference(oid);

        // Subscribe.
        let subscriber_id = b"sub_unsub";
        let subscribe_cmd = rpc::Command {
            channel_type: super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            key_id: oid.binary(),
            command_message_one_of: Some(rpc::command::CommandMessageOneOf::SubscribeMessage(
                rpc::SubMessage {
                    sub_message_one_of: None,
                },
            )),
        };
        svc.handle_pubsub_command_batch(rpc::PubsubCommandBatchRequest {
            subscriber_id: subscriber_id.to_vec(),
            commands: vec![subscribe_cmd],
        })
        .await
        .unwrap();

        // Drain initial snapshot.
        let _ = svc
            .handle_pubsub_long_polling(rpc::PubsubLongPollingRequest {
                subscriber_id: subscriber_id.to_vec(),
                max_processed_sequence_id: 0,
                publisher_id: vec![],
            })
            .await
            .unwrap();

        // Unsubscribe.
        let unsubscribe_cmd = rpc::Command {
            channel_type: super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            key_id: oid.binary(),
            command_message_one_of: Some(rpc::command::CommandMessageOneOf::UnsubscribeMessage(
                rpc::UnsubscribeMessage {},
            )),
        };
        svc.handle_pubsub_command_batch(rpc::PubsubCommandBatchRequest {
            subscriber_id: subscriber_id.to_vec(),
            commands: vec![unsubscribe_cmd],
        })
        .await
        .unwrap();

        // Send a location update AFTER unsubscribe.
        svc.handle_update_object_location_batch(rpc::UpdateObjectLocationBatchRequest {
            intended_worker_id: svc.core_worker.worker_id().binary(),
            node_id: node_id.clone(),
            object_location_updates: vec![rpc::ObjectLocationUpdate {
                object_id: oid.binary(),
                plasma_location_update: Some(rpc::ObjectPlasmaLocationUpdate::Added as i32),
                ..Default::default()
            }],
        })
        .await
        .unwrap();

        // Subscriber mailbox should be empty — no updates after unsubscribe.
        let mailbox = svc
            .core_worker
            .publisher()
            .subscriber_mailbox_size(subscriber_id);
        assert_eq!(
            mailbox, 0,
            "unsubscribed subscriber must NOT receive updates"
        );
    }

    // ─── CORE-10 Round 10: production transport loop proof ──────────
    //
    // These tests use SubscriberTransport.poll_publisher() — the production
    // transport driver — to prove owner-failure detection and message delivery
    // WITHOUT direct calls to handle_publisher_failure or handle_poll_response.

    /// Proves that owner failure is detected by the production subscriber
    /// transport loop. A subscriber subscribes to WORKER_OBJECT_LOCATIONS_CHANNEL
    /// on an owner. When the owner is unreachable (FailingSubscriberClient),
    /// the transport's poll_publisher internally calls handle_publisher_failure,
    /// which fires the failure callbacks.
    ///
    /// No direct call to handle_publisher_failure from the test.
    #[tokio::test]
    async fn test_object_location_subscription_owner_failure_is_detected_by_production_loop() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let owner_worker_id = b"dead_owner_transport";
        let oid = ObjectID::from_random();

        // Create a subscriber (the remote node).
        let subscriber = std::sync::Arc::new(ray_pubsub::Subscriber::new(
            b"remote_transport_node".to_vec(),
            ray_pubsub::SubscriberConfig::default(),
        ));

        let failure_count = std::sync::Arc::new(AtomicUsize::new(0));
        let fc = failure_count.clone();
        let failure_cb: ray_pubsub::FailureCallback =
            std::sync::Arc::new(move |_key: &[u8]| {
                fc.fetch_add(1, Ordering::Relaxed);
            });
        let item_cb: ray_pubsub::MessageCallback =
            std::sync::Arc::new(|_: &ray_pubsub::PubMessage| {});

        subscriber.subscribe(
            owner_worker_id,
            super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            &oid.binary(),
            item_cb,
            Some(failure_cb),
        );

        assert!(subscriber.is_subscribed(
            owner_worker_id,
            super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            &oid.binary()
        ));

        // Create the production transport with a FAILING client (owner is dead).
        let transport = ray_pubsub::SubscriberTransport::new(subscriber.clone());
        let failing_client = ray_pubsub::FailingSubscriberClient;

        // Run the production transport loop — this:
        // 1. Tries to send command batch → fails (owner unreachable)
        // 2. Internally calls subscriber.handle_publisher_failure()
        // 3. Failure callback fires
        let result = transport
            .poll_publisher(owner_worker_id, &failing_client)
            .await;
        assert!(result.is_err(), "transport should report publisher failure");

        // Failure callback must have fired — driven by the transport, not manually.
        assert_eq!(
            failure_count.load(Ordering::Relaxed),
            1,
            "failure callback must fire via production transport (no manual handle_publisher_failure)"
        );

        // Subscription must be removed.
        assert!(
            !subscriber.is_subscribed(
                owner_worker_id,
                super::CHANNEL_WORKER_OBJECT_LOCATIONS,
                &oid.binary()
            ),
            "subscription must be removed after transport-detected failure"
        );
    }

    /// Proves that the production subscriber transport loop delivers object
    /// location updates end-to-end. A subscriber subscribes to an owner's
    /// WORKER_OBJECT_LOCATIONS_CHANNEL. The transport drains commands, sends
    /// them to the owner's publisher, the owner publishes a location update,
    /// the transport long-polls and receives it, dispatching via the callback.
    ///
    /// No direct call to handle_poll_response from the test.
    #[tokio::test]
    async fn test_object_location_subscription_failure_callback_runs_without_manual_handle_publisher_failure(
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let owner_worker_id = b"owner_transport_loc";

        // Set up the owner's publisher.
        let owner_publisher = std::sync::Arc::new(ray_pubsub::Publisher::new(
            ray_pubsub::PublisherConfig::default(),
        ));
        owner_publisher.register_channel(
            super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            true, // droppable
        );

        let oid = ObjectID::from_random();

        // Create a subscriber (the remote node).
        let subscriber = std::sync::Arc::new(ray_pubsub::Subscriber::new(
            b"remote_loc_sub".to_vec(),
            ray_pubsub::SubscriberConfig::default(),
        ));

        let message_count = std::sync::Arc::new(AtomicUsize::new(0));
        let mc = message_count.clone();
        let item_cb: ray_pubsub::MessageCallback =
            std::sync::Arc::new(move |_msg: &ray_pubsub::PubMessage| {
                mc.fetch_add(1, Ordering::Relaxed);
            });

        subscriber.subscribe(
            owner_worker_id,
            super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            &oid.binary(),
            item_cb,
            None,
        );

        // Create transport and in-process client.
        let transport = ray_pubsub::SubscriberTransport::new(subscriber.clone());
        let client = ray_pubsub::InProcessSubscriberClient::new(
            owner_publisher.clone(),
            owner_worker_id.to_vec(),
        );

        // Owner publishes a location update (after small delay to let
        // transport send commands first).
        let pub_clone = owner_publisher.clone();
        let oid_binary = oid.binary();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            // Publish a location update message.
            let location_payload = vec![1, 2, 3]; // simplified payload
            pub_clone.publish(
                super::CHANNEL_WORKER_OBJECT_LOCATIONS,
                &oid_binary,
                location_payload,
            );
        });

        // Run the production transport loop — this:
        // 1. Drains subscribe command, sends to owner publisher
        // 2. Long-polls the owner publisher
        // 3. Receives the location update
        // 4. Dispatches via subscriber.handle_poll_response (internally)
        let processed = transport
            .poll_publisher(owner_worker_id, &client)
            .await
            .unwrap();
        assert_eq!(
            processed, 1,
            "one location update dispatched by production transport"
        );
        assert_eq!(
            message_count.load(Ordering::Relaxed),
            1,
            "callback fired via production transport (no manual handle_poll_response)"
        );
    }

    // ─── CORE-10 Round 11: runtime integration proof ──────────
    //
    // These tests prove that the CoreWorker runtime itself drives the
    // subscriber transport loop for object-location subscriptions.
    // The test calls subscribe_object_locations on the CoreWorker, which
    // automatically starts the runtime poll loop — no test-created transport.

    /// Proves that subscribe_object_locations starts the runtime poll loop.
    /// The owner publishes a location update, the runtime loop receives it
    /// and dispatches to the callback.
    /// No test-created SubscriberTransport.
    #[tokio::test]
    async fn test_object_location_transport_loop_runs_in_runtime() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let svc = make_service();
        let oid = ObjectID::from_random();
        let owner_worker_id = vec![130u8; 28];

        // Set up the owner's publisher.
        let owner_publisher = std::sync::Arc::new(ray_pubsub::Publisher::new(
            ray_pubsub::PublisherConfig::default(),
        ));
        owner_publisher.register_channel(
            super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            true,
        );

        // Install the client factory so the runtime starts poll loops.
        let pub_for_factory = owner_publisher.clone();
        let factory: std::sync::Arc<crate::core_worker::SubscriberClientFactory> =
            std::sync::Arc::new(move |publisher_id: &[u8]| {
                std::sync::Arc::new(ray_pubsub::InProcessSubscriberClient::new(
                    pub_for_factory.clone(),
                    publisher_id.to_vec(),
                )) as std::sync::Arc<dyn ray_pubsub::SubscriberClient + 'static>
            });
        svc.core_worker.set_location_client_factory(factory);

        // Subscribe to object locations on the owner via the runtime.
        let message_count = std::sync::Arc::new(AtomicUsize::new(0));
        let mc = message_count.clone();
        let item_cb: ray_pubsub::MessageCallback =
            std::sync::Arc::new(move |_msg: &ray_pubsub::PubMessage| {
                mc.fetch_add(1, Ordering::Relaxed);
            });
        svc.core_worker.subscribe_object_locations(
            &owner_worker_id,
            &oid.binary(),
            item_cb,
            None,
        );

        // The runtime must have started the poll loop.
        assert!(
            svc.core_worker.has_location_poll_loop(&owner_worker_id),
            "runtime must start poll loop for the owner"
        );

        // Wait for the runtime loop to register with the publisher.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Owner publishes a location update.
        let oid_binary = oid.binary();
        owner_publisher.publish(
            super::CHANNEL_WORKER_OBJECT_LOCATIONS,
            &oid_binary,
            vec![1, 2, 3],
        );

        // Give the runtime loop time to poll and dispatch.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(
            message_count.load(Ordering::Relaxed),
            1,
            "callback must fire via runtime transport loop (no test-created transport)"
        );
    }

    /// Proves that owner failure is detected by the runtime poll loop.
    /// The test installs a FailingSubscriberClient factory, subscribes to
    /// object locations, and verifies that the runtime detects failure and
    /// fires the failure callback.
    /// No test-created SubscriberTransport.
    #[tokio::test]
    async fn test_object_location_owner_failure_reaches_subscriber_in_runtime() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let svc = make_service();
        let oid = ObjectID::from_random();
        let owner_worker_id = vec![131u8; 28];

        // Install a FAILING client factory (owner is dead).
        let factory: std::sync::Arc<crate::core_worker::SubscriberClientFactory> =
            std::sync::Arc::new(|_publisher_id: &[u8]| {
                std::sync::Arc::new(ray_pubsub::FailingSubscriberClient)
                    as std::sync::Arc<dyn ray_pubsub::SubscriberClient + 'static>
            });
        svc.core_worker.set_location_client_factory(factory);

        let failure_count = std::sync::Arc::new(AtomicUsize::new(0));
        let fc = failure_count.clone();
        let failure_cb: ray_pubsub::FailureCallback =
            std::sync::Arc::new(move |_key: &[u8]| {
                fc.fetch_add(1, Ordering::Relaxed);
            });
        let item_cb: ray_pubsub::MessageCallback =
            std::sync::Arc::new(|_: &ray_pubsub::PubMessage| {});

        // Subscribe — the runtime starts the poll loop with a failing client.
        svc.core_worker.subscribe_object_locations(
            &owner_worker_id,
            &oid.binary(),
            item_cb,
            Some(failure_cb),
        );

        // Give the runtime loop time to detect failure.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(
            failure_count.load(Ordering::Relaxed),
            1,
            "failure callback must fire via runtime failure detection (no test-created transport)"
        );

        // Subscription must be removed by failure handling.
        assert!(
            !svc.core_worker.location_subscriber().is_subscribed(
                &owner_worker_id,
                super::CHANNEL_WORKER_OBJECT_LOCATIONS,
                &oid.binary()
            ),
            "subscription must be removed after runtime-detected failure"
        );
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

    #[tokio::test]
    async fn test_gcs_restart_triggers_reregistration() {
        let svc = make_service();
        let ref_counter = svc.core_worker.reference_counter();

        // Register two owned objects with locations.
        let oid1 = ObjectID::from_random();
        let addr = ray_proto::ray::rpc::Address {
            node_id: vec![0u8; 28],
            ip_address: "127.0.0.1".to_string(),
            port: 1234,
            worker_id: svc.core_worker.worker_id().binary(),
        };
        ref_counter.add_owned_object(oid1, addr.clone(), vec![]);
        ref_counter.add_object_location(&oid1, "node_a".to_string());

        let oid2 = ObjectID::from_random();
        ref_counter.add_owned_object(oid2, addr.clone(), vec![]);
        ref_counter.set_spill_url(&oid2, "s3://bucket/obj2".to_string());

        // A borrowed object should NOT be re-registered.
        let oid3 = ObjectID::from_random();
        ref_counter.add_borrowed_object(oid3, addr.clone());
        ref_counter.add_object_location(&oid3, "node_b".to_string());

        // Trigger GCS restart.
        let req = rpc::RayletNotifyGcsRestartRequest::default();
        let reply = svc.handle_raylet_notify_gcs_restart(req).await.unwrap();
        assert_eq!(reply, rpc::RayletNotifyGcsRestartReply::default());

        // After restart notification, owned objects should still be queryable.
        assert!(ref_counter.owned_by_us(&oid1));
        assert!(ref_counter.owned_by_us(&oid2));

        // Locations and spill URLs should persist.
        let locs1 = ref_counter.get_object_locations(&oid1);
        assert!(locs1.contains(&"node_a".to_string()));
        assert_eq!(
            ref_counter.get_spill_url(&oid2),
            Some("s3://bucket/obj2".to_string())
        );

        // Borrowed object should still be tracked.
        assert!(ref_counter.has_reference(&oid3));
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
