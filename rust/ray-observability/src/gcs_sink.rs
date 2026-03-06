// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS event sink.
//!
//! Forwards lifecycle events to the GCS TaskInfo service via
//! `add_task_event_data`. Events are dispatched asynchronously
//! using `tokio::spawn` (fire-and-forget).

use std::sync::Arc;

use ray_gcs_rpc_client::GcsClient;
use ray_proto::ray::rpc;

use crate::events::{EventSeverity, RayEvent};
use crate::export::EventSink;

/// An `EventSink` that reports events to GCS via the TaskInfo gRPC service.
///
/// Events are converted to `TaskEvents` proto messages and sent via
/// `add_task_event_data`. Each `flush()` call fires off an async gRPC
/// request and returns immediately (fire-and-forget).
pub struct GcsEventSink<C: GcsClient + 'static> {
    gcs_client: Arc<C>,
}

impl<C: GcsClient + 'static> GcsEventSink<C> {
    pub fn new(gcs_client: Arc<C>) -> Self {
        Self { gcs_client }
    }
}

impl<C: GcsClient + 'static> EventSink for GcsEventSink<C> {
    fn flush(&self, events: &[RayEvent]) -> usize {
        if events.is_empty() {
            return 0;
        }

        let task_events: Vec<rpc::TaskEvents> =
            events.iter().map(ray_event_to_task_events).collect();
        let count = events.len();
        let client = Arc::clone(&self.gcs_client);

        // Fire-and-forget: dispatch the gRPC call asynchronously.
        tokio::spawn(async move {
            let req = rpc::AddTaskEventDataRequest {
                data: Some(rpc::TaskEventData {
                    events_by_task: task_events,
                    ..Default::default()
                }),
            };
            if let Err(e) = client.add_task_event_data(req).await {
                tracing::warn!(error = %e, "Failed to send events to GCS");
            }
        });

        count
    }
}

/// Convert a `RayEvent` to a `TaskEvents` proto message.
///
/// Since `TaskEvents` is task-centric and `RayEvent` is generic, we map:
/// - `event_id` → `task_id` (as UTF-8 bytes for traceability)
/// - `label` + `message` → `TaskStateUpdate.node_id` field (as a carrier)
/// - `severity` → `TaskStatus` in `state_ts_ns` timestamp map
fn ray_event_to_task_events(event: &RayEvent) -> rpc::TaskEvents {
    let status = match event.severity {
        EventSeverity::Info => rpc::TaskStatus::Running as i32,
        EventSeverity::Warning | EventSeverity::Error | EventSeverity::Fatal => {
            rpc::TaskStatus::Failed as i32
        }
    };

    let mut state_ts_ns = std::collections::HashMap::new();
    state_ts_ns.insert(status, (event.timestamp * 1_000_000) as i64);

    rpc::TaskEvents {
        task_id: event.event_id.as_bytes().to_vec(),
        attempt_number: 0,
        task_info: None,
        state_updates: Some(rpc::TaskStateUpdate {
            node_id: Some(
                format!("[{}] {}: {}", event.source_type_str(), event.label, event.message)
                    .into_bytes(),
            ),
            state_ts_ns,
            ..Default::default()
        }),
        profile_events: None,
        job_id: event
            .custom_fields
            .get("job_id")
            .map(|s| s.as_bytes().to_vec())
            .unwrap_or_default(),
    }
}

impl RayEvent {
    /// Return a string representation of the source type.
    pub fn source_type_str(&self) -> &'static str {
        match self.source_type {
            crate::events::EventSourceType::Core => "CORE",
            crate::events::EventSourceType::Gcs => "GCS",
            crate::events::EventSourceType::Raylet => "RAYLET",
            crate::events::EventSourceType::Worker => "WORKER",
            crate::events::EventSourceType::Driver => "DRIVER",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventSeverity, EventSourceType};

    #[test]
    fn test_ray_event_to_task_events_info() {
        let event = RayEvent::new(
            EventSourceType::Gcs,
            EventSeverity::Info,
            "ACTOR_CREATED",
            "Actor abc created",
        );
        let te = ray_event_to_task_events(&event);
        assert_eq!(te.task_id, event.event_id.as_bytes());
        assert!(te.state_updates.is_some());

        let state = te.state_updates.unwrap();
        let node_id_str = String::from_utf8_lossy(state.node_id.as_deref().unwrap());
        assert!(node_id_str.contains("ACTOR_CREATED"));
        assert!(node_id_str.contains("Actor abc created"));
        assert!(node_id_str.contains("[GCS]"));

        // Info severity maps to Running status.
        assert!(state.state_ts_ns.contains_key(&(rpc::TaskStatus::Running as i32)));
    }

    #[test]
    fn test_ray_event_to_task_events_error() {
        let event = RayEvent::new(
            EventSourceType::Worker,
            EventSeverity::Error,
            "TASK_FAILED",
            "Task xyz failed with OOM",
        );
        let te = ray_event_to_task_events(&event);
        let state = te.state_updates.as_ref().unwrap();
        let node_id_str = String::from_utf8_lossy(state.node_id.as_deref().unwrap());
        assert!(node_id_str.contains("[WORKER]"));
        assert!(node_id_str.contains("TASK_FAILED"));

        // Error severity maps to Failed status.
        assert!(state.state_ts_ns.contains_key(&(rpc::TaskStatus::Failed as i32)));
    }

    #[test]
    fn test_ray_event_to_task_events_with_job_id() {
        let event = RayEvent::new(
            EventSourceType::Core,
            EventSeverity::Info,
            "JOB_STARTED",
            "Job started",
        )
        .with_field("job_id", "01000000");

        let te = ray_event_to_task_events(&event);
        assert_eq!(te.job_id, b"01000000");
    }

    #[test]
    fn test_source_type_str() {
        let event = RayEvent::new(
            EventSourceType::Raylet,
            EventSeverity::Warning,
            "TEST",
            "test",
        );
        assert_eq!(event.source_type_str(), "RAYLET");
    }

    /// Minimal GcsClient stub for testing GcsEventSink construction.
    struct StubGcsClient;

    #[async_trait::async_trait]
    impl ray_gcs_rpc_client::GcsClient for StubGcsClient {
        async fn add_job(&self, _: rpc::AddJobRequest) -> Result<rpc::AddJobReply, tonic::Status> { unimplemented!() }
        async fn mark_job_finished(&self, _: rpc::MarkJobFinishedRequest) -> Result<rpc::MarkJobFinishedReply, tonic::Status> { unimplemented!() }
        async fn get_all_job_info(&self, _: rpc::GetAllJobInfoRequest) -> Result<rpc::GetAllJobInfoReply, tonic::Status> { unimplemented!() }
        async fn get_next_job_id(&self, _: rpc::GetNextJobIdRequest) -> Result<rpc::GetNextJobIdReply, tonic::Status> { unimplemented!() }
        async fn report_job_error(&self, _: rpc::ReportJobErrorRequest) -> Result<rpc::ReportJobErrorReply, tonic::Status> { unimplemented!() }
        async fn register_node(&self, _: rpc::RegisterNodeRequest) -> Result<rpc::RegisterNodeReply, tonic::Status> { unimplemented!() }
        async fn unregister_node(&self, _: rpc::UnregisterNodeRequest) -> Result<rpc::UnregisterNodeReply, tonic::Status> { unimplemented!() }
        async fn get_all_node_info(&self, _: rpc::GetAllNodeInfoRequest) -> Result<rpc::GetAllNodeInfoReply, tonic::Status> { unimplemented!() }
        async fn get_cluster_id(&self, _: rpc::GetClusterIdRequest) -> Result<rpc::GetClusterIdReply, tonic::Status> { unimplemented!() }
        async fn check_alive(&self, _: rpc::CheckAliveRequest) -> Result<rpc::CheckAliveReply, tonic::Status> { unimplemented!() }
        async fn register_actor(&self, _: rpc::RegisterActorRequest) -> Result<rpc::RegisterActorReply, tonic::Status> { unimplemented!() }
        async fn create_actor(&self, _: rpc::CreateActorRequest) -> Result<rpc::CreateActorReply, tonic::Status> { unimplemented!() }
        async fn get_actor_info(&self, _: rpc::GetActorInfoRequest) -> Result<rpc::GetActorInfoReply, tonic::Status> { unimplemented!() }
        async fn get_named_actor_info(&self, _: rpc::GetNamedActorInfoRequest) -> Result<rpc::GetNamedActorInfoReply, tonic::Status> { unimplemented!() }
        async fn get_all_actor_info(&self, _: rpc::GetAllActorInfoRequest) -> Result<rpc::GetAllActorInfoReply, tonic::Status> { unimplemented!() }
        async fn kill_actor_via_gcs(&self, _: rpc::KillActorViaGcsRequest) -> Result<rpc::KillActorViaGcsReply, tonic::Status> { unimplemented!() }
        async fn report_worker_failure(&self, _: rpc::ReportWorkerFailureRequest) -> Result<rpc::ReportWorkerFailureReply, tonic::Status> { unimplemented!() }
        async fn add_worker_info(&self, _: rpc::AddWorkerInfoRequest) -> Result<rpc::AddWorkerInfoReply, tonic::Status> { unimplemented!() }
        async fn get_all_worker_info(&self, _: rpc::GetAllWorkerInfoRequest) -> Result<rpc::GetAllWorkerInfoReply, tonic::Status> { unimplemented!() }
        async fn get_all_resource_usage(&self, _: rpc::GetAllResourceUsageRequest) -> Result<rpc::GetAllResourceUsageReply, tonic::Status> { unimplemented!() }
        async fn create_placement_group(&self, _: rpc::CreatePlacementGroupRequest) -> Result<rpc::CreatePlacementGroupReply, tonic::Status> { unimplemented!() }
        async fn remove_placement_group(&self, _: rpc::RemovePlacementGroupRequest) -> Result<rpc::RemovePlacementGroupReply, tonic::Status> { unimplemented!() }
        async fn get_all_placement_group(&self, _: rpc::GetAllPlacementGroupRequest) -> Result<rpc::GetAllPlacementGroupReply, tonic::Status> { unimplemented!() }
        async fn internal_kv_get(&self, _: rpc::InternalKvGetRequest) -> Result<rpc::InternalKvGetReply, tonic::Status> { unimplemented!() }
        async fn internal_kv_put(&self, _: rpc::InternalKvPutRequest) -> Result<rpc::InternalKvPutReply, tonic::Status> { unimplemented!() }
        async fn internal_kv_del(&self, _: rpc::InternalKvDelRequest) -> Result<rpc::InternalKvDelReply, tonic::Status> { unimplemented!() }
        async fn internal_kv_keys(&self, _: rpc::InternalKvKeysRequest) -> Result<rpc::InternalKvKeysReply, tonic::Status> { unimplemented!() }
        async fn add_task_event_data(&self, _: rpc::AddTaskEventDataRequest) -> Result<rpc::AddTaskEventDataReply, tonic::Status> { unimplemented!() }
        async fn get_task_events(&self, _: rpc::GetTaskEventsRequest) -> Result<rpc::GetTaskEventsReply, tonic::Status> { unimplemented!() }
        async fn drain_node(&self, _: rpc::DrainNodeRequest) -> Result<rpc::DrainNodeReply, tonic::Status> { unimplemented!() }
        async fn get_placement_group(&self, _: rpc::GetPlacementGroupRequest) -> Result<rpc::GetPlacementGroupReply, tonic::Status> { unimplemented!() }
        async fn get_named_placement_group(&self, _: rpc::GetNamedPlacementGroupRequest) -> Result<rpc::GetNamedPlacementGroupReply, tonic::Status> { unimplemented!() }
        async fn wait_placement_group_until_ready(&self, _: rpc::WaitPlacementGroupUntilReadyRequest) -> Result<rpc::WaitPlacementGroupUntilReadyReply, tonic::Status> { unimplemented!() }
        async fn list_named_actors(&self, _: rpc::ListNamedActorsRequest) -> Result<rpc::ListNamedActorsReply, tonic::Status> { unimplemented!() }
        async fn internal_kv_multi_get(&self, _: rpc::InternalKvMultiGetRequest) -> Result<rpc::InternalKvMultiGetReply, tonic::Status> { unimplemented!() }
        async fn internal_kv_exists(&self, _: rpc::InternalKvExistsRequest) -> Result<rpc::InternalKvExistsReply, tonic::Status> { unimplemented!() }
        async fn get_all_available_resources(&self, _: rpc::GetAllAvailableResourcesRequest) -> Result<rpc::GetAllAvailableResourcesReply, tonic::Status> { unimplemented!() }
    }

    #[test]
    fn test_gcs_event_sink_construction_and_empty_flush() {
        let client = Arc::new(StubGcsClient);
        let sink = GcsEventSink::new(client);

        // Flushing with an empty slice should return 0 immediately
        // without touching tokio::spawn (early return path).
        let flushed = sink.flush(&[]);
        assert_eq!(flushed, 0);
    }
}
