//! Ray event export service — converts and stores task events.
//!
//! Implements `ray.rpc.RayEventExportGcsService` (from `gcs_service.proto`),
//! matching C++ `RayEventExportGrpcService` registered in `gcs_server.cc:813`.
//!
//! Maps C++ `GcsTaskManager::HandleAddEvents` (gcs_task_manager.cc:668-679)
//! and `ConvertToTaskEventDataRequests` (gcs_ray_event_converter.cc:223-281).
//!
//! Receives generic `RayEvent` messages via the `AddEvents` RPC, converts
//! them to `TaskEvents`, groups by job_id, and stores them in the task
//! manager for retrieval via `GetTaskEvents`.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use gcs_proto::ray::rpc::events::*;
use gcs_proto::ray::rpc::ray_event_export_gcs_service_server::RayEventExportGcsService;
use gcs_proto::ray::rpc::task_info_gcs_service_server::TaskInfoGcsService;
use gcs_proto::ray::rpc::{AddTaskEventDataRequest, TaskEventData, TaskEvents};

use crate::task_manager::GcsTaskManager;

/// Event aggregator service.
///
/// In C++, the `GcsTaskManager` directly implements the
/// `RayEventExportGcsServiceHandler` interface. In Rust, this struct
/// wraps the task manager and implements the `RayEventExportGcsService`
/// trait, forwarding converted events to the task manager.
pub struct EventExportService {
    task_manager: Arc<GcsTaskManager>,
}

impl EventExportService {
    pub fn new(task_manager: Arc<GcsTaskManager>) -> Self {
        Self { task_manager }
    }
}

/// Keep the old name as a type alias for backward compatibility.
pub type EventExportServiceStub = EventExportService;

/// Convert a `TaskDefinitionEvent` to `TaskEvents`.
/// Maps C++ `ConvertToTaskEvents(TaskDefinitionEvent&&)` (gcs_ray_event_converter.cc:91-126).
fn convert_task_definition(event: TaskDefinitionEvent) -> TaskEvents {
    use gcs_proto::ray::rpc::TaskInfoEntry;

    let mut task_event = TaskEvents {
        task_id: event.task_id.clone(),
        attempt_number: event.task_attempt,
        job_id: event.job_id.clone(),
        ..Default::default()
    };

    let mut task_info = TaskInfoEntry {
        r#type: event.task_type,
        name: event.task_name,
        task_id: event.task_id,
        job_id: event.job_id,
        parent_task_id: event.parent_task_id,
        language: event.language,
        required_resources: event.required_resources,
        runtime_env_info: Some(gcs_proto::ray::rpc::RuntimeEnvInfo {
            serialized_runtime_env: event.serialized_runtime_env,
            ..Default::default()
        }),
        ..Default::default()
    };

    if !event.placement_group_id.is_empty() {
        task_info.placement_group_id = Some(event.placement_group_id);
    }
    if let Some(cs) = event.call_site {
        if !cs.is_empty() {
            task_info.call_site = Some(cs);
        }
    }

    task_event.task_info = Some(task_info);
    task_event
}

/// Convert a `TaskLifecycleEvent` to `TaskEvents`.
/// Maps C++ `ConvertToTaskEvents(TaskLifecycleEvent&&)` (gcs_ray_event_converter.cc:132-164).
fn convert_task_lifecycle(event: TaskLifecycleEvent) -> TaskEvents {
    use gcs_proto::ray::rpc::TaskStateUpdate;

    let mut task_event = TaskEvents {
        task_id: event.task_id,
        attempt_number: event.task_attempt,
        job_id: event.job_id,
        ..Default::default()
    };

    let mut state_update = TaskStateUpdate::default();
    if !event.node_id.is_empty() {
        state_update.node_id = Some(event.node_id);
    }
    if !event.worker_id.is_empty() {
        state_update.worker_id = Some(event.worker_id);
    }
    if event.worker_pid != 0 {
        state_update.worker_pid = Some(event.worker_pid);
    }
    state_update.error_info = event.ray_error_info;
    state_update.is_debugger_paused = event.is_debugger_paused;
    state_update.actor_repr_name = event.actor_repr_name;

    // Convert state transitions to state_ts_ns map.
    for st in &event.state_transitions {
        if let Some(ref ts) = st.timestamp {
            let ns = ts.seconds * 1_000_000_000 + ts.nanos as i64;
            state_update.state_ts_ns.insert(st.state, ns);
        }
    }

    task_event.state_updates = Some(state_update);
    task_event
}

/// Convert an `ActorTaskDefinitionEvent` to `TaskEvents`.
/// Maps C++ `ConvertToTaskEvents(ActorTaskDefinitionEvent&&)` (gcs_ray_event_converter.cc:170-203).
fn convert_actor_task_definition(event: ActorTaskDefinitionEvent) -> TaskEvents {
    use gcs_proto::ray::rpc::TaskInfoEntry;

    let mut task_event = TaskEvents {
        task_id: event.task_id.clone(),
        attempt_number: event.task_attempt,
        job_id: event.job_id.clone(),
        ..Default::default()
    };

    let mut task_info = TaskInfoEntry {
        r#type: 3, // ACTOR_TASK
        name: event.actor_task_name,
        task_id: event.task_id,
        job_id: event.job_id,
        parent_task_id: event.parent_task_id,
        language: event.language,
        required_resources: event.required_resources,
        runtime_env_info: Some(gcs_proto::ray::rpc::RuntimeEnvInfo {
            serialized_runtime_env: event.serialized_runtime_env,
            ..Default::default()
        }),
        ..Default::default()
    };

    if !event.placement_group_id.is_empty() {
        task_info.placement_group_id = Some(event.placement_group_id);
    }
    if !event.actor_id.is_empty() {
        task_info.actor_id = Some(event.actor_id);
    }
    if let Some(cs) = event.call_site {
        if !cs.is_empty() {
            task_info.call_site = Some(cs);
        }
    }

    task_event.task_info = Some(task_info);
    task_event
}

/// Convert `TaskProfileEvents` to `TaskEvents`.
/// Maps C++ `ConvertToTaskEvents(TaskProfileEvents&&)` (gcs_ray_event_converter.cc:209-219).
fn convert_task_profile(event: TaskProfileEvents) -> TaskEvents {
    let mut task_event = TaskEvents {
        task_id: event.task_id,
        attempt_number: event.attempt_number,
        job_id: event.job_id,
        ..Default::default()
    };
    task_event.profile_events = event.profile_events;
    task_event
}

/// Convert a single `RayEvent` to a `TaskEvents`.
/// Returns `None` for unsupported event types.
fn convert_ray_event(event: RayEvent) -> Option<TaskEvents> {
    // Match on which nested event is present (C++ switches on event_type enum).
    if let Some(e) = event.task_definition_event {
        return Some(convert_task_definition(e));
    }
    if let Some(e) = event.task_lifecycle_event {
        return Some(convert_task_lifecycle(e));
    }
    if let Some(e) = event.actor_task_definition_event {
        return Some(convert_actor_task_definition(e));
    }
    if let Some(e) = event.task_profile_events {
        return Some(convert_task_profile(e));
    }
    // Unsupported event type (e.g., driver_job_definition_event).
    debug!(event_type = event.event_type, "Skipping unsupported event type");
    None
}

/// Convert an `AddEventsRequest` to a list of `AddTaskEventDataRequest`,
/// grouped by job_id.
///
/// Maps C++ `ConvertToTaskEventDataRequests` (gcs_ray_event_converter.cc:223-281).
fn convert_to_task_event_data_requests(
    request: AddEventsRequest,
) -> Vec<AddTaskEventDataRequest> {
    let Some(events_data) = request.events_data else {
        return Vec::new();
    };

    let mut requests_per_job: Vec<AddTaskEventDataRequest> = Vec::new();
    let mut job_id_to_index: HashMap<Vec<u8>, usize> = HashMap::new();

    // Convert RayEvents to TaskEvents and group by job_id.
    for event in events_data.events {
        if let Some(task_event) = convert_ray_event(event) {
            let job_id = task_event.job_id.clone();
            if let Some(&idx) = job_id_to_index.get(&job_id) {
                if let Some(ref mut data) = requests_per_job[idx].data {
                    data.events_by_task.push(task_event);
                }
            } else {
                let idx = requests_per_job.len();
                requests_per_job.push(AddTaskEventDataRequest {
                    data: Some(TaskEventData {
                        job_id: job_id.clone(),
                        events_by_task: vec![task_event],
                        ..Default::default()
                    }),
                });
                job_id_to_index.insert(job_id, idx);
            }
        }
    }

    // Add dropped task attempts metadata (grouped by job_id).
    if let Some(metadata) = events_data.task_events_metadata {
        for dropped in metadata.dropped_task_attempts {
            let task_id = &dropped.task_id;
            // Extract job_id from task_id (first 4 bytes are job_id in Ray's ID scheme).
            // If task_id is at least 4 bytes, use the first 4 as job_id.
            let job_id = if task_id.len() >= 4 {
                task_id[..4].to_vec()
            } else {
                task_id.clone()
            };

            if let Some(&idx) = job_id_to_index.get(&job_id) {
                if let Some(ref mut data) = requests_per_job[idx].data {
                    data.dropped_task_attempts.push(dropped);
                }
            } else {
                let idx = requests_per_job.len();
                requests_per_job.push(AddTaskEventDataRequest {
                    data: Some(TaskEventData {
                        job_id: job_id.clone(),
                        dropped_task_attempts: vec![dropped],
                        ..Default::default()
                    }),
                });
                job_id_to_index.insert(job_id, idx);
            }
        }
    }

    requests_per_job
}

#[tonic::async_trait]
impl RayEventExportGcsService for EventExportService {
    /// Receive events, convert to task events, and store in task manager.
    ///
    /// Maps C++ `GcsTaskManager::HandleAddEvents` (gcs_task_manager.cc:668-679):
    /// 1. Convert AddEventsRequest → Vec<AddTaskEventDataRequest>
    /// 2. For each request, call task_manager.add_task_event_data()
    /// 3. Return OK
    async fn add_events(
        &self,
        req: Request<AddEventsRequest>,
    ) -> Result<Response<AddEventsReply>, Status> {
        let inner = req.into_inner();
        let task_event_requests = convert_to_task_event_data_requests(inner);

        for task_req in task_event_requests {
            self.task_manager
                .add_task_event_data(Request::new(task_req))
                .await?;
        }

        Ok(Response::new(AddEventsReply {
            status: Some(AddEventsStatus {
                code: 0,
                message: String::new(),
            }),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_proto::ray::rpc::ray_event_export_gcs_service_server::RayEventExportGcsService;
    use gcs_proto::ray::rpc::task_info_gcs_service_server::TaskInfoGcsService;
    use gcs_proto::ray::rpc::GetTaskEventsRequest;

    fn make_task_manager() -> Arc<GcsTaskManager> {
        Arc::new(GcsTaskManager::new(10000))
    }

    #[tokio::test]
    async fn test_add_events_empty() {
        let svc = EventExportService::new(make_task_manager());
        let reply = svc
            .add_events(Request::new(AddEventsRequest::default()))
            .await;
        assert!(reply.is_ok());
        assert_eq!(reply.unwrap().into_inner().status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_add_events_stores_task_definition() {
        let tm = make_task_manager();
        let svc = EventExportService::new(tm.clone());

        let event = RayEvent {
            event_type: ray_event::EventType::TaskDefinitionEvent as i32,
            task_definition_event: Some(TaskDefinitionEvent {
                task_id: b"task_001".to_vec(),
                task_attempt: 0,
                job_id: b"job1".to_vec(),
                task_name: "my_task".to_string(),
                task_type: 0, // NORMAL_TASK
                ..Default::default()
            }),
            ..Default::default()
        };

        svc.add_events(Request::new(AddEventsRequest {
            events_data: Some(RayEventsData {
                events: vec![event],
                task_events_metadata: None,
            }),
        }))
        .await
        .unwrap();

        // Verify the event was stored in the task manager.
        let reply = tm
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.events_by_task.len(), 1);
        assert_eq!(reply.events_by_task[0].task_id, b"task_001");
        assert_eq!(reply.events_by_task[0].job_id, b"job1");
    }

    #[tokio::test]
    async fn test_add_events_stores_task_lifecycle() {
        let tm = make_task_manager();
        let svc = EventExportService::new(tm.clone());

        // Send a task_definition event first so the merged row has
        // task_info. `GetTaskEvents` correctly drops events without
        // task_info (parity with C++ `gcs_task_manager.cc:491-495`),
        // so a lifecycle-only event for a task that has never been
        // defined is not observable through the RPC.
        let def_event = RayEvent {
            event_type: ray_event::EventType::TaskDefinitionEvent as i32,
            task_definition_event: Some(TaskDefinitionEvent {
                task_id: b"task_002".to_vec(),
                task_attempt: 0,
                job_id: b"job1".to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let lifecycle_event = RayEvent {
            event_type: ray_event::EventType::TaskLifecycleEvent as i32,
            task_lifecycle_event: Some(TaskLifecycleEvent {
                task_id: b"task_002".to_vec(),
                task_attempt: 0,
                job_id: b"job1".to_vec(),
                node_id: b"node1".to_vec(),
                worker_id: b"worker1".to_vec(),
                worker_pid: 12345,
                ..Default::default()
            }),
            ..Default::default()
        };

        svc.add_events(Request::new(AddEventsRequest {
            events_data: Some(RayEventsData {
                events: vec![def_event, lifecycle_event],
                task_events_metadata: None,
            }),
        }))
        .await
        .unwrap();

        let reply = tm
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.events_by_task.len(), 1);
        assert!(reply.events_by_task[0].state_updates.is_some());
        let su = reply.events_by_task[0].state_updates.as_ref().unwrap();
        assert_eq!(su.node_id, Some(b"node1".to_vec()));
        assert_eq!(su.worker_pid, Some(12345));
    }

    #[tokio::test]
    async fn test_add_events_groups_by_job_id() {
        let tm = make_task_manager();
        let svc = EventExportService::new(tm.clone());

        let events = vec![
            RayEvent {
                task_definition_event: Some(TaskDefinitionEvent {
                    task_id: b"t1".to_vec(),
                    job_id: b"job_a".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            RayEvent {
                task_definition_event: Some(TaskDefinitionEvent {
                    task_id: b"t2".to_vec(),
                    job_id: b"job_a".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            RayEvent {
                task_definition_event: Some(TaskDefinitionEvent {
                    task_id: b"t3".to_vec(),
                    job_id: b"job_b".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];

        svc.add_events(Request::new(AddEventsRequest {
            events_data: Some(RayEventsData {
                events,
                task_events_metadata: None,
            }),
        }))
        .await
        .unwrap();

        let reply = tm
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        // 3 task events stored (2 from job_a, 1 from job_b).
        assert_eq!(reply.events_by_task.len(), 3);
    }

    #[tokio::test]
    async fn test_add_events_profile_events() {
        let tm = make_task_manager();
        let svc = EventExportService::new(tm.clone());

        // Send the task definition first so the row carries task_info
        // (C++ `gcs_task_manager.cc:491-495` drops events without it).
        let def_event = RayEvent {
            event_type: ray_event::EventType::TaskDefinitionEvent as i32,
            task_definition_event: Some(TaskDefinitionEvent {
                task_id: b"task_003".to_vec(),
                task_attempt: 1,
                job_id: b"job1".to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let profile_event = RayEvent {
            event_type: ray_event::EventType::TaskProfileEvent as i32,
            task_profile_events: Some(TaskProfileEvents {
                task_id: b"task_003".to_vec(),
                attempt_number: 1,
                job_id: b"job1".to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        };

        svc.add_events(Request::new(AddEventsRequest {
            events_data: Some(RayEventsData {
                events: vec![def_event, profile_event],
                task_events_metadata: None,
            }),
        }))
        .await
        .unwrap();

        let reply = tm
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.events_by_task.len(), 1);
        assert_eq!(reply.events_by_task[0].attempt_number, 1);
    }

    #[tokio::test]
    async fn test_unsupported_event_type_skipped() {
        let tm = make_task_manager();
        let svc = EventExportService::new(tm.clone());

        // Event with no nested event data (unsupported type).
        let event = RayEvent {
            event_type: 999,
            ..Default::default()
        };

        svc.add_events(Request::new(AddEventsRequest {
            events_data: Some(RayEventsData {
                events: vec![event],
                task_events_metadata: None,
            }),
        }))
        .await
        .unwrap();

        let reply = tm
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();
        // Unsupported event type is skipped, no task events stored.
        assert_eq!(reply.events_by_task.len(), 0);
    }
}
