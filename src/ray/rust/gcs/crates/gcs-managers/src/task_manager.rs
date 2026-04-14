//! GCS Task Manager -- manages task event storage and retrieval.
//!
//! Maps C++ `GcsTaskManager` from `src/ray/gcs/gcs_task_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tonic::{Request, Response, Status};
use tracing::debug;

use gcs_proto::ray::rpc::task_info_gcs_service_server::TaskInfoGcsService;
use gcs_proto::ray::rpc::*;

fn ok_status() -> GcsStatus {
    GcsStatus { code: 0, message: String::new() }
}

/// GCS Task Manager. Maps C++ `GcsTaskManager`.
///
/// Stores task events in memory with configurable size limits.
pub struct GcsTaskManager {
    // task_attempt key -> TaskEvents
    task_events: RwLock<HashMap<String, TaskEvents>>,
    max_num_task_events: usize,
}

impl GcsTaskManager {
    pub fn new(max_num_task_events: usize) -> Self {
        Self {
            task_events: RwLock::new(HashMap::new()),
            max_num_task_events,
        }
    }

    fn task_attempt_key(task_id: &[u8], attempt_number: i32) -> String {
        format!("{}_{}", hex::encode(task_id), attempt_number)
    }
}

mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[tonic::async_trait]
impl TaskInfoGcsService for GcsTaskManager {
    async fn add_task_event_data(
        &self,
        request: Request<AddTaskEventDataRequest>,
    ) -> Result<Response<AddTaskEventDataReply>, Status> {
        let req = request.into_inner();
        if let Some(data) = req.data {
            let mut storage = self.task_events.write();
            for events in data.events_by_task {
                let key = Self::task_attempt_key(
                    &events.task_id,
                    events.attempt_number,
                );
                // Evict if over limit.
                if storage.len() >= self.max_num_task_events && !storage.contains_key(&key) {
                    // Simple eviction: remove an arbitrary entry.
                    if let Some(first_key) = storage.keys().next().cloned() {
                        storage.remove(&first_key);
                    }
                }
                storage.insert(key, events);
            }
        }
        Ok(Response::new(AddTaskEventDataReply {
            status: Some(ok_status()),
        }))
    }

    async fn get_task_events(
        &self,
        request: Request<GetTaskEventsRequest>,
    ) -> Result<Response<GetTaskEventsReply>, Status> {
        let req = request.into_inner();
        let storage = self.task_events.read();

        let mut events_by_task: Vec<TaskEvents> = Vec::new();
        let limit = req.limit.filter(|&l| l > 0).map(|l| l as usize).unwrap_or(usize::MAX);

        // Extract job_id filter if present.
        let job_id_filter: Option<&[u8]> = req.filters.as_ref().and_then(|f| {
            f.job_filters.first().map(|jf| jf.job_id.as_slice())
        });

        for (_key, event) in storage.iter() {
            if events_by_task.len() >= limit {
                break;
            }
            if let Some(jid) = job_id_filter {
                if !jid.is_empty() && event.job_id != jid {
                    continue;
                }
            }
            events_by_task.push(event.clone());
        }

        Ok(Response::new(GetTaskEventsReply {
            status: Some(ok_status()),
            events_by_task,
            num_profile_task_events_dropped: 0,
            num_status_task_events_dropped: 0,
            num_total_stored: storage.len() as i64,
            num_filtered_on_gcs: 0,
            num_truncated: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_get_task_events() {
        let mgr = GcsTaskManager::new(1000);

        let events = TaskEvents {
            task_id: b"task1".to_vec(),
            attempt_number: 0,
            job_id: b"job1".to_vec(),
            ..Default::default()
        };

        mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
            data: Some(TaskEventData {
                events_by_task: vec![events],
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.events_by_task.len(), 1);
        assert_eq!(reply.events_by_task[0].task_id, b"task1");
    }

    #[tokio::test]
    async fn test_eviction() {
        let mgr = GcsTaskManager::new(2);

        for i in 0..5 {
            let events = TaskEvents {
                task_id: format!("task{i}").into_bytes(),
                attempt_number: 0,
                ..Default::default()
            };
            mgr.add_task_event_data(Request::new(AddTaskEventDataRequest {
                data: Some(TaskEventData {
                    events_by_task: vec![events],
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        }

        let reply = mgr
            .get_task_events(Request::new(GetTaskEventsRequest::default()))
            .await
            .unwrap()
            .into_inner();

        // Should have at most max_num_task_events entries.
        assert!(reply.events_by_task.len() <= 2);
    }
}
