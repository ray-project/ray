//! GCS Job Manager -- manages job lifecycle.
//!
//! Maps C++ `GcsJobManager` from `src/ray/gcs/gcs_job_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::info;

use gcs_proto::ray::rpc::job_info_gcs_service_server::JobInfoGcsService;
use gcs_proto::ray::rpc::*;
use gcs_pubsub::GcsPublisher;
use gcs_table_storage::GcsTableStorage;

fn ok_status() -> GcsStatus {
    GcsStatus {
        code: 0,
        message: String::new(),
    }
}

/// GCS Job Manager. Maps C++ `GcsJobManager`.
pub struct GcsJobManager {
    jobs: DashMap<Vec<u8>, JobTableData>,
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    finished_listeners: parking_lot::Mutex<Vec<Box<dyn Fn(&JobTableData) + Send + Sync>>>,
}

impl GcsJobManager {
    pub fn new(table_storage: Arc<GcsTableStorage>, publisher: Arc<GcsPublisher>) -> Self {
        Self {
            jobs: DashMap::new(),
            table_storage,
            publisher,
            finished_listeners: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub fn add_job_finished_listener(
        &self,
        listener: Box<dyn Fn(&JobTableData) + Send + Sync>,
    ) {
        self.finished_listeners.lock().push(listener);
    }

    pub fn get_job(&self, job_id: &[u8]) -> Option<JobTableData> {
        self.jobs.get(job_id).map(|r| r.value().clone())
    }

    /// Initialize from persisted data.
    pub fn initialize(&self, jobs: &HashMap<String, JobTableData>) {
        for (_key, job) in jobs {
            self.jobs.insert(job.job_id.clone(), job.clone());
        }
        info!(count = self.jobs.len(), "Job manager initialized");
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[tonic::async_trait]
impl JobInfoGcsService for GcsJobManager {
    async fn add_job(
        &self,
        request: Request<AddJobRequest>,
    ) -> Result<Response<AddJobReply>, Status> {
        let req = request.into_inner();
        if let Some(mut data) = req.data {
            let job_id = data.job_id.clone();
            // Set start time to now if not already set.
            if data.start_time == 0 {
                data.start_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
            }
            let key = Self::hex(&job_id);
            self.table_storage.job_table().put(&key, &data).await;
            self.jobs.insert(job_id.clone(), data.clone());
            self.publisher.publish_job(&job_id, data.encode_to_vec());
            info!(job_id = Self::hex(&job_id), "Job added");
        }
        Ok(Response::new(AddJobReply {
            status: Some(ok_status()),
        }))
    }

    async fn mark_job_finished(
        &self,
        request: Request<MarkJobFinishedRequest>,
    ) -> Result<Response<MarkJobFinishedReply>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        if let Some(mut entry) = self.jobs.get_mut(&job_id) {
            entry.is_dead = true;
            entry.end_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let data = entry.clone();
            drop(entry);

            let key = Self::hex(&job_id);
            self.table_storage.job_table().put(&key, &data).await;
            self.publisher.publish_job(&job_id, data.encode_to_vec());

            // Notify listeners.
            let listeners = self.finished_listeners.lock();
            for listener in listeners.iter() {
                listener(&data);
            }
            info!(job_id = Self::hex(&job_id), "Job marked finished");
        }
        Ok(Response::new(MarkJobFinishedReply {
            status: Some(ok_status()),
        }))
    }

    async fn get_all_job_info(
        &self,
        request: Request<GetAllJobInfoRequest>,
    ) -> Result<Response<GetAllJobInfoReply>, Status> {
        let req = request.into_inner();
        let limit = req.limit.unwrap_or(0) as usize;

        let mut job_list: Vec<JobTableData> = self
            .jobs
            .iter()
            .map(|e| e.value().clone())
            .collect();

        if limit > 0 && job_list.len() > limit {
            job_list.truncate(limit);
        }

        Ok(Response::new(GetAllJobInfoReply {
            status: Some(ok_status()),
            job_info_list: job_list,
        }))
    }

    async fn report_job_error(
        &self,
        _request: Request<ReportJobErrorRequest>,
    ) -> Result<Response<ReportJobErrorReply>, Status> {
        Ok(Response::new(ReportJobErrorReply {
            status: Some(ok_status()),
        }))
    }

    async fn get_next_job_id(
        &self,
        _request: Request<GetNextJobIdRequest>,
    ) -> Result<Response<GetNextJobIdReply>, Status> {
        let id = self.table_storage.get_next_job_id().await;
        Ok(Response::new(GetNextJobIdReply {
            status: Some(ok_status()),
            job_id: id,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;

    fn make_manager() -> GcsJobManager {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(GcsPublisher::new(64));
        GcsJobManager::new(table_storage, publisher)
    }

    #[tokio::test]
    async fn test_add_and_get_job() {
        let mgr = make_manager();
        let req = Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"job1".to_vec(),
                is_dead: false,
                ..Default::default()
            }),
        });
        mgr.add_job(req).await.unwrap();

        let job = mgr.get_job(b"job1").unwrap();
        assert!(!job.is_dead);
        assert!(job.start_time > 0);
    }

    #[tokio::test]
    async fn test_mark_job_finished() {
        let mgr = make_manager();
        mgr.add_job(Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"job1".to_vec(),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        mgr.mark_job_finished(Request::new(MarkJobFinishedRequest {
            job_id: b"job1".to_vec(),
        }))
        .await
        .unwrap();

        let job = mgr.get_job(b"job1").unwrap();
        assert!(job.is_dead);
        assert!(job.end_time > 0);
    }

    #[tokio::test]
    async fn test_get_all_job_info() {
        let mgr = make_manager();
        for i in 0..3 {
            mgr.add_job(Request::new(AddJobRequest {
                data: Some(JobTableData {
                    job_id: format!("job{i}").into_bytes(),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        }
        let reply = mgr
            .get_all_job_info(Request::new(GetAllJobInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.job_info_list.len(), 3);
    }

    #[tokio::test]
    async fn test_get_next_job_id() {
        let mgr = make_manager();
        let r1 = mgr
            .get_next_job_id(Request::new(GetNextJobIdRequest {}))
            .await
            .unwrap()
            .into_inner();
        let r2 = mgr
            .get_next_job_id(Request::new(GetNextJobIdRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r1.job_id, 1);
        assert_eq!(r2.job_id, 2);
    }

    #[tokio::test]
    async fn test_job_finished_listener() {
        let mgr = make_manager();
        let finished = Arc::new(parking_lot::Mutex::new(false));
        let finished_clone = finished.clone();
        mgr.add_job_finished_listener(Box::new(move |_job| {
            *finished_clone.lock() = true;
        }));

        mgr.add_job(Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"job1".to_vec(),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        mgr.mark_job_finished(Request::new(MarkJobFinishedRequest {
            job_id: b"job1".to_vec(),
        }))
        .await
        .unwrap();

        assert!(*finished.lock());
    }

    #[tokio::test]
    async fn test_initialize() {
        let mgr = make_manager();

        let mut jobs = HashMap::new();
        let job1 = JobTableData {
            job_id: b"j1".to_vec(),
            is_dead: false,
            ..Default::default()
        };
        let job2 = JobTableData {
            job_id: b"j2".to_vec(),
            is_dead: true,
            ..Default::default()
        };
        jobs.insert("j1".to_string(), job1);
        jobs.insert("j2".to_string(), job2);

        mgr.initialize(&jobs);

        let got1 = mgr.get_job(b"j1");
        assert!(got1.is_some());
        assert!(!got1.unwrap().is_dead);

        let got2 = mgr.get_job(b"j2");
        assert!(got2.is_some());
        assert!(got2.unwrap().is_dead);
    }

    #[tokio::test]
    async fn test_report_job_error() {
        let mgr = make_manager();
        let reply = mgr
            .report_job_error(Request::new(ReportJobErrorRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }
}
