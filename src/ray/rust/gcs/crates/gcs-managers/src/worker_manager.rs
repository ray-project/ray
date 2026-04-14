//! GCS Worker Manager -- manages worker registration and failure reporting.
//!
//! Maps C++ `GcsWorkerManager` from `src/ray/gcs/gcs_worker_manager.h/cc`.

use std::sync::Arc;

use dashmap::DashMap;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use gcs_proto::ray::rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
use gcs_proto::ray::rpc::*;
use gcs_pubsub::GcsPublisher;
use gcs_table_storage::GcsTableStorage;

fn ok_status() -> GcsStatus {
    GcsStatus { code: 0, message: String::new() }
}

/// GCS Worker Manager. Maps C++ `GcsWorkerManager`.
pub struct GcsWorkerManager {
    workers: DashMap<Vec<u8>, WorkerTableData>,
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    dead_listeners: parking_lot::Mutex<Vec<Box<dyn Fn(&WorkerTableData) + Send + Sync>>>,
}

impl GcsWorkerManager {
    pub fn new(table_storage: Arc<GcsTableStorage>, publisher: Arc<GcsPublisher>) -> Self {
        Self {
            workers: DashMap::new(),
            table_storage,
            publisher,
            dead_listeners: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub fn add_worker_dead_listener(
        &self,
        listener: Box<dyn Fn(&WorkerTableData) + Send + Sync>,
    ) {
        self.dead_listeners.lock().push(listener);
    }

    fn worker_id(data: &WorkerTableData) -> Vec<u8> {
        data.worker_address
            .as_ref()
            .map(|a| a.worker_id.clone())
            .unwrap_or_default()
    }
}

#[tonic::async_trait]
impl WorkerInfoGcsService for GcsWorkerManager {
    async fn add_worker_info(
        &self,
        request: Request<AddWorkerInfoRequest>,
    ) -> Result<Response<AddWorkerInfoReply>, Status> {
        let req = request.into_inner();
        if let Some(data) = req.worker_data {
            let wid = Self::worker_id(&data);
            self.workers.insert(wid, data);
        }
        Ok(Response::new(AddWorkerInfoReply { status: Some(ok_status()) }))
    }

    async fn get_worker_info(
        &self,
        request: Request<GetWorkerInfoRequest>,
    ) -> Result<Response<GetWorkerInfoReply>, Status> {
        let req = request.into_inner();
        let mut reply = GetWorkerInfoReply { status: Some(ok_status()), ..Default::default() };
        if let Some(entry) = self.workers.get(&req.worker_id) {
            reply.worker_table_data = Some(entry.value().clone());
        }
        Ok(Response::new(reply))
    }

    async fn get_all_worker_info(
        &self,
        request: Request<GetAllWorkerInfoRequest>,
    ) -> Result<Response<GetAllWorkerInfoReply>, Status> {
        let req = request.into_inner();
        let limit = req.limit.filter(|&l| l > 0).map(|l| l as usize).unwrap_or(usize::MAX);
        let total = self.workers.len() as i64;

        let mut workers: Vec<WorkerTableData> = Vec::new();
        let mut num_filtered = 0i64;

        for entry in self.workers.iter() {
            let w = entry.value();
            // Apply filters if present.
            if let Some(ref filters) = req.filters {
                if filters.is_alive == Some(true) && !w.is_alive {
                    num_filtered += 1;
                    continue;
                }
                if filters.exist_paused_threads == Some(true) && w.num_paused_threads.unwrap_or(0) == 0 {
                    num_filtered += 1;
                    continue;
                }
            }
            if workers.len() < limit {
                workers.push(w.clone());
            }
        }

        Ok(Response::new(GetAllWorkerInfoReply {
            status: Some(ok_status()),
            worker_table_data: workers,
            total,
            num_filtered,
        }))
    }

    async fn report_worker_failure(
        &self,
        request: Request<ReportWorkerFailureRequest>,
    ) -> Result<Response<ReportWorkerFailureReply>, Status> {
        let req = request.into_inner();
        if let Some(failure) = req.worker_failure {
            let wid = failure
                .worker_address
                .as_ref()
                .map(|a| a.worker_id.clone())
                .unwrap_or_default();

            // Merge failure info into existing worker data or create new.
            let mut worker_data = self
                .workers
                .get(&wid)
                .map(|r| r.value().clone())
                .unwrap_or_default();

            // Copy failure fields.
            worker_data.is_alive = false;
            worker_data.exit_type = failure.exit_type;
            worker_data.exit_detail.clone_from(&failure.exit_detail);
            if worker_data.worker_address.is_none() {
                worker_data.worker_address = failure.worker_address.clone();
            }

            // Notify dead listeners.
            {
                let listeners = self.dead_listeners.lock();
                for listener in listeners.iter() {
                    listener(&worker_data);
                }
            }

            self.workers.insert(wid.clone(), worker_data.clone());
            self.publisher.publish_worker_failure(&wid, vec![]);
        }
        Ok(Response::new(ReportWorkerFailureReply { status: Some(ok_status()) }))
    }

    async fn update_worker_debugger_port(
        &self,
        request: Request<UpdateWorkerDebuggerPortRequest>,
    ) -> Result<Response<UpdateWorkerDebuggerPortReply>, Status> {
        let req = request.into_inner();
        if let Some(mut entry) = self.workers.get_mut(&req.worker_id) {
            entry.debugger_port = Some(req.debugger_port);
        }
        Ok(Response::new(UpdateWorkerDebuggerPortReply { status: Some(ok_status()) }))
    }

    async fn update_worker_num_paused_threads(
        &self,
        request: Request<UpdateWorkerNumPausedThreadsRequest>,
    ) -> Result<Response<UpdateWorkerNumPausedThreadsReply>, Status> {
        let req = request.into_inner();
        if let Some(mut entry) = self.workers.get_mut(&req.worker_id) {
            let current = entry.num_paused_threads.unwrap_or(0) as i32;
            entry.num_paused_threads = Some((current + req.num_paused_threads_delta) as u32);
        }
        Ok(Response::new(UpdateWorkerNumPausedThreadsReply { status: Some(ok_status()) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;

    fn make_manager() -> GcsWorkerManager {
        let store = Arc::new(InMemoryStoreClient::new());
        let ts = Arc::new(GcsTableStorage::new(store));
        let pub_ = Arc::new(GcsPublisher::new(64));
        GcsWorkerManager::new(ts, pub_)
    }

    fn make_worker(id: &[u8], pid: u32) -> WorkerTableData {
        WorkerTableData {
            worker_address: Some(Address {
                worker_id: id.to_vec(),
                ..Default::default()
            }),
            is_alive: true,
            pid,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_add_and_get_worker() {
        let mgr = make_manager();
        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(make_worker(b"w1", 100)),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_worker_info(Request::new(GetWorkerInfoRequest {
                worker_id: b"w1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.worker_table_data.is_some());
        assert_eq!(reply.worker_table_data.unwrap().pid, 100);
    }

    #[tokio::test]
    async fn test_report_worker_failure() {
        let mgr = make_manager();
        let dead_count = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let dc = dead_count.clone();
        mgr.add_worker_dead_listener(Box::new(move |_| {
            dc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }));

        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(make_worker(b"w1", 100)),
        }))
        .await
        .unwrap();

        mgr.report_worker_failure(Request::new(ReportWorkerFailureRequest {
            worker_failure: Some(WorkerTableData {
                worker_address: Some(Address {
                    worker_id: b"w1".to_vec(),
                    ..Default::default()
                }),
                exit_type: Some(1),
                exit_detail: Some("test".into()),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        assert_eq!(dead_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_get_all_worker_info() {
        let mgr = make_manager();

        // Add 3 workers: w1 alive, w2 dead (not alive), w3 alive with paused threads.
        let mut w1 = make_worker(b"w1", 100);
        w1.is_alive = true;
        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(w1),
        }))
        .await
        .unwrap();

        let mut w2 = make_worker(b"w2", 200);
        w2.is_alive = false;
        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(w2),
        }))
        .await
        .unwrap();

        let mut w3 = make_worker(b"w3", 300);
        w3.is_alive = true;
        w3.num_paused_threads = Some(2);
        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(w3),
        }))
        .await
        .unwrap();

        // No filters, no limit -- should return all 3.
        let reply = mgr
            .get_all_worker_info(Request::new(GetAllWorkerInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.len(), 3);
        assert_eq!(reply.total, 3);

        // Filter: is_alive=true -- should return 2 (w1, w3), filter 1 (w2).
        let reply = mgr
            .get_all_worker_info(Request::new(GetAllWorkerInfoRequest {
                filters: Some(get_all_worker_info_request::Filters {
                    is_alive: Some(true),
                    exist_paused_threads: None,
                }),
                limit: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.len(), 2);
        assert_eq!(reply.num_filtered, 1);

        // Filter: exist_paused_threads=true -- should return 1 (w3), filter 2.
        let reply = mgr
            .get_all_worker_info(Request::new(GetAllWorkerInfoRequest {
                filters: Some(get_all_worker_info_request::Filters {
                    is_alive: None,
                    exist_paused_threads: Some(true),
                }),
                limit: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.len(), 1);
        assert_eq!(reply.num_filtered, 2);

        // Limit=1, no filter -- should return at most 1.
        let reply = mgr
            .get_all_worker_info(Request::new(GetAllWorkerInfoRequest {
                filters: None,
                limit: Some(1),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.len(), 1);
    }

    #[tokio::test]
    async fn test_update_debugger_port() {
        let mgr = make_manager();
        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(make_worker(b"w1", 100)),
        }))
        .await
        .unwrap();

        mgr.update_worker_debugger_port(Request::new(
            UpdateWorkerDebuggerPortRequest {
                worker_id: b"w1".to_vec(),
                debugger_port: 5678,
            },
        ))
        .await
        .unwrap();

        let reply = mgr
            .get_worker_info(Request::new(GetWorkerInfoRequest {
                worker_id: b"w1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.unwrap().debugger_port, Some(5678));
    }

    #[tokio::test]
    async fn test_update_num_paused_threads() {
        let mgr = make_manager();

        let mut w = make_worker(b"w1", 100);
        w.num_paused_threads = Some(0);
        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(w),
        }))
        .await
        .unwrap();

        // Add 3 paused threads.
        mgr.update_worker_num_paused_threads(Request::new(
            UpdateWorkerNumPausedThreadsRequest {
                worker_id: b"w1".to_vec(),
                num_paused_threads_delta: 3,
            },
        ))
        .await
        .unwrap();

        let reply = mgr
            .get_worker_info(Request::new(GetWorkerInfoRequest {
                worker_id: b"w1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.unwrap().num_paused_threads, Some(3));
    }
}
