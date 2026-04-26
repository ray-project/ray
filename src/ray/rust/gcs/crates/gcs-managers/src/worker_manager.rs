//! GCS Worker Manager -- manages worker registration and failure reporting.
//!
//! Maps C++ `GcsWorkerManager` from `src/ray/gcs/gcs_worker_manager.h/cc`.
//!
//! All worker state is persisted to `WorkerTable` storage, matching C++
//! which calls `WorkerTable().Put()` on every state change and
//! `WorkerTable().GetAll()` for queries.

use std::sync::Arc;

use dashmap::DashMap;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use gcs_proto::ray::rpc::worker_info_gcs_service_server::WorkerInfoGcsService;
use gcs_proto::ray::rpc::*;
use gcs_pubsub::GcsPublisher;
use gcs_table_storage::GcsTableStorage;

use crate::usage_stats::{TagKey, UsageStatsClient};

fn ok_status() -> GcsStatus {
    GcsStatus { code: 0, message: String::new() }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// GCS Worker Manager. Maps C++ `GcsWorkerManager`.
pub struct GcsWorkerManager {
    /// In-memory cache for fast lookups. Kept in sync with storage.
    workers: DashMap<Vec<u8>, WorkerTableData>,
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    dead_listeners: parking_lot::Mutex<Vec<Box<dyn Fn(&WorkerTableData) + Send + Sync>>>,
    /// Optional usage-stats client. Set by `set_usage_stats_client`
    /// after the server builds the KV-backed recorder. `None` means
    /// usage recording is off; every call site guards on this just
    /// like C++ (`if (usage_stats_client_)` at
    /// `gcs_worker_manager.cc:126`).
    usage_stats_client: parking_lot::Mutex<Option<Arc<UsageStatsClient>>>,
    /// Running counters per exit-type. Match C++
    /// `gcs_worker_manager.cc:104-128` which increments a field
    /// *before* passing the counter value to the usage-stats client.
    worker_crash_system_error_count: parking_lot::Mutex<i64>,
    worker_crash_oom_count: parking_lot::Mutex<i64>,
}

impl GcsWorkerManager {
    pub fn new(table_storage: Arc<GcsTableStorage>, publisher: Arc<GcsPublisher>) -> Self {
        Self {
            workers: DashMap::new(),
            table_storage,
            publisher,
            dead_listeners: parking_lot::Mutex::new(Vec::new()),
            usage_stats_client: parking_lot::Mutex::new(None),
            worker_crash_system_error_count: parking_lot::Mutex::new(0),
            worker_crash_oom_count: parking_lot::Mutex::new(0),
        }
    }

    /// Install the usage-stats recorder. Mirrors C++
    /// `GcsWorkerManager::SetUsageStatsClient`
    /// (`gcs_worker_manager.h:67`), invoked from
    /// `GcsServer::InitUsageStatsClient` (`gcs_server.cc:633`) after
    /// the KV manager is live.
    pub fn set_usage_stats_client(&self, client: Arc<UsageStatsClient>) {
        *self.usage_stats_client.lock() = Some(client);
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
    /// Register a worker — persist to storage.
    /// Maps C++ `HandleAddWorkerInfo` (gcs_worker_manager.cc:201-220).
    async fn add_worker_info(
        &self,
        request: Request<AddWorkerInfoRequest>,
    ) -> Result<Response<AddWorkerInfoReply>, Status> {
        let req = request.into_inner();
        if let Some(data) = req.worker_data {
            let wid = Self::worker_id(&data);
            // Persist to storage (matching C++ WorkerTable().Put).
            self.table_storage
                .worker_table()
                .put(&hex(&wid), &data)
                .await;
            self.workers.insert(wid, data);
        }
        Ok(Response::new(AddWorkerInfoReply { status: Some(ok_status()) }))
    }

    /// Get a single worker's info.
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

    /// Get all workers — reads from storage for complete data.
    /// Maps C++ `HandleGetAllWorkerInfo` (gcs_worker_manager.cc:152-199)
    /// which calls `WorkerTable().GetAll()`.
    async fn get_all_worker_info(
        &self,
        request: Request<GetAllWorkerInfoRequest>,
    ) -> Result<Response<GetAllWorkerInfoReply>, Status> {
        let req = request.into_inner();
        let limit = req.limit.filter(|&l| l > 0).map(|l| l as usize).unwrap_or(usize::MAX);

        // Read from storage (matching C++ WorkerTable().GetAll()).
        let all_stored = self.table_storage.worker_table().get_all().await;
        let total = all_stored.len() as i64;

        let mut workers: Vec<WorkerTableData> = Vec::new();
        let mut num_filtered = 0i64;

        for (_key, w) in &all_stored {
            // Apply filters if present.
            if let Some(ref filters) = req.filters {
                if filters.is_alive == Some(true) && !w.is_alive {
                    num_filtered += 1;
                    continue;
                }
                if filters.exist_paused_threads == Some(true)
                    && w.num_paused_threads.unwrap_or(0) == 0
                {
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

    /// Report a worker failure — persist to storage and notify listeners.
    /// Maps C++ `HandleReportWorkerFailure` (gcs_worker_manager.cc:32-132).
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
            // Matches C++ which fetches via GetWorkerInfo then merges.
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

            // Usage-stats recording. Parity with C++
            // `gcs_worker_manager.cc:104-128`: bump the per-type
            // counter *first*, then forward that new value as the
            // monotonic counter to the usage-stats client. Only the
            // two crash cases (SYSTEM_ERROR, NODE_OUT_OF_MEMORY) are
            // tracked; intentional exits and INTENDED_* are ignored.
            let usage_client = self.usage_stats_client.lock().clone();
            if let Some(client) = usage_client {
                let exit_type = failure.exit_type.unwrap_or(-1);
                let (tag, new_count) =
                    if exit_type == WorkerExitType::SystemError as i32 {
                        let mut n = self.worker_crash_system_error_count.lock();
                        *n += 1;
                        (Some(TagKey::WorkerCrashSystemError), *n)
                    } else if exit_type == WorkerExitType::NodeOutOfMemory as i32 {
                        let mut n = self.worker_crash_oom_count.lock();
                        *n += 1;
                        (Some(TagKey::WorkerCrashOom), *n)
                    } else {
                        (None, 0)
                    };
                if let Some(tag) = tag {
                    // Fire-and-forget: C++'s KV callback runs on
                    // io_context so the handler doesn't block on the
                    // write; we mirror that by spawning.
                    client.record_extra_usage_counter_spawn(tag, new_count);
                }
            }

            // Notify dead listeners.
            {
                let listeners = self.dead_listeners.lock();
                for listener in listeners.iter() {
                    listener(&worker_data);
                }
            }

            // Persist to storage (matching C++ WorkerTable().Put at line 106-107).
            self.table_storage
                .worker_table()
                .put(&hex(&wid), &worker_data)
                .await;
            self.workers.insert(wid.clone(), worker_data);

            // Publish worker failure notification.
            let delta = WorkerDeltaData {
                worker_id: wid.clone(),
                node_id: failure
                    .worker_address
                    .map(|a| a.node_id)
                    .unwrap_or_default(),
            };
            self.publisher
                .publish_worker_failure(&wid, delta.encode_to_vec());
        }
        Ok(Response::new(ReportWorkerFailureReply { status: Some(ok_status()) }))
    }

    /// Update worker debugger port — persist to storage.
    /// Maps C++ `HandleUpdateWorkerDebuggerPort` (gcs_worker_manager.cc:222-262).
    async fn update_worker_debugger_port(
        &self,
        request: Request<UpdateWorkerDebuggerPortRequest>,
    ) -> Result<Response<UpdateWorkerDebuggerPortReply>, Status> {
        let req = request.into_inner();
        if let Some(mut entry) = self.workers.get_mut(&req.worker_id) {
            entry.debugger_port = Some(req.debugger_port);
            let data = entry.clone();
            drop(entry);
            // Persist to storage (matching C++ WorkerTable().Put at line 253-254).
            self.table_storage
                .worker_table()
                .put(&hex(&req.worker_id), &data)
                .await;
        }
        Ok(Response::new(UpdateWorkerDebuggerPortReply { status: Some(ok_status()) }))
    }

    /// Update worker paused threads count — persist to storage.
    /// Maps C++ `HandleUpdateWorkerNumPausedThreads` (gcs_worker_manager.cc:264-310).
    async fn update_worker_num_paused_threads(
        &self,
        request: Request<UpdateWorkerNumPausedThreadsRequest>,
    ) -> Result<Response<UpdateWorkerNumPausedThreadsReply>, Status> {
        let req = request.into_inner();
        if let Some(mut entry) = self.workers.get_mut(&req.worker_id) {
            let current = entry.num_paused_threads.unwrap_or(0) as i32;
            entry.num_paused_threads = Some((current + req.num_paused_threads_delta) as u32);
            let data = entry.clone();
            drop(entry);
            // Persist to storage (matching C++ WorkerTable().Put at line 305-306).
            self.table_storage
                .worker_table()
                .put(&hex(&req.worker_id), &data)
                .await;
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
    async fn test_add_persists_to_storage() {
        let mgr = make_manager();
        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(make_worker(b"w1", 100)),
        }))
        .await
        .unwrap();

        // Verify storage has the worker.
        let stored = mgr.table_storage.worker_table().get(&hex(b"w1")).await;
        assert!(stored.is_some());
        assert_eq!(stored.unwrap().pid, 100);
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

        // Failure should be persisted to storage.
        let stored = mgr.table_storage.worker_table().get(&hex(b"w1")).await;
        assert!(stored.is_some());
        assert!(!stored.unwrap().is_alive);
    }

    #[tokio::test]
    async fn test_get_all_worker_info_reads_from_storage() {
        let mgr = make_manager();

        // Add workers.
        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(make_worker(b"w1", 100)),
        }))
        .await
        .unwrap();
        mgr.add_worker_info(Request::new(AddWorkerInfoRequest {
            worker_data: Some(make_worker(b"w2", 200)),
        }))
        .await
        .unwrap();

        // get_all should read from storage.
        let reply = mgr
            .get_all_worker_info(Request::new(GetAllWorkerInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.worker_table_data.len(), 2);
        assert_eq!(reply.total, 2);
    }

    #[tokio::test]
    async fn test_get_all_worker_info_filters() {
        let mgr = make_manager();

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

        // Filter: is_alive=true — should return 2.
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

        // Filter: exist_paused_threads=true — should return 1.
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

        // Debugger port update should be persisted.
        let stored = mgr.table_storage.worker_table().get(&hex(b"w1")).await;
        assert_eq!(stored.unwrap().debugger_port, Some(5678));
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

        // Paused threads update should be persisted.
        let stored = mgr.table_storage.worker_table().get(&hex(b"w1")).await;
        assert_eq!(stored.unwrap().num_paused_threads, Some(3));
    }
}
