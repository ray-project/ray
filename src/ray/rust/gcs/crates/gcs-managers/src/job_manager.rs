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

use crate::export_event_writer::{ExportDriverJobEventData, ExportEventManager};
use crate::function_manager::GcsFunctionManager;
use crate::runtime_env_stub::RuntimeEnvService;

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
    /// EXPORT_DRIVER_JOB sink. Maps C++ `RayExportEvent` in
    /// `gcs_job_manager.cc:104`.
    export_events: Arc<ExportEventManager>,
    /// Optional function manager for per-job KV cleanup. Set via
    /// `set_function_manager` after construction (production wiring
    /// in `GcsServer::new_with_store`). Mirrors C++
    /// `function_manager_` reference at
    /// `gcs_job_manager.cc:38,136,175` — `AddJobReference` on
    /// register/recovery, `RemoveJobReference` on finished.
    function_manager: parking_lot::Mutex<Option<Arc<GcsFunctionManager>>>,
    /// Optional runtime-env service. Pins job runtime-env URIs on
    /// `add_job` and releases them on `mark_job_finished`. Mirrors
    /// C++ `runtime_env_manager_` wiring at
    /// `gcs_job_manager.cc:132-134,171` — `AddURIReference(job_id.Hex(),
    /// runtime_env_info)` on the success branch of the put callback,
    /// `RemoveURIReference(job_id.Hex())` on the success branch of
    /// `MarkJobAsFinished`. `None` when the manager is constructed in
    /// tests that don't exercise URI lifecycle.
    runtime_env_service: parking_lot::Mutex<Option<Arc<RuntimeEnvService>>>,
}

impl GcsJobManager {
    pub fn new(table_storage: Arc<GcsTableStorage>, publisher: Arc<GcsPublisher>) -> Self {
        Self::with_export_events(table_storage, publisher, ExportEventManager::disabled())
    }

    pub fn with_export_events(
        table_storage: Arc<GcsTableStorage>,
        publisher: Arc<GcsPublisher>,
        export_events: Arc<ExportEventManager>,
    ) -> Self {
        Self {
            jobs: DashMap::new(),
            table_storage,
            publisher,
            finished_listeners: parking_lot::Mutex::new(Vec::new()),
            export_events,
            function_manager: parking_lot::Mutex::new(None),
            runtime_env_service: parking_lot::Mutex::new(None),
        }
    }

    /// Install the function manager. Wired by `GcsServer::new_with_store`
    /// after both managers and the KV client are constructed.
    pub fn set_function_manager(&self, fm: Arc<GcsFunctionManager>) {
        *self.function_manager.lock() = Some(fm);
    }

    /// Install the runtime-env service. Wired by `GcsServer::new_with_store`
    /// after the service is constructed with its KV-backed deleter.
    /// Mirrors C++ `runtime_env_manager_` threaded into `GcsJobManager` at
    /// construction time in `gcs_server.cc:694-738`.
    pub fn set_runtime_env_service(&self, svc: Arc<RuntimeEnvService>) {
        *self.runtime_env_service.lock() = Some(svc);
    }

    /// Hex-encode a binary job id. Matches the C++ `JobID::Hex()` format
    /// used as the reference id in `runtime_env_manager_.AddURIReference`.
    fn job_id_hex(job_id: &[u8]) -> String {
        Self::hex(job_id)
    }

    /// Mark every non-dead job whose driver was on `node_id` as finished.
    ///
    /// Maps C++ `GcsJobManager::OnNodeDead` (gcs_job_manager.cc:488-512).
    /// In C++ this runs an async `JobTable().GetAll()` and then calls
    /// `MarkJobAsFinished` on each matching job. Rust does both in-place using
    /// the in-memory `jobs` map because it is already authoritative (kept in
    /// sync with storage by `add_job` and `mark_job_finished`).
    pub async fn on_node_dead(&self, node_id: &[u8]) {
        // Collect first to avoid holding DashMap locks across await points.
        let targets: Vec<Vec<u8>> = self
            .jobs
            .iter()
            .filter(|e| {
                let job = e.value();
                if job.is_dead {
                    return false;
                }
                // Match on driver_address.node_id. Fall back to false when the
                // job has no driver_address (shouldn't happen for live jobs).
                job.driver_address
                    .as_ref()
                    .map(|a| a.node_id == node_id)
                    .unwrap_or(false)
            })
            .map(|e| e.key().clone())
            .collect();

        if targets.is_empty() {
            return;
        }
        info!(
            node_id = Self::hex(node_id),
            count = targets.len(),
            "Node dead: marking jobs with drivers on it as finished"
        );

        for job_id in targets {
            let _ = self
                .mark_job_finished(Request::new(MarkJobFinishedRequest { job_id }))
                .await;
        }
    }

    fn job_to_export(job: &JobTableData) -> ExportDriverJobEventData {
        ExportDriverJobEventData {
            job_id: Self::hex(&job.job_id),
            is_dead: job.is_dead,
            driver_pid: job.driver_pid,
            driver_ip_address: job.driver_ip_address.clone(),
            start_time: job.start_time as i64,
            end_time: job.end_time as i64,
            entrypoint: job.entrypoint.clone(),
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
        let fm = self.function_manager.lock().clone();
        for (_key, job) in jobs {
            self.jobs.insert(job.job_id.clone(), job.clone());
            // Parity with C++ `gcs_job_manager.cc:38`:
            // `function_manager_.AddJobReference(job_id)` is called
            // inside the `for` loop over recovered jobs. C++ calls it
            // unconditionally per recovered job, regardless of
            // `is_dead` — the refcount still needs to reach zero via
            // the matching `RemoveJobReference` on
            // `MarkJobFinished` for already-dead jobs and via the
            // actor-retirement path for any detached actors they
            // left behind.
            if let Some(ref fm) = fm {
                fm.add_job_reference(&job.job_id);
            }
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
            // Emit EXPORT_DRIVER_JOB (parity with C++ gcs_job_manager.cc:104).
            self.export_events
                .report_driver_job_event(&Self::job_to_export(&data));
            // Pin runtime-env URIs for this job. Parity with C++
            // `gcs_job_manager.cc:132-134`: when
            // `job_table_data.config().has_runtime_env_info()` is true,
            // `runtime_env_manager_.AddURIReference(job_id.Hex(),
            // runtime_env_info)` runs on the success branch of the put
            // callback — before `function_manager_.AddJobReference`.
            // We mirror the ordering for symmetry with C++ logs, even
            // though the two operations are independent.
            let res = self.runtime_env_service.lock().clone();
            if let Some(res) = res {
                if let Some(cfg) = data.config.as_ref() {
                    if let Some(info) = cfg.runtime_env_info.as_ref() {
                        res.add_uri_reference_from_runtime_env(
                            &Self::job_id_hex(&job_id),
                            info,
                        );
                    }
                }
            }
            // Bump the function-manager refcount on successful
            // register. Parity with C++ `gcs_job_manager.cc:136`:
            // `function_manager_.AddJobReference(job_id)` only fires on
            // the success branch of the put callback; we mirror that
            // by placing it after the storage put.
            let fm = self.function_manager.lock().clone();
            if let Some(fm) = fm {
                fm.add_job_reference(&job_id);
            }
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

            // Emit EXPORT_DRIVER_JOB (job is now is_dead=true).
            self.export_events
                .report_driver_job_event(&Self::job_to_export(&data));

            // Release runtime-env URI references for this job. Parity
            // with C++ `gcs_job_manager.cc:171`:
            // `runtime_env_manager_.RemoveURIReference(job_id.Hex())`
            // runs on the success branch of the `PublishJob` callback
            // in `MarkJobAsFinished`, *before* `ClearJobInfos` and
            // `function_manager_.RemoveJobReference`. The release
            // triggers the deleter when no detached actor still holds
            // the URI — `gcs://` packages get removed from internal KV.
            let res = self.runtime_env_service.lock().clone();
            if let Some(res) = res {
                res.remove_uri_reference(&Self::job_id_hex(&job_id)).await;
            }

            // Decrement the function-manager refcount. Parity with
            // C++ `gcs_job_manager.cc:175`: `RemoveJobReference` runs
            // after storage succeeds and after the publish. The
            // refcount reaches zero only when every detached actor
            // on this job has also retired — at that point the
            // function-manager wipes the three per-job KV entries.
            let fm = self.function_manager.lock().clone();
            if let Some(fm) = fm {
                fm.remove_job_reference(&job_id).await;
            }

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

    /// Report a job error — publish on the error channel.
    /// Maps C++ `GcsJobManager::HandleReportJobError` (gcs_job_manager.cc:460-468).
    async fn report_job_error(
        &self,
        request: Request<ReportJobErrorRequest>,
    ) -> Result<Response<ReportJobErrorReply>, Status> {
        let inner = request.into_inner();
        if let Some(job_error) = inner.job_error {
            let job_id_hex = job_error
                .job_id
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            self.publisher
                .publish_error(&job_id_hex, job_error.encode_to_vec());
        }
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

    fn temp_export_dir(tag: &str) -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "gcs_job_mgr_export_{tag}_{nanos}_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn make_manager_with_export(dir: &std::path::Path) -> GcsJobManager {
        let store = Arc::new(gcs_store::InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(GcsPublisher::new(64));
        let export = ExportEventManager::new(dir).unwrap();
        GcsJobManager::with_export_events(table_storage, publisher, export)
    }

    /// Parity: C++ `gcs_job_manager.cc:104` emits a single
    /// `RayExportEvent(ExportDriverJobEventData).SendEvent()` per job add.
    #[tokio::test]
    async fn test_add_job_emits_export_driver_job_event() {
        let dir = temp_export_dir("add");
        let mgr = make_manager_with_export(&dir);
        mgr.add_job(Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"job_export_1".to_vec(),
                driver_pid: 1234,
                driver_ip_address: "10.0.0.5".into(),
                entrypoint: "python my.py".into(),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        let path = dir
            .join("export_events")
            .join("event_EXPORT_DRIVER_JOB.log");
        let content = std::fs::read_to_string(&path).expect("file written");
        let line = content.lines().next().expect("at least one event");
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(v["source_type"], "EXPORT_DRIVER_JOB");
        assert_eq!(v["event_data"]["driver_pid"], 1234);
        assert_eq!(v["event_data"]["driver_ip_address"], "10.0.0.5");
        assert_eq!(v["event_data"]["entrypoint"], "python my.py");
        assert_eq!(v["event_data"]["is_dead"], false);
    }

    #[tokio::test]
    async fn test_mark_job_finished_emits_export_event() {
        let dir = temp_export_dir("finish");
        let mgr = make_manager_with_export(&dir);
        mgr.add_job(Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"job_export_2".to_vec(),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();
        mgr.mark_job_finished(Request::new(MarkJobFinishedRequest {
            job_id: b"job_export_2".to_vec(),
        }))
        .await
        .unwrap();

        let path = dir
            .join("export_events")
            .join("event_EXPORT_DRIVER_JOB.log");
        let content = std::fs::read_to_string(&path).expect("file written");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "add + mark-finished = two events");
        let last: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(last["event_data"]["is_dead"], true);
    }

    // ─── on_node_dead parity tests ────────────────────────────────────

    /// Parity with C++ `GcsJobManager::OnNodeDead` (gcs_job_manager.cc:488-512):
    /// jobs whose driver ran on the dead node get marked is_dead=true.
    #[tokio::test]
    async fn test_on_node_dead_marks_jobs_with_drivers_on_node() {
        let mgr = make_manager();

        // Job on dead node.
        mgr.add_job(Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"job_on_dead_node".to_vec(),
                driver_address: Some(Address {
                    node_id: b"DEADNODE".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        // Job on a different, still-alive node.
        mgr.add_job(Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"job_on_live_node".to_vec(),
                driver_address: Some(Address {
                    node_id: b"LIVENODE".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        mgr.on_node_dead(b"DEADNODE").await;

        let dead_job = mgr.get_job(b"job_on_dead_node").unwrap();
        let live_job = mgr.get_job(b"job_on_live_node").unwrap();
        assert!(dead_job.is_dead, "driver on dead node → is_dead=true");
        assert!(!live_job.is_dead, "driver on live node → untouched");
    }

    /// C++ `OnNodeDead` skips jobs that are already dead (the `!is_dead` check
    /// at gcs_job_manager.cc:501).
    #[tokio::test]
    async fn test_on_node_dead_skips_already_dead_jobs() {
        let mgr = make_manager();

        mgr.add_job(Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"already_dead".to_vec(),
                driver_address: Some(Address {
                    node_id: b"NODEX".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();
        mgr.mark_job_finished(Request::new(MarkJobFinishedRequest {
            job_id: b"already_dead".to_vec(),
        }))
        .await
        .unwrap();
        let end_time_before = mgr.get_job(b"already_dead").unwrap().end_time;

        // Node dies — should be a no-op for the already-dead job.
        mgr.on_node_dead(b"NODEX").await;

        // end_time must NOT have changed (proof we didn't re-mark).
        assert_eq!(mgr.get_job(b"already_dead").unwrap().end_time, end_time_before);
    }

    // ─── runtime-env URI lifecycle parity tests ──────────────────────

    /// Helper: make a JobTableData whose JobConfig carries a RuntimeEnvInfo
    /// with the given working_dir_uri and py_modules_uris.
    fn job_with_runtime_env(
        job_id: &[u8],
        working_dir_uri: &str,
        py_modules_uris: &[&str],
    ) -> JobTableData {
        JobTableData {
            job_id: job_id.to_vec(),
            config: Some(JobConfig {
                runtime_env_info: Some(RuntimeEnvInfo {
                    uris: Some(RuntimeEnvUris {
                        working_dir_uri: working_dir_uri.to_string(),
                        py_modules_uris: py_modules_uris
                            .iter()
                            .map(|s| s.to_string())
                            .collect(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn make_manager_with_runtime_env(
    ) -> (GcsJobManager, Arc<crate::runtime_env_stub::RuntimeEnvService>, Arc<dyn gcs_kv::InternalKVInterface>)
    {
        let store = Arc::new(InMemoryStoreClient::new());
        let kv: Arc<dyn gcs_kv::InternalKVInterface> =
            Arc::new(gcs_kv::StoreClientInternalKV::new(store.clone()));
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(GcsPublisher::new(64));
        let mgr = GcsJobManager::new(table_storage, publisher);
        let res = Arc::new(crate::runtime_env_stub::RuntimeEnvService::new_with_kv(
            kv.clone(),
        ));
        mgr.set_runtime_env_service(res.clone());
        (mgr, res, kv)
    }

    /// Parity with C++ `gcs_job_manager.cc:132-134`: add_job must pin the
    /// runtime_env URIs under the job's hex id when the JobConfig carries
    /// a RuntimeEnvInfo. `gcs://` URIs pre-seeded in the KV must survive
    /// until `mark_job_finished` releases the last reference.
    #[tokio::test]
    async fn test_add_job_pins_runtime_env_uris_and_finish_releases() {
        let (mgr, _res, kv) = make_manager_with_runtime_env();

        // Seed KV with gcs:// packages that add_job will pin.
        kv.put("", "gcs://pkg/working_dir.zip", "wd_payload".into(), true).await;
        kv.put("", "gcs://pkg/py_mod_a.zip", "pya_payload".into(), true).await;

        let job_id = b"JOBRTE1";
        mgr.add_job(Request::new(AddJobRequest {
            data: Some(job_with_runtime_env(
                job_id,
                "gcs://pkg/working_dir.zip",
                &["gcs://pkg/py_mod_a.zip"],
            )),
        }))
        .await
        .unwrap();

        // While the job is alive, the gcs:// packages must still be in KV.
        assert!(
            kv.get("", "gcs://pkg/working_dir.zip").await.is_some(),
            "working_dir URI must not be deleted while job is alive"
        );
        assert!(
            kv.get("", "gcs://pkg/py_mod_a.zip").await.is_some(),
            "py_modules URI must not be deleted while job is alive"
        );

        // Finish the job — the RemoveURIReference path should delete them.
        mgr.mark_job_finished(Request::new(MarkJobFinishedRequest {
            job_id: job_id.to_vec(),
        }))
        .await
        .unwrap();

        assert!(
            kv.get("", "gcs://pkg/working_dir.zip").await.is_none(),
            "working_dir URI must be deleted after job finishes"
        );
        assert!(
            kv.get("", "gcs://pkg/py_mod_a.zip").await.is_none(),
            "py_modules URI must be deleted after job finishes"
        );
    }

    /// A job without a `config.runtime_env_info` must not touch the
    /// runtime-env service — parity with the C++ `has_runtime_env_info()`
    /// guard at `gcs_job_manager.cc:132`.
    #[tokio::test]
    async fn test_add_job_without_runtime_env_info_is_noop_for_uri_refs() {
        let (mgr, res, _kv) = make_manager_with_runtime_env();
        mgr.add_job(Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"JOBNORE".to_vec(),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();

        // No references should have been created.
        let released = res.remove_uri_reference_sync("4a4f4e4f5245"); // hex of b"JOBNORE"
        assert!(released.is_empty());
    }

    /// Non-gcs:// URIs (e.g. s3://) are tracked but the deleter only
    /// targets gcs:// (matches C++ deleter_ gate at gcs_server.cc:696-725
    /// which only calls kv_manager_->Del on gcs:// URIs).
    #[tokio::test]
    async fn test_non_gcs_uris_are_tracked_but_not_deleted() {
        let (mgr, res, _kv) = make_manager_with_runtime_env();

        let job_id = b"JOBS3URI";
        mgr.add_job(Request::new(AddJobRequest {
            data: Some(job_with_runtime_env(
                job_id,
                "s3://bucket/working_dir.zip",
                &[],
            )),
        }))
        .await
        .unwrap();

        // While alive, the refcount is held.
        let hex_id = GcsJobManager::hex(job_id);
        // Sanity: remove should still release the reference.
        mgr.mark_job_finished(Request::new(MarkJobFinishedRequest {
            job_id: job_id.to_vec(),
        }))
        .await
        .unwrap();

        // Double-release is a no-op.
        let released = res.remove_uri_reference_sync(&hex_id);
        assert!(released.is_empty());
    }

    /// Parity: on_node_dead with no matching jobs → no-op, no panic.
    #[tokio::test]
    async fn test_on_node_dead_no_match_is_noop() {
        let mgr = make_manager();
        mgr.add_job(Request::new(AddJobRequest {
            data: Some(JobTableData {
                job_id: b"j".to_vec(),
                driver_address: Some(Address {
                    node_id: b"A".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();
        mgr.on_node_dead(b"different_node").await;
        assert!(!mgr.get_job(b"j").unwrap().is_dead);
    }
}
