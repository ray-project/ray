//! Periodic raylet resource-load pull.
//!
//! C++ `GcsServer::InitGcsResourceManager` (`gcs/gcs_server.cc:415-446`)
//! installs a periodic task that, every
//! `gcs_pull_resource_loads_period_milliseconds` (default 1000ms), iterates
//! every alive node, issues `NodeManagerService::GetResourceLoad`, and
//! feeds the reply into *both* `GcsResourceManager::UpdateResourceLoads`
//! and `GcsAutoscalerStateManager::UpdateResourceLoadAndUsage`.
//!
//! Without this loop the autoscaler only ever sees totals captured at node
//! registration plus drain-state deltas; `pending_resource_requests`,
//! `resource_load_by_shape`, and per-node idle/running state all freeze at
//! startup. This module is that loop's Rust port — a testable
//! `RayletLoadFetcher` trait plus a production gRPC implementation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tonic::Status;
use tracing::{debug, warn};

use gcs_proto::ray::rpc::node_manager_service_client::NodeManagerServiceClient;
use gcs_proto::ray::rpc::{GcsNodeInfo, GetResourceLoadRequest, ResourcesData};

use crate::autoscaler_stub::GcsAutoscalerStateManager;
use crate::node_manager::GcsNodeManager;
use crate::resource_manager::GcsResourceManager;

/// Fetches `GetResourceLoad` replies from raylets. Abstract for tests.
///
/// Production: `TonicRayletLoadFetcher`. Tests: provide a fake whose
/// `fetch` returns scripted `ResourcesData`.
#[async_trait]
pub trait RayletLoadFetcher: Send + Sync {
    /// Issue `NodeManagerService::GetResourceLoad` against the raylet at
    /// `(address, port)` and return the `resources` field from the reply.
    async fn fetch(&self, address: &str, port: u16) -> Result<ResourcesData, Status>;
}

/// Production fetcher: opens a tonic channel per call, matching the
/// lazy-connect pattern used by the placement-group scheduler and
/// autoscaler pool elsewhere in this crate.
pub struct TonicRayletLoadFetcher;

impl TonicRayletLoadFetcher {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TonicRayletLoadFetcher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RayletLoadFetcher for TonicRayletLoadFetcher {
    async fn fetch(&self, address: &str, port: u16) -> Result<ResourcesData, Status> {
        let endpoint = format!("http://{address}:{port}");
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| Status::invalid_argument(format!("invalid raylet endpoint: {e}")))?
            .connect()
            .await
            .map_err(|e| Status::unavailable(format!("connect to {endpoint} failed: {e}")))?;
        let mut client = NodeManagerServiceClient::new(channel);
        let reply = client
            .get_resource_load(GetResourceLoadRequest {})
            .await?
            .into_inner();
        Ok(reply.resources.unwrap_or_default())
    }
}

/// Run one polling pass: for every alive node, fetch `GetResourceLoad` and
/// fan the reply into both downstream consumers.
///
/// Extracted as a plain async function so the loop itself and the tests
/// can share one implementation. Parity with the body of the C++
/// `periodical_runner_->RunFnPeriodically` closure in
/// `gcs_server.cc:415-446`.
pub async fn pull_once(
    fetcher: Arc<dyn RayletLoadFetcher>,
    node_manager: &GcsNodeManager,
    resource_manager: &GcsResourceManager,
    autoscaler: &GcsAutoscalerStateManager,
) -> PullPassStats {
    let alive = node_manager.get_all_alive_nodes();
    let mut stats = PullPassStats::default();

    // Run fetches concurrently so a single slow raylet doesn't block the
    // pass. C++ uses ASIO callbacks for the same reason. We use a
    // `JoinSet` to get the same fan-out without pulling in `futures_util`.
    let mut set: JoinSet<(GcsNodeInfo, Result<ResourcesData, Status>)> = JoinSet::new();
    for (_id, node) in alive {
        let address = node.node_manager_address.clone();
        let port = node.node_manager_port as u16;
        let f = fetcher.clone();
        set.spawn(async move {
            let res = f.fetch(&address, port).await;
            (node, res)
        });
    }

    let mut results: Vec<(GcsNodeInfo, Result<ResourcesData, Status>)> =
        Vec::with_capacity(set.len());
    while let Some(j) = set.join_next().await {
        if let Ok(pair) = j {
            results.push(pair);
        }
    }

    for (node, res) in results {
        match res {
            Ok(mut data) => {
                // C++ keys the downstream updates by the reply's
                // `node_id` when present; if the raylet hasn't filled it
                // in, fall back to the GCS-side node_id so the
                // lookup-by-node tables hit.
                if data.node_id.is_empty() {
                    data.node_id = node.node_id.clone();
                }
                resource_manager.update_resource_loads(&data);
                autoscaler.update_resource_load_and_usage(data);
                stats.ok += 1;
            }
            Err(e) => {
                stats.err += 1;
                // Sampled log to avoid spamming on a flapping raylet —
                // mirrors C++ `RAY_LOG_EVERY_N(WARNING, 10)` at
                // `gcs_server.cc:439-441`. We log every 10th failure
                // observed across the lifetime of the server.
                if should_log_err() {
                    warn!(
                        node = hex_encode(&node.node_id),
                        address = node.node_manager_address,
                        port = node.node_manager_port,
                        error = %e,
                        "Failed to get the resource load"
                    );
                } else {
                    debug!(
                        node = hex_encode(&node.node_id),
                        error = %e,
                        "GetResourceLoad failed (suppressed)"
                    );
                }
            }
        }
    }
    stats
}

/// Aggregate counts produced by one pass. Returned so a caller (in
/// particular, the loop below) or a test can assert on progress.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct PullPassStats {
    pub ok: usize,
    pub err: usize,
}

/// Spawn the long-running polling task. Cadence comes from
/// `ray_config::gcs_pull_resource_loads_period_milliseconds` (default
/// 1000 ms, `ray_config_def.h:62`). A period of 0 disables the loop —
/// parity with other `_period_milliseconds` knobs in the Rust GCS.
pub fn spawn_load_pull_loop(
    fetcher: Arc<dyn RayletLoadFetcher>,
    node_manager: Arc<GcsNodeManager>,
    resource_manager: Arc<GcsResourceManager>,
    autoscaler: Arc<GcsAutoscalerStateManager>,
    period_ms: u64,
) -> Option<tokio::task::JoinHandle<()>> {
    if period_ms == 0 {
        return None;
    }
    let period = Duration::from_millis(period_ms);
    Some(tokio::spawn(async move {
        // Use `tokio::time::interval` with `Skip` missed-tick behavior so
        // a slow pass doesn't queue up a backlog that hammers the raylets
        // after recovery. C++ `PeriodicalRunner` has the same semantics.
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // First tick fires immediately; skip it so we wait `period_ms`
        // before the first pull, aligning with the C++ runner which
        // schedules the first execution after the period elapses.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            pull_once(
                fetcher.clone(),
                &node_manager,
                &resource_manager,
                &autoscaler,
            )
            .await;
        }
    }))
}

// Simple, process-wide counter that emits a warning on every 10th
// failure observed. The exact sampling constant (10) comes from C++
// `RAY_LOG_EVERY_N(WARNING, 10)` at `gcs_server.cc:439`.
fn should_log_err() -> bool {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNT: AtomicUsize = AtomicUsize::new(0);
    COUNT.fetch_add(1, Ordering::Relaxed) % 10 == 0
}

/// Tiny inline hex encoder used for log fields so we don't need a
/// `hex` dependency just for this module.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Test-only accessor used by the gcs-server integration tests.
#[cfg(any(test, feature = "test-support"))]
pub async fn pull_once_for_test(
    fetcher: Arc<dyn RayletLoadFetcher>,
    node_manager: &GcsNodeManager,
    resource_manager: &GcsResourceManager,
    autoscaler: &GcsAutoscalerStateManager,
) -> PullPassStats {
    pull_once(fetcher, node_manager, resource_manager, autoscaler).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autoscaler_stub::{PlacementGroupLoadSource, RayletClientPool};
    use async_trait::async_trait;
    use gcs_kv::{InternalKVInterface, StoreClientInternalKV};
    use gcs_proto::ray::rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsService;
    use gcs_proto::ray::rpc::{GcsNodeInfo, PlacementGroupLoad, ResourceLoad};
    use gcs_pubsub::GcsPublisher;
    use gcs_store::InMemoryStoreClient;
    use gcs_table_storage::GcsTableStorage;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    // ── Fakes ─────────────────────────────────────────────────────

    struct FakeFetcher {
        /// (address, port) → Either a ResourcesData or an error string.
        scripted: Mutex<HashMap<(String, u16), Result<ResourcesData, String>>>,
        calls: Mutex<Vec<(String, u16)>>,
    }

    impl FakeFetcher {
        fn new() -> Self {
            Self {
                scripted: Mutex::new(HashMap::new()),
                calls: Mutex::new(Vec::new()),
            }
        }
        fn set_reply(&self, addr: &str, port: u16, reply: ResourcesData) {
            self.scripted
                .lock()
                .insert((addr.to_string(), port), Ok(reply));
        }
        fn set_error(&self, addr: &str, port: u16, err: &str) {
            self.scripted
                .lock()
                .insert((addr.to_string(), port), Err(err.to_string()));
        }
        fn call_count(&self) -> usize {
            self.calls.lock().len()
        }
    }

    #[async_trait]
    impl RayletLoadFetcher for FakeFetcher {
        async fn fetch(&self, address: &str, port: u16) -> Result<ResourcesData, Status> {
            self.calls.lock().push((address.to_string(), port));
            match self
                .scripted
                .lock()
                .get(&(address.to_string(), port))
                .cloned()
            {
                Some(Ok(r)) => Ok(r),
                Some(Err(e)) => Err(Status::unavailable(e)),
                None => Err(Status::not_found("no script for address")),
            }
        }
    }

    struct NullPgLoad;
    impl PlacementGroupLoadSource for NullPgLoad {
        fn get_placement_group_load(&self) -> PlacementGroupLoad {
            PlacementGroupLoad::default()
        }
    }

    struct NullPool;
    #[async_trait]
    impl RayletClientPool for NullPool {
        async fn resize_local_resource_instances(
            &self,
            _address: &str,
            _port: u16,
            _resources: HashMap<String, f64>,
        ) -> Result<HashMap<String, f64>, Status> {
            Err(Status::unavailable("not used in these tests"))
        }

        async fn drain_raylet(
            &self,
            _address: &str,
            _port: u16,
            _request: gcs_proto::ray::rpc::DrainRayletRequest,
        ) -> Result<gcs_proto::ray::rpc::DrainRayletReply, Status> {
            Err(Status::unavailable("not used in these tests"))
        }

        async fn kill_local_actor(
            &self,
            _address: &str,
            _port: u16,
            _request: gcs_proto::ray::rpc::KillLocalActorRequest,
        ) -> Result<(), Status> {
            Err(Status::unavailable("not used in these tests"))
        }
    }

    fn make_autoscaler(nm: Arc<GcsNodeManager>) -> Arc<GcsAutoscalerStateManager> {
        let kv: Arc<dyn InternalKVInterface> = Arc::new(StoreClientInternalKV::new(Arc::new(
            InMemoryStoreClient::new(),
        )));
        Arc::new(GcsAutoscalerStateManager::new(
            nm,
            Arc::new(NullPgLoad),
            Arc::new(NullPool),
            kv,
            "test-session".to_string(),
        ))
    }

    fn node(id: &[u8], addr: &str, port: i32) -> GcsNodeInfo {
        GcsNodeInfo {
            node_id: id.to_vec(),
            node_manager_address: addr.to_string(),
            node_manager_port: port,
            state: gcs_proto::ray::rpc::gcs_node_info::GcsNodeState::Alive as i32,
            resources_total: {
                let mut m = HashMap::new();
                m.insert("CPU".to_string(), 4.0);
                m
            },
            ..Default::default()
        }
    }

    fn make_node_manager() -> Arc<GcsNodeManager> {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(GcsPublisher::new(64));
        Arc::new(GcsNodeManager::new(
            table_storage,
            publisher,
            b"cluster-test".to_vec(),
        ))
    }

    async fn register_alive(nm: &GcsNodeManager, n: GcsNodeInfo) {
        // Use the regular `add_node` path — it's idempotent against empty
        // storage and exercises the same broadcast/publish hooks prod uses.
        nm.add_node(n).await;
    }

    #[tokio::test]
    async fn pull_once_updates_both_downstreams() {
        let nm = make_node_manager();
        let rm = Arc::new(GcsResourceManager::new());
        let asm = make_autoscaler(nm.clone());

        let n1 = node(b"node-aaaaaaaaaaaaaaaaaaaaaaa1", "1.2.3.4", 7001);
        register_alive(&nm, n1.clone()).await;
        // Seed the autoscaler's cache (normally done on OnNodeAdd).
        asm.on_node_add(&n1);
        // Seed the resource manager so update_resource_loads finds the
        // row (mirroring the node-add listener).
        rm.update_node_resources(n1.node_id.clone(), ResourcesData::default());

        let fake = Arc::new(FakeFetcher::new());
        let mut reply = ResourcesData::default();
        reply.node_id = n1.node_id.clone();
        reply.resource_load = {
            let mut m = HashMap::new();
            m.insert("CPU".to_string(), 3.5);
            m
        };
        reply.resource_load_by_shape = Some(ResourceLoad::default());
        reply.resources_available = {
            let mut m = HashMap::new();
            m.insert("CPU".to_string(), 0.5);
            m
        };
        fake.set_reply("1.2.3.4", 7001, reply.clone());

        let stats = pull_once(fake.clone(), &nm, &rm, &asm).await;
        assert_eq!(stats, PullPassStats { ok: 1, err: 0 });
        assert_eq!(fake.call_count(), 1);

        // Resource manager: resource_load was copied into the per-node row.
        {
            let row = rm
                .get_all_resource_usage(tonic::Request::new(
                    gcs_proto::ray::rpc::GetAllResourceUsageRequest {},
                ))
                .await
                .unwrap()
                .into_inner()
                .resource_usage_data
                .unwrap();
            assert_eq!(row.batch.len(), 1);
            assert_eq!(row.batch[0].resource_load.get("CPU"), Some(&3.5));
        }

        // Autoscaler: the node's cache entry was replaced with the reply
        // (resources_available = 0.5 proves the update landed).
        let state = asm.get_node_resource_info(&n1.node_id);
        assert_eq!(state.unwrap().resources_available.get("CPU"), Some(&0.5));
    }

    #[tokio::test]
    async fn pull_once_swallows_per_node_errors() {
        let nm = make_node_manager();
        let rm = Arc::new(GcsResourceManager::new());
        let asm = make_autoscaler(nm.clone());

        let good = node(b"node-aaaaaaaaaaaaaaaaaaaaaaa1", "good", 1);
        let bad = node(b"node-aaaaaaaaaaaaaaaaaaaaaaa2", "bad", 2);
        register_alive(&nm, good.clone()).await;
        register_alive(&nm, bad.clone()).await;
        asm.on_node_add(&good);
        asm.on_node_add(&bad);
        rm.update_node_resources(good.node_id.clone(), ResourcesData::default());
        rm.update_node_resources(bad.node_id.clone(), ResourcesData::default());

        let fake = Arc::new(FakeFetcher::new());
        let mut reply = ResourcesData::default();
        reply.node_id = good.node_id.clone();
        reply.resource_load = {
            let mut m = HashMap::new();
            m.insert("CPU".to_string(), 1.0);
            m
        };
        fake.set_reply("good", 1, reply);
        fake.set_error("bad", 2, "raylet offline");

        let stats = pull_once(fake.clone(), &nm, &rm, &asm).await;
        assert_eq!(stats, PullPassStats { ok: 1, err: 1 });
    }

    #[tokio::test]
    async fn pull_once_fills_missing_node_id_from_gcs_side() {
        // Parity guard: C++ uses NodeID::FromBinary(data.node_id()) from
        // the reply. If a raylet returns an empty node_id (e.g. older
        // versions), we fall back to the GCS-side id so the downstream
        // lookups still hit.
        let nm = make_node_manager();
        let rm = Arc::new(GcsResourceManager::new());
        let asm = make_autoscaler(nm.clone());

        let n = node(b"node-aaaaaaaaaaaaaaaaaaaaaaa1", "1.2.3.4", 1);
        register_alive(&nm, n.clone()).await;
        asm.on_node_add(&n);
        rm.update_node_resources(n.node_id.clone(), ResourcesData::default());

        let fake = Arc::new(FakeFetcher::new());
        let mut reply = ResourcesData::default(); // node_id empty
        reply.resource_load = {
            let mut m = HashMap::new();
            m.insert("CPU".to_string(), 2.0);
            m
        };
        fake.set_reply("1.2.3.4", 1, reply);

        let stats = pull_once(fake.clone(), &nm, &rm, &asm).await;
        assert_eq!(stats.ok, 1);

        let row = rm
            .get_all_resource_usage(tonic::Request::new(
                gcs_proto::ray::rpc::GetAllResourceUsageRequest {},
            ))
            .await
            .unwrap()
            .into_inner()
            .resource_usage_data
            .unwrap();
        assert_eq!(row.batch.len(), 1);
        assert_eq!(row.batch[0].resource_load.get("CPU"), Some(&2.0));
    }

    #[tokio::test]
    async fn spawn_load_pull_loop_period_zero_is_noop() {
        let nm = make_node_manager();
        let rm = Arc::new(GcsResourceManager::new());
        let asm = make_autoscaler(nm.clone());
        let fake: Arc<dyn RayletLoadFetcher> = Arc::new(FakeFetcher::new());
        let handle = spawn_load_pull_loop(fake, nm, rm, asm, 0);
        assert!(handle.is_none());
    }

    #[tokio::test]
    async fn spawn_load_pull_loop_actually_ticks() {
        let nm = make_node_manager();
        let rm = Arc::new(GcsResourceManager::new());
        let asm = make_autoscaler(nm.clone());

        let n = node(b"node-aaaaaaaaaaaaaaaaaaaaaaa1", "1.2.3.4", 1);
        register_alive(&nm, n.clone()).await;
        asm.on_node_add(&n);
        rm.update_node_resources(n.node_id.clone(), ResourcesData::default());

        let fake = Arc::new(FakeFetcher::new());
        let mut reply = ResourcesData::default();
        reply.node_id = n.node_id.clone();
        reply.resource_load = {
            let mut m = HashMap::new();
            m.insert("CPU".to_string(), 0.25);
            m
        };
        fake.set_reply("1.2.3.4", 1, reply);

        let handle =
            spawn_load_pull_loop(fake.clone(), nm.clone(), rm.clone(), asm.clone(), 30)
                .expect("loop must spawn for non-zero period");

        // Wait long enough for at least two ticks (first tick is skipped
        // by design, so effective delay is ~2*30ms).
        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.abort();

        assert!(
            fake.call_count() >= 1,
            "loop should have called fetch at least once, saw {}",
            fake.call_count()
        );
    }
}
