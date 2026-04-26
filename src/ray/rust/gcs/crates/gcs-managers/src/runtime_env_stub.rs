//! Runtime environment URI pinning service.
//!
//! Implements reference-counted URI pinning with timed expiry, matching
//! C++ `RuntimeEnvHandler` from `src/ray/gcs/runtime_env_handler.cc`.
//!
//! ## Semantics
//!
//! "Pinning" a URI increments a reference counter for that URI, preventing
//! GCS from garbage-collecting the resource. Each pin request generates a
//! unique temporary reference ID and schedules automatic removal after
//! `expiration_s` seconds. When the last reference to a URI is removed,
//! the URI becomes eligible for cleanup.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use gcs_kv::InternalKVInterface;
use gcs_proto::ray::rpc::runtime_env_gcs_service_server::RuntimeEnvGcsService;
use gcs_proto::ray::rpc::*;

fn ok_status() -> GcsStatus {
    GcsStatus {
        code: 0,
        message: String::new(),
    }
}

/// Runtime environment URI pin manager.
///
/// Tracks reference counts per URI and associates temporary IDs with URIs
/// for timed expiry. Matches C++ `RuntimeEnvManager` reference counting.
///
/// When a URI's reference count drops to 0, `gcs://` protocol URIs are
/// deleted from the KV store, matching C++ `RuntimeEnvManager::deleter_`
/// (gcs_server.cc:696-725).
pub struct RuntimeEnvService {
    /// URI -> reference count. A URI with count > 0 is "pinned".
    uri_reference: Arc<Mutex<HashMap<String, i64>>>,
    /// Temporary reference ID -> list of URIs it pins.
    id_to_uris: Arc<Mutex<HashMap<String, Vec<String>>>>,
    /// Monotonic counter for generating unique temporary reference IDs.
    next_ref_id: AtomicU64,
    /// Optional KV store for deleting `gcs://` URIs when refcount reaches 0.
    /// Maps C++ `RuntimeEnvManager::deleter_` which calls `kv_manager_->Del()`.
    kv: Option<Arc<dyn InternalKVInterface>>,
}

impl RuntimeEnvService {
    /// Create a new RuntimeEnvService without KV cleanup (for testing).
    pub fn new() -> Self {
        Self {
            uri_reference: Arc::new(Mutex::new(HashMap::new())),
            id_to_uris: Arc::new(Mutex::new(HashMap::new())),
            next_ref_id: AtomicU64::new(0),
            kv: None,
        }
    }

    /// Create a new RuntimeEnvService with KV cleanup for `gcs://` URIs.
    ///
    /// Maps C++ `GcsServer::InitRuntimeEnvManager` (gcs_server.cc:696-738)
    /// which creates a `RuntimeEnvManager` with a deleter that calls
    /// `kv_manager_->Del()` for `gcs://` protocol URIs.
    pub fn new_with_kv(kv: Arc<dyn InternalKVInterface>) -> Self {
        Self {
            uri_reference: Arc::new(Mutex::new(HashMap::new())),
            id_to_uris: Arc::new(Mutex::new(HashMap::new())),
            next_ref_id: AtomicU64::new(0),
            kv: Some(kv),
        }
    }

    /// Generate a unique hex reference ID (matching C++ UniqueID::FromRandom().Hex()).
    fn generate_ref_id(&self) -> String {
        let id = self.next_ref_id.fetch_add(1, Ordering::SeqCst);
        format!("ref_{:016x}", id)
    }

    /// Add a URI reference under the given ID. Increments the URI's reference count.
    /// Matches C++ `RuntimeEnvManager::AddURIReference(hex_id, uri)`
    /// (runtime_env_manager.cc:20-26).
    pub fn add_uri_reference(&self, ref_id: &str, uri: &str) {
        let mut refs = self.uri_reference.lock();
        *refs.entry(uri.to_string()).or_insert(0) += 1;

        let mut id_map = self.id_to_uris.lock();
        id_map
            .entry(ref_id.to_string())
            .or_default()
            .push(uri.to_string());

        debug!(ref_id, uri, "Added URI reference");
    }

    /// Add URI references extracted from a `RuntimeEnvInfo` under `ref_id`.
    ///
    /// Matches C++ `RuntimeEnvManager::AddURIReference(hex_id, RuntimeEnvInfo)`
    /// (runtime_env_manager.cc:28-43): both `working_dir_uri` and each
    /// `py_modules_uris` entry are added.
    pub fn add_uri_reference_from_runtime_env(
        &self,
        ref_id: &str,
        runtime_env_info: &RuntimeEnvInfo,
    ) {
        let Some(uris) = runtime_env_info.uris.as_ref() else {
            return;
        };
        if !uris.working_dir_uri.is_empty() {
            self.add_uri_reference(ref_id, &uris.working_dir_uri);
        }
        for uri in &uris.py_modules_uris {
            self.add_uri_reference(ref_id, uri);
        }
    }

    /// Remove a URI reference by ID. Decrements reference counts for all URIs
    /// associated with the ID. Returns URIs whose count dropped to 0.
    /// Matches C++ `RuntimeEnvManager::RemoveURIReference`
    /// (runtime_env_manager.cc:53-73). The caller is expected to invoke
    /// `delete_released_uris` for `gcs://` KV cleanup, matching C++ where
    /// the deleter runs synchronously inside `RemoveURIReference`.
    pub fn remove_uri_reference_sync(&self, ref_id: &str) -> Vec<String> {
        let mut id_map = self.id_to_uris.lock();
        let uris = match id_map.remove(ref_id) {
            Some(uris) => uris,
            None => return Vec::new(),
        };

        let mut refs = self.uri_reference.lock();
        let mut released = Vec::new();

        for uri in &uris {
            if let Some(count) = refs.get_mut(uri) {
                *count -= 1;
                if *count <= 0 {
                    refs.remove(uri);
                    released.push(uri.clone());
                    debug!(uri, "URI reference count reached 0");
                }
            }
        }

        debug!(ref_id, released = released.len(), "Removed URI reference");
        released
    }

    /// Remove a URI reference by ID and asynchronously delete any released
    /// `gcs://` URIs from the KV store. Matches the full behavior of C++
    /// `RuntimeEnvManager::RemoveURIReference` which invokes `deleter_` on
    /// each URI whose refcount reached zero (runtime_env_manager.cc:60-71).
    pub async fn remove_uri_reference(&self, ref_id: &str) {
        let released = self.remove_uri_reference_sync(ref_id);
        if !released.is_empty() {
            self.delete_released_uris(&released).await;
        }
    }

    /// Delete released `gcs://` URIs from KV store.
    /// Matches C++ `RuntimeEnvManager::deleter_` which calls
    /// `kv_manager_->Del()` for `gcs://` protocol URIs.
    async fn delete_released_uris(&self, released: &[String]) {
        let Some(kv) = &self.kv else {
            return;
        };
        for uri in released {
            if uri.starts_with("gcs://") {
                // C++ deletes the URI key from the empty namespace.
                kv.del("" /* namespace */, uri, false /* del_by_prefix */)
                    .await;
                debug!(uri, "Deleted released gcs:// URI from KV store");
            }
            // Non-gcs:// URIs are managed externally (S3, GS, etc.)
        }
    }

    /// Get the current reference count for a URI (for testing).
    #[cfg(test)]
    fn uri_ref_count(&self, uri: &str) -> i64 {
        self.uri_reference.lock().get(uri).copied().unwrap_or(0)
    }
}

#[tonic::async_trait]
impl RuntimeEnvGcsService for RuntimeEnvService {
    /// Pin a runtime environment URI with timed expiry.
    ///
    /// Matches C++ `RuntimeEnvHandler::HandlePinRuntimeEnvURI`
    /// (runtime_env_handler.cc:22-42):
    /// 1. Generate a temporary reference ID
    /// 2. Increment URI reference count via AddURIReference
    /// 3. Schedule removal after expiration_s seconds
    /// 4. Return OK immediately
    async fn pin_runtime_env_uri(
        &self,
        req: Request<PinRuntimeEnvUriRequest>,
    ) -> Result<Response<PinRuntimeEnvUriReply>, Status> {
        let inner = req.into_inner();
        let uri = inner.uri;
        let expiration_s = inner.expiration_s;

        if uri.is_empty() {
            return Ok(Response::new(PinRuntimeEnvUriReply {
                status: Some(ok_status()),
            }));
        }

        // Generate a unique temporary reference ID.
        let ref_id = self.generate_ref_id();

        // Add the URI reference (increment count).
        self.add_uri_reference(&ref_id, &uri);

        info!(
            uri,
            expiration_s,
            ref_id,
            "Pinned runtime env URI"
        );

        // Schedule automatic removal after expiration_s seconds.
        // This matches C++ delay_executor_ which uses ASIO deadline timers.
        // C++ always schedules the callback, even when expiration_s == 0 — the
        // timer fires immediately so the temporary pin is released right after
        // the RPC completes (runtime_env_handler.cc:32-38). We mirror that:
        // sleep(0) yields once and resolves on the next executor tick.
        let uri_ref = self.uri_reference.clone();
        let id_map = self.id_to_uris.clone();
        let kv = self.kv.clone();
        let ref_id_owned = ref_id.clone();

        tokio::spawn(async move {
            if expiration_s > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(expiration_s as u64)).await;
            } else {
                // Match C++ delay_executor_(..., 0): yield so the reply is sent
                // first, then run the removal on the next executor tick.
                tokio::task::yield_now().await;
            }

            // Remove the temporary reference (decrement count).
            // Scope the mutex guards explicitly so they're dropped before any .await.
            let released = {
                let uris = {
                    let mut id_map_guard = id_map.lock();
                    match id_map_guard.remove(&ref_id_owned) {
                        Some(uris) => uris,
                        None => return,
                    }
                };

                let mut refs = uri_ref.lock();
                let mut released = Vec::new();
                for uri in &uris {
                    if let Some(count) = refs.get_mut(uri) {
                        *count -= 1;
                        if *count <= 0 {
                            refs.remove(uri);
                            released.push(uri.clone());
                            debug!(uri, "URI pin expired, reference count reached 0");
                        }
                    }
                }
                released
            };

            // Delete released gcs:// URIs from KV store (matching C++ deleter_).
            if let Some(kv) = &kv {
                for uri in &released {
                    if uri.starts_with("gcs://") {
                        kv.del("", uri, false).await;
                        debug!(uri, "Deleted expired gcs:// URI from KV");
                    }
                }
            }

            debug!(ref_id = ref_id_owned.as_str(), "URI pin expired");
        });

        Ok(Response::new(PinRuntimeEnvUriReply {
            status: Some(ok_status()),
        }))
    }
}

// Keep the old name as a type alias for backward compatibility with server wiring.
pub type RuntimeEnvServiceStub = RuntimeEnvService;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pin_runtime_env_uri() {
        let svc = RuntimeEnvService::new();
        let reply = svc
            .pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
                uri: "gcs://test_uri".to_string(),
                expiration_s: 3600,
            }))
            .await;
        assert!(reply.is_ok());
        assert_eq!(reply.unwrap().into_inner().status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_pin_increments_ref_count() {
        let svc = RuntimeEnvService::new();

        // Pin the same URI twice.
        svc.pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
            uri: "gcs://my_env".to_string(),
            expiration_s: 3600,
        }))
        .await
        .unwrap();

        svc.pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
            uri: "gcs://my_env".to_string(),
            expiration_s: 3600,
        }))
        .await
        .unwrap();

        assert_eq!(svc.uri_ref_count("gcs://my_env"), 2);
    }

    #[tokio::test]
    async fn test_pin_expiry_decrements_ref_count() {
        let svc = RuntimeEnvService::new();

        // Pin with 1-second expiry.
        svc.pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
            uri: "gcs://expiring".to_string(),
            expiration_s: 1,
        }))
        .await
        .unwrap();

        assert_eq!(svc.uri_ref_count("gcs://expiring"), 1);

        // Wait for expiry.
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // Reference should be removed.
        assert_eq!(svc.uri_ref_count("gcs://expiring"), 0);
    }

    #[tokio::test]
    async fn test_multiple_pins_partial_expiry() {
        let svc = RuntimeEnvService::new();

        // Pin once with long expiry.
        svc.pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
            uri: "gcs://shared".to_string(),
            expiration_s: 3600,
        }))
        .await
        .unwrap();

        // Pin again with short expiry.
        svc.pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
            uri: "gcs://shared".to_string(),
            expiration_s: 1,
        }))
        .await
        .unwrap();

        assert_eq!(svc.uri_ref_count("gcs://shared"), 2);

        // Wait for the short expiry.
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // Only one reference should remain (the long-lived one).
        assert_eq!(svc.uri_ref_count("gcs://shared"), 1);
    }

    #[tokio::test]
    async fn test_add_and_remove_uri_reference() {
        let svc = RuntimeEnvService::new();

        svc.add_uri_reference("ref_1", "gcs://test");
        svc.add_uri_reference("ref_2", "gcs://test");
        assert_eq!(svc.uri_ref_count("gcs://test"), 2);

        let released = svc.remove_uri_reference_sync("ref_1");
        assert!(released.is_empty()); // Still has ref_2
        assert_eq!(svc.uri_ref_count("gcs://test"), 1);

        let released = svc.remove_uri_reference_sync("ref_2");
        assert_eq!(released, vec!["gcs://test"]); // Last reference removed
        assert_eq!(svc.uri_ref_count("gcs://test"), 0);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_reference() {
        let svc = RuntimeEnvService::new();
        let released = svc.remove_uri_reference_sync("no_such_ref");
        assert!(released.is_empty());
    }

    /// Matches C++ `RuntimeEnvManager::AddURIReference(hex_id, RuntimeEnvInfo)`:
    /// both working_dir_uri and py_modules_uris are pinned.
    #[tokio::test]
    async fn test_add_uri_reference_from_runtime_env_info() {
        let svc = RuntimeEnvService::new();
        let info = RuntimeEnvInfo {
            uris: Some(RuntimeEnvUris {
                working_dir_uri: "gcs://wd".to_string(),
                py_modules_uris: vec!["gcs://py1".to_string(), "gcs://py2".to_string()],
            }),
            ..Default::default()
        };
        svc.add_uri_reference_from_runtime_env("job_abc", &info);
        assert_eq!(svc.uri_ref_count("gcs://wd"), 1);
        assert_eq!(svc.uri_ref_count("gcs://py1"), 1);
        assert_eq!(svc.uri_ref_count("gcs://py2"), 1);

        let released = svc.remove_uri_reference_sync("job_abc");
        assert_eq!(released.len(), 3);
        assert_eq!(svc.uri_ref_count("gcs://wd"), 0);
        assert_eq!(svc.uri_ref_count("gcs://py1"), 0);
        assert_eq!(svc.uri_ref_count("gcs://py2"), 0);
    }

    /// Empty `working_dir_uri` is skipped (matches the C++ `.empty()` check in
    /// runtime_env_manager.cc:31).
    #[tokio::test]
    async fn test_add_uri_reference_from_runtime_env_skips_empty_working_dir() {
        let svc = RuntimeEnvService::new();
        let info = RuntimeEnvInfo {
            uris: Some(RuntimeEnvUris {
                working_dir_uri: String::new(),
                py_modules_uris: vec!["gcs://py".to_string()],
            }),
            ..Default::default()
        };
        svc.add_uri_reference_from_runtime_env("job_x", &info);
        assert_eq!(svc.uri_ref_count("gcs://py"), 1);
        // No empty-key entry was created.
        assert!(svc.uri_reference.lock().get("").is_none());
    }

    /// `remove_uri_reference` (async) must delete released `gcs://` URIs from KV
    /// synchronously as part of the call — parity with the C++ deleter that runs
    /// inside `RemoveURIReference`.
    #[tokio::test]
    async fn test_remove_uri_reference_deletes_gcs_from_kv() {
        use gcs_store::InMemoryStoreClient;
        let store = Arc::new(InMemoryStoreClient::new());
        let kv: Arc<dyn InternalKVInterface> =
            Arc::new(gcs_kv::StoreClientInternalKV::new(store));
        kv.put("", "gcs://pkg1", "payload".to_string(), true).await;

        let svc = RuntimeEnvService::new_with_kv(kv.clone());
        svc.add_uri_reference("job_del", "gcs://pkg1");
        svc.remove_uri_reference("job_del").await;

        assert_eq!(svc.uri_ref_count("gcs://pkg1"), 0);
        assert!(
            kv.get("", "gcs://pkg1").await.is_none(),
            "gcs:// URI should be removed from KV when refcount drops to 0"
        );
    }

    /// Removing a ref that still has another holder must NOT delete from KV.
    #[tokio::test]
    async fn test_remove_uri_reference_keeps_kv_while_others_hold() {
        use gcs_store::InMemoryStoreClient;
        let store = Arc::new(InMemoryStoreClient::new());
        let kv: Arc<dyn InternalKVInterface> =
            Arc::new(gcs_kv::StoreClientInternalKV::new(store));
        kv.put("", "gcs://pkg2", "payload".to_string(), true).await;

        let svc = RuntimeEnvService::new_with_kv(kv.clone());
        svc.add_uri_reference("job_a", "gcs://pkg2");
        svc.add_uri_reference("actor_b", "gcs://pkg2");

        svc.remove_uri_reference("job_a").await;
        // Still has one ref — KV must be intact.
        assert_eq!(svc.uri_ref_count("gcs://pkg2"), 1);
        assert!(kv.get("", "gcs://pkg2").await.is_some());

        svc.remove_uri_reference("actor_b").await;
        assert_eq!(svc.uri_ref_count("gcs://pkg2"), 0);
        assert!(kv.get("", "gcs://pkg2").await.is_none());
    }

    /// Mirrors C++ `TestPinRuntimeEnvURI_ZeroExpiration`
    /// (runtime_env_handler_test.cc:82-98): expiration_s == 0 must still
    /// schedule the removal so the URI is released immediately after the RPC.
    #[tokio::test]
    async fn test_pin_zero_expiration_releases_immediately() {
        let svc = RuntimeEnvService::new();

        svc.pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
            uri: "s3://bucket/my_env.zip".to_string(),
            expiration_s: 0,
        }))
        .await
        .unwrap();

        // The spawned removal yields once before running. Yield enough times
        // to let it complete, regardless of scheduler ordering.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        assert_eq!(svc.uri_ref_count("s3://bucket/my_env.zip"), 0);
        assert!(svc.id_to_uris.lock().is_empty());
    }

    /// Zero-expiration pin of a `gcs://` URI must also trigger KV cleanup.
    #[tokio::test]
    async fn test_pin_zero_expiration_deletes_gcs_uri_from_kv() {
        use gcs_store::InMemoryStoreClient;
        let store = Arc::new(InMemoryStoreClient::new());
        let kv: Arc<dyn InternalKVInterface> =
            Arc::new(gcs_kv::StoreClientInternalKV::new(store));

        // Seed the KV with a fake gcs:// entry so we can detect deletion.
        kv.put("", "gcs://expired_now", "payload".to_string(), true).await;
        assert!(kv.get("", "gcs://expired_now").await.is_some());

        let svc = RuntimeEnvService::new_with_kv(kv.clone());
        svc.pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
            uri: "gcs://expired_now".to_string(),
            expiration_s: 0,
        }))
        .await
        .unwrap();

        // Let the spawned task run.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        assert_eq!(svc.uri_ref_count("gcs://expired_now"), 0);
        assert!(
            kv.get("", "gcs://expired_now").await.is_none(),
            "gcs:// URI should have been deleted from KV after zero-expiration pin"
        );
    }

    #[tokio::test]
    async fn test_pin_empty_uri_is_noop() {
        let svc = RuntimeEnvService::new();
        let reply = svc
            .pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest {
                uri: String::new(),
                expiration_s: 3600,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
        // No references should have been created.
        assert!(svc.uri_reference.lock().is_empty());
    }
}
