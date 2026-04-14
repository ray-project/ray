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
pub struct RuntimeEnvService {
    /// URI -> reference count. A URI with count > 0 is "pinned".
    uri_reference: Arc<Mutex<HashMap<String, i64>>>,
    /// Temporary reference ID -> list of URIs it pins.
    id_to_uris: Arc<Mutex<HashMap<String, Vec<String>>>>,
    /// Monotonic counter for generating unique temporary reference IDs.
    next_ref_id: AtomicU64,
}

impl RuntimeEnvService {
    pub fn new() -> Self {
        Self {
            uri_reference: Arc::new(Mutex::new(HashMap::new())),
            id_to_uris: Arc::new(Mutex::new(HashMap::new())),
            next_ref_id: AtomicU64::new(0),
        }
    }

    /// Generate a unique hex reference ID (matching C++ UniqueID::FromRandom().Hex()).
    fn generate_ref_id(&self) -> String {
        let id = self.next_ref_id.fetch_add(1, Ordering::SeqCst);
        format!("ref_{:016x}", id)
    }

    /// Add a URI reference under the given ID. Increments the URI's reference count.
    /// Matches C++ `RuntimeEnvManager::AddURIReference`.
    fn add_uri_reference(&self, ref_id: &str, uri: &str) {
        let mut refs = self.uri_reference.lock();
        *refs.entry(uri.to_string()).or_insert(0) += 1;

        let mut id_map = self.id_to_uris.lock();
        id_map
            .entry(ref_id.to_string())
            .or_default()
            .push(uri.to_string());

        debug!(ref_id, uri, "Added URI reference");
    }

    /// Remove a URI reference by ID. Decrements reference counts for all URIs
    /// associated with the ID. Returns URIs whose count dropped to 0.
    /// Matches C++ `RuntimeEnvManager::RemoveURIReference`.
    fn remove_uri_reference(&self, ref_id: &str) -> Vec<String> {
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
        if expiration_s > 0 {
            // We need a reference to self for the spawned task.
            // Since RuntimeEnvService is behind Arc in the server, we use
            // a clone of the internal state via the ref_id.
            let uri_ref = self.uri_reference.clone();
            let id_map = self.id_to_uris.clone();
            let ref_id_owned = ref_id.clone();

            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(expiration_s as u64)).await;

                // Remove the temporary reference (decrement count).
                let mut id_map_guard = id_map.lock();
                let uris = match id_map_guard.remove(&ref_id_owned) {
                    Some(uris) => uris,
                    None => return,
                };
                drop(id_map_guard);

                let mut refs = uri_ref.lock();
                for uri in &uris {
                    if let Some(count) = refs.get_mut(uri) {
                        *count -= 1;
                        if *count <= 0 {
                            refs.remove(uri);
                            debug!(uri, "URI pin expired, reference count reached 0");
                        }
                    }
                }

                debug!(ref_id = ref_id_owned.as_str(), "URI pin expired");
            });
        }

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

        let released = svc.remove_uri_reference("ref_1");
        assert!(released.is_empty()); // Still has ref_2
        assert_eq!(svc.uri_ref_count("gcs://test"), 1);

        let released = svc.remove_uri_reference("ref_2");
        assert_eq!(released, vec!["gcs://test"]); // Last reference removed
        assert_eq!(svc.uri_ref_count("gcs://test"), 0);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_reference() {
        let svc = RuntimeEnvService::new();
        let released = svc.remove_uri_reference("no_such_ref");
        assert!(released.is_empty());
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
