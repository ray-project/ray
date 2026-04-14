//! Internal KV store for Ray GCS.
//!
//! Provides:
//! - `InternalKVInterface` trait (maps C++ `InternalKVInterface`)
//! - `StoreClientInternalKV` (maps C++ `StoreClientInternalKV`)
//! - `GcsInternalKVManager` (gRPC handler wrapping InternalKVInterface)

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tonic::{Request, Response, Status};

use gcs_proto::ray::rpc::internal_kv_gcs_service_server::InternalKvGcsService;
use gcs_proto::ray::rpc::*;
use gcs_store::StoreClient;

// ─── InternalKVInterface ────────────────────────────────────────────────────

/// Abstract KV interface. Maps C++ `InternalKVInterface`.
#[async_trait]
pub trait InternalKVInterface: Send + Sync + 'static {
    async fn get(&self, ns: &str, key: &str) -> Option<String>;
    async fn multi_get(&self, ns: &str, keys: &[String]) -> HashMap<String, String>;
    async fn put(&self, ns: &str, key: &str, value: String, overwrite: bool) -> bool;
    async fn del(&self, ns: &str, key: &str, del_by_prefix: bool) -> i64;
    async fn exists(&self, ns: &str, key: &str) -> bool;
    async fn keys(&self, ns: &str, prefix: &str) -> Vec<String>;
}

// ─── StoreClientInternalKV ──────────────────────────────────────────────────

const NAMESPACE_PREFIX: &str = "@namespace_";
const NAMESPACE_SEP: &str = ":";
const TABLE_NAME: &str = "KV";

fn make_key(ns: &str, key: &str) -> String {
    if ns.is_empty() {
        key.to_string()
    } else {
        format!("{NAMESPACE_PREFIX}{ns}{NAMESPACE_SEP}{key}")
    }
}

fn extract_key(key: &str) -> String {
    if let Some(rest) = key.strip_prefix(NAMESPACE_PREFIX) {
        if let Some(pos) = rest.find(NAMESPACE_SEP) {
            return rest[pos + 1..].to_string();
        }
    }
    key.to_string()
}

/// KV backed by StoreClient with namespace prefixing.
/// Maps C++ `StoreClientInternalKV`.
pub struct StoreClientInternalKV {
    delegate: Arc<dyn StoreClient>,
}

impl StoreClientInternalKV {
    pub fn new(delegate: Arc<dyn StoreClient>) -> Self {
        Self { delegate }
    }
}

#[async_trait]
impl InternalKVInterface for StoreClientInternalKV {
    async fn get(&self, ns: &str, key: &str) -> Option<String> {
        self.delegate.get(TABLE_NAME, &make_key(ns, key)).await
    }

    async fn multi_get(&self, ns: &str, keys: &[String]) -> HashMap<String, String> {
        let prefixed: Vec<String> = keys.iter().map(|k| make_key(ns, k)).collect();
        let raw = self.delegate.multi_get(TABLE_NAME, &prefixed).await;
        raw.into_iter()
            .map(|(k, v)| (extract_key(&k), v))
            .collect()
    }

    async fn put(&self, ns: &str, key: &str, value: String, overwrite: bool) -> bool {
        self.delegate
            .put(TABLE_NAME, &make_key(ns, key), value, overwrite)
            .await
    }

    async fn del(&self, ns: &str, key: &str, del_by_prefix: bool) -> i64 {
        if !del_by_prefix {
            if self.delegate.delete(TABLE_NAME, &make_key(ns, key)).await {
                1
            } else {
                0
            }
        } else {
            let prefix = make_key(ns, key);
            let keys = self.delegate.get_keys(TABLE_NAME, &prefix).await;
            if keys.is_empty() {
                return 0;
            }
            self.delegate.batch_delete(TABLE_NAME, &keys).await
        }
    }

    async fn exists(&self, ns: &str, key: &str) -> bool {
        self.delegate.exists(TABLE_NAME, &make_key(ns, key)).await
    }

    async fn keys(&self, ns: &str, prefix: &str) -> Vec<String> {
        let raw_keys = self
            .delegate
            .get_keys(TABLE_NAME, &make_key(ns, prefix))
            .await;
        raw_keys.into_iter().map(|k| extract_key(&k)).collect()
    }
}

// ─── GcsInternalKVManager (gRPC handler) ────────────────────────────────────

fn ok_status() -> GcsStatus {
    GcsStatus {
        code: 0,
        message: String::new(),
    }
}

fn not_found_status(msg: &str) -> GcsStatus {
    GcsStatus {
        code: 17, // Ray StatusCode::NotFound (NOT gRPC status 5)
        message: msg.to_string(),
    }
}

fn bytes_to_string(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_string()
}

/// gRPC service handler for InternalKV. Maps C++ `GcsInternalKVManager`.
pub struct GcsInternalKVManager {
    kv: Arc<dyn InternalKVInterface>,
    raylet_config_list: String,
}

impl GcsInternalKVManager {
    pub fn new(kv: Arc<dyn InternalKVInterface>, raylet_config_list: String) -> Self {
        Self {
            kv,
            raylet_config_list,
        }
    }

    pub fn kv(&self) -> &dyn InternalKVInterface {
        self.kv.as_ref()
    }

    fn validate_key(key: &[u8]) -> Result<(), Status> {
        let key_str = bytes_to_string(key);
        if key_str.starts_with(NAMESPACE_PREFIX) {
            return Err(Status::invalid_argument(format!(
                "Key can't start with {NAMESPACE_PREFIX}"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl InternalKvGcsService for GcsInternalKVManager {
    async fn internal_kv_get(
        &self,
        request: Request<InternalKvGetRequest>,
    ) -> Result<Response<InternalKvGetReply>, Status> {
        let req = request.into_inner();
        Self::validate_key(&req.key)?;
        let ns = bytes_to_string(&req.namespace);
        let key = bytes_to_string(&req.key);

        let mut reply = InternalKvGetReply::default();
        match self.kv.get(&ns, &key).await {
            Some(val) => {
                reply.value = val.into_bytes();
                reply.status = Some(ok_status());
            }
            None => {
                reply.status = Some(not_found_status("Failed to find the key"));
            }
        }
        Ok(Response::new(reply))
    }

    async fn internal_kv_multi_get(
        &self,
        request: Request<InternalKvMultiGetRequest>,
    ) -> Result<Response<InternalKvMultiGetReply>, Status> {
        let req = request.into_inner();
        for key in &req.keys {
            Self::validate_key(key)?;
        }
        let ns = bytes_to_string(&req.namespace);
        let keys: Vec<String> = req.keys.iter().map(|k| bytes_to_string(k)).collect();
        let results = self.kv.multi_get(&ns, &keys).await;

        let mut reply = InternalKvMultiGetReply::default();
        for (k, v) in results {
            reply.results.push(MapFieldEntry {
                key: k.into_bytes(),
                value: v.into_bytes(),
            });
        }
        reply.status = Some(ok_status());
        Ok(Response::new(reply))
    }

    async fn internal_kv_put(
        &self,
        request: Request<InternalKvPutRequest>,
    ) -> Result<Response<InternalKvPutReply>, Status> {
        let req = request.into_inner();
        Self::validate_key(&req.key)?;
        let ns = bytes_to_string(&req.namespace);
        let key = bytes_to_string(&req.key);
        let value = bytes_to_string(&req.value);

        let added = self.kv.put(&ns, &key, value, req.overwrite).await;
        Ok(Response::new(InternalKvPutReply {
            added,
            status: Some(ok_status()),
        }))
    }

    async fn internal_kv_del(
        &self,
        request: Request<InternalKvDelRequest>,
    ) -> Result<Response<InternalKvDelReply>, Status> {
        let req = request.into_inner();
        Self::validate_key(&req.key)?;
        let ns = bytes_to_string(&req.namespace);
        let key = bytes_to_string(&req.key);

        let deleted = self.kv.del(&ns, &key, req.del_by_prefix).await;
        Ok(Response::new(InternalKvDelReply {
            deleted_num: deleted as i32,
            status: Some(ok_status()),
        }))
    }

    async fn internal_kv_exists(
        &self,
        request: Request<InternalKvExistsRequest>,
    ) -> Result<Response<InternalKvExistsReply>, Status> {
        let req = request.into_inner();
        Self::validate_key(&req.key)?;
        let ns = bytes_to_string(&req.namespace);
        let key = bytes_to_string(&req.key);

        let exists = self.kv.exists(&ns, &key).await;
        Ok(Response::new(InternalKvExistsReply {
            exists,
            status: Some(ok_status()),
        }))
    }

    async fn internal_kv_keys(
        &self,
        request: Request<InternalKvKeysRequest>,
    ) -> Result<Response<InternalKvKeysReply>, Status> {
        let req = request.into_inner();
        Self::validate_key(&req.prefix)?;
        let ns = bytes_to_string(&req.namespace);
        let prefix = bytes_to_string(&req.prefix);

        let keys = self.kv.keys(&ns, &prefix).await;
        Ok(Response::new(InternalKvKeysReply {
            results: keys.into_iter().map(|k| k.into_bytes()).collect(),
            status: Some(ok_status()),
        }))
    }

    async fn get_internal_config(
        &self,
        _request: Request<GetInternalConfigRequest>,
    ) -> Result<Response<GetInternalConfigReply>, Status> {
        Ok(Response::new(GetInternalConfigReply {
            config: self.raylet_config_list.clone(),
            status: Some(ok_status()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;

    fn make_kv() -> StoreClientInternalKV {
        StoreClientInternalKV::new(Arc::new(InMemoryStoreClient::new()))
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let kv = make_kv();
        kv.put("ns", "k1", "v1".into(), false).await;
        assert_eq!(kv.get("ns", "k1").await, Some("v1".to_string()));
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let kv = make_kv();
        assert_eq!(kv.get("ns", "nope").await, None);
    }

    #[tokio::test]
    async fn test_overwrite() {
        let kv = make_kv();
        kv.put("ns", "k", "v1".into(), false).await;
        kv.put("ns", "k", "v2".into(), true).await;
        assert_eq!(kv.get("ns", "k").await, Some("v2".to_string()));
    }

    #[tokio::test]
    async fn test_no_overwrite() {
        let kv = make_kv();
        kv.put("ns", "k", "v1".into(), false).await;
        kv.put("ns", "k", "v2".into(), false).await;
        assert_eq!(kv.get("ns", "k").await, Some("v1".to_string()));
    }

    #[tokio::test]
    async fn test_del() {
        let kv = make_kv();
        kv.put("ns", "k", "v".into(), false).await;
        assert_eq!(kv.del("ns", "k", false).await, 1);
        assert_eq!(kv.get("ns", "k").await, None);
    }

    #[tokio::test]
    async fn test_del_by_prefix() {
        let kv = make_kv();
        kv.put("ns", "a/1", "v1".into(), false).await;
        kv.put("ns", "a/2", "v2".into(), false).await;
        kv.put("ns", "b/1", "v3".into(), false).await;
        assert_eq!(kv.del("ns", "a/", true).await, 2);
        assert!(kv.exists("ns", "b/1").await);
        assert!(!kv.exists("ns", "a/1").await);
    }

    #[tokio::test]
    async fn test_exists() {
        let kv = make_kv();
        assert!(!kv.exists("ns", "k").await);
        kv.put("ns", "k", "v".into(), false).await;
        assert!(kv.exists("ns", "k").await);
    }

    #[tokio::test]
    async fn test_keys() {
        let kv = make_kv();
        kv.put("ns", "pre/a", "1".into(), false).await;
        kv.put("ns", "pre/b", "2".into(), false).await;
        kv.put("ns", "other", "3".into(), false).await;
        let mut keys = kv.keys("ns", "pre/").await;
        keys.sort();
        assert_eq!(keys, vec!["pre/a", "pre/b"]);
    }

    #[tokio::test]
    async fn test_multi_get() {
        let kv = make_kv();
        kv.put("ns", "a", "1".into(), false).await;
        kv.put("ns", "b", "2".into(), false).await;
        let result = kv
            .multi_get("ns", &["a".into(), "b".into(), "missing".into()])
            .await;
        assert_eq!(result.len(), 2);
        assert_eq!(result["a"], "1");
    }

    #[tokio::test]
    async fn test_namespace_isolation() {
        let kv = make_kv();
        kv.put("ns1", "k", "v1".into(), false).await;
        kv.put("ns2", "k", "v2".into(), false).await;
        assert_eq!(kv.get("ns1", "k").await, Some("v1".to_string()));
        assert_eq!(kv.get("ns2", "k").await, Some("v2".to_string()));
    }

    #[tokio::test]
    async fn test_validate_key_rejects_namespace_prefix() {
        assert!(GcsInternalKVManager::validate_key(b"@namespace_foo").is_err());
    }

    #[tokio::test]
    async fn test_validate_key_accepts_normal_key() {
        assert!(GcsInternalKVManager::validate_key(b"normal_key").is_ok());
    }

    // ─── gRPC handler tests ────────────────────────────────────────────────

    fn make_grpc_manager() -> GcsInternalKVManager {
        let store = Arc::new(InMemoryStoreClient::new());
        let kv = Arc::new(StoreClientInternalKV::new(store));
        GcsInternalKVManager::new(kv, "test_config".to_string())
    }

    #[tokio::test]
    async fn test_grpc_internal_kv_put_and_get() {
        let mgr = make_grpc_manager();

        // Put a key.
        let put_reply = mgr
            .internal_kv_put(Request::new(InternalKvPutRequest {
                namespace: b"ns".to_vec(),
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
                overwrite: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(put_reply.added);
        assert_eq!(put_reply.status.unwrap().code, 0);

        // Get the key.
        let get_reply = mgr
            .internal_kv_get(Request::new(InternalKvGetRequest {
                namespace: b"ns".to_vec(),
                key: b"hello".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(get_reply.value, b"world");
        assert_eq!(get_reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_grpc_internal_kv_del() {
        let mgr = make_grpc_manager();

        // Put then delete.
        mgr.internal_kv_put(Request::new(InternalKvPutRequest {
            namespace: b"ns".to_vec(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            overwrite: false,
        }))
        .await
        .unwrap();

        let del_reply = mgr
            .internal_kv_del(Request::new(InternalKvDelRequest {
                namespace: b"ns".to_vec(),
                key: b"k1".to_vec(),
                del_by_prefix: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(del_reply.deleted_num, 1);

        // Verify deleted (get should return not found).
        let get_reply = mgr
            .internal_kv_get(Request::new(InternalKvGetRequest {
                namespace: b"ns".to_vec(),
                key: b"k1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(get_reply.status.unwrap().code, 17); // NotFound
    }

    #[tokio::test]
    async fn test_grpc_internal_kv_exists() {
        let mgr = make_grpc_manager();

        // Put a key.
        mgr.internal_kv_put(Request::new(InternalKvPutRequest {
            namespace: b"ns".to_vec(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            overwrite: false,
        }))
        .await
        .unwrap();

        // Check exists=true.
        let exists_reply = mgr
            .internal_kv_exists(Request::new(InternalKvExistsRequest {
                namespace: b"ns".to_vec(),
                key: b"k1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(exists_reply.exists);

        // Delete it.
        mgr.internal_kv_del(Request::new(InternalKvDelRequest {
            namespace: b"ns".to_vec(),
            key: b"k1".to_vec(),
            del_by_prefix: false,
        }))
        .await
        .unwrap();

        // Check exists=false.
        let exists_reply = mgr
            .internal_kv_exists(Request::new(InternalKvExistsRequest {
                namespace: b"ns".to_vec(),
                key: b"k1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!exists_reply.exists);
    }

    #[tokio::test]
    async fn test_grpc_internal_kv_keys() {
        let mgr = make_grpc_manager();

        // Put 3 keys with a common prefix.
        for k in &["pre/a", "pre/b", "pre/c"] {
            mgr.internal_kv_put(Request::new(InternalKvPutRequest {
                namespace: b"ns".to_vec(),
                key: k.as_bytes().to_vec(),
                value: b"val".to_vec(),
                overwrite: false,
            }))
            .await
            .unwrap();
        }

        let keys_reply = mgr
            .internal_kv_keys(Request::new(InternalKvKeysRequest {
                namespace: b"ns".to_vec(),
                prefix: b"pre/".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(keys_reply.results.len(), 3);
        assert_eq!(keys_reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_grpc_internal_kv_multi_get() {
        let mgr = make_grpc_manager();

        // Put 2 keys.
        for (k, v) in &[("a", "1"), ("b", "2")] {
            mgr.internal_kv_put(Request::new(InternalKvPutRequest {
                namespace: b"ns".to_vec(),
                key: k.as_bytes().to_vec(),
                value: v.as_bytes().to_vec(),
                overwrite: false,
            }))
            .await
            .unwrap();
        }

        // Multi-get 3 keys (1 missing).
        let reply = mgr
            .internal_kv_multi_get(Request::new(InternalKvMultiGetRequest {
                namespace: b"ns".to_vec(),
                keys: vec![b"a".to_vec(), b"b".to_vec(), b"missing".to_vec()],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.results.len(), 2);
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_grpc_get_internal_config() {
        let store = Arc::new(InMemoryStoreClient::new());
        let kv = Arc::new(StoreClientInternalKV::new(store));
        let mgr = GcsInternalKVManager::new(kv, "my_raylet_config".to_string());

        let reply = mgr
            .get_internal_config(Request::new(GetInternalConfigRequest {}))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.config, "my_raylet_config");
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_grpc_validate_key_rejection() {
        let mgr = make_grpc_manager();

        // Try to put with a key starting with "@namespace_".
        let result = mgr
            .internal_kv_put(Request::new(InternalKvPutRequest {
                namespace: b"ns".to_vec(),
                key: b"@namespace_bad".to_vec(),
                value: b"val".to_vec(),
                overwrite: false,
            }))
            .await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_kv_accessor() {
        let mgr = make_grpc_manager();
        // Put via the trait interface directly.
        mgr.kv().put("ns", "k", "v".into(), false).await;
        // Get via the trait interface.
        let val = mgr.kv().get("ns", "k").await;
        assert_eq!(val, Some("v".to_string()));
    }
}
