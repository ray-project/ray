// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Internal KV Manager — handles InternalKVGcsService RPCs.
//!
//! Replaces `src/ray/gcs/gcs_kv_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::store_client::InternalKVInterface;

/// Maximum key length for internal KV operations.
const MAX_KEY_LENGTH: usize = 8192;

/// The GCS KV manager wraps an InternalKVInterface and provides
/// RPC handler methods for the InternalKVGcsService.
pub struct GcsInternalKVManager {
    kv: Arc<dyn InternalKVInterface>,
    /// The raylet config list (returned by GetInternalConfig RPC).
    raylet_config_list: String,
}

impl GcsInternalKVManager {
    pub fn new(kv: Arc<dyn InternalKVInterface>, raylet_config_list: String) -> Self {
        Self {
            kv,
            raylet_config_list,
        }
    }

    pub fn kv(&self) -> &Arc<dyn InternalKVInterface> {
        &self.kv
    }

    pub fn raylet_config_list(&self) -> &str {
        &self.raylet_config_list
    }

    /// Validate a key — must not be empty and within length limits.
    pub fn validate_key(key: &[u8]) -> Result<(), String> {
        if key.is_empty() {
            return Err("key must not be empty".to_string());
        }
        if key.len() > MAX_KEY_LENGTH {
            return Err(format!(
                "key length {} exceeds maximum {}",
                key.len(),
                MAX_KEY_LENGTH
            ));
        }
        Ok(())
    }

    // ── RPC handlers ──────────────────────────────────────────────────

    pub async fn handle_get(
        &self,
        namespace: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, tonic::Status> {
        Self::validate_key(key).map_err(tonic::Status::invalid_argument)?;
        self.kv
            .get(namespace, key)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn handle_multi_get(
        &self,
        namespace: &[u8],
        keys: &[Vec<u8>],
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>, tonic::Status> {
        for key in keys {
            Self::validate_key(key).map_err(tonic::Status::invalid_argument)?;
        }
        self.kv
            .multi_get(namespace, keys)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn handle_put(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: Vec<u8>,
        overwrite: bool,
    ) -> Result<bool, tonic::Status> {
        Self::validate_key(key).map_err(tonic::Status::invalid_argument)?;
        self.kv
            .put(namespace, key, value, overwrite)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn handle_del(
        &self,
        namespace: &[u8],
        key: &[u8],
        del_by_prefix: bool,
    ) -> Result<i64, tonic::Status> {
        Self::validate_key(key).map_err(tonic::Status::invalid_argument)?;
        self.kv
            .del(namespace, key, del_by_prefix)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn handle_exists(&self, namespace: &[u8], key: &[u8]) -> Result<bool, tonic::Status> {
        Self::validate_key(key).map_err(tonic::Status::invalid_argument)?;
        self.kv
            .exists(namespace, key)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn handle_keys(
        &self,
        namespace: &[u8],
        prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, tonic::Status> {
        self.kv
            .keys(namespace, prefix)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_client::InMemoryInternalKV;

    #[tokio::test]
    async fn test_kv_manager_crud() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "config".to_string());

        // Put
        let added = mgr
            .handle_put(b"ns", b"key1", b"val1".to_vec(), true)
            .await
            .unwrap();
        assert!(added);

        // Get
        let val = mgr.handle_get(b"ns", b"key1").await.unwrap();
        assert_eq!(val, Some(b"val1".to_vec()));

        // Exists
        assert!(mgr.handle_exists(b"ns", b"key1").await.unwrap());

        // Del
        let count = mgr.handle_del(b"ns", b"key1", false).await.unwrap();
        assert_eq!(count, 1);
        assert!(!mgr.handle_exists(b"ns", b"key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_kv_manager_validate_key() {
        assert!(GcsInternalKVManager::validate_key(b"valid").is_ok());
        assert!(GcsInternalKVManager::validate_key(b"").is_err());
        let long_key = vec![b'x'; MAX_KEY_LENGTH + 1];
        assert!(GcsInternalKVManager::validate_key(&long_key).is_err());
    }

    #[tokio::test]
    async fn test_kv_manager_multi_get() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "config".to_string());

        mgr.handle_put(b"ns", b"a", b"1".to_vec(), true)
            .await
            .unwrap();
        mgr.handle_put(b"ns", b"b", b"2".to_vec(), true)
            .await
            .unwrap();

        let result = mgr
            .handle_multi_get(b"ns", &[b"a".to_vec(), b"b".to_vec(), b"missing".to_vec()])
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[&b"a".to_vec()], b"1");
        assert_eq!(result[&b"b".to_vec()], b"2");
    }

    #[tokio::test]
    async fn test_kv_manager_keys() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "config".to_string());

        mgr.handle_put(b"ns", b"prefix/a", b"1".to_vec(), true)
            .await
            .unwrap();
        mgr.handle_put(b"ns", b"prefix/b", b"2".to_vec(), true)
            .await
            .unwrap();
        mgr.handle_put(b"ns", b"other", b"3".to_vec(), true)
            .await
            .unwrap();

        let keys = mgr.handle_keys(b"ns", b"prefix/").await.unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[tokio::test]
    async fn test_kv_manager_del_by_prefix() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "config".to_string());

        mgr.handle_put(b"ns", b"foo/1", b"a".to_vec(), true)
            .await
            .unwrap();
        mgr.handle_put(b"ns", b"foo/2", b"b".to_vec(), true)
            .await
            .unwrap();
        mgr.handle_put(b"ns", b"bar/1", b"c".to_vec(), true)
            .await
            .unwrap();

        let count = mgr.handle_del(b"ns", b"foo/", true).await.unwrap();
        assert_eq!(count, 2);
        assert!(!mgr.handle_exists(b"ns", b"foo/1").await.unwrap());
        assert!(mgr.handle_exists(b"ns", b"bar/1").await.unwrap());
    }

    #[tokio::test]
    async fn test_kv_manager_overwrite_behavior() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "config".to_string());

        // First put
        let added = mgr
            .handle_put(b"ns", b"key", b"v1".to_vec(), true)
            .await
            .unwrap();
        assert!(added);

        // Overwrite
        let added = mgr
            .handle_put(b"ns", b"key", b"v2".to_vec(), true)
            .await
            .unwrap();
        assert!(!added); // already existed

        let val = mgr.handle_get(b"ns", b"key").await.unwrap();
        assert_eq!(val, Some(b"v2".to_vec()));

        // No overwrite
        let added = mgr
            .handle_put(b"ns", b"key", b"v3".to_vec(), false)
            .await
            .unwrap();
        assert!(!added); // not added because already exists

        let val = mgr.handle_get(b"ns", b"key").await.unwrap();
        assert_eq!(val, Some(b"v2".to_vec())); // unchanged
    }

    #[tokio::test]
    async fn test_kv_manager_raylet_config_list() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "my_base64_config".to_string());
        assert_eq!(mgr.raylet_config_list(), "my_base64_config");
    }

    #[tokio::test]
    async fn test_kv_manager_empty_key_rejected() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "config".to_string());

        let result = mgr.handle_get(b"ns", b"").await;
        assert!(result.is_err());

        let result = mgr.handle_put(b"ns", b"", b"v".to_vec(), true).await;
        assert!(result.is_err());

        let result = mgr.handle_del(b"ns", b"", false).await;
        assert!(result.is_err());

        let result = mgr.handle_exists(b"ns", b"").await;
        assert!(result.is_err());
    }

    // ---- Ported from gcs_kv_manager_test.cc (memory parameterization) ----

    /// Full CRUD cycle matching TestInternalKV from gcs_kv_manager_test.cc
    #[tokio::test]
    async fn test_internal_kv_full_cycle() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "cfg".to_string());

        // Get non-existent
        assert!(mgr.handle_get(b"N1", b"A").await.unwrap().is_none());

        // Put new key (no overwrite)
        let added = mgr
            .handle_put(b"N1", b"A", b"B".to_vec(), false)
            .await
            .unwrap();
        assert!(added); // was new

        // Put existing key (no overwrite) — should fail to overwrite
        let added = mgr
            .handle_put(b"N1", b"A", b"C".to_vec(), false)
            .await
            .unwrap();
        assert!(!added); // not added because exists

        // Value should still be B
        assert_eq!(
            mgr.handle_get(b"N1", b"A").await.unwrap(),
            Some(b"B".to_vec())
        );

        // Put with overwrite
        let added = mgr
            .handle_put(b"N1", b"A", b"C".to_vec(), true)
            .await
            .unwrap();
        assert!(!added); // already existed (overwrite returns false for "was new")
        assert_eq!(
            mgr.handle_get(b"N1", b"A").await.unwrap(),
            Some(b"C".to_vec())
        );

        // Add more keys with prefix
        mgr.handle_put(b"N1", b"A_1", b"B".to_vec(), false)
            .await
            .unwrap();
        mgr.handle_put(b"N1", b"A_2", b"C".to_vec(), false)
            .await
            .unwrap();
        mgr.handle_put(b"N1", b"A_3", b"C".to_vec(), false)
            .await
            .unwrap();

        // Keys with prefix "A_"
        let keys = mgr.handle_keys(b"N1", b"A_").await.unwrap();
        assert_eq!(keys.len(), 3);

        // Different namespace should not see the key
        assert!(mgr.handle_get(b"N2", b"A_1").await.unwrap().is_none());
        // Same namespace should
        assert!(mgr.handle_get(b"N1", b"A_1").await.unwrap().is_some());

        // Multi-get
        let result = mgr
            .handle_multi_get(b"N1", &[b"A_1".to_vec(), b"A_2".to_vec(), b"A_3".to_vec()])
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[&b"A_1".to_vec()], b"B");
        assert_eq!(result[&b"A_2".to_vec()], b"C");
        assert_eq!(result[&b"A_3".to_vec()], b"C");

        // Multi-get with empty keys
        let result = mgr.handle_multi_get(b"N1", &[]).await.unwrap();
        assert!(result.is_empty());

        // Multi-get with non-existent keys
        let result = mgr
            .handle_multi_get(b"N1", &[b"A_4".to_vec(), b"A_5".to_vec()])
            .await
            .unwrap();
        assert!(result.is_empty());

        // Delete by prefix
        let count = mgr.handle_del(b"N1", b"A_", true).await.unwrap();
        assert_eq!(count, 3);

        // Delete from non-existent namespace
        let count = mgr.handle_del(b"NX", b"A_", true).await.unwrap();
        assert_eq!(count, 0);

        // Verify keys are deleted
        assert!(mgr.handle_get(b"N1", b"A_1").await.unwrap().is_none());

        let result = mgr
            .handle_multi_get(b"N1", &[b"A_1".to_vec(), b"A_2".to_vec(), b"A_3".to_vec()])
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_namespace_isolation() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "cfg".to_string());

        mgr.handle_put(b"ns1", b"key", b"val1".to_vec(), true)
            .await
            .unwrap();
        mgr.handle_put(b"ns2", b"key", b"val2".to_vec(), true)
            .await
            .unwrap();

        assert_eq!(
            mgr.handle_get(b"ns1", b"key").await.unwrap(),
            Some(b"val1".to_vec())
        );
        assert_eq!(
            mgr.handle_get(b"ns2", b"key").await.unwrap(),
            Some(b"val2".to_vec())
        );

        // Delete from ns1 should not affect ns2
        mgr.handle_del(b"ns1", b"key", false).await.unwrap();
        assert!(mgr.handle_get(b"ns1", b"key").await.unwrap().is_none());
        assert!(mgr.handle_get(b"ns2", b"key").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_key_at_max_length() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "cfg".to_string());

        // Key at exactly max length should be valid
        let max_key = vec![b'x'; MAX_KEY_LENGTH];
        let result = mgr.handle_put(b"ns", &max_key, b"v".to_vec(), true).await;
        assert!(result.is_ok());

        // Key one byte over should fail
        let over_key = vec![b'x'; MAX_KEY_LENGTH + 1];
        let result = mgr.handle_put(b"ns", &over_key, b"v".to_vec(), true).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_exists_after_del() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "cfg".to_string());

        mgr.handle_put(b"ns", b"k", b"v".to_vec(), true)
            .await
            .unwrap();
        assert!(mgr.handle_exists(b"ns", b"k").await.unwrap());

        mgr.handle_del(b"ns", b"k", false).await.unwrap();
        assert!(!mgr.handle_exists(b"ns", b"k").await.unwrap());
    }

    #[tokio::test]
    async fn test_del_nonexistent_key_returns_zero() {
        let kv = Arc::new(InMemoryInternalKV::new());
        let mgr = GcsInternalKVManager::new(kv, "cfg".to_string());

        let count = mgr.handle_del(b"ns", b"nope", false).await.unwrap();
        assert_eq!(count, 0);
    }
}
