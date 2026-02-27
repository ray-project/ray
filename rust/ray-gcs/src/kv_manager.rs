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
    pub fn validate_key(key: &str) -> Result<(), String> {
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
        namespace: &str,
        key: &str,
    ) -> Result<Option<String>, tonic::Status> {
        Self::validate_key(key).map_err(tonic::Status::invalid_argument)?;
        self.kv
            .get(namespace, key)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn handle_multi_get(
        &self,
        namespace: &str,
        keys: &[String],
    ) -> Result<std::collections::HashMap<String, String>, tonic::Status> {
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
        namespace: &str,
        key: &str,
        value: String,
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
        namespace: &str,
        key: &str,
        del_by_prefix: bool,
    ) -> Result<i64, tonic::Status> {
        Self::validate_key(key).map_err(tonic::Status::invalid_argument)?;
        self.kv
            .del(namespace, key, del_by_prefix)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn handle_exists(&self, namespace: &str, key: &str) -> Result<bool, tonic::Status> {
        Self::validate_key(key).map_err(tonic::Status::invalid_argument)?;
        self.kv
            .exists(namespace, key)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn handle_keys(
        &self,
        namespace: &str,
        prefix: &str,
    ) -> Result<Vec<String>, tonic::Status> {
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
            .handle_put("ns", "key1", "val1".into(), true)
            .await
            .unwrap();
        assert!(added);

        // Get
        let val = mgr.handle_get("ns", "key1").await.unwrap();
        assert_eq!(val, Some("val1".to_string()));

        // Exists
        assert!(mgr.handle_exists("ns", "key1").await.unwrap());

        // Del
        let count = mgr.handle_del("ns", "key1", false).await.unwrap();
        assert_eq!(count, 1);
        assert!(!mgr.handle_exists("ns", "key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_kv_manager_validate_key() {
        assert!(GcsInternalKVManager::validate_key("valid").is_ok());
        assert!(GcsInternalKVManager::validate_key("").is_err());
        let long_key = "x".repeat(MAX_KEY_LENGTH + 1);
        assert!(GcsInternalKVManager::validate_key(&long_key).is_err());
    }
}
