// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Store client abstraction — persistence layer for GCS data.
//!
//! Replaces `src/ray/gcs/store_client/`.
//!
//! Two backends:
//! - `InMemoryStoreClient` — for single-node / non-HA setups
//! - `RedisStoreClient` — for HA GCS with Redis persistence

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;
use thiserror::Error;

/// Errors from store operations.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("key not found")]
    NotFound,
    #[error("redis error: {0}")]
    Redis(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type StoreResult<T> = Result<T, StoreError>;

/// The store client interface — async KV operations organized by table.
///
/// Tables are logical namespaces (e.g., "Job", "Node", "Actor").
/// Each table is an independent key-value map.
#[async_trait::async_trait]
pub trait StoreClient: Send + Sync {
    /// Put a key-value pair. Returns true if the key already existed (overwritten).
    async fn put(
        &self,
        table: &str,
        key: &str,
        data: Vec<u8>,
        overwrite: bool,
    ) -> StoreResult<bool>;

    /// Get a value by key.
    async fn get(&self, table: &str, key: &str) -> StoreResult<Option<Vec<u8>>>;

    /// Get multiple values by keys.
    async fn multi_get(
        &self,
        table: &str,
        keys: &[String],
    ) -> StoreResult<HashMap<String, Vec<u8>>>;

    /// Get all key-value pairs in a table.
    async fn get_all(&self, table: &str) -> StoreResult<HashMap<String, Vec<u8>>>;

    /// Delete a key. Returns true if the key existed.
    async fn delete(&self, table: &str, key: &str) -> StoreResult<bool>;

    /// Delete multiple keys. Returns the count of deleted keys.
    async fn batch_delete(&self, table: &str, keys: &[String]) -> StoreResult<i64>;

    /// Get the next auto-incrementing job ID.
    async fn get_next_job_id(&self) -> StoreResult<i32>;

    /// Get all keys in a table matching a prefix.
    async fn get_keys(&self, table: &str, prefix: &str) -> StoreResult<Vec<String>>;

    /// Check if a key exists.
    async fn exists(&self, table: &str, key: &str) -> StoreResult<bool>;
}

/// Internal KV interface — namespaced key-value store for the InternalKV service.
///
/// This is separate from StoreClient because it uses a string namespace
/// rather than table names, and supports prefix deletion.
#[async_trait::async_trait]
pub trait InternalKVInterface: Send + Sync {
    async fn get(&self, ns: &[u8], key: &[u8]) -> StoreResult<Option<Vec<u8>>>;

    async fn multi_get(
        &self,
        ns: &[u8],
        keys: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, Vec<u8>>>;

    async fn put(
        &self,
        ns: &[u8],
        key: &[u8],
        value: Vec<u8>,
        overwrite: bool,
    ) -> StoreResult<bool>;

    /// Delete a key or all keys with a given prefix.
    async fn del(&self, ns: &[u8], key: &[u8], del_by_prefix: bool) -> StoreResult<i64>;

    async fn exists(&self, ns: &[u8], key: &[u8]) -> StoreResult<bool>;

    async fn keys(&self, ns: &[u8], prefix: &[u8]) -> StoreResult<Vec<Vec<u8>>>;
}

// ─── In-Memory Store Client ────────────────────────────────────────────────

/// Thread-safe in-memory store client for non-HA deployments.
pub struct InMemoryStoreClient {
    /// Table name → (key → value).
    tables: DashMap<String, DashMap<String, Vec<u8>>>,
    /// Auto-incrementing job ID counter.
    next_job_id: AtomicI64,
}

impl InMemoryStoreClient {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            next_job_id: AtomicI64::new(1),
        }
    }

    #[allow(dead_code)]
    fn get_table(
        &self,
        table: &str,
    ) -> dashmap::mapref::one::Ref<'_, String, DashMap<String, Vec<u8>>> {
        self.tables
            .entry(table.to_string())
            .or_default()
            .downgrade()
    }
}

impl Default for InMemoryStoreClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StoreClient for InMemoryStoreClient {
    async fn put(
        &self,
        table: &str,
        key: &str,
        data: Vec<u8>,
        overwrite: bool,
    ) -> StoreResult<bool> {
        let tbl = self.tables.entry(table.to_string()).or_default();
        let existed = tbl.contains_key(key);
        if existed && !overwrite {
            return Ok(true);
        }
        tbl.insert(key.to_string(), data);
        Ok(existed)
    }

    async fn get(&self, table: &str, key: &str) -> StoreResult<Option<Vec<u8>>> {
        if let Some(tbl) = self.tables.get(table) {
            Ok(tbl.get(key).map(|v| v.value().clone()))
        } else {
            Ok(None)
        }
    }

    async fn multi_get(
        &self,
        table: &str,
        keys: &[String],
    ) -> StoreResult<HashMap<String, Vec<u8>>> {
        let mut result = HashMap::new();
        if let Some(tbl) = self.tables.get(table) {
            for key in keys {
                if let Some(v) = tbl.get(key.as_str()) {
                    result.insert(key.clone(), v.value().clone());
                }
            }
        }
        Ok(result)
    }

    async fn get_all(&self, table: &str) -> StoreResult<HashMap<String, Vec<u8>>> {
        if let Some(tbl) = self.tables.get(table) {
            Ok(tbl
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect())
        } else {
            Ok(HashMap::new())
        }
    }

    async fn delete(&self, table: &str, key: &str) -> StoreResult<bool> {
        if let Some(tbl) = self.tables.get(table) {
            Ok(tbl.remove(key).is_some())
        } else {
            Ok(false)
        }
    }

    async fn batch_delete(&self, table: &str, keys: &[String]) -> StoreResult<i64> {
        let mut count = 0i64;
        if let Some(tbl) = self.tables.get(table) {
            for key in keys {
                if tbl.remove(key.as_str()).is_some() {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    async fn get_next_job_id(&self) -> StoreResult<i32> {
        Ok(self.next_job_id.fetch_add(1, Ordering::SeqCst) as i32)
    }

    async fn get_keys(&self, table: &str, prefix: &str) -> StoreResult<Vec<String>> {
        if let Some(tbl) = self.tables.get(table) {
            Ok(tbl
                .iter()
                .filter(|e| e.key().starts_with(prefix))
                .map(|e| e.key().clone())
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    async fn exists(&self, table: &str, key: &str) -> StoreResult<bool> {
        if let Some(tbl) = self.tables.get(table) {
            Ok(tbl.contains_key(key))
        } else {
            Ok(false)
        }
    }
}

// ─── In-Memory Internal KV ────────────────────────────────────────────────

/// In-memory implementation of the InternalKV interface.
pub struct InMemoryInternalKV {
    /// Namespace → (key → value). All stored as raw bytes.
    data: DashMap<Vec<u8>, DashMap<Vec<u8>, Vec<u8>>>,
}

impl InMemoryInternalKV {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
}

impl Default for InMemoryInternalKV {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl InternalKVInterface for InMemoryInternalKV {
    async fn get(&self, ns: &[u8], key: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        if let Some(ns_map) = self.data.get(ns) {
            Ok(ns_map.get(key).map(|v| v.value().clone()))
        } else {
            Ok(None)
        }
    }

    async fn multi_get(
        &self,
        ns: &[u8],
        keys: &[Vec<u8>],
    ) -> StoreResult<HashMap<Vec<u8>, Vec<u8>>> {
        let mut result = HashMap::new();
        if let Some(ns_map) = self.data.get(ns) {
            for key in keys {
                if let Some(v) = ns_map.get(key.as_slice()) {
                    result.insert(key.clone(), v.value().clone());
                }
            }
        }
        Ok(result)
    }

    async fn put(
        &self,
        ns: &[u8],
        key: &[u8],
        value: Vec<u8>,
        overwrite: bool,
    ) -> StoreResult<bool> {
        let ns_map = self.data.entry(ns.to_vec()).or_default();
        let existed = ns_map.contains_key(key);
        if existed && !overwrite {
            return Ok(false);
        }
        ns_map.insert(key.to_vec(), value);
        Ok(!existed)
    }

    async fn del(&self, ns: &[u8], key: &[u8], del_by_prefix: bool) -> StoreResult<i64> {
        if let Some(ns_map) = self.data.get(ns) {
            if del_by_prefix {
                let keys_to_delete: Vec<Vec<u8>> = ns_map
                    .iter()
                    .filter(|e| e.key().starts_with(key))
                    .map(|e| e.key().clone())
                    .collect();
                let count = keys_to_delete.len() as i64;
                for k in keys_to_delete {
                    ns_map.remove(&k);
                }
                Ok(count)
            } else {
                Ok(if ns_map.remove(key).is_some() { 1 } else { 0 })
            }
        } else {
            Ok(0)
        }
    }

    async fn exists(&self, ns: &[u8], key: &[u8]) -> StoreResult<bool> {
        if let Some(ns_map) = self.data.get(ns) {
            Ok(ns_map.contains_key(key))
        } else {
            Ok(false)
        }
    }

    async fn keys(&self, ns: &[u8], prefix: &[u8]) -> StoreResult<Vec<Vec<u8>>> {
        if let Some(ns_map) = self.data.get(ns) {
            Ok(ns_map
                .iter()
                .filter(|e| e.key().starts_with(prefix))
                .map(|e| e.key().clone())
                .collect())
        } else {
            Ok(Vec::new())
        }
    }
}

// ─── Redis Store Client ──────────────────────────────────────────────────

/// Redis-backed store client for HA GCS deployments.
pub struct RedisStoreClient {
    client: redis::Client,
    namespace: String,
}

impl RedisStoreClient {
    pub fn new(redis_url: &str, namespace: String) -> StoreResult<Self> {
        let client =
            redis::Client::open(redis_url).map_err(|e| StoreError::Redis(e.to_string()))?;
        Ok(Self { client, namespace })
    }

    fn table_key(&self, table: &str) -> String {
        format!("RAY{}@{}", self.namespace, table)
    }
}

#[async_trait::async_trait]
impl StoreClient for RedisStoreClient {
    async fn put(
        &self,
        table: &str,
        key: &str,
        data: Vec<u8>,
        overwrite: bool,
    ) -> StoreResult<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        let table_key = self.table_key(table);
        if overwrite {
            let existed: bool = redis::cmd("HEXISTS")
                .arg(&table_key)
                .arg(key)
                .query_async(&mut conn)
                .await
                .map_err(|e| StoreError::Redis(e.to_string()))?;
            let _: () = redis::cmd("HSET")
                .arg(&table_key)
                .arg(key)
                .arg(data)
                .query_async(&mut conn)
                .await
                .map_err(|e| StoreError::Redis(e.to_string()))?;
            Ok(existed)
        } else {
            let added: bool = redis::cmd("HSETNX")
                .arg(&table_key)
                .arg(key)
                .arg(data)
                .query_async(&mut conn)
                .await
                .map_err(|e| StoreError::Redis(e.to_string()))?;
            Ok(!added) // HSETNX returns 1 if set (new), so !added = existed
        }
    }

    async fn get(&self, table: &str, key: &str) -> StoreResult<Option<Vec<u8>>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        let result: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(self.table_key(table))
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        Ok(result)
    }

    async fn multi_get(
        &self,
        table: &str,
        keys: &[String],
    ) -> StoreResult<HashMap<String, Vec<u8>>> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        let table_key = self.table_key(table);
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&table_key);
        for key in keys {
            cmd.arg(key.as_str());
        }
        let values: Vec<Option<Vec<u8>>> = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        let mut result = HashMap::new();
        for (key, val) in keys.iter().zip(values.into_iter()) {
            if let Some(v) = val {
                result.insert(key.clone(), v);
            }
        }
        Ok(result)
    }

    async fn get_all(&self, table: &str) -> StoreResult<HashMap<String, Vec<u8>>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        let result: HashMap<String, Vec<u8>> = redis::cmd("HGETALL")
            .arg(self.table_key(table))
            .query_async(&mut conn)
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        Ok(result)
    }

    async fn delete(&self, table: &str, key: &str) -> StoreResult<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        let removed: i64 = redis::cmd("HDEL")
            .arg(self.table_key(table))
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        Ok(removed > 0)
    }

    async fn batch_delete(&self, table: &str, keys: &[String]) -> StoreResult<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        let mut cmd = redis::cmd("HDEL");
        cmd.arg(self.table_key(table));
        for key in keys {
            cmd.arg(key.as_str());
        }
        let removed: i64 = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        Ok(removed)
    }

    async fn get_next_job_id(&self) -> StoreResult<i32> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        let id: i64 = redis::cmd("INCR")
            .arg(format!("RAY{}@NextJobID", self.namespace))
            .query_async(&mut conn)
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        Ok(id as i32)
    }

    async fn get_keys(&self, table: &str, prefix: &str) -> StoreResult<Vec<String>> {
        // Use HSCAN to iterate keys matching prefix
        let all = self.get_all(table).await?;
        Ok(all.into_keys().filter(|k| k.starts_with(prefix)).collect())
    }

    async fn exists(&self, table: &str, key: &str) -> StoreResult<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        let exists: bool = redis::cmd("HEXISTS")
            .arg(self.table_key(table))
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| StoreError::Redis(e.to_string()))?;
        Ok(exists)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_store_put_get() {
        let store = InMemoryStoreClient::new();
        let existed = store
            .put("TestTable", "key1", b"value1".to_vec(), true)
            .await
            .unwrap();
        assert!(!existed);

        let val = store.get("TestTable", "key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_in_memory_store_no_overwrite() {
        let store = InMemoryStoreClient::new();
        store.put("T", "k", b"v1".to_vec(), true).await.unwrap();
        let existed = store.put("T", "k", b"v2".to_vec(), false).await.unwrap();
        assert!(existed);
        // Value should be unchanged
        let val = store.get("T", "k").await.unwrap();
        assert_eq!(val, Some(b"v1".to_vec()));
    }

    #[tokio::test]
    async fn test_in_memory_store_get_all() {
        let store = InMemoryStoreClient::new();
        store.put("T", "a", b"1".to_vec(), true).await.unwrap();
        store.put("T", "b", b"2".to_vec(), true).await.unwrap();
        let all = store.get_all("T").await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_in_memory_store_delete() {
        let store = InMemoryStoreClient::new();
        store.put("T", "k", b"v".to_vec(), true).await.unwrap();
        assert!(store.delete("T", "k").await.unwrap());
        assert!(!store.delete("T", "k").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_store_batch_delete() {
        let store = InMemoryStoreClient::new();
        store.put("T", "a", b"1".to_vec(), true).await.unwrap();
        store.put("T", "b", b"2".to_vec(), true).await.unwrap();
        store.put("T", "c", b"3".to_vec(), true).await.unwrap();
        let count = store
            .batch_delete("T", &["a".into(), "c".into(), "z".into()])
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_in_memory_store_get_keys() {
        let store = InMemoryStoreClient::new();
        store
            .put("T", "prefix_a", b"1".to_vec(), true)
            .await
            .unwrap();
        store
            .put("T", "prefix_b", b"2".to_vec(), true)
            .await
            .unwrap();
        store.put("T", "other", b"3".to_vec(), true).await.unwrap();
        let keys = store.get_keys("T", "prefix_").await.unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[tokio::test]
    async fn test_in_memory_store_next_job_id() {
        let store = InMemoryStoreClient::new();
        assert_eq!(store.get_next_job_id().await.unwrap(), 1);
        assert_eq!(store.get_next_job_id().await.unwrap(), 2);
        assert_eq!(store.get_next_job_id().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_in_memory_kv_put_get() {
        let kv = InMemoryInternalKV::new();
        let added = kv
            .put(b"ns", b"key1", b"value1".to_vec(), true)
            .await
            .unwrap();
        assert!(added);

        let val = kv.get(b"ns", b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_in_memory_kv_del_by_prefix() {
        let kv = InMemoryInternalKV::new();
        kv.put(b"ns", b"foo/a", b"1".to_vec(), true).await.unwrap();
        kv.put(b"ns", b"foo/b", b"2".to_vec(), true).await.unwrap();
        kv.put(b"ns", b"bar/c", b"3".to_vec(), true).await.unwrap();

        let deleted = kv.del(b"ns", b"foo/", true).await.unwrap();
        assert_eq!(deleted, 2);
        assert!(!kv.exists(b"ns", b"foo/a").await.unwrap());
        assert!(kv.exists(b"ns", b"bar/c").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_kv_keys() {
        let kv = InMemoryInternalKV::new();
        kv.put(b"ns", b"a/1", b"v".to_vec(), true).await.unwrap();
        kv.put(b"ns", b"a/2", b"v".to_vec(), true).await.unwrap();
        kv.put(b"ns", b"b/1", b"v".to_vec(), true).await.unwrap();

        let keys = kv.keys(b"ns", b"a/").await.unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[tokio::test]
    async fn test_in_memory_store_multi_get() {
        let store = InMemoryStoreClient::new();
        store.put("T", "a", b"1".to_vec(), true).await.unwrap();
        store.put("T", "b", b"2".to_vec(), true).await.unwrap();
        store.put("T", "c", b"3".to_vec(), true).await.unwrap();

        let result = store
            .multi_get("T", &["a".into(), "c".into(), "missing".into()])
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result["a"], b"1");
        assert_eq!(result["c"], b"3");
    }

    #[tokio::test]
    async fn test_in_memory_store_multi_get_empty_table() {
        let store = InMemoryStoreClient::new();
        let result = store
            .multi_get("NoTable", &["a".into()])
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_in_memory_store_exists() {
        let store = InMemoryStoreClient::new();
        assert!(!store.exists("T", "k").await.unwrap());
        store.put("T", "k", b"v".to_vec(), true).await.unwrap();
        assert!(store.exists("T", "k").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_store_overwrite() {
        let store = InMemoryStoreClient::new();
        store.put("T", "k", b"v1".to_vec(), true).await.unwrap();
        let existed = store.put("T", "k", b"v2".to_vec(), true).await.unwrap();
        assert!(existed);
        let val = store.get("T", "k").await.unwrap();
        assert_eq!(val, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_in_memory_store_get_nonexistent_table() {
        let store = InMemoryStoreClient::new();
        let val = store.get("NoTable", "k").await.unwrap();
        assert!(val.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_store_get_all_empty() {
        let store = InMemoryStoreClient::new();
        let all = store.get_all("NoTable").await.unwrap();
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_in_memory_store_delete_nonexistent_table() {
        let store = InMemoryStoreClient::new();
        assert!(!store.delete("NoTable", "k").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_store_get_keys_empty_table() {
        let store = InMemoryStoreClient::new();
        let keys = store.get_keys("NoTable", "").await.unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_in_memory_kv_multi_get() {
        let kv = InMemoryInternalKV::new();
        kv.put(b"ns", b"a", b"1".to_vec(), true).await.unwrap();
        kv.put(b"ns", b"b", b"2".to_vec(), true).await.unwrap();

        let result = kv
            .multi_get(b"ns", &[b"a".to_vec(), b"b".to_vec(), b"missing".to_vec()])
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[&b"a".to_vec()], b"1");
    }

    #[tokio::test]
    async fn test_in_memory_kv_no_overwrite() {
        let kv = InMemoryInternalKV::new();
        let added = kv.put(b"ns", b"k", b"v1".to_vec(), true).await.unwrap();
        assert!(added);

        // no overwrite — should not replace
        let added = kv.put(b"ns", b"k", b"v2".to_vec(), false).await.unwrap();
        assert!(!added);

        let val = kv.get(b"ns", b"k").await.unwrap();
        assert_eq!(val, Some(b"v1".to_vec()));
    }

    #[tokio::test]
    async fn test_in_memory_kv_del_single() {
        let kv = InMemoryInternalKV::new();
        kv.put(b"ns", b"k1", b"v1".to_vec(), true).await.unwrap();
        kv.put(b"ns", b"k2", b"v2".to_vec(), true).await.unwrap();

        let deleted = kv.del(b"ns", b"k1", false).await.unwrap();
        assert_eq!(deleted, 1);

        let deleted = kv.del(b"ns", b"k1", false).await.unwrap();
        assert_eq!(deleted, 0); // already gone

        assert!(kv.exists(b"ns", b"k2").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_kv_operations_on_nonexistent_namespace() {
        let kv = InMemoryInternalKV::new();

        assert!(kv.get(b"nope", b"k").await.unwrap().is_none());
        assert!(!kv.exists(b"nope", b"k").await.unwrap());
        assert_eq!(kv.del(b"nope", b"k", false).await.unwrap(), 0);
        assert!(kv.keys(b"nope", b"").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_in_memory_store_default() {
        // Test Default impl
        let store = InMemoryStoreClient::default();
        assert_eq!(store.get_next_job_id().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_in_memory_kv_default() {
        let kv = InMemoryInternalKV::default();
        assert!(kv.get(b"ns", b"k").await.unwrap().is_none());
    }

    // ---- Ported from in_memory_store_client_test.cc / observable_store_client_test.cc ----

    /// Full put-get-delete cycle matching StoreClientTestBase::TestAsyncPutAndAsyncGet
    #[tokio::test]
    async fn test_async_put_and_async_get_cycle() {
        let store = InMemoryStoreClient::new();

        // Put 5000 entries
        let count = 100;
        for i in 0..count {
            let key = format!("key_{}", i);
            let val = format!("val_{}", i);
            store.put("T", &key, val.into_bytes(), true).await.unwrap();
        }

        // Get and verify each
        for i in 0..count {
            let key = format!("key_{}", i);
            let expected = format!("val_{}", i);
            let val = store.get("T", &key).await.unwrap();
            assert_eq!(val, Some(expected.into_bytes()));
        }

        // Get non-existent key returns None
        let val = store.get("T", "nonexistent_key").await.unwrap();
        assert!(val.is_none());
    }

    /// Full get_all and batch_delete matching StoreClientTestBase::TestAsyncGetAllAndBatchDelete
    #[tokio::test]
    async fn test_async_get_all_and_batch_delete_cycle() {
        let store = InMemoryStoreClient::new();
        let count = 100;

        // Put entries
        for i in 0..count {
            let key = format!("key_{}", i);
            let val = format!("val_{}", i);
            store.put("T", &key, val.into_bytes(), true).await.unwrap();
        }

        // Get all
        let all = store.get_all("T").await.unwrap();
        assert_eq!(all.len(), count);

        // Delete half
        let keys_to_delete: Vec<String> = (0..count / 2).map(|i| format!("key_{}", i)).collect();
        let deleted = store.batch_delete("T", &keys_to_delete).await.unwrap();
        assert_eq!(deleted, count as i64 / 2);

        // Verify remaining
        let all = store.get_all("T").await.unwrap();
        assert_eq!(all.len(), count / 2);

        // Delete remaining
        let remaining_keys: Vec<String> =
            (count / 2..count).map(|i| format!("key_{}", i)).collect();
        let deleted = store.batch_delete("T", &remaining_keys).await.unwrap();
        assert_eq!(deleted, count as i64 / 2);

        let all = store.get_all("T").await.unwrap();
        assert!(all.is_empty());
    }

    /// Test table isolation — different tables are independent
    #[tokio::test]
    async fn test_table_isolation() {
        let store = InMemoryStoreClient::new();

        store
            .put("TableA", "key", b"valA".to_vec(), true)
            .await
            .unwrap();
        store
            .put("TableB", "key", b"valB".to_vec(), true)
            .await
            .unwrap();

        let val_a = store.get("TableA", "key").await.unwrap();
        let val_b = store.get("TableB", "key").await.unwrap();
        assert_eq!(val_a, Some(b"valA".to_vec()));
        assert_eq!(val_b, Some(b"valB".to_vec()));

        // Delete from TableA should not affect TableB
        store.delete("TableA", "key").await.unwrap();
        assert!(store.get("TableA", "key").await.unwrap().is_none());
        assert!(store.get("TableB", "key").await.unwrap().is_some());
    }

    /// Test job ID counter is monotonically increasing
    #[tokio::test]
    async fn test_job_id_monotonic() {
        let store = InMemoryStoreClient::new();
        let mut prev = 0;
        for _ in 0..100 {
            let id = store.get_next_job_id().await.unwrap();
            assert!(id > prev, "job IDs must be monotonically increasing");
            prev = id;
        }
    }

    /// Test batch_delete with all non-existent keys
    #[tokio::test]
    async fn test_batch_delete_nonexistent() {
        let store = InMemoryStoreClient::new();
        let count = store
            .batch_delete("T", &["x".into(), "y".into(), "z".into()])
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    /// Test InMemoryInternalKV namespaces are isolated
    #[tokio::test]
    async fn test_internal_kv_namespace_isolation() {
        let kv = InMemoryInternalKV::new();

        kv.put(b"ns1", b"key", b"v1".to_vec(), true).await.unwrap();
        kv.put(b"ns2", b"key", b"v2".to_vec(), true).await.unwrap();

        assert_eq!(
            kv.get(b"ns1", b"key").await.unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            kv.get(b"ns2", b"key").await.unwrap(),
            Some(b"v2".to_vec())
        );

        // Delete in ns1 doesn't affect ns2
        kv.del(b"ns1", b"key", false).await.unwrap();
        assert!(kv.get(b"ns1", b"key").await.unwrap().is_none());
        assert!(kv.get(b"ns2", b"key").await.unwrap().is_some());
    }

    /// Test prefix-based key listing
    #[tokio::test]
    async fn test_internal_kv_keys_empty_prefix() {
        let kv = InMemoryInternalKV::new();
        kv.put(b"ns", b"a", b"v".to_vec(), true).await.unwrap();
        kv.put(b"ns", b"b", b"v".to_vec(), true).await.unwrap();
        kv.put(b"ns", b"c", b"v".to_vec(), true).await.unwrap();

        // Empty prefix returns all keys
        let keys = kv.keys(b"ns", b"").await.unwrap();
        assert_eq!(keys.len(), 3);
    }

    /// Test overwrite flag behavior for InMemoryInternalKV
    #[tokio::test]
    async fn test_internal_kv_overwrite_replaces_value() {
        let kv = InMemoryInternalKV::new();

        kv.put(b"ns", b"k", b"v1".to_vec(), true).await.unwrap();
        // Overwrite with new value
        kv.put(b"ns", b"k", b"v2".to_vec(), true).await.unwrap();
        assert_eq!(
            kv.get(b"ns", b"k").await.unwrap(),
            Some(b"v2".to_vec())
        );
    }

    /// Test put to store returns correct "existed" value
    #[tokio::test]
    async fn test_store_put_returns_existed_flag() {
        let store = InMemoryStoreClient::new();

        // First put: did not exist
        let existed = store
            .put("T", "key", b"v1".to_vec(), true)
            .await
            .unwrap();
        assert!(!existed);

        // Second put with overwrite: existed
        let existed = store
            .put("T", "key", b"v2".to_vec(), true)
            .await
            .unwrap();
        assert!(existed);

        // Third put without overwrite: existed (and value unchanged)
        let existed = store
            .put("T", "key", b"v3".to_vec(), false)
            .await
            .unwrap();
        assert!(existed);
        assert_eq!(
            store.get("T", "key").await.unwrap(),
            Some(b"v2".to_vec())
        );
    }
}
