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
}
