//! In-memory implementation of `StoreClient`.
//!
//! This is the Rust equivalent of C++ `InMemoryStoreClient` from
//! `src/ray/gcs/store_client/in_memory_store_client.h/cc`.
//!
//! Uses `DashMap` for thread-safe concurrent access (replacing the C++ approach
//! of `absl::flat_hash_map` + `absl::Mutex`). DashMap provides lock-free reads
//! and fine-grained locking on writes, which is a performance improvement over
//! the C++ implementation's coarse-grained mutex.

use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::store_client::StoreClient;

/// In-memory storage backend for GCS.
///
/// Stores data in a nested `DashMap<table_name, DashMap<key, value>>` structure.
/// Thread-safe and suitable for testing and development.
pub struct InMemoryStoreClient {
    /// Nested map: table_name -> (key -> value)
    tables: DashMap<String, DashMap<String, String>>,
    /// Atomic counter for job ID generation.
    job_id_counter: AtomicI32,
}

impl InMemoryStoreClient {
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            job_id_counter: AtomicI32::new(0),
        }
    }

    /// Get or create a table by name.
    fn get_table(&self, table_name: &str) -> dashmap::mapref::one::Ref<'_, String, DashMap<String, String>> {
        if !self.tables.contains_key(table_name) {
            self.tables.insert(table_name.to_string(), DashMap::new());
        }
        self.tables.get(table_name).unwrap()
    }
}

impl Default for InMemoryStoreClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StoreClient for InMemoryStoreClient {
    async fn put(
        &self,
        table_name: &str,
        key: &str,
        data: String,
        overwrite: bool,
    ) -> bool {
        let table = self.get_table(table_name);
        if table.contains_key(key) {
            if overwrite {
                table.insert(key.to_string(), data);
                // C++ semantics: returns false when overwriting (not a NEW entry).
                false
            } else {
                // Key exists and overwrite is false: skip.
                false
            }
        } else {
            table.insert(key.to_string(), data);
            // New entry added.
            true
        }
    }

    async fn get(&self, table_name: &str, key: &str) -> Option<String> {
        let table = self.get_table(table_name);
        table.get(key).map(|v| v.value().clone())
    }

    async fn get_all(&self, table_name: &str) -> HashMap<String, String> {
        let table = self.get_table(table_name);
        table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    async fn multi_get(
        &self,
        table_name: &str,
        keys: &[String],
    ) -> HashMap<String, String> {
        let table = self.get_table(table_name);
        let mut result = HashMap::new();
        for key in keys {
            if let Some(val) = table.get(key.as_str()) {
                result.insert(key.clone(), val.value().clone());
            }
        }
        result
    }

    async fn delete(&self, table_name: &str, key: &str) -> bool {
        let table = self.get_table(table_name);
        table.remove(key).is_some()
    }

    async fn batch_delete(&self, table_name: &str, keys: &[String]) -> i64 {
        let table = self.get_table(table_name);
        let mut count = 0i64;
        for key in keys {
            if table.remove(key.as_str()).is_some() {
                count += 1;
            }
        }
        count
    }

    async fn get_next_job_id(&self) -> i32 {
        self.job_id_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    async fn get_keys(&self, table_name: &str, prefix: &str) -> Vec<String> {
        let table = self.get_table(table_name);
        table
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| entry.key().clone())
            .collect()
    }

    async fn exists(&self, table_name: &str, key: &str) -> bool {
        let table = self.get_table(table_name);
        table.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let store = InMemoryStoreClient::new();
        let added = store.put("table1", "key1", "value1".into(), false).await;
        assert!(added);

        let val = store.get("table1", "key1").await;
        assert_eq!(val, Some("value1".to_string()));
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let store = InMemoryStoreClient::new();
        let val = store.get("table1", "no_key").await;
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_put_overwrite() {
        let store = InMemoryStoreClient::new();
        store.put("t", "k", "v1".into(), false).await;
        store.put("t", "k", "v2".into(), true).await;
        assert_eq!(store.get("t", "k").await, Some("v2".to_string()));
    }

    #[tokio::test]
    async fn test_put_no_overwrite() {
        let store = InMemoryStoreClient::new();
        store.put("t", "k", "v1".into(), false).await;
        store.put("t", "k", "v2".into(), false).await;
        assert_eq!(store.get("t", "k").await, Some("v1".to_string()));
    }

    #[tokio::test]
    async fn test_delete() {
        let store = InMemoryStoreClient::new();
        store.put("t", "k", "v".into(), false).await;
        assert!(store.exists("t", "k").await);

        let deleted = store.delete("t", "k").await;
        assert!(deleted);
        assert!(!store.exists("t", "k").await);

        let deleted_again = store.delete("t", "k").await;
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn test_get_all() {
        let store = InMemoryStoreClient::new();
        store.put("t", "a", "1".into(), false).await;
        store.put("t", "b", "2".into(), false).await;
        store.put("t", "c", "3".into(), false).await;

        let all = store.get_all("t").await;
        assert_eq!(all.len(), 3);
        assert_eq!(all["a"], "1");
        assert_eq!(all["b"], "2");
        assert_eq!(all["c"], "3");
    }

    #[tokio::test]
    async fn test_multi_get() {
        let store = InMemoryStoreClient::new();
        store.put("t", "a", "1".into(), false).await;
        store.put("t", "b", "2".into(), false).await;

        let result = store
            .multi_get("t", &["a".into(), "b".into(), "missing".into()])
            .await;
        assert_eq!(result.len(), 2);
        assert_eq!(result["a"], "1");
        assert_eq!(result["b"], "2");
    }

    #[tokio::test]
    async fn test_batch_delete() {
        let store = InMemoryStoreClient::new();
        store.put("t", "a", "1".into(), false).await;
        store.put("t", "b", "2".into(), false).await;
        store.put("t", "c", "3".into(), false).await;

        let count = store
            .batch_delete("t", &["a".into(), "c".into(), "missing".into()])
            .await;
        assert_eq!(count, 2);
        assert!(!store.exists("t", "a").await);
        assert!(store.exists("t", "b").await);
        assert!(!store.exists("t", "c").await);
    }

    #[tokio::test]
    async fn test_get_next_job_id() {
        let store = InMemoryStoreClient::new();
        assert_eq!(store.get_next_job_id().await, 1);
        assert_eq!(store.get_next_job_id().await, 2);
        assert_eq!(store.get_next_job_id().await, 3);
    }

    #[tokio::test]
    async fn test_get_keys() {
        let store = InMemoryStoreClient::new();
        store.put("t", "prefix/a", "1".into(), false).await;
        store.put("t", "prefix/b", "2".into(), false).await;
        store.put("t", "other/c", "3".into(), false).await;

        let mut keys = store.get_keys("t", "prefix/").await;
        keys.sort();
        assert_eq!(keys, vec!["prefix/a", "prefix/b"]);

        let all_keys = store.get_keys("t", "").await;
        assert_eq!(all_keys.len(), 3);
    }

    #[tokio::test]
    async fn test_exists() {
        let store = InMemoryStoreClient::new();
        assert!(!store.exists("t", "k").await);
        store.put("t", "k", "v".into(), false).await;
        assert!(store.exists("t", "k").await);
        store.delete("t", "k").await;
        assert!(!store.exists("t", "k").await);
    }

    #[tokio::test]
    async fn test_table_isolation() {
        let store = InMemoryStoreClient::new();
        store.put("t1", "k", "v1".into(), false).await;
        store.put("t2", "k", "v2".into(), false).await;

        assert_eq!(store.get("t1", "k").await, Some("v1".to_string()));
        assert_eq!(store.get("t2", "k").await, Some("v2".to_string()));
    }
}
