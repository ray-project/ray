//! Redis-backed implementation of `StoreClient`.
//!
//! Matches C++ `RedisStoreClient` from
//! `src/ray/gcs/store_client/redis_store_client.h/cc`.
//!
//! Each GCS table is stored as a Redis HASH keyed by
//! `"RAY{namespace}@{table_name}"`. Row keys are hash fields,
//! row values are hash values (binary-safe strings).
//! The job-ID counter uses a simple key `"RAY{namespace}@JobCounter"`.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use redis::AsyncCommands;
use tracing::{debug, info};

use crate::store_client::StoreClient;

/// Redis-backed storage for GCS.
///
/// Uses `redis::aio::ConnectionManager` which provides automatic
/// reconnection on transient failures.
pub struct RedisStoreClient {
    conn: redis::aio::ConnectionManager,
    /// Namespace prefix that isolates this cluster's data in a shared Redis.
    /// Matches C++ `external_storage_namespace`.
    namespace: String,
}

impl RedisStoreClient {
    /// Connect to Redis and return a new `RedisStoreClient`.
    ///
    /// `redis_address` is a standard Redis URL, e.g. `"redis://127.0.0.1:6379"`.
    /// `namespace` isolates this cluster's keys (maps C++ `external_storage_namespace`).
    pub async fn connect(redis_address: &str, namespace: &str) -> Result<Self> {
        let client = redis::Client::open(redis_address)?;
        let conn = redis::aio::ConnectionManager::new(client).await?;
        info!(redis_address, namespace, "Connected to Redis");
        Ok(Self {
            conn,
            namespace: namespace.to_string(),
        })
    }

    /// Build the Redis HASH key for a GCS table.
    /// Format: `"RAY{namespace}@{table_name}"` (matches C++ `GenRedisKey`).
    fn hash_key(&self, table_name: &str) -> String {
        format!("RAY{}@{}", self.namespace, table_name)
    }

    /// Build the key used for the atomic job-ID counter.
    fn job_counter_key(&self) -> String {
        format!("RAY{}@JobCounter", self.namespace)
    }

    /// Health check — sends PING to Redis.
    pub async fn check_health(&self) -> Result<()> {
        let mut conn = self.conn.clone();
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;
        Ok(())
    }
}

#[async_trait]
impl StoreClient for RedisStoreClient {
    async fn put(
        &self,
        table_name: &str,
        key: &str,
        data: String,
        overwrite: bool,
    ) -> bool {
        let hash_key = self.hash_key(table_name);
        let mut conn = self.conn.clone();
        // Store protobuf bytes as raw bytes in Redis (binary-safe).
        let data_bytes = data.into_bytes();

        if overwrite {
            // HSET returns the number of fields added (1 if new, 0 if updated).
            let added: i64 = conn.hset(&hash_key, key, &data_bytes).await.unwrap_or(0);
            added == 1
        } else {
            // HSETNX returns true if the field was set (new), false if it already existed.
            let was_set: bool = conn.hset_nx(&hash_key, key, &data_bytes).await.unwrap_or(false);
            was_set
        }
    }

    async fn get(&self, table_name: &str, key: &str) -> Option<String> {
        let hash_key = self.hash_key(table_name);
        let mut conn = self.conn.clone();
        let result: Option<Vec<u8>> = conn.hget(&hash_key, key).await.ok()?;
        // Convert raw bytes back to String (mirrors the from_utf8_unchecked in table storage).
        result.map(|bytes| unsafe { String::from_utf8_unchecked(bytes) })
    }

    async fn get_all(&self, table_name: &str) -> HashMap<String, String> {
        let hash_key = self.hash_key(table_name);
        let mut conn = self.conn.clone();
        let mut result = HashMap::new();

        // Use HSCAN to iterate without blocking Redis on large tables.
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, batch): (u64, Vec<(String, Vec<u8>)>) = redis::cmd("HSCAN")
                .arg(&hash_key)
                .arg(cursor)
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut conn)
                .await
                .unwrap_or((0, Vec::new()));

            for (k, v) in batch {
                let val = unsafe { String::from_utf8_unchecked(v) };
                result.insert(k, val);
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        debug!(table = table_name, count = result.len(), "get_all completed");
        result
    }

    async fn multi_get(
        &self,
        table_name: &str,
        keys: &[String],
    ) -> HashMap<String, String> {
        if keys.is_empty() {
            return HashMap::new();
        }

        let hash_key = self.hash_key(table_name);
        let mut conn = self.conn.clone();

        // HMGET returns Option<Vec<u8>> for each key.
        let values: Vec<Option<Vec<u8>>> = conn
            .hget(&hash_key, keys)
            .await
            .unwrap_or_else(|_| vec![None; keys.len()]);

        let mut result = HashMap::new();
        for (k, v) in keys.iter().zip(values) {
            if let Some(bytes) = v {
                let val = unsafe { String::from_utf8_unchecked(bytes) };
                result.insert(k.clone(), val);
            }
        }
        result
    }

    async fn delete(&self, table_name: &str, key: &str) -> bool {
        let hash_key = self.hash_key(table_name);
        let mut conn = self.conn.clone();
        let removed: i64 = conn.hdel(&hash_key, key).await.unwrap_or(0);
        removed > 0
    }

    async fn batch_delete(&self, table_name: &str, keys: &[String]) -> i64 {
        if keys.is_empty() {
            return 0;
        }

        let hash_key = self.hash_key(table_name);
        let mut conn = self.conn.clone();
        let removed: i64 = conn.hdel(&hash_key, keys).await.unwrap_or(0);
        removed
    }

    async fn get_next_job_id(&self) -> i32 {
        let counter_key = self.job_counter_key();
        let mut conn = self.conn.clone();
        let id: i64 = conn.incr(&counter_key, 1i64).await.unwrap_or(1);
        id as i32
    }

    async fn get_keys(&self, table_name: &str, prefix: &str) -> Vec<String> {
        let hash_key = self.hash_key(table_name);
        let mut conn = self.conn.clone();
        let mut result = Vec::new();

        let match_pattern = if prefix.is_empty() {
            "*".to_string()
        } else {
            format!("{}*", prefix)
        };

        // Use HSCAN with MATCH to filter by prefix.
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, batch): (u64, Vec<(String, Vec<u8>)>) = redis::cmd("HSCAN")
                .arg(&hash_key)
                .arg(cursor)
                .arg("MATCH")
                .arg(&match_pattern)
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut conn)
                .await
                .unwrap_or((0, Vec::new()));

            for (k, _v) in batch {
                result.push(k);
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        result
    }

    async fn exists(&self, table_name: &str, key: &str) -> bool {
        let hash_key = self.hash_key(table_name);
        let mut conn = self.conn.clone();
        let exists: bool = conn.hexists(&hash_key, key).await.unwrap_or(false);
        exists
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: connect to Redis for testing. Skips test if REDIS_URL is not set.
    async fn test_client(namespace: &str) -> Option<RedisStoreClient> {
        let url = match std::env::var("REDIS_URL") {
            Ok(url) => url,
            Err(_) => return None,
        };
        let client = RedisStoreClient::connect(&url, namespace).await.ok()?;
        // Flush keys for this namespace to start clean.
        flush_namespace(&client).await;
        Some(client)
    }

    /// Remove all keys for a given namespace (test cleanup).
    async fn flush_namespace(client: &RedisStoreClient) {
        let mut conn = client.conn.clone();
        let pattern = format!("RAY{}@*", client.namespace);
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut conn)
                .await
                .unwrap_or((0, Vec::new()));

            if !keys.is_empty() {
                let _: () = redis::cmd("DEL")
                    .arg(&keys)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(());
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_redis_put_and_get() {
        let Some(store) = test_client("test_put_get").await else { return };
        let added = store.put("table1", "key1", "value1".into(), false).await;
        assert!(added);
        let val = store.get("table1", "key1").await;
        assert_eq!(val, Some("value1".to_string()));
    }

    #[tokio::test]
    async fn test_redis_get_nonexistent() {
        let Some(store) = test_client("test_get_none").await else { return };
        let val = store.get("table1", "no_key").await;
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_redis_put_overwrite() {
        let Some(store) = test_client("test_overwrite").await else { return };
        store.put("t", "k", "v1".into(), false).await;
        store.put("t", "k", "v2".into(), true).await;
        assert_eq!(store.get("t", "k").await, Some("v2".to_string()));
    }

    #[tokio::test]
    async fn test_redis_put_no_overwrite() {
        let Some(store) = test_client("test_no_overwrite").await else { return };
        store.put("t", "k", "v1".into(), false).await;
        store.put("t", "k", "v2".into(), false).await;
        assert_eq!(store.get("t", "k").await, Some("v1".to_string()));
    }

    #[tokio::test]
    async fn test_redis_delete() {
        let Some(store) = test_client("test_delete").await else { return };
        store.put("t", "k", "v".into(), false).await;
        assert!(store.exists("t", "k").await);
        let deleted = store.delete("t", "k").await;
        assert!(deleted);
        assert!(!store.exists("t", "k").await);
        let deleted_again = store.delete("t", "k").await;
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn test_redis_get_all() {
        let Some(store) = test_client("test_get_all").await else { return };
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
    async fn test_redis_multi_get() {
        let Some(store) = test_client("test_multi_get").await else { return };
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
    async fn test_redis_batch_delete() {
        let Some(store) = test_client("test_batch_del").await else { return };
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
    async fn test_redis_get_next_job_id() {
        let Some(store) = test_client("test_job_id").await else { return };
        assert_eq!(store.get_next_job_id().await, 1);
        assert_eq!(store.get_next_job_id().await, 2);
        assert_eq!(store.get_next_job_id().await, 3);
    }

    #[tokio::test]
    async fn test_redis_get_keys() {
        let Some(store) = test_client("test_get_keys").await else { return };
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
    async fn test_redis_exists() {
        let Some(store) = test_client("test_exists").await else { return };
        assert!(!store.exists("t", "k").await);
        store.put("t", "k", "v".into(), false).await;
        assert!(store.exists("t", "k").await);
        store.delete("t", "k").await;
        assert!(!store.exists("t", "k").await);
    }

    #[tokio::test]
    async fn test_redis_table_isolation() {
        let Some(store) = test_client("test_isolation").await else { return };
        store.put("t1", "k", "v1".into(), false).await;
        store.put("t2", "k", "v2".into(), false).await;
        assert_eq!(store.get("t1", "k").await, Some("v1".to_string()));
        assert_eq!(store.get("t2", "k").await, Some("v2".to_string()));
    }

    #[tokio::test]
    async fn test_redis_health_check() {
        let Some(store) = test_client("test_health").await else { return };
        assert!(store.check_health().await.is_ok());
    }
}
