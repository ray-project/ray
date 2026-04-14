//! Abstract storage interface for GCS.
//!
//! This trait mirrors the C++ `StoreClient` interface defined in
//! `src/ray/gcs/store_client/store_client.h`. All operations are async and
//! operate on string key-value pairs organized into named tables.

use async_trait::async_trait;
use std::collections::HashMap;

/// Abstract interface for the GCS storage backend.
///
/// Implementations include:
/// - `InMemoryStoreClient`: HashMap-based, for development and testing
/// - `RedisStoreClient`: Redis-backed, for production (future)
///
/// # Design Notes
///
/// The C++ version uses callback-based async (Postable<T>). The Rust version
/// uses native async/await, which eliminates callback nesting and makes error
/// propagation natural via Result types.
#[async_trait]
pub trait StoreClient: Send + Sync + 'static {
    /// Write data to the given table.
    ///
    /// Returns `true` if a new entry was added, `false` if an existing entry
    /// was overwritten (when `overwrite` is true) or the write was skipped
    /// (when `overwrite` is false and the key already exists).
    async fn put(
        &self,
        table_name: &str,
        key: &str,
        data: String,
        overwrite: bool,
    ) -> bool;

    /// Get data from the given table. Returns `None` if the key doesn't exist.
    async fn get(&self, table_name: &str, key: &str) -> Option<String>;

    /// Get all data from the given table as a key-value map.
    async fn get_all(&self, table_name: &str) -> HashMap<String, String>;

    /// Get multiple keys from the given table. Returns only keys that exist.
    async fn multi_get(
        &self,
        table_name: &str,
        keys: &[String],
    ) -> HashMap<String, String>;

    /// Delete a key from the given table. Returns `true` if the key existed.
    async fn delete(&self, table_name: &str, key: &str) -> bool;

    /// Batch delete keys from the given table. Returns count of deleted entries.
    async fn batch_delete(&self, table_name: &str, keys: &[String]) -> i64;

    /// Get the next job ID by atomically incrementing a counter.
    async fn get_next_job_id(&self) -> i32;

    /// Get all keys matching a prefix from the given table.
    async fn get_keys(&self, table_name: &str, prefix: &str) -> Vec<String>;

    /// Check whether a key exists in the given table.
    async fn exists(&self, table_name: &str, key: &str) -> bool;
}
