//! Typed table storage abstraction for Ray GCS.
//!
//! Provides `GcsTable<V>` wrapping `StoreClient` with protobuf serialization,
//! plus `GcsTableStorage` aggregating all 6 concrete table types.
//!
//! Maps C++ `GcsTable`/`GcsTableWithJobId`/`GcsTableStorage` from
//! `src/ray/gcs/gcs_table_storage.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;

use gcs_store::StoreClient;

/// A generic typed table over a `StoreClient`.
///
/// Keys are plain strings. Values are protobuf messages
/// serialized via `prost::Message`. Always overwrites on put (matching C++).
pub struct GcsTable<V: Message + Default> {
    store: Arc<dyn StoreClient>,
    table_name: String,
    _phantom: std::marker::PhantomData<V>,
}

impl<V: Message + Default> GcsTable<V> {
    pub fn new(store: Arc<dyn StoreClient>, table_name: String) -> Self {
        Self {
            store,
            table_name,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Write a key-value pair (always overwrites). Maps C++ `GcsTable::Put`.
    pub async fn put(&self, key: &str, value: &V) {
        let data = value.encode_to_vec();
        // Store raw bytes as a string (same as C++ SerializeAsString).
        let data_str = unsafe { String::from_utf8_unchecked(data) };
        self.store
            .put(&self.table_name, key, data_str, true)
            .await;
    }

    /// Get a value by key. Maps C++ `GcsTable::Get`.
    pub async fn get(&self, key: &str) -> Option<V> {
        let data = self.store.get(&self.table_name, key).await?;
        V::decode(data.as_bytes()).ok()
    }

    /// Get all entries. Maps C++ `GcsTable::GetAll`.
    pub async fn get_all(&self) -> HashMap<String, V> {
        let raw = self.store.get_all(&self.table_name).await;
        let mut result = HashMap::with_capacity(raw.len());
        for (k, v) in raw {
            if !v.is_empty() {
                if let Ok(data) = V::decode(v.as_bytes()) {
                    result.insert(k, data);
                }
            }
        }
        result
    }

    /// Delete a key. Maps C++ `GcsTable::Delete`.
    pub async fn delete(&self, key: &str) {
        self.store.delete(&self.table_name, key).await;
    }

    /// Batch delete keys. Maps C++ `GcsTable::BatchDelete`.
    pub async fn batch_delete(&self, keys: &[String]) {
        self.store.batch_delete(&self.table_name, keys).await;
    }
}

/// Aggregates all GCS table types. Maps C++ `GcsTableStorage`.
pub struct GcsTableStorage {
    store: Arc<dyn StoreClient>,
    job_table: GcsTable<gcs_proto::ray::rpc::JobTableData>,
    actor_table: GcsTable<gcs_proto::ray::rpc::ActorTableData>,
    actor_task_spec_table: GcsTable<gcs_proto::ray::rpc::TaskSpec>,
    placement_group_table: GcsTable<gcs_proto::ray::rpc::PlacementGroupTableData>,
    node_table: GcsTable<gcs_proto::ray::rpc::GcsNodeInfo>,
    worker_table: GcsTable<gcs_proto::ray::rpc::WorkerTableData>,
}

impl GcsTableStorage {
    pub fn new(store: Arc<dyn StoreClient>) -> Self {
        Self {
            job_table: GcsTable::new(store.clone(), "JOB".into()),
            actor_table: GcsTable::new(store.clone(), "ACTOR".into()),
            actor_task_spec_table: GcsTable::new(store.clone(), "ACTOR_TASK_SPEC".into()),
            placement_group_table: GcsTable::new(store.clone(), "PLACEMENT_GROUP".into()),
            node_table: GcsTable::new(store.clone(), "NODE".into()),
            worker_table: GcsTable::new(store.clone(), "WORKERS".into()),
            store,
        }
    }

    pub fn job_table(&self) -> &GcsTable<gcs_proto::ray::rpc::JobTableData> { &self.job_table }
    pub fn actor_table(&self) -> &GcsTable<gcs_proto::ray::rpc::ActorTableData> { &self.actor_table }
    pub fn actor_task_spec_table(&self) -> &GcsTable<gcs_proto::ray::rpc::TaskSpec> { &self.actor_task_spec_table }
    pub fn placement_group_table(&self) -> &GcsTable<gcs_proto::ray::rpc::PlacementGroupTableData> { &self.placement_group_table }
    pub fn node_table(&self) -> &GcsTable<gcs_proto::ray::rpc::GcsNodeInfo> { &self.node_table }
    pub fn worker_table(&self) -> &GcsTable<gcs_proto::ray::rpc::WorkerTableData> { &self.worker_table }

    pub async fn get_next_job_id(&self) -> i32 {
        self.store.get_next_job_id().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;

    #[tokio::test]
    async fn test_table_put_and_get() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);

        let mut job = gcs_proto::ray::rpc::JobTableData::default();
        job.job_id = b"job123".to_vec();
        job.is_dead = false;

        storage.job_table().put("job123", &job).await;
        let result = storage.job_table().get("job123").await;
        assert!(result.is_some());
        let got = result.unwrap();
        assert_eq!(got.job_id, b"job123".to_vec());
        assert!(!got.is_dead);
    }

    #[tokio::test]
    async fn test_table_get_nonexistent() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);
        assert!(storage.job_table().get("nope").await.is_none());
    }

    #[tokio::test]
    async fn test_table_get_all() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);

        for i in 0..3 {
            let mut node = gcs_proto::ray::rpc::GcsNodeInfo::default();
            node.node_id = format!("node_{i}").into_bytes();
            storage.node_table().put(&format!("node_{i}"), &node).await;
        }
        assert_eq!(storage.node_table().get_all().await.len(), 3);
    }

    #[tokio::test]
    async fn test_table_delete() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);

        let worker = gcs_proto::ray::rpc::WorkerTableData::default();
        storage.worker_table().put("w1", &worker).await;
        assert!(storage.worker_table().get("w1").await.is_some());

        storage.worker_table().delete("w1").await;
        assert!(storage.worker_table().get("w1").await.is_none());
    }

    #[tokio::test]
    async fn test_get_next_job_id() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);
        assert_eq!(storage.get_next_job_id().await, 1);
        assert_eq!(storage.get_next_job_id().await, 2);
    }

    #[tokio::test]
    async fn test_batch_delete() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);

        // Put 3 entries.
        for i in 0..3 {
            let mut node = gcs_proto::ray::rpc::GcsNodeInfo::default();
            node.node_id = format!("node_{i}").into_bytes();
            storage.node_table().put(&format!("node_{i}"), &node).await;
        }
        assert_eq!(storage.node_table().get_all().await.len(), 3);

        // Batch delete 2 of them.
        let keys_to_delete = vec!["node_0".to_string(), "node_1".to_string()];
        storage.node_table().batch_delete(&keys_to_delete).await;

        // Verify only 1 remains.
        let remaining = storage.node_table().get_all().await;
        assert_eq!(remaining.len(), 1);
        assert!(remaining.contains_key("node_2"));
    }

    #[tokio::test]
    async fn test_actor_task_spec_table() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);

        let mut spec = gcs_proto::ray::rpc::TaskSpec::default();
        spec.task_id = b"task_1".to_vec();
        spec.name = "my_actor_task".to_string();

        storage.actor_task_spec_table().put("actor_1", &spec).await;
        let result = storage.actor_task_spec_table().get("actor_1").await;
        assert!(result.is_some());
        let got = result.unwrap();
        assert_eq!(got.task_id, b"task_1".to_vec());
        assert_eq!(got.name, "my_actor_task");
    }

    #[tokio::test]
    async fn test_placement_group_table() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);

        let mut pg = gcs_proto::ray::rpc::PlacementGroupTableData::default();
        pg.placement_group_id = b"pg_1".to_vec();
        pg.name = "test_pg".to_string();

        storage.placement_group_table().put("pg_1", &pg).await;
        let result = storage.placement_group_table().get("pg_1").await;
        assert!(result.is_some());
        let got = result.unwrap();
        assert_eq!(got.placement_group_id, b"pg_1".to_vec());
        assert_eq!(got.name, "test_pg");
    }
}
