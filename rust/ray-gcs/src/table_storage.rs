// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS table storage — typed wrappers over the raw store client.
//!
//! Replaces `src/ray/gcs/gcs_table_storage.h/cc`.
//!
//! Each GCS table (jobs, nodes, actors, etc.) stores protobuf-encoded values
//! keyed by the relevant ID type.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;

use crate::store_client::{StoreClient, StoreResult};

/// Table names used by GCS.
pub mod table_names {
    pub const JOB: &str = "Job";
    pub const NODE: &str = "Node";
    pub const ACTOR: &str = "Actor";
    pub const ACTOR_TASK_SPEC: &str = "ActorTaskSpec";
    pub const PLACEMENT_GROUP: &str = "PlacementGroup";
    pub const WORKER: &str = "Worker";
    pub const TASK: &str = "Task";
    pub const KV: &str = "InternalKV";
}

/// Generic typed table backed by a StoreClient.
///
/// Keys are serialized as hex strings of their binary representation.
/// Values are protobuf-encoded messages.
pub struct GcsTable<V: Message + Default> {
    table_name: String,
    store_client: Arc<dyn StoreClient>,
    _phantom: std::marker::PhantomData<V>,
}

impl<V: Message + Default> GcsTable<V> {
    pub fn new(table_name: &str, store_client: Arc<dyn StoreClient>) -> Self {
        Self {
            table_name: table_name.to_string(),
            store_client,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Store a value by key.
    pub async fn put(&self, key: &str, value: &V) -> StoreResult<bool> {
        let data = value.encode_to_vec();
        self.store_client
            .put(&self.table_name, key, data, true)
            .await
    }

    /// Get a value by key.
    pub async fn get(&self, key: &str) -> StoreResult<Option<V>> {
        match self.store_client.get(&self.table_name, key).await? {
            Some(data) => {
                let value = V::decode(data.as_slice()).map_err(|e| {
                    crate::store_client::StoreError::Internal(format!(
                        "protobuf decode error: {}",
                        e
                    ))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Get all entries in the table.
    pub async fn get_all(&self) -> StoreResult<HashMap<String, V>> {
        let raw = self.store_client.get_all(&self.table_name).await?;
        let mut result = HashMap::new();
        for (key, data) in raw {
            let value = V::decode(data.as_slice()).map_err(|e| {
                crate::store_client::StoreError::Internal(format!("protobuf decode error: {}", e))
            })?;
            result.insert(key, value);
        }
        Ok(result)
    }

    /// Delete a value by key.
    pub async fn delete(&self, key: &str) -> StoreResult<bool> {
        self.store_client.delete(&self.table_name, key).await
    }

    /// Delete multiple values.
    pub async fn batch_delete(&self, keys: &[String]) -> StoreResult<i64> {
        self.store_client.batch_delete(&self.table_name, keys).await
    }
}

/// All GCS tables in one struct, created during server initialization.
pub struct GcsTableStorage {
    store_client: Arc<dyn StoreClient>,
}

impl GcsTableStorage {
    pub fn new(store_client: Arc<dyn StoreClient>) -> Self {
        Self { store_client }
    }

    pub fn store_client(&self) -> &Arc<dyn StoreClient> {
        &self.store_client
    }

    pub fn job_table(&self) -> GcsTable<ray_proto::ray::rpc::JobTableData> {
        GcsTable::new(table_names::JOB, self.store_client.clone())
    }

    pub fn node_table(&self) -> GcsTable<ray_proto::ray::rpc::GcsNodeInfo> {
        GcsTable::new(table_names::NODE, self.store_client.clone())
    }

    pub fn actor_table(&self) -> GcsTable<ray_proto::ray::rpc::ActorTableData> {
        GcsTable::new(table_names::ACTOR, self.store_client.clone())
    }

    pub fn actor_task_spec_table(&self) -> GcsTable<ray_proto::ray::rpc::TaskSpec> {
        GcsTable::new(table_names::ACTOR_TASK_SPEC, self.store_client.clone())
    }

    pub fn placement_group_table(&self) -> GcsTable<ray_proto::ray::rpc::PlacementGroupTableData> {
        GcsTable::new(table_names::PLACEMENT_GROUP, self.store_client.clone())
    }

    pub fn worker_table(&self) -> GcsTable<ray_proto::ray::rpc::WorkerTableData> {
        GcsTable::new(table_names::WORKER, self.store_client.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_client::InMemoryStoreClient;

    #[tokio::test]
    async fn test_gcs_table_put_get() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);
        let table = storage.job_table();

        let job_data = ray_proto::ray::rpc::JobTableData {
            job_id: b"\x01\x00\x00\x00".to_vec(),
            is_dead: false,
            ..Default::default()
        };

        table.put("job1", &job_data).await.unwrap();
        let retrieved = table.get("job1").await.unwrap().unwrap();
        assert_eq!(retrieved.job_id, b"\x01\x00\x00\x00".to_vec());
        assert!(!retrieved.is_dead);
    }

    #[tokio::test]
    async fn test_gcs_table_get_all() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);
        let table = storage.node_table();

        let node1 = ray_proto::ray::rpc::GcsNodeInfo {
            node_id: b"\x01".to_vec(),
            ..Default::default()
        };
        let node2 = ray_proto::ray::rpc::GcsNodeInfo {
            node_id: b"\x02".to_vec(),
            ..Default::default()
        };

        table.put("n1", &node1).await.unwrap();
        table.put("n2", &node2).await.unwrap();

        let all = table.get_all().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_gcs_table_delete() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);
        let table = storage.job_table();

        let job = ray_proto::ray::rpc::JobTableData::default();
        table.put("j1", &job).await.unwrap();
        assert!(table.delete("j1").await.unwrap());
        assert!(table.get("j1").await.unwrap().is_none());
    }
}
