// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Placement Group Manager — manages placement group lifecycle.
//!
//! Replaces `src/ray/gcs/gcs_placement_group_manager.h/cc`.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use ray_common::id::PlacementGroupID;

use crate::table_storage::GcsTableStorage;

/// Placement group states matching the protobuf enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum PlacementGroupState {
    Pending = 0,
    Created = 1,
    Removed = 2,
    Rescheduling = 3,
}

impl From<i32> for PlacementGroupState {
    fn from(v: i32) -> Self {
        match v {
            0 => PlacementGroupState::Pending,
            1 => PlacementGroupState::Created,
            2 => PlacementGroupState::Removed,
            3 => PlacementGroupState::Rescheduling,
            _ => PlacementGroupState::Removed,
        }
    }
}

/// The GCS placement group manager.
pub struct GcsPlacementGroupManager {
    /// All registered placement groups.
    placement_groups:
        RwLock<HashMap<PlacementGroupID, ray_proto::ray::rpc::PlacementGroupTableData>>,
    /// Named placement groups: (namespace, name) → PlacementGroupID.
    named_placement_groups: RwLock<HashMap<(String, String), PlacementGroupID>>,
    /// State counts for metrics.
    state_counts: RwLock<HashMap<PlacementGroupState, usize>>,
    /// Persistence.
    table_storage: Arc<GcsTableStorage>,
}

impl GcsPlacementGroupManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            placement_groups: RwLock::new(HashMap::new()),
            named_placement_groups: RwLock::new(HashMap::new()),
            state_counts: RwLock::new(HashMap::new()),
            table_storage,
        }
    }

    /// Initialize from persisted data.
    pub async fn initialize(&self) -> anyhow::Result<()> {
        let all_pgs = self
            .table_storage
            .placement_group_table()
            .get_all()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        let mut pgs = self.placement_groups.write();
        let mut named = self.named_placement_groups.write();
        let mut counts = self.state_counts.write();

        for (key, pg) in all_pgs {
            let pg_id = PlacementGroupID::from_hex(&key);
            let state = PlacementGroupState::from(pg.state);
            *counts.entry(state).or_insert(0) += 1;

            if !pg.name.is_empty() {
                let ns = pg.ray_namespace.clone();
                named.insert((ns, pg.name.clone()), pg_id);
            }
            pgs.insert(pg_id, pg);
        }
        Ok(())
    }

    /// Handle CreatePlacementGroup RPC.
    pub async fn handle_create_placement_group(
        &self,
        pg_data: ray_proto::ray::rpc::PlacementGroupTableData,
    ) -> Result<(), tonic::Status> {
        let pg_id = PlacementGroupID::from_binary(
            pg_data
                .placement_group_id
                .as_slice()
                .try_into()
                .unwrap_or(&[0u8; 18]),
        );
        let key = hex::encode(&pg_data.placement_group_id);

        // Check name conflict
        if !pg_data.name.is_empty() {
            let ns = pg_data.ray_namespace.clone();
            let named = self.named_placement_groups.read();
            if named.contains_key(&(ns.clone(), pg_data.name.clone())) {
                return Err(tonic::Status::already_exists(format!(
                    "placement group '{}' already exists in namespace '{}'",
                    pg_data.name, ns
                )));
            }
        }

        // Persist
        self.table_storage
            .placement_group_table()
            .put(&key, &pg_data)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Register name
        if !pg_data.name.is_empty() {
            let ns = pg_data.ray_namespace.clone();
            self.named_placement_groups
                .write()
                .insert((ns, pg_data.name.clone()), pg_id);
        }

        let state = PlacementGroupState::from(pg_data.state);
        *self.state_counts.write().entry(state).or_insert(0) += 1;
        self.placement_groups.write().insert(pg_id, pg_data);

        tracing::info!(?pg_id, "Placement group created");
        Ok(())
    }

    /// Handle RemovePlacementGroup RPC.
    pub async fn handle_remove_placement_group(
        &self,
        pg_id_bytes: &[u8],
    ) -> Result<(), tonic::Status> {
        let pg_id = PlacementGroupID::from_binary(pg_id_bytes.try_into().unwrap_or(&[0u8; 18]));

        // Remove from in-memory state (drop lock before await)
        let removed = {
            let pg = self.placement_groups.write().remove(&pg_id);
            if let Some(ref pg) = pg {
                if !pg.name.is_empty() {
                    self.named_placement_groups
                        .write()
                        .remove(&(pg.ray_namespace.clone(), pg.name.clone()));
                }

                let old_state = PlacementGroupState::from(pg.state);
                if let Some(c) = self.state_counts.write().get_mut(&old_state) {
                    *c = c.saturating_sub(1);
                }
            }
            pg.is_some()
        };

        if removed {
            let key = hex::encode(pg_id_bytes);
            let _ = self
                .table_storage
                .placement_group_table()
                .delete(&key)
                .await;

            tracing::info!(?pg_id, "Placement group removed");
        }
        Ok(())
    }

    /// Handle GetPlacementGroup RPC.
    pub fn handle_get_placement_group(
        &self,
        pg_id_bytes: &[u8],
    ) -> Option<ray_proto::ray::rpc::PlacementGroupTableData> {
        let pg_id = PlacementGroupID::from_binary(pg_id_bytes.try_into().unwrap_or(&[0u8; 18]));
        self.placement_groups.read().get(&pg_id).cloned()
    }

    /// Handle GetNamedPlacementGroup RPC.
    pub fn handle_get_named_placement_group(
        &self,
        name: &str,
        namespace: &str,
    ) -> Option<ray_proto::ray::rpc::PlacementGroupTableData> {
        let named = self.named_placement_groups.read();
        let pg_id = named.get(&(namespace.to_string(), name.to_string()))?;
        self.placement_groups.read().get(pg_id).cloned()
    }

    /// Handle GetAllPlacementGroup RPC.
    pub fn handle_get_all_placement_groups(
        &self,
        limit: Option<usize>,
    ) -> Vec<ray_proto::ray::rpc::PlacementGroupTableData> {
        let pgs = self.placement_groups.read();
        if let Some(limit) = limit {
            pgs.values().take(limit).cloned().collect()
        } else {
            pgs.values().cloned().collect()
        }
    }

    pub fn num_placement_groups(&self) -> usize {
        self.placement_groups.read().len()
    }

    pub fn state_counts(&self) -> HashMap<PlacementGroupState, usize> {
        self.state_counts.read().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_client::InMemoryStoreClient;

    fn make_pg(id: u8, name: &str) -> ray_proto::ray::rpc::PlacementGroupTableData {
        let mut pg_id = vec![0u8; 18];
        pg_id[0] = id;
        ray_proto::ray::rpc::PlacementGroupTableData {
            placement_group_id: pg_id,
            name: name.to_string(),
            ray_namespace: "default".to_string(),
            state: PlacementGroupState::Pending as i32,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_create_and_get_placement_group() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        mgr.handle_create_placement_group(make_pg(1, "pg1"))
            .await
            .unwrap();
        assert_eq!(mgr.num_placement_groups(), 1);

        let pg = mgr.handle_get_named_placement_group("pg1", "default");
        assert!(pg.is_some());
    }

    #[tokio::test]
    async fn test_remove_placement_group() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        mgr.handle_create_placement_group(make_pg(1, "pg1"))
            .await
            .unwrap();

        let mut pg_id = [0u8; 18];
        pg_id[0] = 1;
        mgr.handle_remove_placement_group(&pg_id).await.unwrap();
        assert_eq!(mgr.num_placement_groups(), 0);
    }
}
