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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;
use ray_common::id::{NodeID, PlacementGroupID};
use tokio::sync::Notify;

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

impl PlacementGroupState {
    /// Map state to index for atomic counters (only 4 variants).
    fn index(self) -> usize {
        self as usize
    }
}

/// The GCS placement group manager.
pub struct GcsPlacementGroupManager {
    /// All registered placement groups.
    placement_groups:
        DashMap<PlacementGroupID, ray_proto::ray::rpc::PlacementGroupTableData>,
    /// Named placement groups: (namespace, name) → PlacementGroupID.
    named_placement_groups: DashMap<(String, String), PlacementGroupID>,
    /// State counts for metrics — indexed by PlacementGroupState (4 variants).
    state_counts: [AtomicUsize; 4],
    /// Persistence.
    table_storage: Arc<GcsTableStorage>,
    /// Per-PG notifiers for WaitPlacementGroupUntilReady.
    /// When a PG transitions to Created, all waiters are notified.
    pg_ready_notifiers: Mutex<HashMap<PlacementGroupID, Arc<Notify>>>,
}

impl GcsPlacementGroupManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            placement_groups: DashMap::new(),
            named_placement_groups: DashMap::new(),
            state_counts: [
                AtomicUsize::new(0),
                AtomicUsize::new(0),
                AtomicUsize::new(0),
                AtomicUsize::new(0),
            ],
            table_storage,
            pg_ready_notifiers: Mutex::new(HashMap::new()),
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

        for (key, pg) in all_pgs {
            let pg_id = PlacementGroupID::from_hex(&key);
            let state = PlacementGroupState::from(pg.state);
            self.state_counts[state.index()].fetch_add(1, Ordering::Relaxed);

            if !pg.name.is_empty() {
                let ns = pg.ray_namespace.clone();
                self.named_placement_groups.insert((ns, pg.name.clone()), pg_id);
            }
            self.placement_groups.insert(pg_id, pg);
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
            if self.named_placement_groups.contains_key(&(ns.clone(), pg_data.name.clone())) {
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
                .insert((ns, pg_data.name.clone()), pg_id);
        }

        let state = PlacementGroupState::from(pg_data.state);
        self.state_counts[state.index()].fetch_add(1, Ordering::Relaxed);
        let is_created = state == PlacementGroupState::Created;
        self.placement_groups.insert(pg_id, pg_data);

        // If the PG is created immediately, notify any waiters
        if is_created {
            self.notify_pg_ready(&pg_id);
        }

        tracing::info!(?pg_id, "Placement group created");
        Ok(())
    }

    /// Notify waiters that a placement group is ready (state == Created).
    fn notify_pg_ready(&self, pg_id: &PlacementGroupID) {
        let mut notifiers = self.pg_ready_notifiers.lock();
        if let Some(notify) = notifiers.remove(pg_id) {
            notify.notify_waiters();
        }
    }

    /// Transition a placement group to Created state and notify waiters.
    /// Called when scheduling succeeds (e.g., bundles are committed).
    pub fn mark_placement_group_created(&self, pg_id: &PlacementGroupID) {
        if let Some(mut entry) = self.placement_groups.get_mut(pg_id) {
            let old_state = PlacementGroupState::from(entry.state);
            if old_state == PlacementGroupState::Created {
                return;
            }
            entry.state = PlacementGroupState::Created as i32;
            self.state_counts[old_state.index()].fetch_sub(1, Ordering::Relaxed);
            self.state_counts[PlacementGroupState::Created.index()]
                .fetch_add(1, Ordering::Relaxed);
        }
        self.notify_pg_ready(pg_id);
    }

    /// Wait until a placement group transitions to Created state, with a timeout.
    ///
    /// Returns true if the PG is (or became) Created within the timeout,
    /// false if the timeout expired or the PG doesn't exist.
    pub async fn wait_until_ready(
        &self,
        pg_id_bytes: &[u8],
        timeout: std::time::Duration,
    ) -> bool {
        let pg_id = PlacementGroupID::from_binary(pg_id_bytes.try_into().unwrap_or(&[0u8; 18]));

        // Check current state first
        if let Some(entry) = self.placement_groups.get(&pg_id) {
            if PlacementGroupState::from(entry.state) == PlacementGroupState::Created {
                return true;
            }
        } else {
            return false; // PG doesn't exist
        }

        // Register a notifier and wait
        let notify = {
            let mut notifiers = self.pg_ready_notifiers.lock();
            notifiers
                .entry(pg_id)
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone()
        };

        match tokio::time::timeout(timeout, notify.notified()).await {
            Ok(()) => {
                // Double-check the state (it could have been removed)
                self.placement_groups
                    .get(&pg_id)
                    .map(|e| PlacementGroupState::from(e.state) == PlacementGroupState::Created)
                    .unwrap_or(false)
            }
            Err(_) => {
                // Timeout — clean up the notifier if we're the only waiter
                let mut notifiers = self.pg_ready_notifiers.lock();
                notifiers.remove(&pg_id);
                false
            }
        }
    }

    /// Handle RemovePlacementGroup RPC.
    ///
    /// Instead of deleting the PG, we transition it to REMOVED state and persist it.
    /// This matches C++ behavior: removed PGs remain queryable so recovery can see
    /// they were intentionally removed.
    pub async fn handle_remove_placement_group(
        &self,
        pg_id_bytes: &[u8],
    ) -> Result<(), tonic::Status> {
        let pg_id = PlacementGroupID::from_binary(pg_id_bytes.try_into().unwrap_or(&[0u8; 18]));

        // Transition to REMOVED state (keep in placement_groups map)
        let updated = if let Some(mut entry) = self.placement_groups.get_mut(&pg_id) {
            let old_state = PlacementGroupState::from(entry.state);
            if old_state == PlacementGroupState::Removed {
                // Already removed, no-op
                return Ok(());
            }
            entry.state = PlacementGroupState::Removed as i32;

            // Update state counts
            self.state_counts[old_state.index()].fetch_sub(1, Ordering::Relaxed);
            self.state_counts[PlacementGroupState::Removed.index()]
                .fetch_add(1, Ordering::Relaxed);

            // Remove from named lookup (removed PGs should not be found by name)
            if !entry.name.is_empty() {
                self.named_placement_groups
                    .remove(&(entry.ray_namespace.clone(), entry.name.clone()));
            }

            Some(entry.clone())
        } else {
            None
        };

        if let Some(ref pg_data) = updated {
            // Persist with REMOVED state (not delete)
            let key = hex::encode(pg_id_bytes);
            let _ = self
                .table_storage
                .placement_group_table()
                .put(&key, pg_data)
                .await;

            tracing::info!(?pg_id, "Placement group marked as removed");
        }
        Ok(())
    }

    /// Handle GetPlacementGroup RPC.
    pub fn handle_get_placement_group(
        &self,
        pg_id_bytes: &[u8],
    ) -> Option<ray_proto::ray::rpc::PlacementGroupTableData> {
        let pg_id = PlacementGroupID::from_binary(pg_id_bytes.try_into().unwrap_or(&[0u8; 18]));
        self.placement_groups.get(&pg_id).map(|r| r.value().clone())
    }

    /// Handle GetNamedPlacementGroup RPC.
    pub fn handle_get_named_placement_group(
        &self,
        name: &str,
        namespace: &str,
    ) -> Option<ray_proto::ray::rpc::PlacementGroupTableData> {
        let pg_id = self
            .named_placement_groups
            .get(&(namespace.to_string(), name.to_string()))
            .map(|r| *r.value())?;
        self.placement_groups.get(&pg_id).map(|r| r.value().clone())
    }

    /// Handle GetAllPlacementGroup RPC.
    pub fn handle_get_all_placement_groups(
        &self,
        limit: Option<usize>,
    ) -> Vec<ray_proto::ray::rpc::PlacementGroupTableData> {
        if let Some(limit) = limit {
            self.placement_groups
                .iter()
                .take(limit)
                .map(|entry| entry.value().clone())
                .collect()
        } else {
            self.placement_groups
                .iter()
                .map(|entry| entry.value().clone())
                .collect()
        }
    }

    /// Handle node death — mark placement groups with bundles on the dead node
    /// as needing rescheduling.
    pub fn on_node_dead(&self, node_id: &NodeID) {
        let node_id_bytes = node_id.binary().to_vec();

        for mut entry in self.placement_groups.iter_mut() {
            let pg = entry.value_mut();
            let state = PlacementGroupState::from(pg.state);
            if state == PlacementGroupState::Removed {
                continue;
            }

            // Check if any bundle in this PG was placed on the dead node
            let affected = pg.bundles.iter().any(|b| b.node_id == node_id_bytes);
            if affected {
                let old_state = PlacementGroupState::from(pg.state);
                pg.state = PlacementGroupState::Rescheduling as i32;

                // Clear node assignments for bundles on the dead node
                for bundle in pg.bundles.iter_mut() {
                    if bundle.node_id == node_id_bytes {
                        bundle.node_id.clear();
                    }
                }

                self.state_counts[old_state.index()].fetch_sub(1, Ordering::Relaxed);
                self.state_counts[PlacementGroupState::Rescheduling.index()]
                    .fetch_add(1, Ordering::Relaxed);

                tracing::info!(
                    pg_name = %pg.name,
                    "Placement group affected by node death, rescheduling"
                );
            }
        }
    }

    pub fn num_placement_groups(&self) -> usize {
        self.placement_groups.len()
    }

    pub fn state_counts(&self) -> HashMap<PlacementGroupState, usize> {
        let mut map = HashMap::new();
        for (i, state) in [
            PlacementGroupState::Pending,
            PlacementGroupState::Created,
            PlacementGroupState::Removed,
            PlacementGroupState::Rescheduling,
        ]
        .iter()
        .enumerate()
        {
            let count = self.state_counts[i].load(Ordering::Relaxed);
            if count > 0 {
                map.insert(*state, count);
            }
        }
        map
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

        // Get by name and verify fields match what was created
        let pg = mgr
            .handle_get_named_placement_group("pg1", "default")
            .expect("placement group should be found by name");
        assert_eq!(pg.name, "pg1");
        assert_eq!(pg.ray_namespace, "default");
        assert_eq!(pg.placement_group_id[0], 1);
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

        // PG should still be in the map with REMOVED state (not deleted)
        assert_eq!(mgr.num_placement_groups(), 1);
        let pg = mgr.handle_get_placement_group(&pg_id).unwrap();
        assert_eq!(PlacementGroupState::from(pg.state), PlacementGroupState::Removed);

        // But should NOT be findable by name
        assert!(mgr.handle_get_named_placement_group("pg1", "default").is_none());
    }

    #[tokio::test]
    async fn test_removed_pg_remains_queryable() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        mgr.handle_create_placement_group(make_pg(1, "persistent_pg"))
            .await
            .unwrap();

        let mut pg_id = [0u8; 18];
        pg_id[0] = 1;
        mgr.handle_remove_placement_group(&pg_id).await.unwrap();

        // Should be queryable by ID
        let pg = mgr.handle_get_placement_group(&pg_id);
        assert!(pg.is_some());
        let pg = pg.unwrap();
        assert_eq!(PlacementGroupState::from(pg.state), PlacementGroupState::Removed);
        assert_eq!(pg.name, "persistent_pg");

        // Should appear in get_all
        let all = mgr.handle_get_all_placement_groups(None);
        assert_eq!(all.len(), 1);
        assert_eq!(PlacementGroupState::from(all[0].state), PlacementGroupState::Removed);
    }

    #[tokio::test]
    async fn test_remove_already_removed_pg_is_noop() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        mgr.handle_create_placement_group(make_pg(1, "pg1"))
            .await
            .unwrap();

        let mut pg_id = [0u8; 18];
        pg_id[0] = 1;
        mgr.handle_remove_placement_group(&pg_id).await.unwrap();
        // Remove again — should be a no-op
        mgr.handle_remove_placement_group(&pg_id).await.unwrap();

        let pg = mgr.handle_get_placement_group(&pg_id).unwrap();
        assert_eq!(PlacementGroupState::from(pg.state), PlacementGroupState::Removed);
    }

    #[tokio::test]
    async fn test_on_node_dead_reschedules_affected_pgs() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        // Create a PG with bundles on node 1
        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        let mut pg = make_pg(1, "affected_pg");
        pg.state = PlacementGroupState::Created as i32;
        pg.bundles = vec![ray_proto::ray::rpc::Bundle {
            node_id: node_id.clone(),
            ..Default::default()
        }];
        mgr.handle_create_placement_group(pg).await.unwrap();

        // Create another PG on node 2 (should NOT be affected)
        let mut node2_id = vec![0u8; 28];
        node2_id[0] = 2;
        let mut pg2 = make_pg(2, "unaffected_pg");
        pg2.state = PlacementGroupState::Created as i32;
        pg2.bundles = vec![ray_proto::ray::rpc::Bundle {
            node_id: node2_id.clone(),
            ..Default::default()
        }];
        mgr.handle_create_placement_group(pg2).await.unwrap();

        // Kill node 1
        let dead_node = NodeID::from_binary(node_id.as_slice().try_into().unwrap());
        mgr.on_node_dead(&dead_node);

        // Affected PG should be Rescheduling
        let mut pg_id = [0u8; 18];
        pg_id[0] = 1;
        let pg = mgr.handle_get_placement_group(&pg_id).unwrap();
        assert_eq!(
            PlacementGroupState::from(pg.state),
            PlacementGroupState::Rescheduling
        );
        // Bundle node_id should be cleared
        assert!(pg.bundles[0].node_id.is_empty());

        // Unaffected PG should still be Created
        let mut pg_id2 = [0u8; 18];
        pg_id2[0] = 2;
        let pg2 = mgr.handle_get_placement_group(&pg_id2).unwrap();
        assert_eq!(
            PlacementGroupState::from(pg2.state),
            PlacementGroupState::Created
        );
    }

    #[tokio::test]
    async fn test_on_node_dead_no_effect_on_removed_pgs() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;
        let mut pg = make_pg(1, "removed_pg");
        pg.state = PlacementGroupState::Removed as i32;
        pg.bundles = vec![ray_proto::ray::rpc::Bundle {
            node_id: node_id.clone(),
            ..Default::default()
        }];
        mgr.handle_create_placement_group(pg).await.unwrap();

        let dead_node = NodeID::from_binary(node_id.as_slice().try_into().unwrap());
        mgr.on_node_dead(&dead_node);

        // Should still be Removed, not Rescheduling
        let mut pg_id = [0u8; 18];
        pg_id[0] = 1;
        let pg = mgr.handle_get_placement_group(&pg_id).unwrap();
        assert_eq!(
            PlacementGroupState::from(pg.state),
            PlacementGroupState::Removed
        );
    }

    #[tokio::test]
    async fn test_duplicate_named_placement_group() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        mgr.handle_create_placement_group(make_pg(1, "dup_pg"))
            .await
            .unwrap();
        let result = mgr
            .handle_create_placement_group(make_pg(2, "dup_pg"))
            .await;
        assert!(result.is_err()); // AlreadyExists
    }

    #[tokio::test]
    async fn test_same_name_different_namespace_allowed() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        let mut pg1 = make_pg(1, "shared_name");
        pg1.ray_namespace = "ns1".to_string();
        mgr.handle_create_placement_group(pg1).await.unwrap();

        let mut pg2 = make_pg(2, "shared_name");
        pg2.ray_namespace = "ns2".to_string();
        mgr.handle_create_placement_group(pg2).await.unwrap();

        assert_eq!(mgr.num_placement_groups(), 2);
    }

    #[tokio::test]
    async fn test_get_all_placement_groups_with_limit() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        for i in 1..=5u8 {
            mgr.handle_create_placement_group(make_pg(i, &format!("pg_{}", i)))
                .await
                .unwrap();
        }

        let all = mgr.handle_get_all_placement_groups(None);
        assert_eq!(all.len(), 5);

        let limited = mgr.handle_get_all_placement_groups(Some(3));
        assert_eq!(limited.len(), 3);
    }

    #[tokio::test]
    async fn test_state_counts_tracking() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        // Create pending PG
        mgr.handle_create_placement_group(make_pg(1, "pg1"))
            .await
            .unwrap();
        let counts = mgr.state_counts();
        assert_eq!(*counts.get(&PlacementGroupState::Pending).unwrap_or(&0), 1);

        // Create a "Created" PG
        let mut pg2 = make_pg(2, "pg2");
        pg2.state = PlacementGroupState::Created as i32;
        mgr.handle_create_placement_group(pg2).await.unwrap();

        let counts = mgr.state_counts();
        assert_eq!(*counts.get(&PlacementGroupState::Created).unwrap_or(&0), 1);

        // Remove pg1 — Pending count should decrement, Removed count should increment
        let mut pg_id = [0u8; 18];
        pg_id[0] = 1;
        mgr.handle_remove_placement_group(&pg_id).await.unwrap();

        let counts = mgr.state_counts();
        assert_eq!(*counts.get(&PlacementGroupState::Pending).unwrap_or(&0), 0);
        assert_eq!(*counts.get(&PlacementGroupState::Removed).unwrap_or(&0), 1);
    }

    #[tokio::test]
    async fn test_unnamed_placement_group() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        // Unnamed PGs should not be findable by name
        let pg = make_pg(1, "");
        mgr.handle_create_placement_group(pg).await.unwrap();
        assert_eq!(mgr.num_placement_groups(), 1);

        assert!(mgr
            .handle_get_named_placement_group("", "default")
            .is_none());

        // But should be findable by ID
        let mut pg_id = [0u8; 18];
        pg_id[0] = 1;
        assert!(mgr.handle_get_placement_group(&pg_id).is_some());
    }

    #[tokio::test]
    async fn test_get_nonexistent_placement_group() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        let pg_id = [0u8; 18];
        assert!(mgr.handle_get_placement_group(&pg_id).is_none());
        assert!(mgr
            .handle_get_named_placement_group("nope", "default")
            .is_none());
    }

    #[tokio::test]
    async fn test_placement_group_state_conversion() {
        assert_eq!(PlacementGroupState::from(0), PlacementGroupState::Pending);
        assert_eq!(PlacementGroupState::from(1), PlacementGroupState::Created);
        assert_eq!(PlacementGroupState::from(2), PlacementGroupState::Removed);
        assert_eq!(
            PlacementGroupState::from(3),
            PlacementGroupState::Rescheduling
        );
        // Unknown values default to Removed
        assert_eq!(PlacementGroupState::from(99), PlacementGroupState::Removed);
    }

    #[tokio::test]
    async fn test_wait_until_ready_already_created() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        // Create PG with Created state
        let mut pg = make_pg(1, "ready_pg");
        pg.state = PlacementGroupState::Created as i32;
        mgr.handle_create_placement_group(pg).await.unwrap();

        let mut pg_id = [0u8; 18];
        pg_id[0] = 1;
        let ready = mgr
            .wait_until_ready(&pg_id, std::time::Duration::from_millis(100))
            .await;
        assert!(ready);
    }

    #[tokio::test]
    async fn test_wait_until_ready_nonexistent_pg() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        let pg_id = [0u8; 18];
        let ready = mgr
            .wait_until_ready(&pg_id, std::time::Duration::from_millis(100))
            .await;
        assert!(!ready);
    }

    #[tokio::test]
    async fn test_wait_until_ready_timeout() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        // Create PG in Pending state — will never become Created
        mgr.handle_create_placement_group(make_pg(1, "pending_pg"))
            .await
            .unwrap();

        let mut pg_id = [0u8; 18];
        pg_id[0] = 1;
        let ready = mgr
            .wait_until_ready(&pg_id, std::time::Duration::from_millis(50))
            .await;
        assert!(!ready);
    }

    #[tokio::test]
    async fn test_wait_until_ready_notify_on_transition() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = Arc::new(GcsPlacementGroupManager::new(storage));

        // Create PG in Pending state
        mgr.handle_create_placement_group(make_pg(1, "transition_pg"))
            .await
            .unwrap();

        let mut pg_id_bytes = [0u8; 18];
        pg_id_bytes[0] = 1;
        let pg_id = PlacementGroupID::from_binary(&pg_id_bytes);

        let mgr_clone = Arc::clone(&mgr);
        let wait_handle = tokio::spawn(async move {
            mgr_clone
                .wait_until_ready(&pg_id_bytes, std::time::Duration::from_secs(5))
                .await
        });

        // Give the waiter time to register
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Transition to Created
        mgr.mark_placement_group_created(&pg_id);

        let ready = wait_handle.await.unwrap();
        assert!(ready);
    }

    #[tokio::test]
    async fn test_mark_placement_group_created() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsPlacementGroupManager::new(storage);

        mgr.handle_create_placement_group(make_pg(1, "pg1"))
            .await
            .unwrap();

        let mut pg_id_bytes = [0u8; 18];
        pg_id_bytes[0] = 1;
        let pg_id = PlacementGroupID::from_binary(&pg_id_bytes);

        mgr.mark_placement_group_created(&pg_id);

        let pg = mgr.handle_get_placement_group(&pg_id_bytes).unwrap();
        assert_eq!(
            PlacementGroupState::from(pg.state),
            PlacementGroupState::Created
        );

        let counts = mgr.state_counts();
        assert_eq!(*counts.get(&PlacementGroupState::Created).unwrap_or(&0), 1);
        assert_eq!(*counts.get(&PlacementGroupState::Pending).unwrap_or(&0), 0);
    }
}
