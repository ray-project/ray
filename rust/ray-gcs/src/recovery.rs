// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS recovery orchestrator.
//!
//! Handles state recovery when GCS restarts — loads cluster ID from
//! persistent storage (Redis), rebuilds in-memory state from tables,
//! and detects stale nodes that didn't re-register within a timeout.
//!
//! Replaces `src/ray/gcs/gcs_init_data.cc`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ray_common::id::NodeID;

use crate::store_client::StoreClient;

/// Persistent keys used by the recovery system.
const CLUSTER_ID_KEY: &str = "__cluster_id__";
const CLUSTER_TABLE: &str = "GcsCluster";

/// Configuration for GCS recovery behavior.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Grace period after GCS restart for nodes to re-register.
    /// Nodes that don't re-register within this window are marked dead.
    pub node_reregistration_timeout: Duration,
    /// Whether to perform recovery from persistent storage.
    pub enable_recovery: bool,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            node_reregistration_timeout: Duration::from_secs(30),
            enable_recovery: true,
        }
    }
}

/// Initial data loaded from persistent storage during GCS recovery.
#[derive(Debug, Default)]
pub struct GcsInitData {
    /// The persisted cluster ID (if any).
    pub cluster_id: Option<Vec<u8>>,
    /// Node IDs that were alive when GCS last shut down.
    pub previously_alive_nodes: HashSet<NodeID>,
    /// Number of jobs loaded from persistent storage.
    pub num_jobs_loaded: usize,
    /// Number of actors loaded from persistent storage.
    pub num_actors_loaded: usize,
    /// Number of placement groups loaded.
    pub num_placement_groups_loaded: usize,
}

/// The GCS recovery orchestrator.
pub struct GcsRecovery {
    config: RecoveryConfig,
    store_client: Arc<dyn StoreClient>,
}

impl GcsRecovery {
    pub fn new(config: RecoveryConfig, store_client: Arc<dyn StoreClient>) -> Self {
        Self {
            config,
            store_client,
        }
    }

    /// Load the persisted cluster ID from storage.
    ///
    /// Returns `None` if no cluster ID was previously stored (first boot).
    pub async fn load_cluster_id(&self) -> anyhow::Result<Option<Vec<u8>>> {
        if !self.config.enable_recovery {
            return Ok(None);
        }

        match self.store_client.get(CLUSTER_TABLE, CLUSTER_ID_KEY).await {
            Ok(Some(data)) => {
                tracing::info!(
                    cluster_id = hex::encode(&data),
                    "Loaded cluster ID from persistent storage"
                );
                Ok(Some(data))
            }
            Ok(None) => {
                tracing::info!("No persisted cluster ID found (first boot)");
                Ok(None)
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load cluster ID, generating new one");
                Ok(None)
            }
        }
    }

    /// Persist the cluster ID to storage.
    pub async fn persist_cluster_id(&self, cluster_id: &[u8]) -> anyhow::Result<()> {
        self.store_client
            .put(CLUSTER_TABLE, CLUSTER_ID_KEY, cluster_id.to_vec(), true)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to persist cluster ID: {}", e))?;
        tracing::info!(
            cluster_id = hex::encode(cluster_id),
            "Persisted cluster ID to storage"
        );
        Ok(())
    }

    /// Get or create the cluster ID.
    ///
    /// If recovery is enabled and a cluster ID exists in storage, use it.
    /// Otherwise generate a new random 28-byte ID and persist it.
    pub async fn get_or_create_cluster_id(&self) -> anyhow::Result<Vec<u8>> {
        if let Some(existing) = self.load_cluster_id().await? {
            return Ok(existing);
        }

        // Generate a new random 28-byte cluster ID.
        let cluster_id: Vec<u8> = (0..28).map(|_| rand::random::<u8>()).collect();
        self.persist_cluster_id(&cluster_id).await?;
        Ok(cluster_id)
    }

    /// Detect nodes that were previously alive but haven't re-registered.
    ///
    /// This should be called after the re-registration timeout has elapsed.
    /// Returns the set of node IDs that should be marked as dead.
    pub fn detect_stale_nodes(
        &self,
        previously_alive: &HashSet<NodeID>,
        currently_alive: &HashSet<NodeID>,
    ) -> Vec<NodeID> {
        previously_alive
            .difference(currently_alive)
            .copied()
            .collect()
    }

    /// The configured re-registration timeout.
    pub fn reregistration_timeout(&self) -> Duration {
        self.config.node_reregistration_timeout
    }

    pub fn config(&self) -> &RecoveryConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_client::InMemoryStoreClient;

    fn make_nid(val: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = val;
        NodeID::from_binary(&data)
    }

    #[tokio::test]
    async fn test_cluster_id_first_boot() {
        let store = Arc::new(InMemoryStoreClient::new());
        let recovery = GcsRecovery::new(RecoveryConfig::default(), store);

        // First boot — no persisted cluster ID.
        let id = recovery.load_cluster_id().await.unwrap();
        assert!(id.is_none());
    }

    #[tokio::test]
    async fn test_cluster_id_persist_and_load() {
        let store = Arc::new(InMemoryStoreClient::new());
        let recovery = GcsRecovery::new(RecoveryConfig::default(), store);

        let cluster_id = vec![1u8; 28];
        recovery.persist_cluster_id(&cluster_id).await.unwrap();

        let loaded = recovery.load_cluster_id().await.unwrap();
        assert_eq!(loaded, Some(cluster_id));
    }

    #[tokio::test]
    async fn test_get_or_create_first_boot() {
        let store = Arc::new(InMemoryStoreClient::new());
        let recovery = GcsRecovery::new(RecoveryConfig::default(), store.clone());

        let id1 = recovery.get_or_create_cluster_id().await.unwrap();
        assert_eq!(id1.len(), 28);

        // Second call should return the same ID.
        let id2 = recovery.get_or_create_cluster_id().await.unwrap();
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_get_or_create_existing() {
        let store = Arc::new(InMemoryStoreClient::new());
        let recovery = GcsRecovery::new(RecoveryConfig::default(), store.clone());

        let original = vec![42u8; 28];
        recovery.persist_cluster_id(&original).await.unwrap();

        let loaded = recovery.get_or_create_cluster_id().await.unwrap();
        assert_eq!(loaded, original);
    }

    #[tokio::test]
    async fn test_recovery_disabled() {
        let store = Arc::new(InMemoryStoreClient::new());
        let config = RecoveryConfig {
            enable_recovery: false,
            ..Default::default()
        };
        let recovery = GcsRecovery::new(config, store.clone());

        // Even if there's a persisted ID, disabled recovery shouldn't load it.
        store
            .put(CLUSTER_TABLE, CLUSTER_ID_KEY, vec![1u8; 28], true)
            .await
            .unwrap();
        let id = recovery.load_cluster_id().await.unwrap();
        assert!(id.is_none());
    }

    #[test]
    fn test_detect_stale_nodes() {
        let store = Arc::new(InMemoryStoreClient::new());
        let recovery = GcsRecovery::new(RecoveryConfig::default(), store);

        let prev: HashSet<NodeID> = [make_nid(1), make_nid(2), make_nid(3)]
            .into_iter()
            .collect();
        let current: HashSet<NodeID> = [make_nid(1), make_nid(3)].into_iter().collect();

        let stale = recovery.detect_stale_nodes(&prev, &current);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0], make_nid(2));
    }

    #[test]
    fn test_detect_stale_nodes_all_reregistered() {
        let store = Arc::new(InMemoryStoreClient::new());
        let recovery = GcsRecovery::new(RecoveryConfig::default(), store);

        let prev: HashSet<NodeID> = [make_nid(1), make_nid(2)].into_iter().collect();
        let current: HashSet<NodeID> = [make_nid(1), make_nid(2), make_nid(3)]
            .into_iter()
            .collect();

        let stale = recovery.detect_stale_nodes(&prev, &current);
        assert!(stale.is_empty());
    }

    #[test]
    fn test_detect_stale_nodes_empty_prev() {
        let store = Arc::new(InMemoryStoreClient::new());
        let recovery = GcsRecovery::new(RecoveryConfig::default(), store);

        let prev: HashSet<NodeID> = HashSet::new();
        let current: HashSet<NodeID> = [make_nid(1)].into_iter().collect();

        let stale = recovery.detect_stale_nodes(&prev, &current);
        assert!(stale.is_empty());
    }

    #[test]
    fn test_default_config() {
        let config = RecoveryConfig::default();
        assert_eq!(config.node_reregistration_timeout, Duration::from_secs(30));
        assert!(config.enable_recovery);
    }
}
