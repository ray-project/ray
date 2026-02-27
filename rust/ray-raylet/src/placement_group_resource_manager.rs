// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Placement group resource manager — manages bundle resource allocation.
//!
//! Replaces `src/ray/raylet/placement_group_resource_manager.h/cc`.
//!
//! Uses a two-phase commit (2PC) protocol:
//! 1. Prepare: Lock raw resources for bundles
//! 2. Commit: Convert locked resources into PG-specific custom resources

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use ray_common::id::PlacementGroupID;
use ray_common::scheduling::ResourceSet;

use crate::local_resource_manager::LocalResourceManager;
use crate::scheduling_resources::TaskResourceInstances;

/// A bundle ID: (placement_group_id, bundle_index).
pub type BundleID = (PlacementGroupID, i32);

/// State of a bundle in the 2PC protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitState {
    Prepared,
    Committed,
}

/// Per-bundle transaction state.
#[allow(dead_code)]
struct BundleTransactionState {
    state: CommitState,
    resources: ResourceSet,
    allocation: TaskResourceInstances,
}

/// The placement group resource manager handles bundle resource allocation.
pub struct PlacementGroupResourceManager {
    /// Per-bundle state.
    bundles: RwLock<HashMap<BundleID, BundleTransactionState>>,
    /// Local resource manager reference.
    local_resource_manager: Arc<LocalResourceManager>,
}

impl PlacementGroupResourceManager {
    pub fn new(local_resource_manager: Arc<LocalResourceManager>) -> Self {
        Self {
            bundles: RwLock::new(HashMap::new()),
            local_resource_manager,
        }
    }

    /// Phase 1 — Prepare: Lock raw resources for a set of bundles.
    /// All bundles must succeed or all fail.
    pub fn prepare_bundles(&self, bundle_specs: &[(BundleID, ResourceSet)]) -> Result<(), String> {
        let mut allocated = Vec::new();

        for (bundle_id, resources) in bundle_specs {
            match self
                .local_resource_manager
                .allocate_local_task_resources(resources)
            {
                Some(allocation) => {
                    allocated.push((*bundle_id, resources.clone(), allocation));
                }
                None => {
                    // Rollback all previous allocations
                    for (_, _, alloc) in &allocated {
                        self.local_resource_manager.release_worker_resources(alloc);
                    }
                    return Err(format!("insufficient resources for bundle {:?}", bundle_id));
                }
            }
        }

        // All succeeded — record the prepared state
        let mut bundles = self.bundles.write();
        for (bundle_id, resources, allocation) in allocated {
            bundles.insert(
                bundle_id,
                BundleTransactionState {
                    state: CommitState::Prepared,
                    resources,
                    allocation,
                },
            );
        }

        Ok(())
    }

    /// Phase 2 — Commit: Convert locked resources into PG-specific resources.
    pub fn commit_bundles(&self, bundle_ids: &[BundleID]) -> Result<(), String> {
        let mut bundles = self.bundles.write();

        for bundle_id in bundle_ids {
            let state = bundles
                .get_mut(bundle_id)
                .ok_or_else(|| format!("bundle {:?} not found", bundle_id))?;

            if state.state != CommitState::Prepared {
                return Err(format!("bundle {:?} not in prepared state", bundle_id));
            }

            state.state = CommitState::Committed;

            // In production, this would create PG-specific custom resources
            // (e.g., CPU_group_<pg_id>, CPU_1_<pg_id>)
            // For now, we just track the committed state.
        }

        Ok(())
    }

    /// Return a bundle's resources (rollback or cleanup).
    pub fn return_bundle(&self, bundle_id: &BundleID) -> bool {
        let state = self.bundles.write().remove(bundle_id);
        if let Some(state) = state {
            self.local_resource_manager
                .release_worker_resources(&state.allocation);
            true
        } else {
            false
        }
    }

    /// Return all bundles not in the given set (cleanup after GCS restart).
    pub fn return_unused_bundles(&self, in_use: &[BundleID]) {
        let all_bundles: Vec<BundleID> = self.bundles.read().keys().copied().collect();
        for bundle_id in all_bundles {
            if !in_use.contains(&bundle_id) {
                self.return_bundle(&bundle_id);
            }
        }
    }

    /// Get the PG-specific resource name for a resource within a bundle.
    pub fn get_pg_resource_name(
        pg_id: &PlacementGroupID,
        bundle_index: i32,
        resource: &str,
    ) -> String {
        format!(
            "{}_group_{}_{}",
            resource,
            hex::encode(pg_id.binary()),
            bundle_index
        )
    }

    /// Get wildcard PG resource name (any bundle in the group).
    pub fn get_pg_wildcard_resource_name(pg_id: &PlacementGroupID, resource: &str) -> String {
        format!("{}_group_{}", resource, hex::encode(pg_id.binary()))
    }

    /// Number of tracked bundles.
    pub fn num_bundles(&self) -> usize {
        self.bundles.read().len()
    }

    /// Number of committed bundles.
    pub fn num_committed_bundles(&self) -> usize {
        self.bundles
            .read()
            .values()
            .filter(|s| s.state == CommitState::Committed)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_common::scheduling::FixedPoint;

    fn make_pg_id(id: u8) -> PlacementGroupID {
        let mut bytes = [0u8; 18];
        bytes[0] = id;
        PlacementGroupID::from_binary(&bytes)
    }

    fn make_manager() -> PlacementGroupResourceManager {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(4.0));

        let local_mgr = Arc::new(LocalResourceManager::new(
            "node1".to_string(),
            total,
            HashMap::new(),
        ));
        PlacementGroupResourceManager::new(local_mgr)
    }

    #[test]
    fn test_prepare_and_commit() {
        let mgr = make_manager();
        let pg_id = make_pg_id(1);

        let mut bundle_resources = ResourceSet::new();
        bundle_resources.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let bundle_id = (pg_id, 0);
        mgr.prepare_bundles(&[(bundle_id, bundle_resources)])
            .unwrap();
        assert_eq!(mgr.num_bundles(), 1);

        mgr.commit_bundles(&[bundle_id]).unwrap();
        assert_eq!(mgr.num_committed_bundles(), 1);
    }

    #[test]
    fn test_prepare_insufficient_resources() {
        let mgr = make_manager();
        let pg_id = make_pg_id(1);

        let mut bundle_resources = ResourceSet::new();
        bundle_resources.set("CPU".to_string(), FixedPoint::from_f64(100.0));

        let result = mgr.prepare_bundles(&[((pg_id, 0), bundle_resources)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_return_bundle() {
        let mgr = make_manager();
        let pg_id = make_pg_id(1);

        let mut bundle_resources = ResourceSet::new();
        bundle_resources.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let bundle_id = (pg_id, 0);
        mgr.prepare_bundles(&[(bundle_id, bundle_resources)])
            .unwrap();
        assert_eq!(mgr.num_bundles(), 1);

        mgr.return_bundle(&bundle_id);
        assert_eq!(mgr.num_bundles(), 0);
    }

    #[test]
    fn test_pg_resource_names() {
        let pg_id = make_pg_id(1);
        let name = PlacementGroupResourceManager::get_pg_resource_name(&pg_id, 0, "CPU");
        assert!(name.starts_with("CPU_group_"));
        assert!(name.ends_with("_0"));

        let wildcard = PlacementGroupResourceManager::get_pg_wildcard_resource_name(&pg_id, "CPU");
        assert!(wildcard.starts_with("CPU_group_"));
    }
}
