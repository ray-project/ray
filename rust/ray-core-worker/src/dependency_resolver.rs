// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Dependency resolution for task arguments.
//!
//! Tracks pending object dependencies and notifies when they become available.
//! Supports timeout, local object availability checks, and cancellation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use ray_common::id::ObjectID;

use crate::error::{CoreWorkerError, CoreWorkerResult};
use crate::memory_store::CoreWorkerMemoryStore;

/// Callback to check if an object is locally available.
pub type LocalAvailabilityCheck = Box<dyn Fn(&ObjectID) -> bool + Send + Sync>;

/// Tracks pending object dependencies and provides async wait for resolution.
pub struct DependencyResolver {
    /// Map from object ID to the list of oneshot senders waiting for it.
    pending: Mutex<HashMap<ObjectID, Vec<oneshot::Sender<()>>>>,
    /// Optional reference to the memory store for local availability checks.
    memory_store: Mutex<Option<Arc<CoreWorkerMemoryStore>>>,
}

impl DependencyResolver {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            memory_store: Mutex::new(None),
        }
    }

    /// Create a DependencyResolver with a memory store for local availability checks.
    pub fn with_memory_store(store: Arc<CoreWorkerMemoryStore>) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            memory_store: Mutex::new(Some(store)),
        }
    }

    /// Set the memory store for local availability checks.
    pub fn set_memory_store(&self, store: Arc<CoreWorkerMemoryStore>) {
        *self.memory_store.lock() = Some(store);
    }

    /// Wait for all dependencies to become available.
    ///
    /// Returns immediately if `dependencies` is empty or all are locally available.
    /// Otherwise registers waiters for each missing object and awaits them all.
    pub async fn resolve_dependencies(&self, dependencies: &[ObjectID]) -> CoreWorkerResult<()> {
        if dependencies.is_empty() {
            return Ok(());
        }

        // Check which dependencies are already locally available
        let missing: Vec<ObjectID> = {
            let store = self.memory_store.lock();
            if let Some(ref store) = *store {
                dependencies
                    .iter()
                    .filter(|oid| !store.contains(oid))
                    .copied()
                    .collect()
            } else {
                dependencies.to_vec()
            }
        };

        if missing.is_empty() {
            return Ok(());
        }

        let mut receivers = Vec::new();
        {
            let mut pending = self.pending.lock();
            for oid in &missing {
                let (tx, rx) = oneshot::channel();
                pending.entry(*oid).or_default().push(tx);
                receivers.push(rx);
            }
        }

        for rx in receivers {
            rx.await.map_err(|_| {
                CoreWorkerError::Internal("dependency resolver channel closed".into())
            })?;
        }

        Ok(())
    }

    /// Wait for all dependencies with a timeout.
    ///
    /// Returns `Ok(())` if all resolved, `Err(TimedOut)` if the timeout expires.
    pub async fn resolve_dependencies_with_timeout(
        &self,
        dependencies: &[ObjectID],
        timeout: Duration,
    ) -> CoreWorkerResult<()> {
        match tokio::time::timeout(timeout, self.resolve_dependencies(dependencies)).await {
            Ok(result) => result,
            Err(_) => Err(CoreWorkerError::TimedOut(format!(
                "dependency resolution timed out after {:?} for {} objects",
                timeout,
                dependencies.len()
            ))),
        }
    }

    /// Signal that an object is now available, waking all waiters.
    pub fn object_available(&self, object_id: &ObjectID) {
        let mut pending = self.pending.lock();
        if let Some(waiters) = pending.remove(object_id) {
            for tx in waiters {
                let _ = tx.send(());
            }
        }
    }

    /// Cancel all waiters for an object (e.g., when the object is lost).
    pub fn cancel_object(&self, object_id: &ObjectID) -> usize {
        let mut pending = self.pending.lock();
        if let Some(waiters) = pending.remove(object_id) {
            let count = waiters.len();
            // Drop the senders, which causes receivers to get an error
            drop(waiters);
            count
        } else {
            0
        }
    }

    /// Number of objects with pending waiters.
    pub fn num_pending(&self) -> usize {
        self.pending.lock().len()
    }

    /// Number of total waiters across all objects.
    pub fn num_waiters(&self) -> usize {
        self.pending.lock().values().map(|v| v.len()).sum()
    }
}

impl Default for DependencyResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(v: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = v;
        ObjectID::from_binary(&data)
    }

    #[tokio::test]
    async fn test_empty_dependencies_resolves_immediately() {
        let resolver = DependencyResolver::new();
        resolver.resolve_dependencies(&[]).await.unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    #[tokio::test]
    async fn test_single_dependency_resolved() {
        let resolver = Arc::new(DependencyResolver::new());
        let oid = make_oid(1);

        let resolver_clone = Arc::clone(&resolver);
        let handle: tokio::task::JoinHandle<CoreWorkerResult<()>> =
            tokio::spawn(async move { resolver_clone.resolve_dependencies(&[oid]).await });

        // Give the resolver time to register the waiter
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 1);

        resolver.object_available(&oid);

        handle.await.unwrap().unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    #[tokio::test]
    async fn test_multiple_dependencies() {
        let resolver = Arc::new(DependencyResolver::new());
        let o1 = make_oid(1);
        let o2 = make_oid(2);

        let resolver_clone = Arc::clone(&resolver);
        let handle: tokio::task::JoinHandle<CoreWorkerResult<()>> =
            tokio::spawn(async move { resolver_clone.resolve_dependencies(&[o1, o2]).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 2);

        resolver.object_available(&o1);
        resolver.object_available(&o2);

        handle.await.unwrap().unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    #[tokio::test]
    async fn test_object_available_nonexistent_is_noop() {
        let resolver = DependencyResolver::new();
        let oid = make_oid(99);
        // Should not panic
        resolver.object_available(&oid);
        assert_eq!(resolver.num_pending(), 0);
    }

    #[tokio::test]
    async fn test_multiple_waiters_on_same_object() {
        let resolver = Arc::new(DependencyResolver::new());
        let oid = make_oid(1);

        let r1 = Arc::clone(&resolver);
        let r2 = Arc::clone(&resolver);
        let h1: tokio::task::JoinHandle<CoreWorkerResult<()>> =
            tokio::spawn(async move { r1.resolve_dependencies(&[oid]).await });
        let h2: tokio::task::JoinHandle<CoreWorkerResult<()>> =
            tokio::spawn(async move { r2.resolve_dependencies(&[oid]).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        // Both registered waiters on the same object
        assert_eq!(resolver.num_pending(), 1);
        assert_eq!(resolver.num_waiters(), 2);

        resolver.object_available(&oid);

        h1.await.unwrap().unwrap();
        h2.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_resolve_with_timeout_success() {
        let resolver = Arc::new(DependencyResolver::new());
        let oid = make_oid(1);

        let resolver_clone = Arc::clone(&resolver);
        let handle = tokio::spawn(async move {
            resolver_clone
                .resolve_dependencies_with_timeout(&[oid], Duration::from_secs(5))
                .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        resolver.object_available(&oid);

        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_resolve_with_timeout_expires() {
        let resolver = DependencyResolver::new();
        let oid = make_oid(1);

        let result = resolver
            .resolve_dependencies_with_timeout(&[oid], Duration::from_millis(20))
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            CoreWorkerError::TimedOut(_) => {}
            other => panic!("expected TimedOut, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_resolve_skips_locally_available() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        // Put oid1 into the store so it's locally available
        use crate::memory_store::RayObject;
        use bytes::Bytes;
        store
            .put(
                oid1,
                RayObject::new(Bytes::from("data"), Bytes::new(), vec![]),
            )
            .unwrap();

        let resolver = Arc::new(DependencyResolver::with_memory_store(store));

        let resolver_clone = Arc::clone(&resolver);
        let handle =
            tokio::spawn(async move { resolver_clone.resolve_dependencies(&[oid1, oid2]).await });

        // Only oid2 should be pending (oid1 is locally available)
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 1);

        resolver.object_available(&oid2);
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_all_locally_available_returns_immediately() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid1 = make_oid(1);

        use crate::memory_store::RayObject;
        use bytes::Bytes;
        store
            .put(
                oid1,
                RayObject::new(Bytes::from("data"), Bytes::new(), vec![]),
            )
            .unwrap();

        let resolver = DependencyResolver::with_memory_store(store);
        resolver.resolve_dependencies(&[oid1]).await.unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    #[test]
    fn test_cancel_object() {
        let resolver = DependencyResolver::new();
        let oid = make_oid(1);

        // Register some waiters
        {
            let mut pending = resolver.pending.lock();
            let (tx1, _rx1) = oneshot::channel();
            let (tx2, _rx2) = oneshot::channel();
            pending.entry(oid).or_default().push(tx1);
            pending.entry(oid).or_default().push(tx2);
        }

        assert_eq!(resolver.num_waiters(), 2);
        let cancelled = resolver.cancel_object(&oid);
        assert_eq!(cancelled, 2);
        assert_eq!(resolver.num_pending(), 0);
    }

    // ── Ported from dependency_resolver_test.cc ─────────────────────

    /// Port of TestNoDependencies: resolving empty dependencies succeeds
    /// immediately.
    #[tokio::test]
    async fn test_no_dependencies_resolves_immediately_inline() {
        let resolver = DependencyResolver::new();
        let result = resolver.resolve_dependencies(&[]).await;
        assert!(result.is_ok());
        assert_eq!(resolver.num_pending(), 0);
        assert_eq!(resolver.num_waiters(), 0);
    }

    /// Port of TestHandlePlasmaPromotion: an object already in the local
    /// store should resolve without pending.
    #[tokio::test]
    async fn test_plasma_promotion_already_local() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid = make_oid(1);

        use crate::memory_store::RayObject;
        use bytes::Bytes;
        // Pre-populate with an "in plasma" marker object.
        store
            .put(oid, RayObject::new(Bytes::new(), Bytes::from("3"), vec![]))
            .unwrap();

        let resolver = DependencyResolver::with_memory_store(store);
        resolver.resolve_dependencies(&[oid]).await.unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    /// Port of TestInlineLocalDependencies: two objects already in local
    /// store should both resolve immediately.
    #[tokio::test]
    async fn test_inline_local_dependencies() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        use crate::memory_store::RayObject;
        use bytes::Bytes;
        let data = RayObject::from_data(Bytes::from("value"));
        store.put(oid1, data.clone()).unwrap();
        store.put(oid2, data.clone()).unwrap();

        let resolver = DependencyResolver::with_memory_store(store);
        resolver.resolve_dependencies(&[oid1, oid2]).await.unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    /// Port of TestInlinePendingDependencies: dependencies not yet local
    /// should block until they are made available.
    #[tokio::test]
    async fn test_inline_pending_dependencies() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        let resolver = Arc::new(DependencyResolver::with_memory_store(store.clone()));

        let resolver_clone = Arc::clone(&resolver);
        let handle =
            tokio::spawn(async move { resolver_clone.resolve_dependencies(&[oid1, oid2]).await });

        // Both are pending.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 2);

        // Make them available.
        // Note: for DependencyResolver, we use object_available() to notify.
        resolver.object_available(&oid1);
        resolver.object_available(&oid2);

        handle.await.unwrap().unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    /// Port of TestCancelDependencyResolution: cancelling waiters for
    /// pending dependencies should not invoke the resolution callback.
    #[tokio::test]
    async fn test_cancel_dependency_resolution() {
        let resolver = Arc::new(DependencyResolver::new());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        let resolver_clone = Arc::clone(&resolver);
        let handle =
            tokio::spawn(async move { resolver_clone.resolve_dependencies(&[oid1, oid2]).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 2);

        // Cancel one dependency.
        let cancelled = resolver.cancel_object(&oid1);
        assert_eq!(cancelled, 1);
        // Cancel the other.
        let cancelled = resolver.cancel_object(&oid2);
        assert_eq!(cancelled, 1);

        // The resolution should fail because the channels were dropped.
        let result = handle.await.unwrap();
        assert!(result.is_err());
        assert_eq!(resolver.num_pending(), 0);
    }

    /// Port of TestDependenciesAlreadyLocal: even though dependencies
    /// are locally available, the resolve call should succeed.
    #[tokio::test]
    async fn test_dependencies_already_local_with_store() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid = make_oid(10);

        use crate::memory_store::RayObject;
        use bytes::Bytes;
        store
            .put(oid, RayObject::from_data(Bytes::from("data")))
            .unwrap();

        let resolver = DependencyResolver::with_memory_store(store);
        resolver.resolve_dependencies(&[oid]).await.unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    /// Port of TestActorAndObjectDependencies: actor dependency + object
    /// dependency resolved in sequence.
    #[tokio::test]
    async fn test_actor_and_object_dependencies() {
        let resolver = Arc::new(DependencyResolver::new());
        let obj = make_oid(1);
        let actor_handle_obj = make_oid(2);

        let resolver_clone = Arc::clone(&resolver);
        let handle = tokio::spawn(async move {
            resolver_clone
                .resolve_dependencies(&[obj, actor_handle_obj])
                .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 2);

        // Resolve actor dependency first.
        resolver.object_available(&actor_handle_obj);
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Still pending because obj is not resolved.
        assert_eq!(resolver.num_pending(), 1);

        // Resolve the object dependency.
        resolver.object_available(&obj);
        handle.await.unwrap().unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    /// Port of TestInlinedObjectIds: nested object references within
    /// inlined objects.
    #[tokio::test]
    async fn test_inlined_object_ids_with_nested_refs() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);
        let nested_oid = make_oid(3);

        use crate::memory_store::RayObject;
        use bytes::Bytes;
        let data = RayObject::new(Bytes::from("val"), Bytes::new(), vec![nested_oid]);
        store.put(oid1, data.clone()).unwrap();
        store.put(oid2, data.clone()).unwrap();

        let resolver = DependencyResolver::with_memory_store(store);
        resolver.resolve_dependencies(&[oid1, oid2]).await.unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    /// Port of TestMixedTensorTransport concept: objects with different
    /// local availability are handled correctly (some skip, some wait).
    #[tokio::test]
    async fn test_mixed_local_and_remote_dependencies() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let local_oid = make_oid(1);
        let remote_oid = make_oid(2);

        use crate::memory_store::RayObject;
        use bytes::Bytes;
        store
            .put(local_oid, RayObject::from_data(Bytes::from("local")))
            .unwrap();

        let resolver = Arc::new(DependencyResolver::with_memory_store(store));

        let resolver_clone = Arc::clone(&resolver);
        let handle = tokio::spawn(async move {
            resolver_clone
                .resolve_dependencies(&[local_oid, remote_oid])
                .await
        });

        // Only remote_oid should be pending.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 1);

        resolver.object_available(&remote_oid);
        handle.await.unwrap().unwrap();
    }

    /// Cancel nonexistent object is a no-op.
    #[test]
    fn test_cancel_nonexistent_object() {
        let resolver = DependencyResolver::new();
        let oid = make_oid(99);
        let cancelled = resolver.cancel_object(&oid);
        assert_eq!(cancelled, 0);
    }

    // ── Additional tests ported from dependency_resolver_test.cc ────

    /// Port of TestCancelDependencyResolution with partial resolve:
    /// resolve one dependency, cancel the other.
    #[tokio::test]
    async fn test_partial_resolve_then_cancel() {
        let resolver = Arc::new(DependencyResolver::new());
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        let resolver_clone = Arc::clone(&resolver);
        let handle =
            tokio::spawn(async move { resolver_clone.resolve_dependencies(&[oid1, oid2]).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 2);

        // Resolve one.
        resolver.object_available(&oid1);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(resolver.num_pending(), 1);

        // Cancel the other.
        resolver.cancel_object(&oid2);

        // The overall resolution should fail.
        let result = handle.await.unwrap();
        assert!(result.is_err());
    }

    /// Port of TestMultipleDependenciesResolveSequentially: objects
    /// resolved one at a time.
    #[tokio::test]
    async fn test_sequential_resolution() {
        let resolver = Arc::new(DependencyResolver::new());
        let oids: Vec<_> = (1..=5).map(make_oid).collect();

        let resolver_clone = Arc::clone(&resolver);
        let oids_clone = oids.clone();
        let handle =
            tokio::spawn(async move { resolver_clone.resolve_dependencies(&oids_clone).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 5);

        // Resolve one at a time.
        for (i, oid) in oids.iter().enumerate() {
            resolver.object_available(oid);
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert_eq!(resolver.num_pending(), 4 - i);
        }

        handle.await.unwrap().unwrap();
    }

    /// Port of TestDuplicateDependencies: resolving the same object
    /// twice should notify both waiters.
    #[tokio::test]
    async fn test_duplicate_dependencies() {
        let resolver = Arc::new(DependencyResolver::new());
        let oid = make_oid(1);

        // Two separate resolve calls for the same object.
        let r1 = Arc::clone(&resolver);
        let r2 = Arc::clone(&resolver);
        let h1 = tokio::spawn(async move { r1.resolve_dependencies(&[oid]).await });
        let h2 = tokio::spawn(async move { r2.resolve_dependencies(&[oid]).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(resolver.num_pending(), 1);
        assert_eq!(resolver.num_waiters(), 2);

        resolver.object_available(&oid);

        h1.await.unwrap().unwrap();
        h2.await.unwrap().unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    /// Port of TestSetMemoryStore: verify set_memory_store works
    /// after construction.
    #[tokio::test]
    async fn test_set_memory_store_after_creation() {
        let resolver = DependencyResolver::new();
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid = make_oid(1);

        use crate::memory_store::RayObject;
        use bytes::Bytes;
        store
            .put(oid, RayObject::from_data(Bytes::from("data")))
            .unwrap();

        // Without store, the object would be pending.
        resolver.set_memory_store(store);

        // Now it should resolve immediately.
        resolver.resolve_dependencies(&[oid]).await.unwrap();
        assert_eq!(resolver.num_pending(), 0);
    }

    /// Port of TestTimeoutWithAvailableObject: timeout should not
    /// apply if objects are already local.
    #[tokio::test]
    async fn test_timeout_not_triggered_when_local() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid = make_oid(1);

        use crate::memory_store::RayObject;
        use bytes::Bytes;
        store
            .put(oid, RayObject::from_data(Bytes::from("data")))
            .unwrap();

        let resolver = DependencyResolver::with_memory_store(store);
        // Even with a very short timeout, it should succeed because
        // the object is already local.
        let result = resolver
            .resolve_dependencies_with_timeout(&[oid], Duration::from_millis(1))
            .await;
        assert!(result.is_ok());
    }

    /// Test default impl.
    #[test]
    fn test_default_impl() {
        let resolver = DependencyResolver::default();
        assert_eq!(resolver.num_pending(), 0);
        assert_eq!(resolver.num_waiters(), 0);
    }

    /// Test num_pending and num_waiters track independently.
    #[test]
    fn test_pending_vs_waiters_tracking() {
        let resolver = DependencyResolver::new();
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        // Manually add waiters.
        {
            let mut pending = resolver.pending.lock();
            let (tx1, _rx1) = oneshot::channel();
            let (tx2, _rx2) = oneshot::channel();
            let (tx3, _rx3) = oneshot::channel();
            pending.entry(oid1).or_default().push(tx1);
            pending.entry(oid1).or_default().push(tx2);
            pending.entry(oid2).or_default().push(tx3);
        }

        assert_eq!(resolver.num_pending(), 2); // 2 distinct objects
        assert_eq!(resolver.num_waiters(), 3); // 3 total waiters
    }
}
