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
    pub async fn resolve_dependencies(
        &self,
        dependencies: &[ObjectID],
    ) -> CoreWorkerResult<()> {
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
        use bytes::Bytes;
        use crate::memory_store::RayObject;
        store
            .put(oid1, RayObject::new(Bytes::from("data"), Bytes::new(), vec![]))
            .unwrap();

        let resolver = Arc::new(DependencyResolver::with_memory_store(store));

        let resolver_clone = Arc::clone(&resolver);
        let handle = tokio::spawn(async move {
            resolver_clone.resolve_dependencies(&[oid1, oid2]).await
        });

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

        use bytes::Bytes;
        use crate::memory_store::RayObject;
        store
            .put(oid1, RayObject::new(Bytes::from("data"), Bytes::new(), vec![]))
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
}
