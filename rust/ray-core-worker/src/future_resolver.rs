// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object future resolution for borrowed objects.
//!
//! Ports C++ `core_worker/future_resolver.cc`.
//!
//! When a worker receives an ObjectRef for an object it doesn't own, it
//! needs to resolve the object's value by contacting the owner. The
//! FutureResolver manages this process: querying the owner for object
//! status, handling inline returns, and storing resolved values.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use ray_common::id::ObjectID;
use ray_proto::ray::rpc::Address;

use crate::memory_store::{CoreWorkerMemoryStore, RayObject};
use crate::reference_counter::ReferenceCounter;

/// Status of an object as reported by its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStatus {
    /// Object has been created and is available.
    Created,
    /// Object is out of scope (owner deleted it).
    OutOfScope,
    /// Object owner has died.
    OwnerDied,
    /// Object is still being created (task in progress).
    WaitingForCreation,
}

/// Information about a resolved object.
#[derive(Debug, Clone)]
pub struct ResolvedObject {
    /// The resolved object value (data + metadata).
    pub object: Option<RayObject>,
    /// Object status.
    pub status: ObjectStatus,
    /// Node ID where the object is stored (for scheduling locality).
    pub node_id: Option<Vec<u8>>,
    /// Size of the object in bytes.
    pub object_size: i64,
}

/// Reply data from the object owner when resolving an object.
#[derive(Debug, Clone)]
pub struct ResolutionReply {
    /// The object ID being resolved.
    pub object_id: ObjectID,
    /// Status reported by the owner.
    pub status: ObjectStatus,
    /// Inline data (if the object was small enough to include).
    pub data: Option<Bytes>,
    /// Inline metadata.
    pub metadata: Option<Bytes>,
    /// Nested object references inside this object.
    pub nested_refs: Vec<ObjectID>,
    /// Node ID where the object is stored.
    pub node_id: Option<Vec<u8>>,
    /// Object size in bytes.
    pub object_size: i64,
}

/// A pending resolution — someone is waiting for this object.
struct PendingResolution {
    /// The owner address to query.
    owner_address: Address,
    /// Senders for all waiters.
    waiters: Vec<oneshot::Sender<ResolvedObject>>,
    /// When this resolution was created.
    created_at: std::time::Instant,
}

/// Resolves borrowed object references by querying their owners.
///
/// When a worker receives an ObjectRef for an object it doesn't own,
/// it calls `resolve_object_async()` to asynchronously resolve the value.
/// The resolver contacts the owner via RPC (or handles inline results)
/// and stores the result in the memory store.
pub struct FutureResolver {
    inner: Mutex<FutureResolverInner>,
    memory_store: Arc<CoreWorkerMemoryStore>,
    reference_counter: Arc<ReferenceCounter>,
    /// Timeout for pending resolutions. If a resolution has been pending for
    /// longer than this, it is marked as OwnerDied.
    resolution_timeout: Duration,
}

struct FutureResolverInner {
    /// Objects currently being resolved.
    pending_resolutions: HashMap<ObjectID, PendingResolution>,
    /// Total number of resolutions completed.
    total_resolved: u64,
    /// Total number of resolutions that failed (owner died, out of scope).
    total_failed: u64,
    /// Total number of resolutions that timed out.
    total_timed_out: u64,
}

impl FutureResolver {
    /// Default resolution timeout (30 seconds).
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

    /// Create a new FutureResolver with the default timeout.
    pub fn new(
        memory_store: Arc<CoreWorkerMemoryStore>,
        reference_counter: Arc<ReferenceCounter>,
    ) -> Self {
        Self::with_timeout(memory_store, reference_counter, Self::DEFAULT_TIMEOUT)
    }

    /// Create a new FutureResolver with a custom timeout.
    pub fn with_timeout(
        memory_store: Arc<CoreWorkerMemoryStore>,
        reference_counter: Arc<ReferenceCounter>,
        resolution_timeout: Duration,
    ) -> Self {
        Self {
            inner: Mutex::new(FutureResolverInner {
                pending_resolutions: HashMap::new(),
                total_resolved: 0,
                total_failed: 0,
                total_timed_out: 0,
            }),
            memory_store,
            reference_counter,
            resolution_timeout,
        }
    }

    /// Request async resolution of an object from its owner.
    ///
    /// Returns a oneshot receiver that will be notified when the object
    /// is resolved (or an error occurs). If the object is already being
    /// resolved, this piggybacks on the existing resolution.
    pub fn resolve_object_async(
        &self,
        object_id: ObjectID,
        owner_address: Address,
    ) -> oneshot::Receiver<ResolvedObject> {
        let (tx, rx) = oneshot::channel();

        let mut inner = self.inner.lock();

        if let Some(pending) = inner.pending_resolutions.get_mut(&object_id) {
            // Already resolving — add this waiter.
            pending.waiters.push(tx);
        } else {
            // New resolution request.
            inner.pending_resolutions.insert(
                object_id,
                PendingResolution {
                    owner_address,
                    waiters: vec![tx],
                    created_at: std::time::Instant::now(),
                },
            );
        }

        rx
    }

    /// Process a resolved object from the owner.
    ///
    /// Called when the owner responds to a `GetObjectStatus` RPC.
    /// Stores the object in the memory store and notifies all waiters.
    pub fn process_resolved_object(&self, reply: ResolutionReply) {
        let ResolutionReply {
            object_id,
            status,
            data,
            metadata,
            nested_refs,
            node_id,
            object_size,
        } = reply;
        let resolved = match status {
            ObjectStatus::Created => {
                // Store the object if it was provided inline.
                if let Some(data) = data.clone() {
                    let obj = RayObject::new(
                        data,
                        metadata.clone().unwrap_or_default(),
                        nested_refs.clone(),
                    );
                    let _ = self.memory_store.put(object_id, obj);

                    // Register nested references as borrowed.
                    for nested_id in &nested_refs {
                        if let Some(owner) = self.reference_counter.get_owner(nested_id) {
                            self.reference_counter
                                .add_borrowed_object(*nested_id, owner);
                        }
                    }
                }

                // Update object size in reference counter.
                if object_size > 0 {
                    self.reference_counter
                        .update_object_size(&object_id, object_size);
                }

                // Update locality information.
                if let Some(ref nid) = node_id {
                    self.reference_counter
                        .add_object_location(&object_id, hex::encode(nid));
                }

                ResolvedObject {
                    object: data
                        .map(|d| RayObject::new(d, metadata.unwrap_or_default(), nested_refs)),
                    status: ObjectStatus::Created,
                    node_id,
                    object_size,
                }
            }
            ObjectStatus::OutOfScope | ObjectStatus::OwnerDied => {
                // Store an error marker in the memory store.
                let error_msg = match status {
                    ObjectStatus::OutOfScope => "object out of scope",
                    ObjectStatus::OwnerDied => "object owner died",
                    _ => unreachable!(),
                };
                let error_obj =
                    RayObject::new(Bytes::from(error_msg), Bytes::from("ERROR"), Vec::new());
                let _ = self.memory_store.put(object_id, error_obj);

                ResolvedObject {
                    object: None,
                    status: status.clone(),
                    node_id: None,
                    object_size: 0,
                }
            }
            ObjectStatus::WaitingForCreation => {
                // Object not ready yet — don't notify waiters, keep pending.
                return;
            }
        };

        // Notify all waiters and clean up.
        let mut inner = self.inner.lock();
        if let Some(pending) = inner.pending_resolutions.remove(&object_id) {
            for waiter in pending.waiters {
                let _ = waiter.send(resolved.clone());
            }
        }

        match resolved.status {
            ObjectStatus::Created => inner.total_resolved += 1,
            _ => inner.total_failed += 1,
        }
    }

    /// Check if an object is currently being resolved.
    pub fn is_resolving(&self, object_id: &ObjectID) -> bool {
        self.inner
            .lock()
            .pending_resolutions
            .contains_key(object_id)
    }

    /// Number of objects currently being resolved.
    pub fn num_pending(&self) -> usize {
        self.inner.lock().pending_resolutions.len()
    }

    /// Get the owner address for a pending resolution.
    pub fn get_pending_owner(&self, object_id: &ObjectID) -> Option<Address> {
        self.inner
            .lock()
            .pending_resolutions
            .get(object_id)
            .map(|p| p.owner_address.clone())
    }

    /// Check for timed-out pending resolutions and mark them as OwnerDied.
    ///
    /// Returns the list of object IDs that timed out.
    pub fn check_timeouts(&self) -> Vec<ObjectID> {
        let now = std::time::Instant::now();
        let timed_out_ids: Vec<ObjectID>;
        {
            let inner = self.inner.lock();
            timed_out_ids = inner
                .pending_resolutions
                .iter()
                .filter(|(_, p)| now.duration_since(p.created_at) >= self.resolution_timeout)
                .map(|(oid, _)| *oid)
                .collect();
        }

        // Process each timed-out resolution as OwnerDied.
        for oid in &timed_out_ids {
            self.process_resolved_object(ResolutionReply {
                object_id: *oid,
                status: ObjectStatus::OwnerDied,
                data: None,
                metadata: None,
                nested_refs: Vec::new(),
                node_id: None,
                object_size: 0,
            });
        }

        // Update timeout counter.
        if !timed_out_ids.is_empty() {
            let mut inner = self.inner.lock();
            inner.total_timed_out += timed_out_ids.len() as u64;
        }

        timed_out_ids
    }

    /// Get the configured resolution timeout.
    pub fn resolution_timeout(&self) -> Duration {
        self.resolution_timeout
    }

    /// Statistics about resolution activity: (total_resolved, total_failed).
    pub fn stats(&self) -> (u64, u64) {
        let inner = self.inner.lock();
        (inner.total_resolved, inner.total_failed)
    }

    /// Extended statistics: (total_resolved, total_failed, total_timed_out).
    pub fn stats_extended(&self) -> (u64, u64, u64) {
        let inner = self.inner.lock();
        (
            inner.total_resolved,
            inner.total_failed,
            inner.total_timed_out,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_address() -> Address {
        Address {
            node_id: vec![0u8; 28],
            ip_address: "127.0.0.1".to_string(),
            port: 1234,
            worker_id: vec![0u8; 28],
        }
    }

    fn make_resolver() -> FutureResolver {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let rc = Arc::new(ReferenceCounter::new());
        FutureResolver::new(store, rc)
    }

    #[test]
    fn test_resolve_object_created_inline() {
        let resolver = make_resolver();
        let oid = ObjectID::from_random();

        let mut rx = resolver.resolve_object_async(oid, make_address());
        assert!(resolver.is_resolving(&oid));
        assert_eq!(resolver.num_pending(), 1);

        // Simulate owner responding with inline data.
        resolver.process_resolved_object(ResolutionReply {
            object_id: oid,
            status: ObjectStatus::Created,
            data: Some(Bytes::from("hello")),
            metadata: Some(Bytes::from("meta")),
            nested_refs: Vec::new(),
            node_id: None,
            object_size: 5,
        });

        assert!(!resolver.is_resolving(&oid));
        assert_eq!(resolver.stats(), (1, 0));

        // Waiter should receive the result.
        let result = rx.try_recv().unwrap();
        assert_eq!(result.status, ObjectStatus::Created);
        assert_eq!(result.object.as_ref().unwrap().data.as_ref(), b"hello");
    }

    #[test]
    fn test_resolve_object_out_of_scope() {
        let resolver = make_resolver();
        let oid = ObjectID::from_random();

        let mut rx = resolver.resolve_object_async(oid, make_address());

        resolver.process_resolved_object(ResolutionReply {
            object_id: oid,
            status: ObjectStatus::OutOfScope,
            data: None,
            metadata: None,
            nested_refs: Vec::new(),
            node_id: None,
            object_size: 0,
        });

        let result = rx.try_recv().unwrap();
        assert_eq!(result.status, ObjectStatus::OutOfScope);
        assert!(result.object.is_none());
        assert_eq!(resolver.stats(), (0, 1));
    }

    #[test]
    fn test_resolve_object_owner_died() {
        let resolver = make_resolver();
        let oid = ObjectID::from_random();

        let mut rx = resolver.resolve_object_async(oid, make_address());

        resolver.process_resolved_object(ResolutionReply {
            object_id: oid,
            status: ObjectStatus::OwnerDied,
            data: None,
            metadata: None,
            nested_refs: Vec::new(),
            node_id: None,
            object_size: 0,
        });

        let result = rx.try_recv().unwrap();
        assert_eq!(result.status, ObjectStatus::OwnerDied);
        assert_eq!(resolver.stats(), (0, 1));
    }

    #[test]
    fn test_resolve_waiting_for_creation_keeps_pending() {
        let resolver = make_resolver();
        let oid = ObjectID::from_random();

        let _rx = resolver.resolve_object_async(oid, make_address());

        // WaitingForCreation should NOT notify waiters.
        resolver.process_resolved_object(ResolutionReply {
            object_id: oid,
            status: ObjectStatus::WaitingForCreation,
            data: None,
            metadata: None,
            nested_refs: Vec::new(),
            node_id: None,
            object_size: 0,
        });

        assert!(resolver.is_resolving(&oid));
        assert_eq!(resolver.num_pending(), 1);
    }

    #[test]
    fn test_multiple_waiters_for_same_object() {
        let resolver = make_resolver();
        let oid = ObjectID::from_random();

        let mut rx1 = resolver.resolve_object_async(oid, make_address());
        let mut rx2 = resolver.resolve_object_async(oid, make_address());
        assert_eq!(resolver.num_pending(), 1); // Same object, single entry.

        resolver.process_resolved_object(ResolutionReply {
            object_id: oid,
            status: ObjectStatus::Created,
            data: Some(Bytes::from("data")),
            metadata: None,
            nested_refs: Vec::new(),
            node_id: None,
            object_size: 4,
        });

        // Both waiters should receive the result.
        let r1 = rx1.try_recv().unwrap();
        let r2 = rx2.try_recv().unwrap();
        assert_eq!(r1.status, ObjectStatus::Created);
        assert_eq!(r2.status, ObjectStatus::Created);
    }

    #[test]
    fn test_resolve_stores_in_memory_store() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let rc = Arc::new(ReferenceCounter::new());
        let resolver = FutureResolver::new(store.clone(), rc);

        let oid = ObjectID::from_random();
        let _rx = resolver.resolve_object_async(oid, make_address());

        resolver.process_resolved_object(ResolutionReply {
            object_id: oid,
            status: ObjectStatus::Created,
            data: Some(Bytes::from("stored_data")),
            metadata: Some(Bytes::from("stored_meta")),
            nested_refs: Vec::new(),
            node_id: None,
            object_size: 11,
        });

        // Object should be in the memory store.
        let obj = store.get(&oid).unwrap();
        assert_eq!(obj.data.as_ref(), b"stored_data");
        assert_eq!(obj.metadata.as_ref(), b"stored_meta");
    }

    #[test]
    fn test_resolve_error_stores_error_marker() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let rc = Arc::new(ReferenceCounter::new());
        let resolver = FutureResolver::new(store.clone(), rc);

        let oid = ObjectID::from_random();
        let _rx = resolver.resolve_object_async(oid, make_address());

        resolver.process_resolved_object(ResolutionReply {
            object_id: oid,
            status: ObjectStatus::OwnerDied,
            data: None,
            metadata: None,
            nested_refs: Vec::new(),
            node_id: None,
            object_size: 0,
        });

        // Error marker should be in the memory store.
        let obj = store.get(&oid).unwrap();
        assert_eq!(obj.metadata.as_ref(), b"ERROR");
    }

    #[test]
    fn test_get_pending_owner() {
        let resolver = make_resolver();
        let oid = ObjectID::from_random();
        let addr = Address {
            ip_address: "10.0.0.5".to_string(),
            port: 9999,
            ..make_address()
        };

        let _rx = resolver.resolve_object_async(oid, addr.clone());

        let owner = resolver.get_pending_owner(&oid).unwrap();
        assert_eq!(owner.ip_address, "10.0.0.5");
        assert_eq!(owner.port, 9999);

        // Non-pending object returns None.
        let other = ObjectID::from_random();
        assert!(resolver.get_pending_owner(&other).is_none());
    }

    #[test]
    fn test_resolution_timeout_marks_owner_died() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let rc = Arc::new(ReferenceCounter::new());
        // Use a very short timeout for testing.
        let resolver =
            FutureResolver::with_timeout(store.clone(), rc, Duration::from_millis(10));

        let oid = ObjectID::from_random();
        let mut rx = resolver.resolve_object_async(oid, make_address());

        assert!(resolver.is_resolving(&oid));

        // Wait for the timeout to expire.
        std::thread::sleep(Duration::from_millis(50));

        // Check timeouts — should mark the object as OwnerDied.
        let timed_out = resolver.check_timeouts();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], oid);

        // The pending resolution should be resolved now.
        assert!(!resolver.is_resolving(&oid));

        // Waiter should receive OwnerDied.
        let result = rx.try_recv().unwrap();
        assert_eq!(result.status, ObjectStatus::OwnerDied);

        // Stats should reflect the timeout.
        let (resolved, failed, timed_out_count) = resolver.stats_extended();
        assert_eq!(resolved, 0);
        assert_eq!(failed, 1); // OwnerDied counts as failed
        assert_eq!(timed_out_count, 1);

        // Error marker should be in the memory store.
        let obj = store.get(&oid).unwrap();
        assert_eq!(obj.metadata.as_ref(), b"ERROR");
    }

    #[test]
    fn test_resolution_timeout_does_not_expire_early() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let rc = Arc::new(ReferenceCounter::new());
        // Use a long timeout.
        let resolver =
            FutureResolver::with_timeout(store, rc, Duration::from_secs(300));

        let oid = ObjectID::from_random();
        let _rx = resolver.resolve_object_async(oid, make_address());

        // Check timeouts immediately — nothing should expire.
        let timed_out = resolver.check_timeouts();
        assert!(timed_out.is_empty());
        assert!(resolver.is_resolving(&oid));
    }

    #[test]
    fn test_default_timeout() {
        let resolver = make_resolver();
        assert_eq!(resolver.resolution_timeout(), FutureResolver::DEFAULT_TIMEOUT);
        assert_eq!(resolver.resolution_timeout(), Duration::from_secs(30));
    }

    #[test]
    fn test_resolve_with_locality() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let rc = Arc::new(ReferenceCounter::new());
        let resolver = FutureResolver::new(store, rc.clone());

        let oid = ObjectID::from_random();
        // Must have a reference for location tracking to work.
        rc.add_local_reference(oid);

        let mut rx = resolver.resolve_object_async(oid, make_address());

        let node_id = vec![42u8; 28];
        resolver.process_resolved_object(ResolutionReply {
            object_id: oid,
            status: ObjectStatus::Created,
            data: Some(Bytes::from("data")),
            metadata: None,
            nested_refs: Vec::new(),
            node_id: Some(node_id.clone()),
            object_size: 4,
        });

        let result = rx.try_recv().unwrap();
        assert_eq!(result.node_id, Some(node_id.clone()));

        // Location should be tracked in reference counter.
        let locs = rc.get_object_locations(&oid);
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0], hex::encode(&node_id));
    }
}
