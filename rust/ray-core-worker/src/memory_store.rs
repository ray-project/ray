// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! In-process object store for the core worker.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::Notify;

use ray_common::id::ObjectID;

use crate::error::{CoreWorkerError, CoreWorkerResult};

/// A Ray object stored in memory.
#[derive(Debug, Clone)]
pub struct RayObject {
    pub data: Bytes,
    pub metadata: Bytes,
    pub nested_refs: Vec<ObjectID>,
}

impl RayObject {
    pub fn new(data: Bytes, metadata: Bytes, nested_refs: Vec<ObjectID>) -> Self {
        Self {
            data,
            metadata,
            nested_refs,
        }
    }

    /// Create a simple data-only object.
    pub fn from_data(data: Bytes) -> Self {
        Self::new(data, Bytes::new(), Vec::new())
    }
}

/// Thread-safe in-process memory store for Ray objects.
pub struct CoreWorkerMemoryStore {
    objects: Mutex<HashMap<ObjectID, RayObject>>,
    /// Notification channel for async waiters.
    notify: Arc<Notify>,
}

impl CoreWorkerMemoryStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Put an object into the store. Returns error if the object already exists.
    pub fn put(&self, object_id: ObjectID, object: RayObject) -> CoreWorkerResult<()> {
        let mut store = self.objects.lock();
        if store.contains_key(&object_id) {
            return Err(CoreWorkerError::ObjectAlreadyExists(object_id.hex()));
        }
        store.insert(object_id, object);
        // Wake up any waiters.
        self.notify.notify_waiters();
        Ok(())
    }

    /// Get an object from the store. Returns `None` if not found.
    pub fn get(&self, object_id: &ObjectID) -> Option<RayObject> {
        self.objects.lock().get(object_id).cloned()
    }

    /// Delete an object from the store.
    pub fn delete(&self, object_id: &ObjectID) -> bool {
        self.objects.lock().remove(object_id).is_some()
    }

    /// Check if an object exists in the store.
    pub fn contains(&self, object_id: &ObjectID) -> bool {
        self.objects.lock().contains_key(object_id)
    }

    /// Number of objects in the store.
    pub fn size(&self) -> usize {
        self.objects.lock().len()
    }

    /// Return all object IDs currently in the store.
    pub fn all_object_ids(&self) -> Vec<ObjectID> {
        self.objects.lock().keys().copied().collect()
    }

    /// Get an object, or wait for it to arrive, up to a timeout.
    ///
    /// Returns `Ok(object)` if found, or `Err(TimedOut)` if the timeout expires.
    pub async fn get_or_wait(
        &self,
        object_id: &ObjectID,
        timeout: Duration,
    ) -> CoreWorkerResult<RayObject> {
        // Fast path: check immediately.
        if let Some(obj) = self.get(object_id) {
            return Ok(obj);
        }

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(CoreWorkerError::TimedOut(format!(
                    "waiting for object {}",
                    object_id.hex()
                )));
            }
            // Wait for a notification or timeout.
            match tokio::time::timeout(remaining, self.notify.notified()).await {
                Ok(()) => {
                    if let Some(obj) = self.get(object_id) {
                        return Ok(obj);
                    }
                    // Spurious wake or different object arrived — loop again.
                }
                Err(_) => {
                    // Final check before returning timeout.
                    if let Some(obj) = self.get(object_id) {
                        return Ok(obj);
                    }
                    return Err(CoreWorkerError::TimedOut(format!(
                        "waiting for object {}",
                        object_id.hex()
                    )));
                }
            }
        }
    }
}

impl Default for CoreWorkerMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_object(data: &[u8]) -> RayObject {
        RayObject::from_data(Bytes::copy_from_slice(data))
    }

    #[test]
    fn test_put_and_get() {
        let store = CoreWorkerMemoryStore::new();
        let oid = ObjectID::from_random();
        let obj = make_object(b"hello");
        store.put(oid, obj).unwrap();
        let got = store.get(&oid).unwrap();
        assert_eq!(got.data.as_ref(), b"hello");
    }

    #[test]
    fn test_put_and_get_with_metadata_and_refs() {
        let store = CoreWorkerMemoryStore::new();
        let oid = ObjectID::from_random();
        let nested = ObjectID::from_random();
        let obj = RayObject::new(Bytes::from("payload"), Bytes::from("msgpack"), vec![nested]);
        store.put(oid, obj).unwrap();
        let got = store.get(&oid).unwrap();
        assert_eq!(got.data.as_ref(), b"payload");
        assert_eq!(got.metadata.as_ref(), b"msgpack");
        assert_eq!(got.nested_refs.len(), 1);
        assert_eq!(got.nested_refs[0], nested);
    }

    #[test]
    fn test_get_nonexistent_returns_none() {
        let store = CoreWorkerMemoryStore::new();
        assert!(store.get(&ObjectID::from_random()).is_none());
    }

    #[test]
    fn test_distinct_objects_not_confused() {
        let store = CoreWorkerMemoryStore::new();
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();
        store.put(oid1, make_object(b"first")).unwrap();
        store.put(oid2, make_object(b"second")).unwrap();
        assert_eq!(store.get(&oid1).unwrap().data.as_ref(), b"first");
        assert_eq!(store.get(&oid2).unwrap().data.as_ref(), b"second");
    }

    #[test]
    fn test_size_after_delete() {
        let store = CoreWorkerMemoryStore::new();
        let oid = ObjectID::from_random();
        store.put(oid, make_object(b"x")).unwrap();
        assert_eq!(store.size(), 1);
        store.delete(&oid);
        assert_eq!(store.size(), 0);
    }

    #[test]
    fn test_duplicate_put_errors() {
        let store = CoreWorkerMemoryStore::new();
        let oid = ObjectID::from_random();
        store.put(oid, make_object(b"a")).unwrap();
        let err = store.put(oid, make_object(b"b")).unwrap_err();
        assert!(matches!(err, CoreWorkerError::ObjectAlreadyExists(_)));
    }

    #[test]
    fn test_delete() {
        let store = CoreWorkerMemoryStore::new();
        let oid = ObjectID::from_random();
        store.put(oid, make_object(b"x")).unwrap();
        assert!(store.delete(&oid));
        assert!(!store.delete(&oid));
        assert!(store.get(&oid).is_none());
    }

    #[test]
    fn test_contains() {
        let store = CoreWorkerMemoryStore::new();
        let oid = ObjectID::from_random();
        assert!(!store.contains(&oid));
        store.put(oid, make_object(b"y")).unwrap();
        assert!(store.contains(&oid));
    }

    #[test]
    fn test_size() {
        let store = CoreWorkerMemoryStore::new();
        assert_eq!(store.size(), 0);
        store
            .put(ObjectID::from_random(), make_object(b"a"))
            .unwrap();
        store
            .put(ObjectID::from_random(), make_object(b"b"))
            .unwrap();
        assert_eq!(store.size(), 2);
    }

    #[tokio::test]
    async fn test_get_or_wait_immediate() {
        let store = CoreWorkerMemoryStore::new();
        let oid = ObjectID::from_random();
        store.put(oid, make_object(b"ready")).unwrap();
        let obj = store
            .get_or_wait(&oid, Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(obj.data.as_ref(), b"ready");
    }

    #[tokio::test]
    async fn test_get_or_wait_timeout() {
        let store = CoreWorkerMemoryStore::new();
        let oid = ObjectID::from_random();
        let result = store.get_or_wait(&oid, Duration::from_millis(50)).await;
        assert!(matches!(result, Err(CoreWorkerError::TimedOut(_))));
    }

    #[tokio::test]
    async fn test_get_or_wait_async_arrival() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid = ObjectID::from_random();

        let store2 = store.clone();
        let oid2 = oid;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            store2.put(oid2, make_object(b"arrived")).unwrap();
        });

        let obj = store
            .get_or_wait(&oid, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(obj.data.as_ref(), b"arrived");
    }

    // ─── Ported from C++ memory_store_test.cc ────────────────────────

    #[test]
    fn test_put_get_delete_stats() {
        // Port of TestMemoryStoreStats: put objects, delete some, verify size tracks.
        let store = CoreWorkerMemoryStore::new();
        let id1 = ObjectID::from_random();
        let id2 = ObjectID::from_random();
        let id3 = ObjectID::from_random();

        store.put(id1, make_object(b"aaa")).unwrap();
        store.put(id2, make_object(b"bbb")).unwrap();
        store.put(id3, make_object(b"ccc")).unwrap();
        assert_eq!(store.size(), 3);

        // Delete one.
        store.delete(&id3);
        assert_eq!(store.size(), 2);
        assert!(store.contains(&id1));
        assert!(store.contains(&id2));
        assert!(!store.contains(&id3));

        // Delete remaining.
        store.delete(&id1);
        store.delete(&id2);
        assert_eq!(store.size(), 0);

        // Re-put after delete should succeed.
        store.put(id1, make_object(b"new1")).unwrap();
        store.put(id2, make_object(b"new2")).unwrap();
        store.put(id3, make_object(b"new3")).unwrap();
        assert_eq!(store.size(), 3);
    }

    #[test]
    fn test_all_object_ids() {
        let store = CoreWorkerMemoryStore::new();
        let id1 = ObjectID::from_random();
        let id2 = ObjectID::from_random();
        store.put(id1, make_object(b"x")).unwrap();
        store.put(id2, make_object(b"y")).unwrap();

        let ids = store.all_object_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }

    #[test]
    fn test_delete_nonexistent_returns_false() {
        let store = CoreWorkerMemoryStore::new();
        assert!(!store.delete(&ObjectID::from_random()));
    }

    #[tokio::test]
    async fn test_get_or_wait_multiple_objects_different_arrival_times() {
        // Port of TestWaitWithWaiting: objects arriving at different times.
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let oid1 = ObjectID::from_random();
        let oid2 = ObjectID::from_random();

        // oid1 is put immediately.
        store.put(oid1, make_object(b"first")).unwrap();

        // oid2 arrives after a delay.
        let store2 = store.clone();
        let oid2_copy = oid2;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            store2.put(oid2_copy, make_object(b"second")).unwrap();
        });

        // oid1 should be immediately available.
        let obj1 = store
            .get_or_wait(&oid1, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(obj1.data.as_ref(), b"first");

        // oid2 should arrive within timeout.
        let obj2 = store
            .get_or_wait(&oid2, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(obj2.data.as_ref(), b"second");
    }

    #[tokio::test]
    async fn test_get_or_wait_spurious_wake() {
        // Simulate a spurious wake: another object is put but we are waiting for a different one.
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let target = ObjectID::from_random();
        let other = ObjectID::from_random();

        let store2 = store.clone();
        let target_copy = target;
        tokio::spawn(async move {
            // First put a different object (spurious wake).
            tokio::time::sleep(Duration::from_millis(10)).await;
            store2.put(other, make_object(b"other")).unwrap();
            // Then put the target.
            tokio::time::sleep(Duration::from_millis(20)).await;
            store2.put(target_copy, make_object(b"target")).unwrap();
        });

        let obj = store
            .get_or_wait(&target, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(obj.data.as_ref(), b"target");
    }

    #[test]
    fn test_put_many_objects_and_verify() {
        // Port of TestObjectAllocator: put many objects and verify counts.
        let store = CoreWorkerMemoryStore::new();
        let count = 100;
        let mut ids = Vec::new();
        for i in 0..count {
            let oid = ObjectID::from_random();
            let data = format!("data_{}", i);
            store
                .put(oid, RayObject::from_data(Bytes::from(data)))
                .unwrap();
            ids.push(oid);
        }
        assert_eq!(store.size(), count);

        // Verify each object is retrievable.
        for (i, oid) in ids.iter().enumerate() {
            let obj = store.get(oid).unwrap();
            let expected = format!("data_{}", i);
            assert_eq!(obj.data.as_ref(), expected.as_bytes());
        }
    }

    #[test]
    fn test_default_impl() {
        let store = CoreWorkerMemoryStore::default();
        assert_eq!(store.size(), 0);
    }

    #[test]
    fn test_object_with_empty_data() {
        let store = CoreWorkerMemoryStore::new();
        let oid = ObjectID::from_random();
        let obj = RayObject::from_data(Bytes::new());
        store.put(oid, obj).unwrap();
        let got = store.get(&oid).unwrap();
        assert!(got.data.is_empty());
    }

    // ── Additional tests ported from memory_store_test.cc ───────────

    /// Port of TestReportUnhandledErrors concept: put, then delete
    /// without get should work without panic.
    #[test]
    fn test_put_delete_without_get() {
        let store = CoreWorkerMemoryStore::new();
        let id1 = ObjectID::from_random();
        let id2 = ObjectID::from_random();

        store.put(id1, make_object(b"error1")).unwrap();
        store.put(id2, make_object(b"error2")).unwrap();

        // Delete without getting.
        assert!(store.delete(&id1));
        assert!(store.delete(&id2));
        assert_eq!(store.size(), 0);
    }

    /// Port of TestReportUnhandledErrors: put, get, then delete
    /// should succeed without issues.
    #[test]
    fn test_put_get_then_delete() {
        let store = CoreWorkerMemoryStore::new();
        let id1 = ObjectID::from_random();
        let id2 = ObjectID::from_random();

        store.put(id1, make_object(b"data1")).unwrap();
        store.put(id2, make_object(b"data2")).unwrap();

        // Get both.
        let _obj1 = store.get(&id1).unwrap();
        let _obj2 = store.get(&id2).unwrap();

        // Delete both.
        store.delete(&id1);
        store.delete(&id2);
        assert_eq!(store.size(), 0);
    }

    /// Port of TestWaitNoWaiting: multiple objects, some immediately
    /// available, some not.
    #[tokio::test]
    async fn test_wait_no_waiting_mixed_availability() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let id1 = ObjectID::from_random();
        let id2 = ObjectID::from_random();
        let id3 = ObjectID::from_random();

        // id1 and id3 are immediately available.
        store.put(id1, make_object(b"ready1")).unwrap();
        store.put(id3, make_object(b"ready3")).unwrap();

        // id1 and id3 should resolve immediately.
        let obj1 = store
            .get_or_wait(&id1, Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(obj1.data.as_ref(), b"ready1");

        let obj3 = store
            .get_or_wait(&id3, Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(obj3.data.as_ref(), b"ready3");

        // id2 should time out.
        let result = store.get_or_wait(&id2, Duration::from_millis(10)).await;
        assert!(matches!(result, Err(CoreWorkerError::TimedOut(_))));
    }

    /// Port of TestWaitTimeout: waiting for more objects than
    /// available should timeout.
    #[tokio::test]
    async fn test_wait_timeout_insufficient_objects() {
        let store = CoreWorkerMemoryStore::new();
        let id1 = ObjectID::from_random();

        // No objects in store.
        let result = store.get_or_wait(&id1, Duration::from_millis(10)).await;
        assert!(matches!(result, Err(CoreWorkerError::TimedOut(_))));
    }

    /// Port of TestObjectAllocator concept: put many objects with
    /// increasing data sizes and verify all are stored correctly.
    #[test]
    fn test_many_objects_with_varying_sizes() {
        let store = CoreWorkerMemoryStore::new();
        let mut ids_and_sizes = Vec::new();

        for i in 1..=50 {
            let oid = ObjectID::from_random();
            let data = vec![i as u8; i * 10];
            let obj = RayObject::from_data(Bytes::from(data.clone()));
            store.put(oid, obj).unwrap();
            ids_and_sizes.push((oid, data));
        }

        assert_eq!(store.size(), 50);

        // Verify each object.
        for (oid, expected_data) in &ids_and_sizes {
            let obj = store.get(oid).unwrap();
            assert_eq!(obj.data.as_ref(), expected_data.as_slice());
        }
    }

    /// Port of TestWaitWithWaiting: objects arriving at different
    /// times, parallel waiting.
    #[tokio::test]
    async fn test_parallel_wait_different_objects() {
        let store = Arc::new(CoreWorkerMemoryStore::new());
        let id1 = ObjectID::from_random();
        let id2 = ObjectID::from_random();

        let store2 = store.clone();
        let id1_copy = id1;
        let id2_copy = id2;

        // Spawn a background task that puts objects with delays.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            store2.put(id1_copy, make_object(b"first")).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            store2.put(id2_copy, make_object(b"second")).unwrap();
        });

        // Wait for both in parallel.
        let store_a = store.clone();
        let store_b = store.clone();

        let h1 =
            tokio::spawn(async move { store_a.get_or_wait(&id1, Duration::from_secs(2)).await });
        let h2 =
            tokio::spawn(async move { store_b.get_or_wait(&id2, Duration::from_secs(2)).await });

        let r1 = h1.await.unwrap().unwrap();
        let r2 = h2.await.unwrap().unwrap();
        assert_eq!(r1.data.as_ref(), b"first");
        assert_eq!(r2.data.as_ref(), b"second");
    }

    /// Port of TestMemoryStoreStats concept: re-putting after delete
    /// should work for the same object IDs.
    #[test]
    fn test_reput_after_delete_cycle() {
        let store = CoreWorkerMemoryStore::new();
        let ids: Vec<_> = (0..5).map(|_| ObjectID::from_random()).collect();

        // Put all.
        for (i, oid) in ids.iter().enumerate() {
            store
                .put(*oid, make_object(format!("round1_{}", i).as_bytes()))
                .unwrap();
        }
        assert_eq!(store.size(), 5);

        // Delete all.
        for oid in &ids {
            store.delete(oid);
        }
        assert_eq!(store.size(), 0);

        // Re-put with different data.
        for (i, oid) in ids.iter().enumerate() {
            store
                .put(*oid, make_object(format!("round2_{}", i).as_bytes()))
                .unwrap();
        }
        assert_eq!(store.size(), 5);

        // Verify new data.
        for (i, oid) in ids.iter().enumerate() {
            let obj = store.get(oid).unwrap();
            assert_eq!(obj.data.as_ref(), format!("round2_{}", i).as_bytes());
        }
    }

    /// Test that RayObject metadata and nested refs are preserved.
    #[test]
    fn test_ray_object_full_fields() {
        let nested1 = ObjectID::from_random();
        let nested2 = ObjectID::from_random();
        let obj = RayObject::new(
            Bytes::from("payload_data"),
            Bytes::from("application/msgpack"),
            vec![nested1, nested2],
        );

        assert_eq!(obj.data.as_ref(), b"payload_data");
        assert_eq!(obj.metadata.as_ref(), b"application/msgpack");
        assert_eq!(obj.nested_refs.len(), 2);
        assert_eq!(obj.nested_refs[0], nested1);
        assert_eq!(obj.nested_refs[1], nested2);
    }
}
