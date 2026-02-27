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
            return Err(CoreWorkerError::ObjectAlreadyExists(
                object_id.hex(),
            ));
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
}
