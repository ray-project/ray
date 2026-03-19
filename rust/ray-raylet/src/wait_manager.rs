// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Wait manager — handles `ray.wait()` requests.
//!
//! Replaces `src/ray/raylet/wait_manager.h/cc`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use ray_common::id::ObjectID;
use tokio::sync::oneshot;

use std::fmt;
use std::sync::Arc;

/// Callback for wait completion: (ready_ids, remaining_ids).
type WaitCallback = oneshot::Sender<(Vec<ObjectID>, Vec<ObjectID>)>;

/// Errors returned by `WaitManager::wait()`.
#[derive(Debug, Clone)]
pub enum WaitError {
    /// A duplicate object ID was found in the request.
    DuplicateObjectId(ObjectID),
    /// `num_required` is out of bounds.
    NumRequiredOutOfBounds {
        num_required: usize,
        num_objects: usize,
    },
}

impl fmt::Display for WaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaitError::DuplicateObjectId(oid) => {
                write!(f, "duplicate object ID in wait request: {:?}", oid)
            }
            WaitError::NumRequiredOutOfBounds {
                num_required,
                num_objects,
            } => write!(
                f,
                "num_required ({}) out of bounds for {} objects",
                num_required, num_objects
            ),
        }
    }
}

impl std::error::Error for WaitError {}

/// A pending wait request.
struct WaitRequest {
    /// Object IDs being waited on.
    object_ids: Vec<ObjectID>,
    /// How many need to be ready before completion.
    num_required: usize,
    /// Objects that are already ready.
    ready: HashSet<ObjectID>,
    /// Completion callback.
    callback: Option<WaitCallback>,
    /// Timeout handle (cancel on completion).
    timeout_cancel: Option<oneshot::Sender<()>>,
}

/// The wait manager handles `ray.wait()` for local objects.
pub struct WaitManager {
    /// Active wait requests (wrapped in Arc for sharing with timeout tasks).
    requests: Arc<Mutex<HashMap<u64, WaitRequest>>>,
    /// Reverse index: object_id → set of wait_ids (wrapped in Arc for sharing).
    object_to_waits: Arc<Mutex<HashMap<ObjectID, HashSet<u64>>>>,
    /// Next wait ID.
    next_wait_id: AtomicU64,
}

impl WaitManager {
    pub fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(HashMap::new())),
            object_to_waits: Arc::new(Mutex::new(HashMap::new())),
            next_wait_id: AtomicU64::new(1),
        }
    }

    /// Get a clone of the requests Arc (for spawned timeout tasks).
    fn requests_arc(&self) -> Arc<Mutex<HashMap<u64, WaitRequest>>> {
        Arc::clone(&self.requests)
    }

    /// Get a clone of the object_to_waits Arc (for spawned timeout tasks).
    fn object_to_waits_arc(&self) -> Arc<Mutex<HashMap<ObjectID, HashSet<u64>>>> {
        Arc::clone(&self.object_to_waits)
    }

    /// Static version of complete_wait that operates on the Arc references directly.
    /// Used by spawned timeout tasks.
    fn complete_wait_static(
        requests: &Mutex<HashMap<u64, WaitRequest>>,
        object_to_waits: &Mutex<HashMap<ObjectID, HashSet<u64>>>,
        wait_id: u64,
    ) {
        let request = requests.lock().remove(&wait_id);
        if let Some(mut req) = request {
            // Clean up reverse index
            {
                let mut obj_to_waits = object_to_waits.lock();
                for oid in &req.object_ids {
                    if let Some(waits) = obj_to_waits.get_mut(oid) {
                        waits.remove(&wait_id);
                        if waits.is_empty() {
                            obj_to_waits.remove(oid);
                        }
                    }
                }
            }

            if let Some(callback) = req.callback.take() {
                let ready: Vec<ObjectID> = req.ready.iter().copied().collect();
                let remaining: Vec<ObjectID> = req
                    .object_ids
                    .iter()
                    .filter(|id| !req.ready.contains(id))
                    .copied()
                    .collect();
                let _ = callback.send((ready, remaining));
            }

            // Note: we do NOT send on timeout_cancel here — this IS the timeout firing.
        }
    }

    /// Register a wait request. Returns a receiver that will fire on completion.
    ///
    /// # Errors
    /// Returns `Err` (with the receiver already resolved to an error) if:
    /// - `object_ids` contains duplicates
    /// - `num_required` is 0 or greater than `object_ids.len()`
    ///
    /// (num_required == 0 is allowed for backward compatibility when all objects
    /// happen to be ready, but the caller should typically pass >= 1.)
    pub fn wait(
        &self,
        object_ids: Vec<ObjectID>,
        num_required: usize,
        timeout_ms: Option<u64>,
        is_object_local: impl Fn(&ObjectID) -> bool,
    ) -> Result<oneshot::Receiver<(Vec<ObjectID>, Vec<ObjectID>)>, WaitError> {
        // Validate: reject duplicate object IDs.
        {
            let mut seen = HashSet::with_capacity(object_ids.len());
            for oid in &object_ids {
                if !seen.insert(*oid) {
                    return Err(WaitError::DuplicateObjectId(*oid));
                }
            }
        }

        // Validate: num_required must be in (0, num_objects].
        // We allow num_required == 0 only as a degenerate case (completes immediately).
        if num_required > object_ids.len() {
            return Err(WaitError::NumRequiredOutOfBounds {
                num_required,
                num_objects: object_ids.len(),
            });
        }

        let wait_id = self.next_wait_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();

        // Check which objects are already local
        let mut ready = HashSet::new();
        for oid in &object_ids {
            if is_object_local(oid) {
                ready.insert(*oid);
            }
        }

        // Register reverse index for non-ready objects
        {
            let mut obj_to_waits = self.object_to_waits.lock();
            for oid in &object_ids {
                if !ready.contains(oid) {
                    obj_to_waits.entry(*oid).or_default().insert(wait_id);
                }
            }
        }

        // Check if already satisfied
        if ready.len() >= num_required {
            // Clean up reverse index for objects we just registered
            {
                let mut obj_to_waits = self.object_to_waits.lock();
                for oid in &object_ids {
                    if let Some(waits) = obj_to_waits.get_mut(oid) {
                        waits.remove(&wait_id);
                        if waits.is_empty() {
                            obj_to_waits.remove(oid);
                        }
                    }
                }
            }
            let remaining: Vec<ObjectID> = object_ids
                .iter()
                .filter(|id| !ready.contains(id))
                .copied()
                .collect();
            let _ = tx.send((ready.into_iter().collect(), remaining));
            return Ok(rx);
        }

        // Set up timeout if specified — spawn a tokio task that fires complete_wait.
        let timeout_cancel = timeout_ms.map(|ms| {
            let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
            // We need a reference to `self` in the spawned task, but `self` is
            // behind a non-'static reference. Instead, we capture the Arcs via
            // a small helper that takes the two Mutexes + wait_id.
            let requests = self.requests_arc();
            let object_to_waits = self.object_to_waits_arc();
            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(ms)) => {
                        // Timeout fired — force-complete the wait.
                        Self::complete_wait_static(&requests, &object_to_waits, wait_id);
                    }
                    _ = cancel_rx => {
                        // Wait was completed before timeout — nothing to do.
                    }
                }
            });
            cancel_tx
        });

        let request = WaitRequest {
            object_ids,
            num_required,
            ready,
            callback: Some(tx),
            timeout_cancel,
        };

        self.requests.lock().insert(wait_id, request);
        Ok(rx)
    }

    /// Notify that an object has become local.
    pub fn handle_object_local(&self, object_id: &ObjectID) {
        let wait_ids = {
            let mut obj_to_waits = self.object_to_waits.lock();
            obj_to_waits.remove(object_id).unwrap_or_default()
        };

        let mut to_complete = Vec::new();

        {
            let mut requests = self.requests.lock();
            for wait_id in wait_ids {
                if let Some(req) = requests.get_mut(&wait_id) {
                    req.ready.insert(*object_id);
                    if req.ready.len() >= req.num_required {
                        to_complete.push(wait_id);
                    }
                }
            }
        }

        for wait_id in to_complete {
            self.complete_wait(wait_id);
        }
    }

    /// Force-complete a wait (e.g., on timeout or manual trigger).
    pub fn complete_wait(&self, wait_id: u64) {
        let request = self.requests.lock().remove(&wait_id);
        if let Some(mut req) = request {
            // Clean up reverse index
            {
                let mut obj_to_waits = self.object_to_waits.lock();
                for oid in &req.object_ids {
                    if let Some(waits) = obj_to_waits.get_mut(oid) {
                        waits.remove(&wait_id);
                        if waits.is_empty() {
                            obj_to_waits.remove(oid);
                        }
                    }
                }
            }

            if let Some(callback) = req.callback.take() {
                let ready: Vec<ObjectID> = req.ready.iter().copied().collect();
                let remaining: Vec<ObjectID> = req
                    .object_ids
                    .iter()
                    .filter(|id| !req.ready.contains(id))
                    .copied()
                    .collect();
                let _ = callback.send((ready, remaining));
            }

            // Cancel timeout if any
            if let Some(cancel) = req.timeout_cancel.take() {
                let _ = cancel.send(());
            }
        }
    }

    /// Number of active wait requests.
    pub fn num_pending_waits(&self) -> usize {
        self.requests.lock().len()
    }
}

impl Default for WaitManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_object_id(id: u8) -> ObjectID {
        let mut bytes = [0u8; 28];
        bytes[0] = id;
        ObjectID::from_binary(&bytes)
    }

    #[test]
    fn test_wait_already_local() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);
        let oid2 = make_object_id(2);

        let mut rx = mgr.wait(
            vec![oid1, oid2],
            1,
            None,
            |oid| *oid == oid1, // oid1 is local
        ).unwrap();

        // Should complete immediately since num_required=1 and oid1 is local
        let (ready, remaining) = rx.try_recv().unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_wait_for_object() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);
        let oid2 = make_object_id(2);

        let mut rx = mgr.wait(
            vec![oid1, oid2],
            2,
            None,
            |_| false, // nothing local yet
        ).unwrap();

        assert!(rx.try_recv().is_err()); // not ready yet
        assert_eq!(mgr.num_pending_waits(), 1);

        mgr.handle_object_local(&oid1);
        assert!(rx.try_recv().is_err()); // still need 1 more

        mgr.handle_object_local(&oid2);
        let (ready, remaining) = rx.try_recv().unwrap();
        assert_eq!(ready.len(), 2);
        assert_eq!(remaining.len(), 0);
    }

    #[test]
    fn test_force_complete() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);

        let mut rx = mgr.wait(vec![oid1], 1, None, |_| false).unwrap();
        assert_eq!(mgr.num_pending_waits(), 1);

        // Simulate timeout
        mgr.complete_wait(1); // wait_id starts at 1
        let (ready, remaining) = rx.try_recv().unwrap();
        assert_eq!(ready.len(), 0);
        assert_eq!(remaining.len(), 1);
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test timeout expiration: force-completing a wait that has some objects
    /// ready should report partial results (ready + remaining).
    #[tokio::test]
    async fn test_timeout_expiration_partial_results() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);
        let oid2 = make_object_id(2);
        let oid3 = make_object_id(3);

        // Wait for all 3 objects, none local yet
        let mut rx = mgr.wait(vec![oid1, oid2, oid3], 3, Some(1000), |_| false).unwrap();

        // Only oid1 arrives before timeout
        mgr.handle_object_local(&oid1);

        // Still pending — need 3 but only 1 is ready
        assert!(rx.try_recv().is_err());
        assert_eq!(mgr.num_pending_waits(), 1);

        // Simulate timeout by force-completing
        mgr.complete_wait(1);

        let (ready, remaining) = rx.try_recv().unwrap();
        assert_eq!(ready.len(), 1);
        assert!(ready.contains(&oid1));
        assert_eq!(remaining.len(), 2);
        assert!(remaining.contains(&oid2));
        assert!(remaining.contains(&oid3));
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test concurrent wait calls: multiple waits on overlapping objects
    /// should each complete independently when their conditions are met.
    #[test]
    fn test_concurrent_wait_calls() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);
        let oid2 = make_object_id(2);
        let oid3 = make_object_id(3);

        // Wait 1: needs 1 out of {oid1, oid2}
        let mut rx1 = mgr.wait(vec![oid1, oid2], 1, None, |_| false).unwrap();
        // Wait 2: needs 2 out of {oid2, oid3}
        let mut rx2 = mgr.wait(vec![oid2, oid3], 2, None, |_| false).unwrap();

        assert_eq!(mgr.num_pending_waits(), 2);

        // oid1 arrives — satisfies wait1 (needs 1) but not wait2 (doesn't contain oid1)
        mgr.handle_object_local(&oid1);

        let (ready1, remaining1) = rx1.try_recv().unwrap();
        assert_eq!(ready1.len(), 1);
        assert!(ready1.contains(&oid1));
        assert_eq!(remaining1.len(), 1);
        assert!(remaining1.contains(&oid2));

        // wait2 still pending
        assert!(rx2.try_recv().is_err());
        assert_eq!(mgr.num_pending_waits(), 1);

        // oid2 arrives — wait2 now has 1/2
        mgr.handle_object_local(&oid2);
        assert!(rx2.try_recv().is_err());

        // oid3 arrives — wait2 now has 2/2
        mgr.handle_object_local(&oid3);
        let (ready2, remaining2) = rx2.try_recv().unwrap();
        assert_eq!(ready2.len(), 2);
        assert!(ready2.contains(&oid2));
        assert!(ready2.contains(&oid3));
        assert_eq!(remaining2.len(), 0);
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test wait with all objects already available: should complete
    /// immediately without adding to pending waits.
    #[test]
    fn test_wait_all_objects_already_available() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);
        let oid2 = make_object_id(2);
        let oid3 = make_object_id(3);

        // All 3 objects are already local
        let mut rx = mgr.wait(vec![oid1, oid2, oid3], 3, None, |_| true).unwrap();

        // Should complete immediately
        let (ready, remaining) = rx.try_recv().unwrap();
        assert_eq!(ready.len(), 3);
        assert!(ready.contains(&oid1));
        assert!(ready.contains(&oid2));
        assert!(ready.contains(&oid3));
        assert_eq!(remaining.len(), 0);

        // No pending waits should be registered
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    // ─── Additional ports from C++ wait_manager_test.cc ───────────────

    /// Port of TestMultiWaits: two waits on the same object both complete
    /// when the object becomes local.
    #[test]
    fn test_multi_waits_same_object() {
        let mgr = WaitManager::new();
        let obj1 = make_object_id(1);

        let mut rx1 = mgr.wait(vec![obj1], 1, None, |_| false).unwrap();
        let mut rx2 = mgr.wait(vec![obj1], 1, None, |_| false).unwrap();

        assert_eq!(mgr.num_pending_waits(), 2);
        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_err());

        // Object arrives, both waits should complete
        mgr.handle_object_local(&obj1);

        let (ready1, remaining1) = rx1.try_recv().unwrap();
        assert_eq!(ready1.len(), 1);
        assert!(ready1.contains(&obj1));
        assert_eq!(remaining1.len(), 0);

        let (ready2, remaining2) = rx2.try_recv().unwrap();
        assert_eq!(ready2.len(), 1);
        assert!(ready2.contains(&obj1));
        assert_eq!(remaining2.len(), 0);

        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Port of TestWaitTimeout with zero timeout: wait with timeout=0
    /// should complete immediately via force_complete.
    #[tokio::test]
    async fn test_wait_timeout_zero_immediate_completion() {
        let mgr = WaitManager::new();
        let obj1 = make_object_id(1);
        let obj2 = make_object_id(2);

        // Wait with timeout (simulated as immediate force-complete)
        let mut rx = mgr.wait(vec![obj1, obj2], 1, Some(0), |_| false).unwrap();

        // Force complete immediately (simulating timeout=0)
        mgr.complete_wait(1);

        let (ready, remaining) = rx.try_recv().unwrap();
        assert_eq!(ready.len(), 0);
        assert_eq!(remaining.len(), 2);
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: handle_object_local for an object no one is waiting for
    /// should be a no-op.
    #[test]
    fn test_handle_object_local_no_waiters() {
        let mgr = WaitManager::new();
        let obj1 = make_object_id(1);

        // No waits registered
        mgr.handle_object_local(&obj1); // should not panic
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: complete_wait for a non-existent wait_id should be a no-op.
    #[test]
    fn test_complete_nonexistent_wait() {
        let mgr = WaitManager::new();
        mgr.complete_wait(999); // should not panic
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: wait with num_required=0 should complete immediately.
    #[test]
    fn test_wait_zero_required() {
        let mgr = WaitManager::new();
        let obj1 = make_object_id(1);

        let mut rx = mgr.wait(vec![obj1], 0, None, |_| false).unwrap();

        // num_required=0 → ready.len()=0 >= 0 → completes immediately
        let (ready, remaining) = rx.try_recv().unwrap();
        assert_eq!(ready.len(), 0);
        assert_eq!(remaining.len(), 1);
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: handle_object_local called multiple times for same object
    /// should be idempotent.
    #[test]
    fn test_handle_object_local_idempotent() {
        let mgr = WaitManager::new();
        let obj1 = make_object_id(1);

        let _rx = mgr.wait(vec![obj1], 1, None, |_| false).unwrap();
        mgr.handle_object_local(&obj1);
        mgr.handle_object_local(&obj1); // second call should be no-op
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    // ─── RAYLET-11: Validation and timeout tests ─────────────────────

    /// Test: duplicate object IDs in a single wait call should be rejected.
    #[test]
    fn test_reject_duplicate_object_ids() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);

        let result = mgr.wait(vec![oid1, oid1], 1, None, |_| false);
        assert!(result.is_err());
        match result.unwrap_err() {
            WaitError::DuplicateObjectId(oid) => assert_eq!(oid, oid1),
            other => panic!("expected DuplicateObjectId, got {:?}", other),
        }
        // No pending waits should be registered
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: num_required > num_objects should be rejected.
    #[test]
    fn test_reject_num_required_out_of_bounds() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);
        let oid2 = make_object_id(2);

        // num_required=3 but only 2 objects
        let result = mgr.wait(vec![oid1, oid2], 3, None, |_| false);
        assert!(result.is_err());
        match result.unwrap_err() {
            WaitError::NumRequiredOutOfBounds {
                num_required,
                num_objects,
            } => {
                assert_eq!(num_required, 3);
                assert_eq!(num_objects, 2);
            }
            other => panic!("expected NumRequiredOutOfBounds, got {:?}", other),
        }
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: timeout actually fires and completes the wait.
    #[tokio::test]
    async fn test_timeout_fires_automatically() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);

        // Wait with a 50ms timeout, nothing local
        let rx = mgr.wait(vec![oid1], 1, Some(50), |_| false).unwrap();

        // The wait should not be complete yet (just registered)
        assert_eq!(mgr.num_pending_waits(), 1);

        // Wait for the timeout to fire
        let (ready, remaining) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            rx,
        )
        .await
        .expect("timed out waiting for wait completion")
        .expect("channel closed");

        // Timeout completed the wait with 0 ready objects
        assert_eq!(ready.len(), 0);
        assert_eq!(remaining.len(), 1);
        assert!(remaining.contains(&oid1));
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: timeout fires with partial results when some objects arrived.
    #[tokio::test]
    async fn test_timeout_fires_with_partial_results() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);
        let oid2 = make_object_id(2);
        let oid3 = make_object_id(3);

        // Wait for all 3 with 200ms timeout
        let rx = mgr.wait(vec![oid1, oid2, oid3], 3, Some(200), |_| false).unwrap();

        // One object arrives before timeout
        mgr.handle_object_local(&oid1);
        assert_eq!(mgr.num_pending_waits(), 1);

        // Wait for the timeout to fire
        let (ready, remaining) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            rx,
        )
        .await
        .expect("timed out waiting for wait completion")
        .expect("channel closed");

        assert_eq!(ready.len(), 1);
        assert!(ready.contains(&oid1));
        assert_eq!(remaining.len(), 2);
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: timeout is cancelled when wait completes before it fires.
    #[tokio::test]
    async fn test_timeout_cancelled_on_early_completion() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);

        // Wait with a long timeout (5 seconds)
        let rx = mgr.wait(vec![oid1], 1, Some(5000), |_| false).unwrap();

        // Object arrives immediately
        mgr.handle_object_local(&oid1);

        // Should complete well before the timeout
        let (ready, remaining) = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            rx,
        )
        .await
        .expect("should complete before timeout")
        .expect("channel closed");

        assert_eq!(ready.len(), 1);
        assert!(ready.contains(&oid1));
        assert_eq!(remaining.len(), 0);
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: empty object list with num_required=0 completes immediately.
    #[test]
    fn test_empty_wait_zero_required() {
        let mgr = WaitManager::new();
        let mut rx = mgr.wait(vec![], 0, None, |_| false).unwrap();
        let (ready, remaining) = rx.try_recv().unwrap();
        assert_eq!(ready.len(), 0);
        assert_eq!(remaining.len(), 0);
        assert_eq!(mgr.num_pending_waits(), 0);
    }

    /// Test: three duplicate IDs should be rejected.
    #[test]
    fn test_reject_triple_duplicate() {
        let mgr = WaitManager::new();
        let oid1 = make_object_id(1);
        let oid2 = make_object_id(2);

        let result = mgr.wait(vec![oid1, oid2, oid1], 2, None, |_| false);
        assert!(result.is_err());
    }
}
