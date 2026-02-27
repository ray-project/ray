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

/// Callback for wait completion: (ready_ids, remaining_ids).
type WaitCallback = oneshot::Sender<(Vec<ObjectID>, Vec<ObjectID>)>;

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
    /// Active wait requests.
    requests: Mutex<HashMap<u64, WaitRequest>>,
    /// Reverse index: object_id → set of wait_ids.
    object_to_waits: Mutex<HashMap<ObjectID, HashSet<u64>>>,
    /// Next wait ID.
    next_wait_id: AtomicU64,
}

impl WaitManager {
    pub fn new() -> Self {
        Self {
            requests: Mutex::new(HashMap::new()),
            object_to_waits: Mutex::new(HashMap::new()),
            next_wait_id: AtomicU64::new(1),
        }
    }

    /// Register a wait request. Returns a receiver that will fire on completion.
    pub fn wait(
        &self,
        object_ids: Vec<ObjectID>,
        num_required: usize,
        timeout_ms: Option<u64>,
        is_object_local: impl Fn(&ObjectID) -> bool,
    ) -> oneshot::Receiver<(Vec<ObjectID>, Vec<ObjectID>)> {
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
            let remaining: Vec<ObjectID> = object_ids
                .iter()
                .filter(|id| !ready.contains(id))
                .copied()
                .collect();
            let _ = tx.send((ready.into_iter().collect(), remaining));
            return rx;
        }

        // Set up timeout if specified
        let timeout_cancel = timeout_ms.map(|ms| {
            let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
            let wait_id_copy = wait_id;
            // The timeout task would be spawned externally.
            // For simplicity, we store the cancel sender.
            // In production, the NodeManager would spawn a tokio::time::sleep.
            let _ = (cancel_rx, wait_id_copy, ms);
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
        rx
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

    /// Force-complete a wait (e.g., on timeout).
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
        );

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
        );

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

        let mut rx = mgr.wait(vec![oid1], 1, None, |_| false);
        assert_eq!(mgr.num_pending_waits(), 1);

        // Simulate timeout
        mgr.complete_wait(1); // wait_id starts at 1
        let (ready, remaining) = rx.try_recv().unwrap();
        assert_eq!(ready.len(), 0);
        assert_eq!(remaining.len(), 1);
        assert_eq!(mgr.num_pending_waits(), 0);
    }
}
