// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Push manager — rate-limited outbound object transfers.
//!
//! Replaces `src/ray/object_manager/push_manager.h/cc`.
//!
//! Deduplicates concurrent pushes and limits the number of chunks in flight
//! to enforce `max_bytes_in_flight`.

use std::collections::HashMap;

use ray_common::id::{NodeID, ObjectID};

/// State for an active push of an object to a remote node.
#[derive(Debug)]
struct PushState {
    node_id: NodeID,
    object_id: ObjectID,
    num_chunks: i64,
    next_chunk_id: i64,
    num_chunks_remaining: i64,
    /// When this push was started (monotonic ms).
    start_time_ms: f64,
    /// Object size in bytes.
    object_size: u64,
}

/// The push manager coordinates rate-limited outbound object transfers.
#[allow(dead_code)]
pub struct PushManager {
    /// Active pushes keyed by (node_id, object_id).
    push_state_map: HashMap<(NodeID, ObjectID), PushState>,
    /// Maximum bytes allowed in flight at once.
    max_bytes_in_flight: u64,
    /// Current bytes in flight.
    bytes_in_flight: u64,
    /// Object chunk size.
    chunk_size: u64,
    /// Number of chunks currently in flight.
    chunks_in_flight: i64,
    /// Maximum chunks in flight (derived from max_bytes_in_flight / chunk_size).
    max_chunks_in_flight: i64,
}

impl PushManager {
    pub fn new(max_bytes_in_flight: u64, chunk_size: u64) -> Self {
        let max_chunks = (max_bytes_in_flight / chunk_size.max(1)) as i64;
        Self {
            push_state_map: HashMap::new(),
            max_bytes_in_flight,
            bytes_in_flight: 0,
            chunk_size,
            chunks_in_flight: 0,
            max_chunks_in_flight: max_chunks.max(1),
        }
    }

    /// Start pushing an object to a remote node.
    ///
    /// Returns `true` if the push was started (not a duplicate).
    /// Returns `false` if a push for this (node, object) is already active.
    pub fn start_push(&mut self, node_id: NodeID, object_id: ObjectID, object_size: u64) -> bool {
        let key = (node_id, object_id);
        if self.push_state_map.contains_key(&key) {
            return false; // Deduplicate
        }

        let num_chunks = object_size.div_ceil(self.chunk_size) as i64;

        self.push_state_map.insert(
            key,
            PushState {
                node_id,
                object_id,
                num_chunks,
                next_chunk_id: 0,
                num_chunks_remaining: num_chunks,
                start_time_ms: 0.0,
                object_size,
            },
        );

        true
    }

    /// Start pushing with a timestamp for timeout tracking.
    pub fn start_push_with_time(
        &mut self,
        node_id: NodeID,
        object_id: ObjectID,
        object_size: u64,
        now_ms: f64,
    ) -> bool {
        let key = (node_id, object_id);
        if self.push_state_map.contains_key(&key) {
            return false;
        }

        let num_chunks = object_size.div_ceil(self.chunk_size) as i64;

        self.push_state_map.insert(
            key,
            PushState {
                node_id,
                object_id,
                num_chunks,
                next_chunk_id: 0,
                num_chunks_remaining: num_chunks,
                start_time_ms: now_ms,
                object_size,
            },
        );

        true
    }

    /// Cancel a push to a specific (node, object) pair.
    ///
    /// Returns `true` if the push was cancelled.
    pub fn cancel_push(&mut self, node_id: &NodeID, object_id: &ObjectID) -> bool {
        let key = (*node_id, *object_id);
        if let Some(state) = self.push_state_map.remove(&key) {
            // Release in-flight quota for unsent chunks
            let sent_chunks = state.next_chunk_id - state.num_chunks_remaining.max(0);
            let in_flight = sent_chunks.max(0);
            self.chunks_in_flight = (self.chunks_in_flight - in_flight).max(0);
            self.bytes_in_flight = self
                .bytes_in_flight
                .saturating_sub(in_flight as u64 * self.chunk_size);
            true
        } else {
            false
        }
    }

    /// Get pushes that have timed out.
    pub fn get_timed_out_pushes(&self, now_ms: f64, timeout_ms: f64) -> Vec<(NodeID, ObjectID)> {
        self.push_state_map
            .values()
            .filter(|s| s.start_time_ms > 0.0 && now_ms - s.start_time_ms > timeout_ms)
            .map(|s| (s.node_id, s.object_id))
            .collect()
    }

    /// Total bytes across all active push objects.
    pub fn total_push_bytes(&self) -> u64 {
        self.push_state_map.values().map(|s| s.object_size).sum()
    }

    /// Get the next chunks to send, respecting the in-flight limit.
    ///
    /// Returns a list of `(node_id, object_id, chunk_index)` tuples.
    pub fn get_chunks_to_send(&mut self) -> Vec<(NodeID, ObjectID, i64)> {
        let mut chunks = Vec::new();

        for state in self.push_state_map.values_mut() {
            while state.next_chunk_id < state.num_chunks
                && self.chunks_in_flight < self.max_chunks_in_flight
            {
                chunks.push((state.node_id, state.object_id, state.next_chunk_id));
                state.next_chunk_id += 1;
                self.chunks_in_flight += 1;
                self.bytes_in_flight += self.chunk_size;
            }
        }

        chunks
    }

    /// Acknowledge that a chunk has been sent successfully.
    pub fn chunk_sent(&mut self, node_id: NodeID, object_id: ObjectID) {
        self.chunks_in_flight -= 1;
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(self.chunk_size);

        let key = (node_id, object_id);
        if let Some(state) = self.push_state_map.get_mut(&key) {
            state.num_chunks_remaining -= 1;
            if state.num_chunks_remaining <= 0 {
                self.push_state_map.remove(&key);
            }
        }
    }

    /// Check if a push is already active for a (node, object) pair.
    pub fn is_pushing(&self, node_id: &NodeID, object_id: &ObjectID) -> bool {
        self.push_state_map.contains_key(&(*node_id, *object_id))
    }

    pub fn num_active_pushes(&self) -> usize {
        self.push_state_map.len()
    }

    pub fn bytes_in_flight(&self) -> u64 {
        self.bytes_in_flight
    }

    pub fn chunks_in_flight(&self) -> i64 {
        self.chunks_in_flight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    fn make_nid(val: u8) -> NodeID {
        let mut data = [0u8; 28];
        data[0] = val;
        NodeID::from_binary(&data)
    }

    #[test]
    fn test_push_deduplication() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node = make_nid(1);
        let obj = make_oid(1);

        assert!(pm.start_push(node, obj, 16384));
        assert!(!pm.start_push(node, obj, 16384)); // Duplicate
        assert_eq!(pm.num_active_pushes(), 1);
    }

    #[test]
    fn test_chunk_sending() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node = make_nid(1);
        let obj = make_oid(1);

        pm.start_push(node, obj, 24576); // 3 chunks at 8192 each

        let chunks = pm.get_chunks_to_send();
        assert_eq!(chunks.len(), 3);

        for (n, o, _idx) in chunks {
            pm.chunk_sent(n, o);
        }
        assert_eq!(pm.num_active_pushes(), 0);
    }

    #[test]
    fn test_rate_limiting() {
        // Only allow 1 chunk in flight (8KB max / 8KB chunk = 1)
        let mut pm = PushManager::new(8192, 8192);
        let node = make_nid(1);
        let obj = make_oid(1);

        pm.start_push(node, obj, 24576); // 3 chunks

        let chunks = pm.get_chunks_to_send();
        assert_eq!(chunks.len(), 1); // Only 1 allowed

        pm.chunk_sent(node, obj);
        let chunks = pm.get_chunks_to_send();
        assert_eq!(chunks.len(), 1); // Next one
    }

    #[test]
    fn test_push_to_different_nodes() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node1 = make_nid(1);
        let node2 = make_nid(2);
        let obj = make_oid(1);

        // Same object to two different nodes is allowed
        assert!(pm.start_push(node1, obj, 8192));
        assert!(pm.start_push(node2, obj, 8192));
        assert_eq!(pm.num_active_pushes(), 2);
    }

    #[test]
    fn test_is_pushing() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node = make_nid(1);
        let obj = make_oid(1);

        assert!(!pm.is_pushing(&node, &obj));
        pm.start_push(node, obj, 8192);
        assert!(pm.is_pushing(&node, &obj));
    }

    #[test]
    fn test_bytes_in_flight_tracking() {
        let mut pm = PushManager::new(1024 * 1024, 1024);
        let node = make_nid(1);
        let obj = make_oid(1);

        pm.start_push(node, obj, 3072); // 3 chunks at 1024 each
        assert_eq!(pm.bytes_in_flight(), 0);

        let chunks = pm.get_chunks_to_send();
        assert_eq!(chunks.len(), 3);
        assert_eq!(pm.bytes_in_flight(), 3072);
        assert_eq!(pm.chunks_in_flight(), 3);

        pm.chunk_sent(node, obj);
        assert_eq!(pm.bytes_in_flight(), 2048);
        assert_eq!(pm.chunks_in_flight(), 2);
    }

    #[test]
    fn test_push_completes_removes_state() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node = make_nid(1);
        let obj = make_oid(1);

        pm.start_push(node, obj, 8192); // 1 chunk
        let chunks = pm.get_chunks_to_send();
        assert_eq!(chunks.len(), 1);

        pm.chunk_sent(node, obj);
        assert_eq!(pm.num_active_pushes(), 0);
        assert!(!pm.is_pushing(&node, &obj));

        // Can start a new push for the same pair
        assert!(pm.start_push(node, obj, 8192));
    }

    #[test]
    fn test_zero_size_object_push() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node = make_nid(1);
        let obj = make_oid(1);

        // div_ceil(0, 8192) = 0 chunks, but min should ensure at least 1
        pm.start_push(node, obj, 0);
        // Object with 0 size still gets tracked
        assert!(pm.is_pushing(&node, &obj));
    }

    #[test]
    fn test_cancel_push() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node = make_nid(1);
        let obj = make_oid(1);

        pm.start_push(node, obj, 16384);
        assert!(pm.is_pushing(&node, &obj));

        assert!(pm.cancel_push(&node, &obj));
        assert!(!pm.is_pushing(&node, &obj));
        assert_eq!(pm.num_active_pushes(), 0);
    }

    #[test]
    fn test_cancel_nonexistent_push() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node = make_nid(1);
        let obj = make_oid(1);

        assert!(!pm.cancel_push(&node, &obj));
    }

    #[test]
    fn test_start_push_with_time() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node = make_nid(1);
        let obj = make_oid(1);

        assert!(pm.start_push_with_time(node, obj, 16384, 1000.0));
        assert!(pm.is_pushing(&node, &obj));
        // Duplicate returns false
        assert!(!pm.start_push_with_time(node, obj, 16384, 1000.0));
    }

    #[test]
    fn test_timed_out_pushes() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node1 = make_nid(1);
        let node2 = make_nid(2);
        let obj1 = make_oid(1);
        let obj2 = make_oid(2);

        pm.start_push_with_time(node1, obj1, 8192, 1000.0);
        pm.start_push_with_time(node2, obj2, 8192, 5000.0);

        // At t=6000, with 4000ms timeout: obj1 timed out, obj2 not yet
        let timed_out = pm.get_timed_out_pushes(6000.0, 4000.0);
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], (node1, obj1));
    }

    #[test]
    fn test_total_push_bytes() {
        let mut pm = PushManager::new(1024 * 1024, 8192);
        let node = make_nid(1);

        pm.start_push(node, make_oid(1), 1000);
        pm.start_push(node, make_oid(2), 2000);

        assert_eq!(pm.total_push_bytes(), 3000);
    }
}
