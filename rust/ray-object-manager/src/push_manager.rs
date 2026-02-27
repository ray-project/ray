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
            },
        );

        true
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
}
