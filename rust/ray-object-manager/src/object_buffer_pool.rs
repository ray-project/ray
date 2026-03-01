// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Object buffer pool — manages local object access for transfers.
//!
//! Replaces `src/ray/object_manager/object_buffer_pool.h/cc`.

use std::collections::HashMap;

use ray_common::id::ObjectID;

use crate::common::ObjectInfo;

/// Information about a chunk of an object.
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    /// Index of this chunk within the object.
    pub chunk_index: u64,
    /// Size of this chunk's data.
    pub buffer_length: u64,
}

/// State for an object being written (received from remote).
#[allow(dead_code)]
struct CreateState {
    /// Object metadata.
    object_info: ObjectInfo,
    /// Total chunks expected.
    num_chunks: u64,
    /// Chunks received so far.
    chunks_received: u64,
}

/// Manages object access for inter-node transfers.
///
/// Handles both reading (for outbound pushes) and writing
/// (for inbound pushes) of object data.
pub struct ObjectBufferPool {
    /// Default chunk size for splitting objects.
    chunk_size: u64,
    /// Objects currently being created (received from remote).
    create_states: HashMap<ObjectID, CreateState>,
}

impl ObjectBufferPool {
    pub fn new(chunk_size: u64) -> Self {
        Self {
            chunk_size,
            create_states: HashMap::new(),
        }
    }

    /// Calculate the number of chunks for an object of the given size.
    pub fn num_chunks(&self, data_size: u64) -> u64 {
        if data_size == 0 {
            return 1;
        }
        data_size.div_ceil(self.chunk_size)
    }

    /// Begin creating an object for inbound transfer.
    pub fn create_object(&mut self, object_info: ObjectInfo) -> bool {
        let object_id = object_info.object_id;
        if self.create_states.contains_key(&object_id) {
            return false;
        }

        let num_chunks = self.num_chunks(object_info.data_size as u64);
        self.create_states.insert(
            object_id,
            CreateState {
                object_info,
                num_chunks,
                chunks_received: 0,
            },
        );
        true
    }

    /// Write a chunk for an object being received.
    /// Returns `true` if all chunks have been received (object complete).
    pub fn write_chunk(&mut self, object_id: &ObjectID, chunk_index: u64, _data: &[u8]) -> bool {
        if let Some(state) = self.create_states.get_mut(object_id) {
            let _ = chunk_index; // In real impl, write to plasma buffer
            state.chunks_received += 1;
            if state.chunks_received >= state.num_chunks {
                self.create_states.remove(object_id);
                return true; // Object complete
            }
        }
        false
    }

    /// Abort creation of an object (e.g., transfer failed).
    pub fn abort_create(&mut self, object_id: &ObjectID) {
        self.create_states.remove(object_id);
    }

    /// Check if an object is currently being created.
    pub fn is_creating(&self, object_id: &ObjectID) -> bool {
        self.create_states.contains_key(object_id)
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn num_creating(&self) -> usize {
        self.create_states.len()
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

    #[test]
    fn test_chunk_calculation() {
        let pool = ObjectBufferPool::new(8192);
        assert_eq!(pool.num_chunks(0), 1);
        assert_eq!(pool.num_chunks(4096), 1);
        assert_eq!(pool.num_chunks(8192), 1);
        assert_eq!(pool.num_chunks(8193), 2);
        assert_eq!(pool.num_chunks(16384), 2);
    }

    #[test]
    fn test_create_and_write() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 2048,
            ..Default::default()
        };

        assert!(pool.create_object(info));
        assert!(pool.is_creating(&oid));

        // First chunk — not done yet
        assert!(!pool.write_chunk(&oid, 0, &[0; 1024]));
        // Second chunk — complete
        assert!(pool.write_chunk(&oid, 1, &[0; 1024]));
        assert!(!pool.is_creating(&oid));
    }

    #[test]
    fn test_duplicate_create_returns_false() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 512,
            ..Default::default()
        };
        assert!(pool.create_object(info.clone()));
        assert!(!pool.create_object(info)); // Duplicate
        assert_eq!(pool.num_creating(), 1);
    }

    #[test]
    fn test_abort_create() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 2048,
            ..Default::default()
        };
        pool.create_object(info);
        assert!(pool.is_creating(&oid));

        pool.abort_create(&oid);
        assert!(!pool.is_creating(&oid));
        assert_eq!(pool.num_creating(), 0);
    }

    #[test]
    fn test_write_chunk_nonexistent_object() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(99);
        // Writing to non-existent object should return false
        assert!(!pool.write_chunk(&oid, 0, &[0; 1024]));
    }

    #[test]
    fn test_zero_size_object_one_chunk() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(1);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 0,
            ..Default::default()
        };
        pool.create_object(info);
        // Zero-size object = 1 chunk, so first write completes it
        assert!(pool.write_chunk(&oid, 0, &[]));
        assert!(!pool.is_creating(&oid));
    }

    #[test]
    fn test_exact_chunk_boundary() {
        let pool = ObjectBufferPool::new(1024);
        // Exactly 1 chunk
        assert_eq!(pool.num_chunks(1024), 1);
        // Just over boundary
        assert_eq!(pool.num_chunks(1025), 2);
        // Exactly 2 chunks
        assert_eq!(pool.num_chunks(2048), 2);
    }

    #[test]
    fn test_multiple_objects_tracked_independently() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid1 = make_oid(1);
        let oid2 = make_oid(2);

        pool.create_object(ObjectInfo {
            object_id: oid1,
            data_size: 1024,
            ..Default::default()
        });
        pool.create_object(ObjectInfo {
            object_id: oid2,
            data_size: 2048,
            ..Default::default()
        });
        assert_eq!(pool.num_creating(), 2);

        // Complete first object
        assert!(pool.write_chunk(&oid1, 0, &[0; 1024]));
        assert_eq!(pool.num_creating(), 1);
        assert!(!pool.is_creating(&oid1));
        assert!(pool.is_creating(&oid2));
    }
}
