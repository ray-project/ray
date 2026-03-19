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
struct CreateState {
    /// Object metadata.
    object_info: ObjectInfo,
    /// Total chunks expected.
    num_chunks: u64,
    /// Per-chunk received data. `None` means the chunk has not been received yet.
    chunk_data: Vec<Option<Vec<u8>>>,
    /// Number of distinct chunks received so far.
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
                chunk_data: vec![None; num_chunks as usize],
                chunks_received: 0,
            },
        );
        true
    }

    /// Write a chunk for an object being received.
    ///
    /// Returns `true` if all chunks have been received (object complete).
    /// Returns `false` if:
    /// - The object is not being created
    /// - The chunk_index is out of range
    /// - The chunk was already received (duplicate)
    /// - The chunk data is the wrong size (validated against expected chunk size)
    pub fn write_chunk(&mut self, object_id: &ObjectID, chunk_index: u64, data: &[u8]) -> bool {
        if let Some(state) = self.create_states.get_mut(object_id) {
            let idx = chunk_index as usize;

            // Reject out-of-range chunk index.
            if idx >= state.num_chunks as usize {
                return false;
            }

            // Reject duplicate chunk.
            if state.chunk_data[idx].is_some() {
                return false;
            }

            // Validate chunk size. For a non-empty object the expected size of
            // each chunk is `chunk_size`, except for the last chunk which may be
            // smaller.
            let total_data = state.object_info.data_size as u64;
            if total_data > 0 {
                let is_last = idx as u64 == state.num_chunks - 1;
                let expected = if is_last {
                    let remainder = total_data % self.chunk_size;
                    if remainder == 0 {
                        self.chunk_size
                    } else {
                        remainder
                    }
                } else {
                    self.chunk_size
                };
                if data.len() as u64 != expected {
                    return false;
                }
            }

            // Store the chunk data.
            state.chunk_data[idx] = Some(data.to_vec());
            state.chunks_received += 1;

            if state.chunks_received >= state.num_chunks {
                self.create_states.remove(object_id);
                return true; // Object complete
            }
        }
        false
    }

    /// Retrieve the assembled data for a completed object.
    ///
    /// This is a convenience for testing — in production, chunks would be
    /// written directly into a plasma buffer.  Returns `None` if the object
    /// is still being created (not yet complete).
    pub fn get_completed_data(&self, _object_id: &ObjectID) -> Option<Vec<u8>> {
        // Once complete, the CreateState is removed — so the data is only
        // available transiently.  We could store completed objects separately
        // but for now we keep the API minimal.
        None
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

    #[test]
    fn test_parity_duplicate_chunk_does_not_complete_object() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(9);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 2048,
            ..Default::default()
        };

        assert!(pool.create_object(info));
        assert!(!pool.write_chunk(&oid, 0, &[1; 1024]));

        // In the C++ implementation a duplicate chunk is rejected, not counted
        // as a second completed chunk.
        assert!(!pool.write_chunk(&oid, 0, &[1; 1024]));
        assert!(pool.is_creating(&oid));
    }

    #[test]
    fn test_parity_wrong_sized_chunk_does_not_complete_object() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(10);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 1024,
            ..Default::default()
        };

        assert!(pool.create_object(info));

        // C++ validates the chunk length against the expected buffer length.
        assert!(!pool.write_chunk(&oid, 0, &[]));
        assert!(pool.is_creating(&oid));
    }

    #[test]
    fn test_out_of_range_chunk_index_rejected() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(11);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 1024,
            ..Default::default()
        };
        pool.create_object(info);

        // Only chunk index 0 is valid for a 1-chunk object.
        assert!(!pool.write_chunk(&oid, 1, &[0; 1024]));
        assert!(pool.is_creating(&oid));
    }

    #[test]
    fn test_last_chunk_smaller_accepted() {
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(12);
        // 1500 bytes = 2 chunks: first 1024, last 476.
        let info = ObjectInfo {
            object_id: oid,
            data_size: 1500,
            ..Default::default()
        };
        pool.create_object(info);

        assert!(!pool.write_chunk(&oid, 0, &[0; 1024]));
        assert!(pool.write_chunk(&oid, 1, &[0; 476]));
        assert!(!pool.is_creating(&oid));
    }

    #[test]
    fn test_out_of_order_chunks_accepted() {
        // Out-of-order delivery is fine as long as each chunk is valid.
        let mut pool = ObjectBufferPool::new(1024);
        let oid = make_oid(13);
        let info = ObjectInfo {
            object_id: oid,
            data_size: 2048,
            ..Default::default()
        };
        pool.create_object(info);

        // Send chunk 1 before chunk 0.
        assert!(!pool.write_chunk(&oid, 1, &[2; 1024]));
        assert!(pool.is_creating(&oid)); // still creating
        assert!(pool.write_chunk(&oid, 0, &[1; 1024]));
        assert!(!pool.is_creating(&oid));
    }

    #[test]
    fn test_three_chunk_object_full_flow() {
        let mut pool = ObjectBufferPool::new(100);
        let oid = make_oid(14);
        // 250 bytes = 3 chunks: 100, 100, 50.
        let info = ObjectInfo {
            object_id: oid,
            data_size: 250,
            ..Default::default()
        };
        pool.create_object(info);

        assert!(!pool.write_chunk(&oid, 0, &[0xAA; 100]));
        assert!(!pool.write_chunk(&oid, 2, &[0xCC; 50]));
        // Duplicate of chunk 2 rejected.
        assert!(!pool.write_chunk(&oid, 2, &[0xCC; 50]));
        assert!(pool.is_creating(&oid));
        // Final chunk completes the object.
        assert!(pool.write_chunk(&oid, 1, &[0xBB; 100]));
        assert!(!pool.is_creating(&oid));
    }
}
