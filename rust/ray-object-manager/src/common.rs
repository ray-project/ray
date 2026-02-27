// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Common types for the object manager.
//!
//! Replaces `src/ray/object_manager/common.h`.

use ray_common::id::{NodeID, ObjectID, WorkerID};

/// Information about an object stored in the object system.
///
/// Matches C++ `ObjectInfo` struct.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectInfo {
    pub object_id: ObjectID,
    pub is_mutable: bool,
    pub data_size: i64,
    pub metadata_size: i64,
    pub owner_node_id: NodeID,
    pub owner_ip_address: String,
    pub owner_port: i32,
    pub owner_worker_id: WorkerID,
}

impl ObjectInfo {
    /// Total object size: data + metadata + optional header for mutable objects.
    pub fn get_object_size(&self) -> i64 {
        let header_size = if self.is_mutable {
            std::mem::size_of::<PlasmaObjectHeader>() as i64
        } else {
            0
        };
        self.data_size + self.metadata_size + header_size
    }
}

impl Default for ObjectInfo {
    fn default() -> Self {
        Self {
            object_id: ObjectID::nil(),
            is_mutable: false,
            data_size: 0,
            metadata_size: 0,
            owner_node_id: NodeID::nil(),
            owner_ip_address: String::new(),
            owner_port: 0,
            owner_worker_id: WorkerID::nil(),
        }
    }
}

/// Header for mutable plasma objects. Stored at the beginning of the
/// shared memory region. Used to coordinate readers/writers via semaphores.
///
/// Matches C++ `PlasmaObjectHeader` struct.
#[repr(C)]
pub struct PlasmaObjectHeader {
    pub version: i64,
    pub is_sealed: bool,
    pub num_readers: i64,
    pub num_read_acquires_remaining: u64,
    pub num_read_releases_remaining: u64,
    pub data_size: u64,
    pub metadata_size: u64,
}

impl PlasmaObjectHeader {
    pub fn new() -> Self {
        Self {
            version: 0,
            is_sealed: false,
            num_readers: 0,
            num_read_acquires_remaining: 0,
            num_read_releases_remaining: 0,
            data_size: 0,
            metadata_size: 0,
        }
    }
}

impl Default for PlasmaObjectHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// Describes a plasma object's memory layout in shared memory.
///
/// Matches C++ `PlasmaObject` struct.
#[derive(Debug, Clone)]
pub struct PlasmaObject {
    /// File descriptor for the memory-mapped region.
    pub store_fd: i32,
    /// Offset to the PlasmaObjectHeader.
    pub header_offset: isize,
    /// Offset to the data payload.
    pub data_offset: isize,
    /// Offset to the metadata payload.
    pub metadata_offset: isize,
    /// Size of the data payload in bytes.
    pub data_size: i64,
    /// Size of the metadata in bytes.
    pub metadata_size: i64,
    /// Total allocated size in bytes.
    pub allocated_size: i64,
    /// Device number (0 = host CPU, >0 = GPU).
    pub device_num: i32,
    /// Total size of the memory-mapped region.
    pub mmap_size: i64,
    /// Whether this was allocated from the fallback (disk) allocator.
    pub fallback_allocated: bool,
    /// Whether this is an experimental mutable object.
    pub is_experimental_mutable_object: bool,
}

/// Object location in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectLocation {
    Local,
    Remote,
    Nonexistent,
}

/// Source of an object in the plasma store.
///
/// Matches C++ `plasma::flatbuf::ObjectSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectSource {
    /// Created by a worker via ray.put() or task return.
    CreatedByWorker = 0,
    /// Restored from external storage (spilled object).
    RestoredFromStorage = 1,
    /// Received from a remote raylet via push.
    ReceivedFromRemoteRaylet = 2,
    /// Error object stored by the raylet (failed task).
    ErrorStoredByRaylet = 3,
}

/// Plasma error codes matching the C++ flatbuf enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PlasmaError {
    #[error("object already exists")]
    ObjectExists,
    #[error("object does not exist")]
    ObjectNonexistent,
    #[error("out of memory")]
    OutOfMemory,
    #[error("object not sealed")]
    ObjectNotSealed,
    #[error("object in use")]
    ObjectInUse,
    #[error("unexpected error")]
    UnexpectedError,
    #[error("object already sealed")]
    ObjectSealed,
    #[error("out of disk")]
    OutOfDisk,
}

/// Object manager configuration.
///
/// Matches C++ `ObjectManagerConfig`.
#[derive(Debug, Clone)]
pub struct ObjectManagerConfig {
    pub object_manager_address: String,
    pub object_manager_port: u16,
    pub timer_freq_ms: u32,
    pub pull_timeout_ms: u32,
    pub object_chunk_size: u64,
    pub max_bytes_in_flight: u64,
    pub store_socket_name: String,
    pub push_timeout_ms: i32,
    pub rpc_service_threads_number: i32,
    pub object_store_memory: i64,
    pub plasma_directory: String,
    pub fallback_directory: String,
    pub huge_pages: bool,
}

impl Default for ObjectManagerConfig {
    fn default() -> Self {
        Self {
            object_manager_address: String::new(),
            object_manager_port: 0,
            timer_freq_ms: 100,
            pull_timeout_ms: 10_000,
            object_chunk_size: 8 * 1024 * 1024,     // 8MB
            max_bytes_in_flight: 256 * 1024 * 1024, // 256MB
            store_socket_name: String::new(),
            push_timeout_ms: 10_000,
            rpc_service_threads_number: 2,
            object_store_memory: -1,
            plasma_directory: String::new(),
            fallback_directory: String::new(),
            huge_pages: false,
        }
    }
}

/// Callback types matching C++ function signatures.
pub type AddObjectCallback = Box<dyn Fn(&ObjectInfo) + Send + Sync>;
pub type DeleteObjectCallback = Box<dyn Fn(&ObjectID) + Send + Sync>;
pub type SpillObjectsCallback = Box<dyn Fn() -> bool + Send + Sync>;
pub type SpaceReleasedCallback = Box<dyn Fn() + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_info_size() {
        let info = ObjectInfo {
            data_size: 1024,
            metadata_size: 64,
            is_mutable: false,
            ..Default::default()
        };
        assert_eq!(info.get_object_size(), 1088);
    }

    #[test]
    fn test_default_config() {
        let config = ObjectManagerConfig::default();
        assert_eq!(config.object_chunk_size, 8 * 1024 * 1024);
    }
}
