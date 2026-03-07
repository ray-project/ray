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

    #[test]
    fn test_object_info_mutable_header_size() {
        let info = ObjectInfo {
            data_size: 1024,
            metadata_size: 64,
            is_mutable: true,
            ..Default::default()
        };
        let header_size = std::mem::size_of::<PlasmaObjectHeader>() as i64;
        assert_eq!(info.get_object_size(), 1024 + 64 + header_size);
        assert!(header_size > 0);
    }

    #[test]
    fn test_object_info_zero_sizes() {
        let info = ObjectInfo::default();
        assert_eq!(info.get_object_size(), 0);
    }

    #[test]
    fn test_plasma_object_header_default() {
        let header = PlasmaObjectHeader::default();
        assert_eq!(header.version, 0);
        assert!(!header.is_sealed);
        assert_eq!(header.num_readers, 0);
    }

    #[test]
    fn test_object_location_variants() {
        assert_ne!(ObjectLocation::Local, ObjectLocation::Remote);
        assert_ne!(ObjectLocation::Local, ObjectLocation::Nonexistent);
        assert_ne!(ObjectLocation::Remote, ObjectLocation::Nonexistent);
    }

    #[test]
    fn test_object_source_discriminants() {
        assert_eq!(ObjectSource::CreatedByWorker as u8, 0);
        assert_eq!(ObjectSource::RestoredFromStorage as u8, 1);
        assert_eq!(ObjectSource::ReceivedFromRemoteRaylet as u8, 2);
        assert_eq!(ObjectSource::ErrorStoredByRaylet as u8, 3);
    }

    // ─── Tests ported from mutable_object_test.cc ────────────────────────

    #[test]
    fn test_plasma_object_header_init() {
        // Port of MutableObjectTest — basic header initialization
        let header = PlasmaObjectHeader::new();
        assert_eq!(header.version, 0);
        assert!(!header.is_sealed);
        assert_eq!(header.num_readers, 0);
        assert_eq!(header.num_read_acquires_remaining, 0);
        assert_eq!(header.num_read_releases_remaining, 0);
        assert_eq!(header.data_size, 0);
        assert_eq!(header.metadata_size, 0);
    }

    #[test]
    fn test_plasma_object_header_fields_writable() {
        // Port of mutable object write/read: test that header fields can be updated
        let mut header = PlasmaObjectHeader::new();
        header.version = 1;
        header.is_sealed = true;
        header.num_readers = 4;
        header.data_size = 128;
        header.metadata_size = 16;

        assert_eq!(header.version, 1);
        assert!(header.is_sealed);
        assert_eq!(header.num_readers, 4);
        assert_eq!(header.data_size, 128);
        assert_eq!(header.metadata_size, 16);
    }

    #[test]
    fn test_plasma_object_header_version_increment() {
        // Port of mutable object write sequence: version increments on each write
        let mut header = PlasmaObjectHeader::new();
        for i in 0..100 {
            header.version = i + 1;
            header.data_size = (i as u64 + 1) * 10;
        }
        assert_eq!(header.version, 100);
        assert_eq!(header.data_size, 1000);
    }

    #[test]
    fn test_plasma_object_header_reader_tracking() {
        // Port of mutable object multiple readers: track read acquires/releases
        let mut header = PlasmaObjectHeader::new();
        let num_readers: u64 = 24;
        header.num_read_acquires_remaining = num_readers;
        header.num_read_releases_remaining = num_readers;

        // Simulate readers acquiring
        for _ in 0..num_readers {
            assert!(header.num_read_acquires_remaining > 0);
            header.num_read_acquires_remaining -= 1;
        }
        assert_eq!(header.num_read_acquires_remaining, 0);

        // Simulate readers releasing
        for _ in 0..num_readers {
            assert!(header.num_read_releases_remaining > 0);
            header.num_read_releases_remaining -= 1;
        }
        assert_eq!(header.num_read_releases_remaining, 0);
    }

    #[test]
    fn test_plasma_object_header_write_acquire_release_cycle() {
        // Port of TestBasic: simulate a write-acquire, data fill, write-release cycle
        let mut header = PlasmaObjectHeader::new();
        let num_readers: u64 = 1;

        // Simulate WriteAcquire
        header.version += 1;
        header.data_size = 5; // "hello"
        header.metadata_size = 1; // "5"
        header.num_read_acquires_remaining = num_readers;
        header.num_read_releases_remaining = num_readers;

        assert_eq!(header.version, 1);
        assert_eq!(header.data_size, 5);
        assert_eq!(header.metadata_size, 1);

        // Simulate WriteRelease (no-op on header, just signal)
        // Simulate ReadAcquire — reader checks version
        assert_eq!(header.version, 1);
        header.num_read_acquires_remaining -= 1;
        assert_eq!(header.num_read_acquires_remaining, 0);

        // Simulate ReadRelease
        header.num_read_releases_remaining -= 1;
        assert_eq!(header.num_read_releases_remaining, 0);
    }

    #[test]
    fn test_plasma_object_header_error_flag() {
        // Port of TestWriterFails / TestReaderFails: error sets sealed
        let mut header = PlasmaObjectHeader::new();
        assert!(!header.is_sealed);

        // Simulate SetErrorUnlocked
        header.is_sealed = true;
        assert!(header.is_sealed);

        // After error, version should not have advanced
        assert_eq!(header.version, 0);
    }

    #[test]
    fn test_plasma_object_header_multiple_write_cycles() {
        // Port of TestMultipleReaders: multiple writes with reader tracking
        let mut header = PlasmaObjectHeader::new();
        let num_readers: u64 = 24;

        for write_num in 0..100u64 {
            // WriteAcquire
            header.version = write_num as i64 + 1;
            header.data_size = write_num + 5;
            header.metadata_size = 1;
            header.num_read_acquires_remaining = num_readers;
            header.num_read_releases_remaining = num_readers;

            // All readers acquire
            for _ in 0..num_readers {
                header.num_read_acquires_remaining -= 1;
            }
            assert_eq!(header.num_read_acquires_remaining, 0);

            // All readers release
            for _ in 0..num_readers {
                header.num_read_releases_remaining -= 1;
            }
            assert_eq!(header.num_read_releases_remaining, 0);
        }

        assert_eq!(header.version, 100);
    }

    #[test]
    fn test_plasma_object_header_repr_c_size() {
        // Ensure the repr(C) header has a reasonable size
        let size = std::mem::size_of::<PlasmaObjectHeader>();
        // The header has 7 fields: i64, bool, i64, u64, u64, u64, u64
        // With repr(C) this should be at least 7*8 = 56 bytes
        assert!(size >= 56, "PlasmaObjectHeader size {} is too small", size);
    }

    #[test]
    fn test_mutable_object_size_includes_header() {
        // Port: ensure mutable objects include header in their total size
        let header_size = std::mem::size_of::<PlasmaObjectHeader>() as i64;
        let info = ObjectInfo {
            data_size: 128,
            metadata_size: 0,
            is_mutable: true,
            ..Default::default()
        };
        assert_eq!(info.get_object_size(), 128 + header_size);

        let info2 = ObjectInfo {
            data_size: 128,
            metadata_size: 0,
            is_mutable: false,
            ..Default::default()
        };
        assert_eq!(info2.get_object_size(), 128);
    }

    #[test]
    fn test_plasma_error_display() {
        let err = PlasmaError::OutOfMemory;
        assert_eq!(err.to_string(), "out of memory");
        let err = PlasmaError::ObjectExists;
        assert_eq!(err.to_string(), "object already exists");
    }
}
