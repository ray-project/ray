// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Ray ID types, byte-for-byte compatible with the C++ implementation.
//!
//! Replaces `src/ray/common/id.h/cc`.
//!
//! ID hierarchy:
//! - `JobID` (4 bytes)
//! - `ActorID` (16 bytes = 12 unique + 4 JobID)
//! - `TaskID` (24 bytes = 8 unique + 16 ActorID)
//! - `ObjectID` (28 bytes = 4 index + 24 TaskID)
//! - `PlacementGroupID` (18 bytes = 14 unique + 4 JobID)
//! - `UniqueID` / `WorkerID` / `NodeID` / etc. (28 bytes)
//! - `LeaseID` (32 bytes = 4 unique + 28 WorkerID)

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::constants::UNIQUE_ID_SIZE;

// ─── MurmurHash64A ──────────────────────────────────────────────────────────
// This must match the C++ implementation exactly for cross-language ID hashing.

fn murmur_hash_64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: i32 = 47;

    let len = key.len();
    let mut h: u64 = seed ^ ((len as u64).wrapping_mul(M));

    // Process 8-byte chunks
    let n_blocks = len / 8;
    for i in 0..n_blocks {
        let offset = i * 8;
        let mut k = u64::from_le_bytes(key[offset..offset + 8].try_into().unwrap());

        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h ^= k;
        h = h.wrapping_mul(M);
    }

    // Process remaining bytes
    let tail = &key[n_blocks * 8..];
    let remaining = len & 7;
    if remaining >= 7 {
        h ^= (tail[6] as u64) << 48;
    }
    if remaining >= 6 {
        h ^= (tail[5] as u64) << 40;
    }
    if remaining >= 5 {
        h ^= (tail[4] as u64) << 32;
    }
    if remaining >= 4 {
        h ^= (tail[3] as u64) << 24;
    }
    if remaining >= 3 {
        h ^= (tail[2] as u64) << 16;
    }
    if remaining >= 2 {
        h ^= (tail[1] as u64) << 8;
    }
    if remaining >= 1 {
        h ^= tail[0] as u64;
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;

    h
}

// ─── ID Macro ────────────────────────────────────────────────────────────────

/// Generates a fixed-size Ray ID type.
///
/// Each ID is a `[u8; N]` newtype with:
/// - `from_binary` / `from_hex` / `from_random` constructors
/// - `binary()` / `hex()` / `data()` accessors
/// - `Hash`, `Eq`, `PartialEq`, `Clone`, `Copy`, `Debug`, `Display`
/// - `Nil` default (all 0xFF bytes, matching C++)
macro_rules! define_ray_id {
    ($name:ident, $size:expr) => {
        #[derive(Clone, Copy)]
        #[repr(C)]
        pub struct $name {
            data: [u8; $size],
        }

        impl $name {
            /// The fixed byte size of this ID type.
            pub const SIZE: usize = $size;

            /// Create a nil ID (all 0xFF bytes, matching C++ BaseID default).
            pub const fn nil() -> Self {
                Self {
                    data: [0xFF; $size],
                }
            }

            /// Create an ID from raw bytes. Panics if `bytes.len() != SIZE`.
            pub fn from_binary(bytes: &[u8]) -> Self {
                assert_eq!(
                    bytes.len(),
                    $size,
                    "expected {} bytes for {}, got {}",
                    $size,
                    stringify!($name),
                    bytes.len()
                );
                let mut data = [0u8; $size];
                data.copy_from_slice(bytes);
                Self { data }
            }

            /// Create an ID from a hex string. Returns `Nil` on invalid input.
            pub fn from_hex(hex_str: &str) -> Self {
                if hex_str.len() != $size * 2 {
                    tracing::error!(
                        "incorrect hex string length for {}: expected {}, got {}",
                        stringify!($name),
                        $size * 2,
                        hex_str.len()
                    );
                    return Self::nil();
                }
                match hex::decode(hex_str) {
                    Ok(bytes) => Self::from_binary(&bytes),
                    Err(_) => {
                        tracing::error!("invalid hex string for {}", stringify!($name));
                        Self::nil()
                    }
                }
            }

            /// Create a random ID.
            pub fn from_random() -> Self {
                let mut data = [0u8; $size];
                ray_util::random::fill_random(&mut data);
                Self { data }
            }

            /// Returns true if this is the nil ID (all 0xFF).
            pub fn is_nil(&self) -> bool {
                self.data == [0xFF; $size]
            }

            /// Raw byte slice reference.
            pub fn data(&self) -> &[u8; $size] {
                &self.data
            }

            /// Raw byte slice as `&[u8]`.
            pub fn as_bytes(&self) -> &[u8] {
                &self.data
            }

            /// Binary string (owned copy of the bytes).
            pub fn binary(&self) -> Vec<u8> {
                self.data.to_vec()
            }

            /// Hex-encoded string (lowercase), matching C++ `Hex()`.
            pub fn hex(&self) -> String {
                hex::encode(self.data)
            }

            /// Compute MurmurHash64A, matching the C++ `BaseID::Hash()`.
            pub fn murmur_hash(&self) -> u64 {
                murmur_hash_64a(&self.data, 0)
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::nil()
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.data == other.data
            }
        }

        impl Eq for $name {}

        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                // Use MurmurHash for deterministic cross-language compatibility.
                // We hash the murmur output so Rust HashMap works correctly.
                self.murmur_hash().hash(state);
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.hex())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.hex())
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                &self.data
            }
        }
    };
}

// ─── ID Type Definitions ────────────────────────────────────────────────────

// UniqueID: 28 bytes (kUniqueIDSize)
define_ray_id!(UniqueID, UNIQUE_ID_SIZE);

// JobID: 4 bytes
define_ray_id!(JobID, 4);

// ActorID: 16 bytes (12 unique + 4 JobID)
define_ray_id!(ActorID, 16);

// TaskID: 24 bytes (8 unique + 16 ActorID)
define_ray_id!(TaskID, 24);

// ObjectID: 28 bytes (4 index + 24 TaskID)
define_ray_id!(ObjectID, 28);

// PlacementGroupID: 18 bytes (14 unique + 4 JobID)
define_ray_id!(PlacementGroupID, 18);

// LeaseID: 32 bytes (4 unique + 28 WorkerID)
define_ray_id!(LeaseID, 32);

// Types defined via DEFINE_UNIQUE_ID macro in C++ (all 28 bytes = UniqueID size):
define_ray_id!(FunctionID, UNIQUE_ID_SIZE);
define_ray_id!(ActorClassID, UNIQUE_ID_SIZE);
define_ray_id!(WorkerID, UNIQUE_ID_SIZE);
define_ray_id!(ConfigID, UNIQUE_ID_SIZE);
define_ray_id!(NodeID, UNIQUE_ID_SIZE);
define_ray_id!(ClusterID, UNIQUE_ID_SIZE);

// ─── JobID extras ───────────────────────────────────────────────────────────

impl JobID {
    /// Create a JobID from a u32 integer (matching C++ `JobID::FromInt`).
    pub fn from_int(value: u32) -> Self {
        Self {
            data: value.to_be_bytes(),
        }
    }

    /// Convert to a u32 integer (matching C++ `JobID::ToInt`).
    pub fn to_int(&self) -> u32 {
        u32::from_be_bytes(self.data)
    }
}

// ─── ActorID extras ─────────────────────────────────────────────────────────

impl ActorID {
    const UNIQUE_BYTES_LENGTH: usize = 12;

    /// Create an ActorID from a job ID and the hashed task info.
    /// Matches C++ `ActorID::Of(job_id, parent_task_id, parent_task_counter)`.
    pub fn of(job_id: &JobID, parent_task_id: &TaskID, parent_task_counter: usize) -> Self {
        let mut data = [0u8; 16];

        // Hash (parent_task_id, parent_task_counter) to produce unique bytes
        let mut hash_input = Vec::with_capacity(TaskID::SIZE + 8);
        hash_input.extend_from_slice(parent_task_id.as_bytes());
        hash_input.extend_from_slice(&(parent_task_counter as u64).to_le_bytes());

        let hash = murmur_hash_64a(&hash_input, 0);
        let hash_bytes = hash.to_le_bytes();

        // First 12 bytes: derived from hash (padded)
        data[..8].copy_from_slice(&hash_bytes);
        // Fill remaining unique bytes with more hash
        let hash2 = murmur_hash_64a(&hash_bytes, 1);
        let hash2_bytes = hash2.to_le_bytes();
        data[8..Self::UNIQUE_BYTES_LENGTH].copy_from_slice(&hash2_bytes[..4]);

        // Last 4 bytes: JobID
        data[Self::UNIQUE_BYTES_LENGTH..].copy_from_slice(job_id.data());

        Self { data }
    }

    /// Extract the embedded JobID (last 4 bytes).
    pub fn job_id(&self) -> JobID {
        JobID::from_binary(&self.data[Self::UNIQUE_BYTES_LENGTH..])
    }
}

// ─── TaskID extras ──────────────────────────────────────────────────────────

impl TaskID {
    const UNIQUE_BYTES_LENGTH: usize = 8;

    /// Create a TaskID for an actor creation task.
    pub fn for_actor_creation_task(actor_id: &ActorID) -> Self {
        let mut data = [0u8; 24];
        // First 8 bytes: all zeros for creation task
        // Last 16 bytes: ActorID
        data[Self::UNIQUE_BYTES_LENGTH..].copy_from_slice(actor_id.data());
        Self { data }
    }

    /// Create a TaskID for a normal task.
    pub fn for_normal_task(
        job_id: &JobID,
        parent_task_id: &TaskID,
        parent_task_counter: usize,
    ) -> Self {
        Self::for_actor_task(job_id, parent_task_id, parent_task_counter, &ActorID::nil())
    }

    /// Create a TaskID for an actor task.
    pub fn for_actor_task(
        _job_id: &JobID,
        parent_task_id: &TaskID,
        parent_task_counter: usize,
        actor_id: &ActorID,
    ) -> Self {
        let mut data = [0u8; 24];

        // Hash (parent_task_id, parent_task_counter) for unique bytes
        let mut hash_input = Vec::with_capacity(TaskID::SIZE + 8);
        hash_input.extend_from_slice(parent_task_id.as_bytes());
        hash_input.extend_from_slice(&(parent_task_counter as u64).to_le_bytes());
        let hash = murmur_hash_64a(&hash_input, 0);

        data[..Self::UNIQUE_BYTES_LENGTH].copy_from_slice(&hash.to_le_bytes());
        data[Self::UNIQUE_BYTES_LENGTH..].copy_from_slice(actor_id.data());

        Self { data }
    }

    /// Create a TaskID for the driver task of a given job.
    pub fn for_driver_task(job_id: &JobID) -> Self {
        let actor_id = ActorID::of(job_id, &TaskID::nil(), 0);
        Self::for_actor_creation_task(&actor_id)
    }

    /// Extract the embedded ActorID (last 16 bytes).
    pub fn actor_id(&self) -> ActorID {
        ActorID::from_binary(&self.data[Self::UNIQUE_BYTES_LENGTH..])
    }

    /// Extract the embedded JobID (from the ActorID).
    pub fn job_id(&self) -> JobID {
        self.actor_id().job_id()
    }

    /// Check if this is an actor creation task (unique bytes are all zeros).
    pub fn is_for_actor_creation_task(&self) -> bool {
        self.data[..Self::UNIQUE_BYTES_LENGTH]
            .iter()
            .all(|&b| b == 0)
    }
}

// ─── ObjectID extras ────────────────────────────────────────────────────────

impl ObjectID {
    const INDEX_BYTES_LENGTH: usize = 4; // sizeof(ObjectIDIndexType) = sizeof(u32)

    /// Maximum number of objects returnable by a single task.
    pub const MAX_OBJECT_INDEX: u64 = (1u64 << 32) - 1;

    /// Create an ObjectID from a TaskID and an object index.
    pub fn from_index(task_id: &TaskID, index: u32) -> Self {
        let mut data = [0u8; 28];
        data[..Self::INDEX_BYTES_LENGTH].copy_from_slice(&index.to_be_bytes());
        data[Self::INDEX_BYTES_LENGTH..].copy_from_slice(task_id.data());
        Self { data }
    }

    /// Get the object index (first 4 bytes as big-endian u32).
    pub fn object_index(&self) -> u32 {
        u32::from_be_bytes(self.data[..Self::INDEX_BYTES_LENGTH].try_into().unwrap())
    }

    /// Extract the embedded TaskID (last 24 bytes).
    pub fn task_id(&self) -> TaskID {
        TaskID::from_binary(&self.data[Self::INDEX_BYTES_LENGTH..])
    }

    /// Create an ObjectID for tracking an actor's lifetime handle.
    pub fn for_actor_handle(actor_id: &ActorID) -> Self {
        let task_id = TaskID::for_actor_creation_task(actor_id);
        Self::from_index(&task_id, 1)
    }

    /// Check if this ObjectID represents an actor handle.
    pub fn is_actor_id(object_id: &ObjectID) -> bool {
        object_id.object_index() == 1 && object_id.task_id().is_for_actor_creation_task()
    }

    /// Extract the ActorID from an ObjectID (via TaskID → ActorID).
    pub fn to_actor_id(object_id: &ObjectID) -> ActorID {
        object_id.task_id().actor_id()
    }
}

// ─── PlacementGroupID extras ────────────────────────────────────────────────

impl PlacementGroupID {
    const UNIQUE_BYTES_LENGTH: usize = 14;

    /// Create a random PlacementGroupID for a given job.
    pub fn of(job_id: &JobID) -> Self {
        let mut data = [0u8; 18];
        ray_util::random::fill_random(&mut data[..Self::UNIQUE_BYTES_LENGTH]);
        data[Self::UNIQUE_BYTES_LENGTH..].copy_from_slice(job_id.data());
        Self { data }
    }

    /// Extract the embedded JobID (last 4 bytes).
    pub fn job_id(&self) -> JobID {
        JobID::from_binary(&self.data[Self::UNIQUE_BYTES_LENGTH..])
    }
}

// ─── LeaseID extras ─────────────────────────────────────────────────────────

impl LeaseID {
    /// Create a LeaseID from a worker ID and a counter.
    pub fn from_worker(worker_id: &WorkerID, counter: u32) -> Self {
        let mut data = [0u8; 32];
        data[..4].copy_from_slice(&counter.to_be_bytes());
        data[4..].copy_from_slice(worker_id.data());
        Self { data }
    }

    /// Extract the embedded WorkerID (last 28 bytes).
    pub fn worker_id(&self) -> WorkerID {
        WorkerID::from_binary(&self.data[4..])
    }
}

// ─── WorkerID extras ────────────────────────────────────────────────────────

impl WorkerID {
    /// Compute a driver ID from a JobID, matching C++ `ComputeDriverIdFromJob`.
    pub fn compute_driver_id_from_job(job_id: &JobID) -> Self {
        let mut data = [0u8; UNIQUE_ID_SIZE];
        // Place the JobID bytes at the start, rest is zeros
        data[..JobID::SIZE].copy_from_slice(job_id.data());
        Self { data }
    }
}

// ─── Special constants ──────────────────────────────────────────────────────

/// The GCS node ID is all zeros (used as a sentinel).
pub const GCS_NODE_ID: NodeID = NodeID {
    data: [0u8; UNIQUE_ID_SIZE],
};

/// Bundle ID type: (PlacementGroupID, bundle_index).
pub type BundleID = (PlacementGroupID, i64);

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nil_id() {
        let id = JobID::nil();
        assert!(id.is_nil());
        assert_eq!(id.data(), &[0xFF; 4]);
    }

    #[test]
    fn test_job_id_from_int() {
        let job_id = JobID::from_int(42);
        assert_eq!(job_id.to_int(), 42);
        assert!(!job_id.is_nil());
    }

    #[test]
    fn test_job_id_roundtrip() {
        for val in [0u32, 1, 100, u32::MAX] {
            let id = JobID::from_int(val);
            assert_eq!(id.to_int(), val);

            let hex_str = id.hex();
            let id2 = JobID::from_hex(&hex_str);
            assert_eq!(id, id2);

            let bin = id.binary();
            let id3 = JobID::from_binary(&bin);
            assert_eq!(id, id3);
        }
    }

    #[test]
    fn test_actor_id_embeds_job_id() {
        let job_id = JobID::from_int(7);
        let task_id = TaskID::nil();
        let actor_id = ActorID::of(&job_id, &task_id, 0);
        assert_eq!(actor_id.job_id(), job_id);
    }

    #[test]
    fn test_task_id_embeds_actor_id() {
        let job_id = JobID::from_int(3);
        let parent = TaskID::nil();
        let actor_id = ActorID::of(&job_id, &parent, 1);
        let task_id = TaskID::for_actor_creation_task(&actor_id);
        assert_eq!(task_id.actor_id(), actor_id);
        assert!(task_id.is_for_actor_creation_task());
    }

    #[test]
    fn test_object_id_from_index() {
        let task_id = TaskID::from_random();
        let obj_id = ObjectID::from_index(&task_id, 5);
        assert_eq!(obj_id.object_index(), 5);
        assert_eq!(obj_id.task_id(), task_id);
    }

    #[test]
    fn test_object_id_for_actor_handle() {
        let job_id = JobID::from_int(1);
        let actor_id = ActorID::of(&job_id, &TaskID::nil(), 0);
        let obj_id = ObjectID::for_actor_handle(&actor_id);
        assert!(ObjectID::is_actor_id(&obj_id));
        assert_eq!(ObjectID::to_actor_id(&obj_id), actor_id);
    }

    #[test]
    fn test_placement_group_id_embeds_job_id() {
        let job_id = JobID::from_int(99);
        let pg_id = PlacementGroupID::of(&job_id);
        assert_eq!(pg_id.job_id(), job_id);
    }

    #[test]
    fn test_lease_id_from_worker() {
        let worker_id = WorkerID::from_random();
        let lease = LeaseID::from_worker(&worker_id, 42);
        assert_eq!(lease.worker_id(), worker_id);
    }

    #[test]
    fn test_unique_id_size() {
        assert_eq!(UniqueID::SIZE, 28);
        assert_eq!(WorkerID::SIZE, 28);
        assert_eq!(NodeID::SIZE, 28);
        assert_eq!(ClusterID::SIZE, 28);
    }

    #[test]
    fn test_hex_roundtrip() {
        let id = UniqueID::from_random();
        let hex_str = id.hex();
        assert_eq!(hex_str.len(), 56);
        let id2 = UniqueID::from_hex(&hex_str);
        assert_eq!(id, id2);
    }

    #[test]
    fn test_hash_deterministic() {
        let id = UniqueID::from_random();
        let h1 = id.murmur_hash();
        let h2 = id.murmur_hash();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_gcs_node_id_is_zero() {
        assert_eq!(GCS_NODE_ID.data(), &[0u8; UNIQUE_ID_SIZE]);
    }
}
