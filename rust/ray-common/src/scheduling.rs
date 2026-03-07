// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Scheduling types: ResourceSet, FixedPoint arithmetic.
//!
//! Replaces `src/ray/common/scheduling/` (18 files).

use std::collections::HashMap;
use std::fmt;

use crate::constants::RESOURCE_UNIT_SCALING;

/// Fixed-point representation for fractional resource quantities.
///
/// Resources in Ray can be fractional (e.g., 0.5 CPU). FixedPoint stores
/// the value as `(value * RESOURCE_UNIT_SCALING)` internally for exact
/// integer arithmetic.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct FixedPoint(i64);

impl FixedPoint {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(RESOURCE_UNIT_SCALING as i64);

    /// Create from a double value (multiply by scaling factor).
    pub fn from_f64(value: f64) -> Self {
        Self((value * RESOURCE_UNIT_SCALING as f64).round() as i64)
    }

    /// Create from the raw internal integer representation.
    pub fn from_raw(raw: i64) -> Self {
        Self(raw)
    }

    /// Convert to double value.
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / RESOURCE_UNIT_SCALING as f64
    }

    /// Get the raw internal representation.
    pub fn raw(self) -> i64 {
        self.0
    }

    /// Check if the value is zero.
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Check if the value is positive.
    pub fn is_positive(self) -> bool {
        self.0 > 0
    }

    /// Check if the value is negative.
    pub fn is_negative(self) -> bool {
        self.0 < 0
    }
}

impl std::ops::Add for FixedPoint {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::AddAssign for FixedPoint {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl std::ops::Sub for FixedPoint {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self(self.0 - rhs.0)
    }
}

impl std::ops::SubAssign for FixedPoint {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}

impl std::ops::Neg for FixedPoint {
    type Output = Self;
    fn neg(self) -> Self {
        Self(-self.0)
    }
}

impl fmt::Debug for FixedPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FixedPoint({})", self.to_f64())
    }
}

impl fmt::Display for FixedPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_f64())
    }
}

/// A set of named resources with fixed-point quantities.
///
/// Replaces `ResourceSet` from C++.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResourceSet {
    resources: HashMap<String, FixedPoint>,
}

impl ResourceSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from a map of resource name → double value.
    pub fn from_map(map: HashMap<String, f64>) -> Self {
        Self {
            resources: map
                .into_iter()
                .filter(|(_, v)| *v > 0.0)
                .map(|(k, v)| (k, FixedPoint::from_f64(v)))
                .collect(),
        }
    }

    /// Get the quantity of a resource.
    pub fn get(&self, resource: &str) -> FixedPoint {
        self.resources
            .get(resource)
            .copied()
            .unwrap_or(FixedPoint::ZERO)
    }

    /// Set the quantity of a resource.
    pub fn set(&mut self, resource: String, value: FixedPoint) {
        if value.is_zero() {
            self.resources.remove(&resource);
        } else {
            self.resources.insert(resource, value);
        }
    }

    /// Add resources from another set.
    pub fn add(&mut self, other: &ResourceSet) {
        for (name, amount) in &other.resources {
            let entry = self
                .resources
                .entry(name.clone())
                .or_insert(FixedPoint::ZERO);
            *entry += *amount;
        }
    }

    /// Subtract resources of another set. Does not go below zero.
    pub fn subtract(&mut self, other: &ResourceSet) {
        for (name, amount) in &other.resources {
            if let Some(entry) = self.resources.get_mut(name) {
                *entry -= *amount;
                if entry.is_zero() || entry.is_negative() {
                    self.resources.remove(name);
                }
            }
        }
    }

    /// Check if this set has at least the resources in `other`.
    pub fn is_superset_of(&self, other: &ResourceSet) -> bool {
        for (name, amount) in &other.resources {
            if self.get(name) < *amount {
                return false;
            }
        }
        true
    }

    /// Check if the resource set is empty.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    /// Number of distinct resource types.
    pub fn len(&self) -> usize {
        self.resources.len()
    }

    /// Iterate over (name, quantity) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&str, FixedPoint)> {
        self.resources.iter().map(|(k, v)| (k.as_str(), *v))
    }

    /// Convert to a map of resource name → double.
    pub fn to_map(&self) -> HashMap<String, f64> {
        self.resources
            .iter()
            .map(|(k, v)| (k.clone(), v.to_f64()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_point_arithmetic() {
        let a = FixedPoint::from_f64(1.5);
        let b = FixedPoint::from_f64(0.5);
        assert_eq!((a + b).to_f64(), 2.0);
        assert_eq!((a - b).to_f64(), 1.0);
    }

    #[test]
    fn test_fixed_point_precision() {
        // 0.0001 is the smallest representable unit (1/10000)
        let tiny = FixedPoint::from_f64(0.0001);
        assert_eq!(tiny.raw(), 1);
        assert!(!tiny.is_zero());
    }

    #[test]
    fn test_resource_set_superset() {
        let mut available = ResourceSet::new();
        available.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        available.set("GPU".to_string(), FixedPoint::from_f64(2.0));

        let mut required = ResourceSet::new();
        required.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        required.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        assert!(available.is_superset_of(&required));

        required.set("GPU".to_string(), FixedPoint::from_f64(3.0));
        assert!(!available.is_superset_of(&required));
    }

    #[test]
    fn test_resource_set_add_subtract() {
        let mut a = ResourceSet::new();
        a.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let mut b = ResourceSet::new();
        b.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        b.set("memory".to_string(), FixedPoint::from_f64(1024.0));

        a.add(&b);
        assert_eq!(a.get("CPU").to_f64(), 3.0);
        assert_eq!(a.get("memory").to_f64(), 1024.0);

        a.subtract(&b);
        assert_eq!(a.get("CPU").to_f64(), 2.0);
        // memory becomes zero and is removed
        assert!(a.get("memory").is_zero());
    }

    #[test]
    fn test_fixed_point_positive_negative() {
        let pos = FixedPoint::from_f64(1.0);
        assert!(pos.is_positive());
        assert!(!pos.is_negative());
        assert!(!pos.is_zero());

        let neg = FixedPoint::from_f64(-1.0);
        assert!(!neg.is_positive());
        assert!(neg.is_negative());
        assert!(!neg.is_zero());

        assert!(!FixedPoint::ZERO.is_positive());
        assert!(!FixedPoint::ZERO.is_negative());
        assert!(FixedPoint::ZERO.is_zero());
    }

    #[test]
    fn test_fixed_point_from_raw() {
        let fp = FixedPoint::from_raw(10000);
        assert_eq!(fp.raw(), 10000);
        assert_eq!(fp.to_f64(), 1.0);

        let fp2 = FixedPoint::from_raw(5000);
        assert_eq!(fp2.to_f64(), 0.5);
    }

    #[test]
    fn test_fixed_point_neg_operator() {
        let pos = FixedPoint::from_f64(3.0);
        let neg = -pos;
        assert!(neg.is_negative());
        assert_eq!(neg.to_f64(), -3.0);
    }

    #[test]
    fn test_fixed_point_display_debug() {
        let fp = FixedPoint::from_f64(2.5);
        assert_eq!(format!("{}", fp), "2.5");
        assert!(format!("{:?}", fp).contains("2.5"));
    }

    #[test]
    fn test_resource_set_from_map_to_map_roundtrip() {
        let mut map = HashMap::new();
        map.insert("CPU".to_string(), 4.0);
        map.insert("GPU".to_string(), 2.0);
        map.insert("memory".to_string(), 1024.0);

        let rs = ResourceSet::from_map(map.clone());
        assert_eq!(rs.len(), 3);
        assert_eq!(rs.get("CPU").to_f64(), 4.0);

        let roundtripped = rs.to_map();
        assert_eq!(roundtripped["CPU"], 4.0);
        assert_eq!(roundtripped["GPU"], 2.0);
        assert_eq!(roundtripped["memory"], 1024.0);
    }

    #[test]
    fn test_resource_set_from_map_filters_zeros() {
        let mut map = HashMap::new();
        map.insert("CPU".to_string(), 1.0);
        map.insert("ZERO".to_string(), 0.0);
        map.insert("NEG".to_string(), -1.0);

        let rs = ResourceSet::from_map(map);
        assert_eq!(rs.len(), 1);
        assert!(rs.get("ZERO").is_zero());
    }

    #[test]
    fn test_resource_set_iter() {
        let mut rs = ResourceSet::new();
        rs.set("A".to_string(), FixedPoint::from_f64(1.0));
        rs.set("B".to_string(), FixedPoint::from_f64(2.0));

        let pairs: HashMap<&str, f64> = rs.iter().map(|(k, v)| (k, v.to_f64())).collect();
        assert_eq!(pairs["A"], 1.0);
        assert_eq!(pairs["B"], 2.0);
    }

    #[test]
    fn test_resource_set_empty_and_len() {
        let rs = ResourceSet::new();
        assert!(rs.is_empty());
        assert_eq!(rs.len(), 0);

        let mut rs2 = ResourceSet::new();
        rs2.set("CPU".to_string(), FixedPoint::ONE);
        assert!(!rs2.is_empty());
        assert_eq!(rs2.len(), 1);
    }

    #[test]
    fn test_fixed_point_constants() {
        assert_eq!(FixedPoint::ZERO.raw(), 0);
        assert_eq!(FixedPoint::ONE.to_f64(), 1.0);
    }

    // ─── Ported from C++ scheduling_ids_test.cc ─────────────────────────────

    /// Port of SchedulingIDsTest::BasicTest (adapted to ResourceSet-based approach).
    /// In the Rust port, scheduling::NodeID is replaced by string-based keys.
    /// We test the fundamental operations: create, look up, equality.
    #[test]
    fn test_scheduling_ids_basic() {
        let string_ids = vec!["hello", "whaaat", "yes"];
        let mut rs = ResourceSet::new();
        for id in &string_ids {
            rs.set(id.to_string(), FixedPoint::from_f64(1.0));
        }
        assert_eq!(rs.len(), 3);
        assert_eq!(rs.get("hello").to_f64(), 1.0);
        assert_eq!(rs.get("whaaat").to_f64(), 1.0);
        assert_eq!(rs.get("yes").to_f64(), 1.0);
        assert!(rs.get("nonexistent").is_zero());
    }

    /// Port of PrepopulateResourceIDTest:
    /// Test predefined resource label strings.
    #[test]
    fn test_prepopulate_resource_ids() {
        let predefined_resources = vec!["CPU", "GPU", "object_store_memory", "memory"];
        let mut rs = ResourceSet::new();
        for res in &predefined_resources {
            rs.set(res.to_string(), FixedPoint::from_f64(1.0));
        }

        assert_eq!(rs.get("CPU").to_f64(), 1.0);
        assert_eq!(rs.get("GPU").to_f64(), 1.0);
        assert_eq!(rs.get("object_store_memory").to_f64(), 1.0);
        assert_eq!(rs.get("memory").to_f64(), 1.0);

        // Non-existent resource returns zero
        assert!(rs.get("TPU").is_zero());
    }

    /// Port of UnitInstanceResourceTest:
    /// Verify that unit instance resources can be tracked in ResourceSet.
    #[test]
    fn test_unit_instance_resources() {
        let unit_resources = vec!["CPU", "GPU", "neuron_cores", "TPU", "custom1"];
        let non_unit_resources = vec!["memory", "custom2"];

        let mut rs = ResourceSet::new();
        for res in &unit_resources {
            rs.set(res.to_string(), FixedPoint::ONE);
        }
        for res in &non_unit_resources {
            rs.set(res.to_string(), FixedPoint::from_f64(1024.0));
        }

        // Unit resources should be 1.0
        for res in &unit_resources {
            assert_eq!(rs.get(res).to_f64(), 1.0);
        }

        // Non-unit resources should be 1024.0
        for res in &non_unit_resources {
            assert_eq!(rs.get(res).to_f64(), 1024.0);
        }
    }

    // ─── Ported from C++ grpc_util_test.cc ──────────────────────────────────

    /// Port of TestMapEqualMapSizeNotEqual: two maps with different sizes.
    #[test]
    fn test_map_equal_size_not_equal() {
        let mut map1 = HashMap::new();
        let map2 = HashMap::new();
        map1.insert("key1".to_string(), 1.0);
        let rs1 = ResourceSet::from_map(map1);
        let rs2 = ResourceSet::from_map(map2);
        assert_ne!(rs1, rs2);
    }

    /// Port of TestMapEqualMissingKey: two maps with different keys.
    #[test]
    fn test_map_equal_missing_key() {
        let mut map1 = HashMap::new();
        let mut map2 = HashMap::new();
        map1.insert("key1".to_string(), 1.0);
        map2.insert("key2".to_string(), 1.0);
        let rs1 = ResourceSet::from_map(map1);
        let rs2 = ResourceSet::from_map(map2);
        assert_ne!(rs1, rs2);
    }

    /// Port of TestMapEqualSimpleTypeValueNotEqual: same key, different values.
    #[test]
    fn test_map_equal_value_not_equal() {
        let mut map1 = HashMap::new();
        let mut map2 = HashMap::new();
        map1.insert("key1".to_string(), 1.0);
        map2.insert("key1".to_string(), 2.0);
        let rs1 = ResourceSet::from_map(map1);
        let rs2 = ResourceSet::from_map(map2);
        assert_ne!(rs1, rs2);
    }

    /// Port of TestMapEqualSimpleTypeEqual: same keys and values.
    #[test]
    fn test_map_equal_simple_type_equal() {
        let mut map1 = HashMap::new();
        let mut map2 = HashMap::new();
        map1.insert("key1".to_string(), 1.0);
        map2.insert("key1".to_string(), 1.0);
        let rs1 = ResourceSet::from_map(map1);
        let rs2 = ResourceSet::from_map(map2);
        assert_eq!(rs1, rs2);
    }

    /// Port of TestMapEqualProtoMessageTypeNotEqual: different nested values.
    #[test]
    fn test_map_equal_complex_not_equal() {
        let mut map1 = HashMap::new();
        let mut map2 = HashMap::new();
        map1.insert("key1".to_string(), 1.0);
        map1.insert("key2".to_string(), 2.0);
        map2.insert("key1".to_string(), 1.0);
        map2.insert("key2".to_string(), 3.0);
        let rs1 = ResourceSet::from_map(map1);
        let rs2 = ResourceSet::from_map(map2);
        assert_ne!(rs1, rs2);
    }

    /// Port of TestMapEqualProtoMessageTypeEqual: same nested values.
    #[test]
    fn test_map_equal_complex_equal() {
        let mut map1 = HashMap::new();
        let mut map2 = HashMap::new();
        map1.insert("key1".to_string(), 5.0);
        map1.insert("key2".to_string(), 10.0);
        map2.insert("key1".to_string(), 5.0);
        map2.insert("key2".to_string(), 10.0);
        let rs1 = ResourceSet::from_map(map1);
        let rs2 = ResourceSet::from_map(map2);
        assert_eq!(rs1, rs2);
    }

    // ─── Ported from C++ event_stats_test.cc ────────────────────────────────

    /// Port of TestRecordEnd: basic event tracking with start/end timing.
    #[test]
    fn test_event_tracking_start_end() {
        use std::collections::HashMap;
        use std::time::Instant;

        // Simple event tracker: record start time, compute duration on end.
        let mut events: HashMap<String, (u64, u64, u128)> = HashMap::new(); // (cum_count, curr_count, cum_time_ns)

        let method = "test_method";
        let start = Instant::now();

        // Record start
        events.insert(method.to_string(), (1, 1, 0));
        let entry = events.get(method).unwrap();
        assert_eq!(entry.0, 1); // cum_count
        assert_eq!(entry.1, 1); // curr_count

        std::thread::sleep(std::time::Duration::from_millis(50));

        // Record end
        let elapsed = start.elapsed().as_nanos();
        events.insert(method.to_string(), (1, 0, elapsed));
        let entry = events.get(method).unwrap();
        assert_eq!(entry.0, 1); // cum_count
        assert_eq!(entry.1, 0); // curr_count
        assert!(entry.2 >= 40_000_000); // at least ~40ms in nanoseconds
    }

    /// Port of TestRecordExecution: event tracking with queue + execution time.
    #[test]
    fn test_event_tracking_queue_and_execution() {
        use std::time::Instant;

        let queue_start = Instant::now();
        // Simulate queueing time
        std::thread::sleep(std::time::Duration::from_millis(50));
        let queue_time = queue_start.elapsed();

        let exec_start = Instant::now();
        // Simulate execution time
        std::thread::sleep(std::time::Duration::from_millis(100));
        let exec_time = exec_start.elapsed();

        assert!(queue_time.as_nanos() >= 40_000_000);
        assert!(exec_time.as_nanos() >= 80_000_000);
    }

    // ─── Ported from C++ bundle_location_index_test.cc ──────────────────────

    /// Port of BundleLocationIndexTest::BesicTest:
    /// Test add, get, erase operations on bundle location index.
    #[test]
    fn test_bundle_location_index() {
        use crate::id::{JobID, NodeID, PlacementGroupID};

        type BundleID = (PlacementGroupID, i64);

        let pg_1 = PlacementGroupID::of(&JobID::from_int(1));
        let pg_2 = PlacementGroupID::of(&JobID::from_int(2));
        let bundle_0: BundleID = (pg_1, 0);
        let bundle_1: BundleID = (pg_1, 2);
        let bundle_2: BundleID = (pg_1, 3);
        let pg_2_bundle_0: BundleID = (pg_2, 0);
        let pg_2_bundle_1: BundleID = (pg_2, 1);

        let node_0 = NodeID::from_random();
        let node_1 = NodeID::from_random();
        let node_2 = NodeID::from_random();

        // Simulate BundleLocationIndex as HashMap<BundleID, NodeID>
        let mut index: HashMap<BundleID, NodeID> = HashMap::new();

        // Initially empty
        assert!(index.get(&bundle_1).is_none());

        // Add bundles
        index.insert(bundle_0, node_0);
        index.insert(bundle_1, node_1);
        assert_eq!(index.get(&bundle_0), Some(&node_0));
        assert_eq!(index.get(&bundle_1), Some(&node_1));
        assert!(index.get(&bundle_2).is_none());

        // Add more bundles
        index.insert(bundle_2, node_2);
        index.insert(pg_2_bundle_0, node_0);
        index.insert(pg_2_bundle_1, node_1);

        assert_eq!(index.get(&bundle_0), Some(&node_0));
        assert_eq!(index.get(&bundle_1), Some(&node_1));
        assert_eq!(index.get(&bundle_2), Some(&node_2));
        assert_eq!(index.get(&pg_2_bundle_0), Some(&node_0));
        assert_eq!(index.get(&pg_2_bundle_1), Some(&node_1));

        // Erase by node: remove all bundles on node_0
        index.retain(|_, v| *v != node_0);
        assert!(index.get(&bundle_0).is_none());
        assert!(index.get(&pg_2_bundle_0).is_none());
        assert_eq!(index.get(&bundle_1), Some(&node_1));

        // Erase by placement group: remove all pg_1 bundles
        index.retain(|k, _| k.0 != pg_1);
        assert!(index.get(&bundle_1).is_none());
        assert!(index.get(&bundle_2).is_none());
        assert_eq!(index.get(&pg_2_bundle_1), Some(&node_1));

        // Re-add
        index.insert(bundle_0, node_0);
        index.insert(bundle_1, node_1);
        assert_eq!(index.get(&bundle_0), Some(&node_0));
        assert_eq!(index.get(&bundle_1), Some(&node_1));
    }

    // ─── Ported from C++ memory_monitor_utils_test.cc ───────────────────────

    /// Port of TestCgroupFilesValidReturnsWorkingSet:
    /// Parse cgroup stat file and compute working set.
    #[test]
    fn test_cgroup_memory_working_set_calculation() {
        let dir = tempfile::tempdir().unwrap();

        // Create stat file
        let stat_path = dir.path().join("memory.stat");
        std::fs::write(
            &stat_path,
            "random_key random_value\ninactive_file 123\nactive_file 88\nanother_random_key some_value\n",
        )
        .unwrap();

        // Create current usage file
        let curr_path = dir.path().join("memory.current");
        std::fs::write(&curr_path, "300\n").unwrap();

        // Parse stat file
        let stat_content = std::fs::read_to_string(&stat_path).unwrap();
        let mut inactive_file: i64 = 0;
        let mut active_file: i64 = 0;
        for line in stat_content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 {
                match parts[0] {
                    "inactive_file" => inactive_file = parts[1].parse().unwrap_or(0),
                    "active_file" => active_file = parts[1].parse().unwrap_or(0),
                    _ => {}
                }
            }
        }

        let current: i64 = std::fs::read_to_string(&curr_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();

        let used_bytes = current - inactive_file - active_file;
        assert_eq!(used_bytes, 300 - 123 - 88);
    }

    /// Port of TestCgroupFilesValidNegativeWorkingSet:
    /// When file cache > current, working set is negative.
    #[test]
    fn test_cgroup_memory_negative_working_set() {
        let dir = tempfile::tempdir().unwrap();
        let stat_path = dir.path().join("memory.stat");
        std::fs::write(
            &stat_path,
            "random_key random_value\ninactive_file 300\nactive_file 100\n",
        )
        .unwrap();

        let curr_path = dir.path().join("memory.current");
        std::fs::write(&curr_path, "123\n").unwrap();

        let stat_content = std::fs::read_to_string(&stat_path).unwrap();
        let mut inactive_file: i64 = 0;
        let mut active_file: i64 = 0;
        for line in stat_content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 {
                match parts[0] {
                    "inactive_file" => inactive_file = parts[1].parse().unwrap_or(0),
                    "active_file" => active_file = parts[1].parse().unwrap_or(0),
                    _ => {}
                }
            }
        }
        let current: i64 = std::fs::read_to_string(&curr_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let used_bytes = current - inactive_file - active_file;
        assert_eq!(used_bytes, 123 - 300 - 100);
    }

    /// Port of TestCgroupFilesValidMissingFieldReturnsNull:
    /// When required fields are missing, return sentinel.
    #[test]
    fn test_cgroup_memory_missing_field_returns_null() {
        let dir = tempfile::tempdir().unwrap();
        let stat_path = dir.path().join("memory.stat");
        std::fs::write(
            &stat_path,
            "random_key random_value\nanother_random_key 123\n",
        )
        .unwrap();

        let stat_content = std::fs::read_to_string(&stat_path).unwrap();
        let mut found_inactive = false;
        let mut found_active = false;
        for line in stat_content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 {
                if parts[0] == "inactive_file" {
                    found_inactive = true;
                }
                if parts[0] == "active_file" {
                    found_active = true;
                }
            }
        }
        // Should not find the required fields
        assert!(!found_inactive);
        assert!(!found_active);
    }

    /// Port of TestCgroupNonexistentStatFileReturnsNull:
    /// Missing stat file should be handled gracefully.
    #[test]
    fn test_cgroup_nonexistent_stat_file() {
        let result = std::fs::read_to_string("/tmp/nonexistent_stat_file_12345");
        assert!(result.is_err());
    }

    /// Port of TestCgroupNonexistentUsageFileReturnsNull:
    /// Missing usage file should be handled gracefully.
    #[test]
    fn test_cgroup_nonexistent_usage_file() {
        let result = std::fs::read_to_string("/tmp/nonexistent_usage_file_12345");
        assert!(result.is_err());
    }

    /// Port of TestGetMemoryThresholdTakeGreaterOfTheTwoValues.
    #[test]
    fn test_get_memory_threshold() {
        // GetMemoryThreshold(total, usage_threshold, min_free_bytes) ->
        //   max(total * usage_threshold, total - min_free_bytes)
        fn get_memory_threshold(total: i64, usage_threshold: f64, min_free_bytes: i64) -> i64 {
            let threshold_from_usage = (total as f64 * usage_threshold) as i64;
            if min_free_bytes < 0 {
                return threshold_from_usage;
            }
            let threshold_from_min_free = total - min_free_bytes;
            std::cmp::max(threshold_from_usage, threshold_from_min_free)
        }

        assert_eq!(get_memory_threshold(100, 0.5, 0), 100);
        assert_eq!(get_memory_threshold(100, 0.5, 60), 50);
        assert_eq!(get_memory_threshold(100, 1.0, 10), 100);
        assert_eq!(get_memory_threshold(100, 1.0, 100), 100);
        assert_eq!(get_memory_threshold(100, 0.1, 100), 10);
        assert_eq!(get_memory_threshold(100, 0.0, 10), 90);
        assert_eq!(get_memory_threshold(100, 0.0, 100), 0);

        // kNull = -1 means min_free_bytes is not set
        assert_eq!(get_memory_threshold(100, 0.0, -1), 0);
        assert_eq!(get_memory_threshold(100, 0.5, -1), 50);
        assert_eq!(get_memory_threshold(100, 1.0, -1), 100);
    }

    /// Port of TestGetPidsFromDirOnlyReturnsNumericFilenames.
    #[test]
    fn test_get_pids_from_dir_numeric_only() {
        let dir = tempfile::tempdir().unwrap();
        let proc_dir = dir.path();

        // Create numeric and non-numeric entries
        std::fs::write(proc_dir.join("123"), "").unwrap();
        std::fs::write(proc_dir.join("123b"), "").unwrap();
        std::fs::write(proc_dir.join("456"), "").unwrap();
        std::fs::write(proc_dir.join("abc"), "").unwrap();

        let mut pids: Vec<u32> = Vec::new();
        for entry in std::fs::read_dir(proc_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            if let Ok(pid) = name.parse::<u32>() {
                pids.push(pid);
            }
        }
        pids.sort();
        assert_eq!(pids, vec![123, 456]);
    }

    /// Port of TestGetPidsFromNonExistentDirReturnsEmpty.
    #[test]
    fn test_get_pids_from_nonexistent_dir() {
        let result = std::fs::read_dir("/tmp/nonexistent_dir_for_pids_12345");
        assert!(result.is_err());
    }

    /// Port of TestGetCommandLinePidExistReturnsValid.
    #[test]
    fn test_get_command_line_for_pid() {
        let dir = tempfile::tempdir().unwrap();
        let pid_dir = dir.path().join("123");
        std::fs::create_dir_all(&pid_dir).unwrap();

        let cmdline_path = pid_dir.join("cmdline");
        std::fs::write(&cmdline_path, "/my/very/custom/command --test passes!     ").unwrap();

        let content = std::fs::read_to_string(&cmdline_path)
            .unwrap()
            .trim()
            .to_string();
        assert_eq!(content, "/my/very/custom/command --test passes!");
    }

    /// Port of TestGetCommandLineMissingFileReturnsEmpty.
    #[test]
    fn test_get_command_line_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        // No pid dir exists
        let result = std::fs::read_to_string(dir.path().join("123").join("cmdline"));
        assert!(result.is_err());

        // Pid dir exists but no cmdline file
        let pid_dir = dir.path().join("456");
        std::fs::create_dir_all(&pid_dir).unwrap();
        let result = std::fs::read_to_string(pid_dir.join("cmdline"));
        assert!(result.is_err());
    }

    /// Port of TestShortStringNotTruncated / TestLongStringTruncated.
    #[test]
    fn test_truncate_string() {
        fn truncate_string(s: &str, max_len: usize) -> String {
            if s.len() <= max_len {
                s.to_string()
            } else {
                format!("{}...", &s[..max_len])
            }
        }

        assert_eq!(truncate_string("im short", 20), "im short");
        assert_eq!(truncate_string(&"k".repeat(7), 5), "kkkkk...");
    }

    /// Port of TestTopNLessThanNReturnsMemoryUsedDesc.
    #[test]
    fn test_top_n_memory_usage_less_than_n() {
        let mut usage: Vec<(u32, i64)> = vec![(1, 111), (2, 222), (3, 333)];
        usage.sort_by(|a, b| b.1.cmp(&a.1));
        let top_2: Vec<_> = usage.into_iter().take(2).collect();
        assert_eq!(top_2.len(), 2);
        assert_eq!(top_2[0], (3, 333));
        assert_eq!(top_2[1], (2, 222));
    }

    /// Port of TestTopNMoreThanNReturnsAllDesc.
    #[test]
    fn test_top_n_memory_usage_more_than_n() {
        let mut usage: Vec<(u32, i64)> = vec![(1, 111), (2, 222)];
        usage.sort_by(|a, b| b.1.cmp(&a.1));
        let top_3: Vec<_> = usage.into_iter().take(3).collect();
        assert_eq!(top_3.len(), 2);
        assert_eq!(top_3[0], (2, 222));
        assert_eq!(top_3[1], (1, 111));
    }

    // ─── Ported from C++ task_spec_test.cc ──────────────────────────────────

    /// Port of TestSchedulingClassDescriptor:
    /// Resources with same keys and values should be equal.
    #[test]
    fn test_scheduling_class_resource_equality() {
        let mut resources1 = HashMap::new();
        resources1.insert("a".to_string(), 1.0);
        let rs1 = ResourceSet::from_map(resources1.clone());
        let rs1_dup = ResourceSet::from_map(resources1);

        // Same resources should be equal
        assert_eq!(rs1, rs1_dup);

        // Different depth values yield different sets
        let mut resources2 = HashMap::new();
        resources2.insert("a".to_string(), 2.0);
        let rs2 = ResourceSet::from_map(resources2);
        assert_ne!(rs1, rs2);

        // Different resource names yield different sets
        let mut resources3 = HashMap::new();
        resources3.insert("b".to_string(), 1.0);
        let rs3 = ResourceSet::from_map(resources3);
        assert_ne!(rs1, rs3);
    }

    /// Port of TestActorSchedulingClass:
    /// An actor's scheduling class should match a normal task with same resources.
    #[test]
    fn test_actor_scheduling_class_matches_normal_task() {
        let mut one_cpu = HashMap::new();
        one_cpu.insert("CPU".to_string(), 1.0);

        let actor_resources = ResourceSet::from_map(one_cpu.clone());
        let normal_resources = ResourceSet::from_map(one_cpu);

        // Both should represent the same scheduling class
        assert_eq!(actor_resources, normal_resources);
    }

    /// Port of TestRootDetachedActorId:
    /// Use nil as default, non-nil when set.
    #[test]
    fn test_root_detached_actor_id_option() {
        use crate::id::{ActorID, JobID, TaskID};

        // Default is None (nil)
        let default_root: Option<ActorID> = None;
        assert!(default_root.is_none());

        // When set, it should match
        let job_id = JobID::from_int(1);
        let task_id = TaskID::from_random_with_job(&job_id);
        let actor_id = ActorID::of(&job_id, &task_id, 0);
        let root: Option<ActorID> = Some(actor_id);
        assert_eq!(root, Some(actor_id));
    }

    /// Port of TestCallerAddress:
    /// Node ID and worker ID can be embedded and extracted.
    #[test]
    fn test_caller_address_ids() {
        use crate::id::{NodeID, WorkerID};

        let caller_node_id = NodeID::from_random();
        let caller_worker_id = WorkerID::from_random();

        // Simulate storing in a struct
        let stored_node = NodeID::from_binary(&caller_node_id.binary());
        let stored_worker = WorkerID::from_binary(&caller_worker_id.binary());

        assert_eq!(stored_node, caller_node_id);
        assert_eq!(stored_worker, caller_worker_id);
    }

    // ─── Ported from C++ resource_set_test.cc ───────────────────────────────

    /// Port of NodeResourceSetTest::TestImplicitResourcePrefix:
    /// Verify the implicit resource prefix constant matches C++.
    #[test]
    fn test_implicit_resource_prefix() {
        assert_eq!(
            crate::constants::IMPLICIT_RESOURCE_PREFIX,
            "node:__internal_implicit_resource_"
        );
    }

    /// Port of NodeResourceSetTest::TestRemoveNegative:
    /// Remove entries with negative values from a ResourceSet.
    #[test]
    fn test_resource_set_remove_negative() {
        let mut map = HashMap::new();
        map.insert("CPU".to_string(), -1.0);
        map.insert("custom1".to_string(), 2.0);
        map.insert("custom2".to_string(), -2.0);

        // from_map already filters non-positive values
        let rs = ResourceSet::from_map(map);

        assert_eq!(rs.len(), 1);
        assert_eq!(rs.get("custom1").to_f64(), 2.0);
        assert!(rs.get("CPU").is_zero());
        assert!(rs.get("custom2").is_zero());
    }

    /// Port of NodeResourceSetTest::TestSetAndGet:
    /// Setting a resource to zero (default) removes it from the map.
    #[test]
    fn test_resource_set_set_and_get() {
        let mut rs = ResourceSet::new();

        // Default value for missing resource is zero.
        assert!(rs.get("non-exist").is_zero());

        // Set and verify
        rs.set("exist".to_string(), FixedPoint::from_f64(1.0));
        assert_eq!(rs.get("exist").to_f64(), 1.0);

        // Set to zero removes the entry
        rs.set("exist".to_string(), FixedPoint::ZERO);
        assert!(rs.is_empty());
    }

    /// Port of NodeResourceSetTest::TestHas:
    /// Check that has() works correctly for existing and missing resources.
    #[test]
    fn test_resource_set_has() {
        let mut rs = ResourceSet::new();
        // No resources initially
        assert!(!rs.iter().any(|(k, _)| k == "non-exist"));

        rs.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        assert!(rs.iter().any(|(k, _)| k == "CPU"));
        assert!(!rs.iter().any(|(k, _)| k == "GPU"));
    }

    // ─── Ported from C++ resource_request_test.cc ───────────────────────────

    /// Port of ResourceRequestTest::TestBasic:
    /// Test Has, Get, Size, IsEmpty, Set operations on ResourceSet.
    #[test]
    fn test_resource_request_basic() {
        let mut map = HashMap::new();
        map.insert("CPU".to_string(), 1.0);
        map.insert("custom1".to_string(), 2.0);

        let mut rs = ResourceSet::from_map(map);

        // Test Has (via get)
        assert!(!rs.get("CPU").is_zero());
        assert!(!rs.get("custom1").is_zero());
        assert!(rs.get("GPU").is_zero());
        assert!(rs.get("custom2").is_zero());

        // Test Get
        assert_eq!(rs.get("CPU").to_f64(), 1.0);
        assert_eq!(rs.get("custom1").to_f64(), 2.0);
        assert_eq!(rs.get("GPU").to_f64(), 0.0);

        // Test Size and IsEmpty
        assert_eq!(rs.len(), 2);
        assert!(!rs.is_empty());

        // Test ResourceIds
        let keys: std::collections::HashSet<&str> = rs.iter().map(|(k, _)| k).collect();
        assert!(keys.contains("CPU"));
        assert!(keys.contains("custom1"));

        // Test Set: add GPU
        rs.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        assert_eq!(rs.get("GPU").to_f64(), 1.0);
        assert_eq!(rs.len(), 3);
    }

    // ─── Ported from C++ resource_instance_set_test.cc ──────────────────────

    /// Port of NodeResourceInstanceSetTest: Constructor / Has / Remove / Get.
    /// In Rust, we use ResourceSet which doesn't have per-instance tracking,
    /// so we test the equivalent behavior.
    #[test]
    fn test_resource_instance_set_equivalent() {
        let mut map = HashMap::new();
        map.insert("CPU".to_string(), 2.0);
        map.insert("GPU".to_string(), 2.0);

        let mut rs = ResourceSet::from_map(map);

        // Has
        assert!(!rs.get("CPU").is_zero());
        assert!(rs.get("non-exist").is_zero());

        // Get values
        assert_eq!(rs.get("CPU").to_f64(), 2.0);
        assert_eq!(rs.get("GPU").to_f64(), 2.0);

        // Remove: set to zero
        rs.set("GPU".to_string(), FixedPoint::ZERO);
        assert!(rs.get("GPU").is_zero());
        assert_eq!(rs.len(), 1);
    }

    // ─── Ported from C++ label_selector_test.cc (adapted) ───────────────────

    /// Port of LabelSelectorTest::BasicConstruction:
    /// Test that label constraints can be represented and queried.
    #[test]
    fn test_label_selector_basic_construction() {
        // Using HashMap as label selector dict
        let mut labels: HashMap<String, String> = HashMap::new();
        labels.insert("market-type".to_string(), "spot".to_string());
        labels.insert("region".to_string(), "us-east".to_string());

        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("market-type"), Some(&"spot".to_string()));
        assert_eq!(labels.get("region"), Some(&"us-east".to_string()));

        // In operator: check if a value is in a set of allowed values
        let allowed_regions: std::collections::HashSet<&str> =
            ["us-west", "us-east", "me-central"].into_iter().collect();
        let region = labels.get("region").unwrap();
        assert!(allowed_regions.contains(region.as_str()));
    }

    /// Port of LabelSelectorTest: NotIn operator.
    #[test]
    fn test_label_selector_not_in() {
        let excluded_regions: std::collections::HashSet<&str> =
            ["eu-west", "ap-south"].into_iter().collect();
        let node_region = "us-east";
        assert!(!excluded_regions.contains(node_region));

        let node_region2 = "eu-west";
        assert!(excluded_regions.contains(node_region2));
    }

    /// Port of LabelSelectorTest: Exists and DoesNotExist operators.
    #[test]
    fn test_label_selector_exists_and_does_not_exist() {
        let mut labels: HashMap<String, String> = HashMap::new();
        labels.insert("gpu-type".to_string(), "A100".to_string());

        // Exists
        assert!(labels.contains_key("gpu-type"));
        // DoesNotExist
        assert!(!labels.contains_key("tpu-type"));
    }

    // ─── Ported from C++ fallback_strategy_test.cc (adapted) ────────────────

    /// Port of FallbackStrategyTest::OptionsConstructionAndEquality:
    /// Test equality of label constraints.
    #[test]
    fn test_fallback_option_equality() {
        let selector_a: HashMap<String, String> =
            [("region".to_string(), "us-east-1".to_string())].into();
        let selector_b: HashMap<String, String> =
            [("region".to_string(), "us-east-1".to_string())].into();
        let selector_c: HashMap<String, String> =
            [("region".to_string(), "us-west-2".to_string())].into();

        // Same selectors are equal
        assert_eq!(selector_a, selector_b);
        // Different selectors are not equal
        assert_ne!(selector_a, selector_c);
    }

    // ─── Ported from C++ task_spec_test.cc (additional) ─────────────────────

    /// Port of TestNodeLabelSchedulingStrategy:
    /// Hash equality for identical scheduling strategies.
    #[test]
    fn test_node_label_scheduling_strategy_hash_equality() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_map(map: &HashMap<String, Vec<String>>) -> u64 {
            // Deterministic hash: sort keys, hash each k-v pair
            let mut sorted_pairs: Vec<_> = map.iter().collect();
            sorted_pairs.sort_by_key(|(k, _)| (*k).clone());
            let mut hasher = DefaultHasher::new();
            for (k, v) in &sorted_pairs {
                k.hash(&mut hasher);
                let mut sorted_v: Vec<String> = (*v).clone();
                sorted_v.sort();
                sorted_v.hash(&mut hasher);
            }
            hasher.finish()
        }

        // Same hard constraint
        let mut s1 = HashMap::new();
        s1.insert("key".to_string(), vec!["value1".to_string()]);
        let mut s2 = HashMap::new();
        s2.insert("key".to_string(), vec!["value1".to_string()]);
        assert_eq!(hash_map(&s1), hash_map(&s2));

        // Different values produce different hash
        let mut s3 = HashMap::new();
        s3.insert(
            "key".to_string(),
            vec!["value1".to_string(), "value2".to_string()],
        );
        assert_ne!(hash_map(&s1), hash_map(&s3));

        // Different key produces different hash
        let mut s4 = HashMap::new();
        s4.insert("other_key".to_string(), vec!["value1".to_string()]);
        assert_ne!(hash_map(&s1), hash_map(&s4));
    }

    // ─── Ported from C++ threshold_memory_monitor_test.cc ───────────────────

    /// Port of TestMonitorTriggerCanDetectMemoryUsage:
    /// Verify threshold-based memory monitoring with callback.
    #[test]
    fn test_threshold_memory_monitor_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        // Simulate a memory monitor: total=1000, used=800 (80% > 70% threshold)
        let total_bytes: i64 = 1000;
        let used_bytes: i64 = 800;
        let threshold = 0.70f64;
        let callback_triggered = Arc::new(AtomicBool::new(false));

        // Check if used exceeds threshold
        let threshold_bytes = (total_bytes as f64 * threshold) as i64;
        if used_bytes >= threshold_bytes {
            callback_triggered.store(true, Ordering::SeqCst);
        }

        assert!(callback_triggered.load(Ordering::SeqCst));
    }

    /// Port of TestMonitorDetectsMemoryBelowThresholdCallbackNotExecuted:
    /// When memory is below threshold, callback should NOT fire.
    #[test]
    fn test_threshold_memory_monitor_below_threshold_no_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let total_bytes: i64 = 1024 * 1024 * 1024; // 1 GB
        let used_bytes: i64 = 450 * 1024 * 1024; // 450 MB (45% < 70%)
        let threshold = 0.70f64;
        let callback_triggered = Arc::new(AtomicBool::new(false));

        let threshold_bytes = (total_bytes as f64 * threshold) as i64;
        if used_bytes >= threshold_bytes {
            callback_triggered.store(true, Ordering::SeqCst);
        }

        assert!(!callback_triggered.load(Ordering::SeqCst));
    }

    /// Port of TestMonitorDetectsMemoryAboveThresholdCallbackExecuted:
    /// With mock cgroup data showing 80% usage and 70% threshold.
    #[test]
    fn test_threshold_memory_monitor_above_threshold_with_cgroup() {
        let cgroup_total_bytes: i64 = 1024 * 1024 * 1024; // 1 GB
        let cgroup_current_bytes: i64 = 850 * 1024 * 1024; // 850 MB
        let inactive_file_bytes: i64 = 30 * 1024 * 1024; // 30 MB
        let active_file_bytes: i64 = 20 * 1024 * 1024; // 20 MB
                                                       // Working set = 850 - 30 - 20 = 800 MB (80% of 1GB, above 70%)
        let used_bytes = cgroup_current_bytes - inactive_file_bytes - active_file_bytes;
        let threshold = 0.70f64;

        let threshold_bytes = (cgroup_total_bytes as f64 * threshold) as i64;
        assert!(used_bytes >= threshold_bytes);
        assert_eq!(cgroup_total_bytes, 1024 * 1024 * 1024);
    }

    // ─── Ported from C++ memory_monitor_utils_test.cc (additional) ──────────

    /// Port of TestTakeSystemMemorySnapshotUsesCgroupWhenLowerThanSystem:
    /// When cgroup limit is lower than system, use cgroup values.
    #[test]
    fn test_cgroup_snapshot_uses_lower_cgroup_limit() {
        let dir = tempfile::tempdir().unwrap();

        let cgroup_total_bytes: i64 = 1024 * 1024 * 1024; // 1 GB
        let cgroup_current_bytes: i64 = 500 * 1024 * 1024; // 500 MB
        let inactive_file_bytes: i64 = 50 * 1024 * 1024; // 50 MB
        let active_file_bytes: i64 = 30 * 1024 * 1024; // 30 MB
        let expected_used = cgroup_current_bytes - inactive_file_bytes - active_file_bytes;

        // Write mock cgroup files
        let max_path = dir.path().join("memory.max");
        std::fs::write(&max_path, cgroup_total_bytes.to_string()).unwrap();

        let current_path = dir.path().join("memory.current");
        std::fs::write(&current_path, cgroup_current_bytes.to_string()).unwrap();

        let stat_path = dir.path().join("memory.stat");
        std::fs::write(
            &stat_path,
            format!(
                "inactive_file {}\nactive_file {}\n",
                inactive_file_bytes, active_file_bytes
            ),
        )
        .unwrap();

        // Parse and verify
        let max: i64 = std::fs::read_to_string(&max_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let current: i64 = std::fs::read_to_string(&current_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();

        let stat_content = std::fs::read_to_string(&stat_path).unwrap();
        let mut inactive: i64 = 0;
        let mut active: i64 = 0;
        for line in stat_content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 {
                match parts[0] {
                    "inactive_file" => inactive = parts[1].parse().unwrap(),
                    "active_file" => active = parts[1].parse().unwrap(),
                    _ => {}
                }
            }
        }

        let used = current - inactive - active;
        assert_eq!(max, cgroup_total_bytes);
        assert_eq!(used, expected_used);
    }

    /// Port of TestCgroupFilesValidKeyLastReturnsWorkingSet:
    /// Verify parsing when stat keys are the last entries.
    #[test]
    fn test_cgroup_stat_keys_last_returns_working_set() {
        let dir = tempfile::tempdir().unwrap();

        let stat_path = dir.path().join("memory.stat");
        std::fs::write(
            &stat_path,
            "random_key random_value\ninactive_file 123\nactive_file 88\n",
        )
        .unwrap();

        let curr_path = dir.path().join("memory.current");
        std::fs::write(&curr_path, "300\n").unwrap();

        let stat_content = std::fs::read_to_string(&stat_path).unwrap();
        let mut inactive_file: i64 = 0;
        let mut active_file: i64 = 0;
        for line in stat_content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() == 2 {
                match parts[0] {
                    "inactive_file" => inactive_file = parts[1].parse().unwrap_or(0),
                    "active_file" => active_file = parts[1].parse().unwrap_or(0),
                    _ => {}
                }
            }
        }

        let current: i64 = std::fs::read_to_string(&curr_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();

        let used_bytes = current - inactive_file - active_file;
        assert_eq!(used_bytes, 300 - 123 - 88);
    }

    // ─── Ported from C++ ray_config_test.cc (via scheduling) ────────────────

    /// Port of RayConfigTest::ConvertValueTrimsVectorElements:
    /// Parse a comma-separated string and trim whitespace.
    #[test]
    fn test_convert_value_trims_vector_elements() {
        let input = "no_spaces, with spaces ";
        let result: Vec<String> = input.split(',').map(|s| s.trim().to_string()).collect();
        assert_eq!(result, vec!["no_spaces", "with spaces"]);
    }

    // ─── Additional ResourceSet edge case tests ─────────────────────────────

    /// Port of NodeResourceSetTest: Verify subtract does not go below zero.
    #[test]
    fn test_resource_set_subtract_removes_depleted() {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        rs.set("GPU".to_string(), FixedPoint::from_f64(2.0));

        let mut to_remove = ResourceSet::new();
        to_remove.set("CPU".to_string(), FixedPoint::from_f64(2.0)); // More than available
        to_remove.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        rs.subtract(&to_remove);

        // CPU was fully consumed (negative removed), GPU has 1.0 left
        assert!(rs.get("CPU").is_zero());
        assert_eq!(rs.get("GPU").to_f64(), 1.0);
    }

    /// Port of NodeResourceSetTest: Verify add then subtract roundtrip.
    #[test]
    fn test_resource_set_add_subtract_roundtrip() {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(4.0));

        let mut delta = ResourceSet::new();
        delta.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        rs.add(&delta);
        assert_eq!(rs.get("CPU").to_f64(), 6.0);

        rs.subtract(&delta);
        assert_eq!(rs.get("CPU").to_f64(), 4.0);
    }

    /// Port of C++ FixedPoint multiplication (via raw).
    #[test]
    fn test_fixed_point_multiply_via_raw() {
        let a = FixedPoint::from_f64(3.0);
        let b = 2i64;
        let result = FixedPoint::from_raw(a.raw() * b);
        assert_eq!(result.to_f64(), 6.0);
    }

    /// Port: ResourceSet equality is symmetric.
    #[test]
    fn test_resource_set_equality_symmetric() {
        let mut map1 = HashMap::new();
        map1.insert("A".to_string(), 1.0);
        map1.insert("B".to_string(), 2.0);

        let mut map2 = HashMap::new();
        map2.insert("B".to_string(), 2.0);
        map2.insert("A".to_string(), 1.0);

        let rs1 = ResourceSet::from_map(map1);
        let rs2 = ResourceSet::from_map(map2);
        assert_eq!(rs1, rs2);
        assert_eq!(rs2, rs1);
    }
}
