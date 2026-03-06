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
}
