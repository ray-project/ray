// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Resource demand calculator.
//!
//! Aggregates pending task and actor resource demands for the autoscaler.
//! The autoscaler uses these demands to determine when to scale up the cluster.
//!
//! Replaces parts of `src/ray/raylet/scheduling/cluster_resource_manager.cc`
//! related to demand tracking.

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;
use ray_common::scheduling::{FixedPoint, ResourceSet};

/// Configuration for the demand calculator.
#[derive(Debug, Clone)]
pub struct DemandCalculatorConfig {
    /// Maximum number of individual task demands to report
    /// (beyond this, demands are aggregated by shape).
    pub max_reported_demands: usize,

    /// How long to keep demand entries that haven't been refreshed.
    pub demand_ttl_secs: u64,
}

impl Default for DemandCalculatorConfig {
    fn default() -> Self {
        Self {
            max_reported_demands: 10_000,
            demand_ttl_secs: 60,
        }
    }
}

/// A demand shape — a unique combination of resource requirements.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DemandShape {
    /// Resource requirements as sorted (name, amount) pairs.
    /// Sorted for deterministic hashing/equality.
    requirements: Vec<(String, i64)>,
}

impl DemandShape {
    /// Create a demand shape from a ResourceSet.
    pub fn from_resource_set(resources: &ResourceSet) -> Self {
        let mut requirements: Vec<(String, i64)> = resources
            .iter()
            .filter(|(_, v)| *v > FixedPoint::ZERO)
            .map(|(k, v)| (k.to_string(), v.raw()))
            .collect();
        requirements.sort_by(|a, b| a.0.cmp(&b.0));
        Self { requirements }
    }

    /// Convert back to a ResourceSet.
    pub fn to_resource_set(&self) -> ResourceSet {
        let mut rs = ResourceSet::new();
        for (name, raw) in &self.requirements {
            rs.set(name.clone(), FixedPoint::from_raw(*raw));
        }
        rs
    }
}

/// Aggregated demand for a particular resource shape.
#[derive(Debug, Clone)]
pub struct AggregatedDemand {
    /// The resource shape.
    pub shape: DemandShape,
    /// Number of tasks/actors with this shape.
    pub count: u64,
    /// When this demand was last updated.
    pub last_updated: Instant,
}

/// A snapshot of the current resource demands.
#[derive(Debug, Clone, Default)]
pub struct DemandSnapshot {
    /// Aggregated demands by shape.
    pub demands: Vec<AggregatedDemand>,
    /// Total demanded resources (sum across all shapes).
    pub total_demand: ResourceSet,
    /// Total number of pending requests.
    pub total_count: u64,
}

/// Resource demand calculator that aggregates pending task/actor demands.
pub struct DemandCalculator {
    config: DemandCalculatorConfig,
    /// Demand aggregated by resource shape.
    demands: Mutex<HashMap<DemandShape, AggregatedDemand>>,
    /// Infeasible demands — demands that no node in the cluster can satisfy.
    infeasible: Mutex<HashMap<DemandShape, AggregatedDemand>>,
}

impl DemandCalculator {
    pub fn new(config: DemandCalculatorConfig) -> Self {
        Self {
            config,
            demands: Mutex::new(HashMap::new()),
            infeasible: Mutex::new(HashMap::new()),
        }
    }

    /// Add a resource demand (a pending task or actor).
    pub fn add_demand(&self, resources: &ResourceSet) {
        let shape = DemandShape::from_resource_set(resources);
        let mut demands = self.demands.lock();

        let entry = demands
            .entry(shape.clone())
            .or_insert_with(|| AggregatedDemand {
                shape,
                count: 0,
                last_updated: Instant::now(),
            });
        entry.count += 1;
        entry.last_updated = Instant::now();
    }

    /// Remove a resource demand (task was scheduled or cancelled).
    pub fn remove_demand(&self, resources: &ResourceSet) {
        let shape = DemandShape::from_resource_set(resources);
        let mut demands = self.demands.lock();

        if let Some(entry) = demands.get_mut(&shape) {
            entry.count = entry.count.saturating_sub(1);
            if entry.count == 0 {
                demands.remove(&shape);
            }
        }
    }

    /// Mark a demand shape as infeasible (no node can satisfy it).
    pub fn add_infeasible_demand(&self, resources: &ResourceSet) {
        let shape = DemandShape::from_resource_set(resources);
        let mut infeasible = self.infeasible.lock();

        let entry = infeasible
            .entry(shape.clone())
            .or_insert_with(|| AggregatedDemand {
                shape,
                count: 0,
                last_updated: Instant::now(),
            });
        entry.count += 1;
        entry.last_updated = Instant::now();
    }

    /// Remove an infeasible demand.
    pub fn remove_infeasible_demand(&self, resources: &ResourceSet) {
        let shape = DemandShape::from_resource_set(resources);
        let mut infeasible = self.infeasible.lock();

        if let Some(entry) = infeasible.get_mut(&shape) {
            entry.count = entry.count.saturating_sub(1);
            if entry.count == 0 {
                infeasible.remove(&shape);
            }
        }
    }

    /// Clear all demand entries.
    pub fn clear(&self) {
        self.demands.lock().clear();
        self.infeasible.lock().clear();
    }

    /// Get a snapshot of all pending demands.
    pub fn get_demand_snapshot(&self) -> DemandSnapshot {
        let demands = self.demands.lock();
        let mut total_demand = ResourceSet::new();
        let mut total_count = 0u64;
        let mut demand_list = Vec::with_capacity(demands.len());

        for (_, demand) in demands.iter() {
            let rs = demand.shape.to_resource_set();
            // Add count * resources to total.
            for (name, amount) in rs.iter() {
                let current = total_demand.get(name);
                // FixedPoint doesn't implement Mul, so scale via raw values.
                let scaled = FixedPoint::from_raw(amount.raw() * demand.count as i64);
                total_demand.set(name.to_string(), current + scaled);
            }
            total_count += demand.count;
            demand_list.push(demand.clone());
        }

        // Sort by count descending for reporting.
        demand_list.sort_by(|a, b| b.count.cmp(&a.count));
        if demand_list.len() > self.config.max_reported_demands {
            demand_list.truncate(self.config.max_reported_demands);
        }

        DemandSnapshot {
            demands: demand_list,
            total_demand,
            total_count,
        }
    }

    /// Get a snapshot of infeasible demands.
    pub fn get_infeasible_snapshot(&self) -> DemandSnapshot {
        let infeasible = self.infeasible.lock();
        let mut total_demand = ResourceSet::new();
        let mut total_count = 0u64;
        let mut demand_list = Vec::with_capacity(infeasible.len());

        for (_, demand) in infeasible.iter() {
            let rs = demand.shape.to_resource_set();
            for (name, amount) in rs.iter() {
                let current = total_demand.get(name);
                let scaled = FixedPoint::from_raw(amount.raw() * demand.count as i64);
                total_demand.set(name.to_string(), current + scaled);
            }
            total_count += demand.count;
            demand_list.push(demand.clone());
        }

        demand_list.sort_by(|a, b| b.count.cmp(&a.count));

        DemandSnapshot {
            demands: demand_list,
            total_demand,
            total_count,
        }
    }

    /// Get the total pending demand count.
    pub fn total_pending_count(&self) -> u64 {
        self.demands.lock().values().map(|d| d.count).sum()
    }

    /// Get the total infeasible demand count.
    pub fn total_infeasible_count(&self) -> u64 {
        self.infeasible.lock().values().map(|d| d.count).sum()
    }

    /// Number of distinct demand shapes.
    pub fn num_demand_shapes(&self) -> usize {
        self.demands.lock().len()
    }

    /// Remove stale demands older than the configured TTL.
    pub fn evict_stale_demands(&self) {
        let ttl = std::time::Duration::from_secs(self.config.demand_ttl_secs);
        let cutoff = Instant::now() - ttl;

        self.demands.lock().retain(|_, d| d.last_updated >= cutoff);
        self.infeasible
            .lock()
            .retain(|_, d| d.last_updated >= cutoff);
    }
}

impl Default for DemandCalculator {
    fn default() -> Self {
        Self::new(DemandCalculatorConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cpu_demand(cpus: f64) -> ResourceSet {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(cpus));
        rs
    }

    fn gpu_demand(gpus: f64) -> ResourceSet {
        let mut rs = ResourceSet::new();
        rs.set("GPU".to_string(), FixedPoint::from_f64(gpus));
        rs
    }

    fn mixed_demand(cpus: f64, gpus: f64) -> ResourceSet {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(cpus));
        rs.set("GPU".to_string(), FixedPoint::from_f64(gpus));
        rs
    }

    #[test]
    fn test_add_and_remove_demand() {
        let calc = DemandCalculator::default();
        calc.add_demand(&cpu_demand(4.0));
        calc.add_demand(&cpu_demand(4.0));
        assert_eq!(calc.total_pending_count(), 2);
        assert_eq!(calc.num_demand_shapes(), 1);

        calc.remove_demand(&cpu_demand(4.0));
        assert_eq!(calc.total_pending_count(), 1);

        calc.remove_demand(&cpu_demand(4.0));
        assert_eq!(calc.total_pending_count(), 0);
        assert_eq!(calc.num_demand_shapes(), 0);
    }

    #[test]
    fn test_different_shapes() {
        let calc = DemandCalculator::default();
        calc.add_demand(&cpu_demand(2.0));
        calc.add_demand(&cpu_demand(4.0));
        calc.add_demand(&gpu_demand(1.0));
        assert_eq!(calc.num_demand_shapes(), 3);
        assert_eq!(calc.total_pending_count(), 3);
    }

    #[test]
    fn test_demand_snapshot() {
        let calc = DemandCalculator::default();
        calc.add_demand(&cpu_demand(2.0));
        calc.add_demand(&cpu_demand(2.0));
        calc.add_demand(&cpu_demand(2.0));
        calc.add_demand(&gpu_demand(1.0));

        let snap = calc.get_demand_snapshot();
        assert_eq!(snap.total_count, 4);
        assert_eq!(snap.demands.len(), 2);
        // Total demand: 6 CPUs + 1 GPU
        assert_eq!(snap.total_demand.get("CPU"), FixedPoint::from_f64(6.0));
        assert_eq!(snap.total_demand.get("GPU"), FixedPoint::from_f64(1.0));

        // First entry should be CPU demand (count=3), sorted descending.
        assert_eq!(snap.demands[0].count, 3);
        assert_eq!(snap.demands[1].count, 1);
    }

    #[test]
    fn test_infeasible_demands() {
        let calc = DemandCalculator::default();
        calc.add_infeasible_demand(&mixed_demand(100.0, 8.0));
        calc.add_infeasible_demand(&mixed_demand(100.0, 8.0));
        assert_eq!(calc.total_infeasible_count(), 2);

        let snap = calc.get_infeasible_snapshot();
        assert_eq!(snap.total_count, 2);
        assert_eq!(snap.total_demand.get("CPU"), FixedPoint::from_f64(200.0));
        assert_eq!(snap.total_demand.get("GPU"), FixedPoint::from_f64(16.0));
    }

    #[test]
    fn test_remove_more_than_added() {
        let calc = DemandCalculator::default();
        calc.add_demand(&cpu_demand(1.0));
        calc.remove_demand(&cpu_demand(1.0));
        calc.remove_demand(&cpu_demand(1.0)); // Extra remove should not panic.
        assert_eq!(calc.total_pending_count(), 0);
    }

    #[test]
    fn test_clear() {
        let calc = DemandCalculator::default();
        calc.add_demand(&cpu_demand(1.0));
        calc.add_infeasible_demand(&gpu_demand(1.0));
        calc.clear();
        assert_eq!(calc.total_pending_count(), 0);
        assert_eq!(calc.total_infeasible_count(), 0);
    }

    #[test]
    fn test_demand_shape_roundtrip() {
        let rs = mixed_demand(4.0, 2.0);
        let shape = DemandShape::from_resource_set(&rs);
        let rs2 = shape.to_resource_set();
        assert_eq!(rs.get("CPU"), rs2.get("CPU"));
        assert_eq!(rs.get("GPU"), rs2.get("GPU"));
    }

    #[test]
    fn test_snapshot_truncation() {
        let config = DemandCalculatorConfig {
            max_reported_demands: 2,
            ..Default::default()
        };
        let calc = DemandCalculator::new(config);

        // Add 5 different shapes.
        for i in 1..=5 {
            calc.add_demand(&cpu_demand(i as f64));
        }
        let snap = calc.get_demand_snapshot();
        assert_eq!(snap.demands.len(), 2); // Truncated to max 2.
        assert_eq!(snap.total_count, 5); // Total still 5.
    }

    #[test]
    fn test_zero_resources_ignored() {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        rs.set("GPU".to_string(), FixedPoint::ZERO);
        let shape = DemandShape::from_resource_set(&rs);
        // GPU=0 should be filtered out.
        assert_eq!(shape.requirements.len(), 1);
        assert_eq!(shape.requirements[0].0, "CPU");
    }
}
