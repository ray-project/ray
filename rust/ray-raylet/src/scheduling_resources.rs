// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Extended scheduling resource types for the raylet.
//!
//! Builds on `ray_common::scheduling::{FixedPoint, ResourceSet}` to add
//! instance-level tracking (`NodeResourceInstances`), per-node resource
//! state (`NodeResources`), label matching, and scheduling options.

use std::collections::HashMap;

use ray_common::scheduling::{FixedPoint, ResourceSet};

/// Predefined resource names.
pub const CPU: &str = "CPU";
pub const MEM: &str = "memory";
pub const GPU: &str = "GPU";
pub const OBJECT_STORE_MEM: &str = "object_store_memory";

/// Per-instance resource tracking (e.g., individual GPU slots).
///
/// Maps resource name → vector of per-instance available capacities.
/// For unit-instance resources like GPU, each slot has capacity 1.0.
/// For non-instance resources like CPU, there is a single "instance".
#[derive(Debug, Clone, Default)]
pub struct NodeResourceInstanceSet {
    resources: HashMap<String, Vec<FixedPoint>>,
}

impl NodeResourceInstanceSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from a ResourceSet, treating unit-instance resources (CPU, GPU)
    /// as having N individual slots of capacity 1.0.
    pub fn from_resource_set(resources: &ResourceSet) -> Self {
        let mut inst = Self::new();
        for (name, amount) in resources.iter() {
            if is_unit_instance_resource(name) {
                let count = amount.to_f64().ceil() as usize;
                let mut slots = vec![FixedPoint::ONE; count];
                // Last slot may be fractional
                let remainder = amount - FixedPoint::from_f64(count.saturating_sub(1) as f64);
                if count > 0 && remainder < FixedPoint::ONE {
                    slots[count - 1] = remainder;
                }
                inst.resources.insert(name.to_string(), slots);
            } else {
                inst.resources.insert(name.to_string(), vec![amount]);
            }
        }
        inst
    }

    /// Try to allocate resources. Returns per-instance allocation on success.
    /// This is all-or-nothing across all resource types.
    pub fn try_allocate(&mut self, request: &ResourceSet) -> Option<TaskResourceInstances> {
        // First check feasibility
        for (name, amount) in request.iter() {
            let total_available: FixedPoint = self
                .resources
                .get(name)
                .map(|slots| slots.iter().copied().fold(FixedPoint::ZERO, |a, b| a + b))
                .unwrap_or(FixedPoint::ZERO);
            if total_available < amount {
                return None;
            }
        }

        // Now allocate
        let mut allocation = TaskResourceInstances::new();
        for (name, amount) in request.iter() {
            if let Some(slots) = self.resources.get_mut(name) {
                let alloc = allocate_from_instances(slots, amount);
                allocation.resources.insert(name.to_string(), alloc);
            }
        }
        Some(allocation)
    }

    /// Release previously allocated resources.
    pub fn free(&mut self, allocation: &TaskResourceInstances) {
        for (name, alloc_slots) in &allocation.resources {
            if let Some(slots) = self.resources.get_mut(name) {
                for (i, amount) in alloc_slots.iter().enumerate() {
                    if i < slots.len() {
                        slots[i] += *amount;
                    }
                }
            }
        }
    }

    /// Get total available for a resource.
    pub fn total_available(&self, resource: &str) -> FixedPoint {
        self.resources
            .get(resource)
            .map(|slots| slots.iter().copied().fold(FixedPoint::ZERO, |a, b| a + b))
            .unwrap_or(FixedPoint::ZERO)
    }

    /// Convert to a flat ResourceSet (summing all instances).
    pub fn to_resource_set(&self) -> ResourceSet {
        let mut result = ResourceSet::new();
        for (name, slots) in &self.resources {
            let total = slots.iter().copied().fold(FixedPoint::ZERO, |a, b| a + b);
            if total.is_positive() {
                result.set(name.clone(), total);
            }
        }
        result
    }

    /// Get the instance slots for a resource.
    pub fn get_instances(&self, resource: &str) -> Option<&[FixedPoint]> {
        self.resources.get(resource).map(|v| v.as_slice())
    }

    /// Set the instances for a resource.
    pub fn set_instances(&mut self, resource: String, instances: Vec<FixedPoint>) {
        self.resources.insert(resource, instances);
    }

    /// Get all resource names.
    pub fn resource_names(&self) -> impl Iterator<Item = &str> {
        self.resources.keys().map(|s| s.as_str())
    }
}

/// Per-instance allocation for a single task.
#[derive(Debug, Clone, Default)]
pub struct TaskResourceInstances {
    pub resources: HashMap<String, Vec<FixedPoint>>,
}

impl TaskResourceInstances {
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert to a flat ResourceSet.
    pub fn to_resource_set(&self) -> ResourceSet {
        let mut result = ResourceSet::new();
        for (name, slots) in &self.resources {
            let total = slots.iter().copied().fold(FixedPoint::ZERO, |a, b| a + b);
            if total.is_positive() {
                result.set(name.clone(), total);
            }
        }
        result
    }

    /// Check if this allocation is empty.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
            || self
                .resources
                .values()
                .all(|slots| slots.iter().all(|s| s.is_zero()))
    }
}

/// Per-node resources: total capacity, available, load, labels, drain state.
#[derive(Debug, Clone)]
pub struct NodeResources {
    pub total: ResourceSet,
    pub available: ResourceSet,
    pub load: ResourceSet,
    pub labels: HashMap<String, String>,
    pub is_draining: bool,
    pub draining_deadline_ms: u64,
}

impl NodeResources {
    pub fn new(total: ResourceSet) -> Self {
        Self {
            available: total.clone(),
            total,
            load: ResourceSet::new(),
            labels: HashMap::new(),
            is_draining: false,
            draining_deadline_ms: 0,
        }
    }

    /// Check if resources are available for the given request.
    pub fn is_available(&self, request: &ResourceSet) -> bool {
        self.available.is_superset_of(request)
    }

    /// Check if resources could ever be available (total capacity is sufficient).
    pub fn is_feasible(&self, request: &ResourceSet) -> bool {
        self.total.is_superset_of(request)
    }

    /// Check if the node has matching labels for the selector.
    pub fn has_required_labels(&self, selector: &LabelSelector) -> bool {
        selector.matches(&self.labels)
    }

    /// Calculate the critical resource utilization (max utilization across resources).
    pub fn critical_resource_utilization(&self) -> f64 {
        let mut max_util = 0.0_f64;
        for (name, total_amount) in self.total.iter() {
            if total_amount.is_zero() {
                continue;
            }
            let avail = self.available.get(name);
            let used = (total_amount - avail).to_f64();
            let util = used / total_amount.to_f64();
            max_util = max_util.max(util);
        }
        max_util
    }

    /// Check if this node has GPU resources.
    pub fn has_gpu(&self) -> bool {
        self.total.get(GPU).is_positive()
    }
}

impl Default for NodeResources {
    fn default() -> Self {
        Self::new(ResourceSet::new())
    }
}

/// Instance-level node resources (used for the local node only).
#[derive(Debug, Clone)]
pub struct NodeResourceInstances {
    pub total: NodeResourceInstanceSet,
    pub available: NodeResourceInstanceSet,
    pub labels: HashMap<String, String>,
}

impl NodeResourceInstances {
    pub fn new(total: ResourceSet, labels: HashMap<String, String>) -> Self {
        let total_inst = NodeResourceInstanceSet::from_resource_set(&total);
        let available_inst = NodeResourceInstanceSet::from_resource_set(&total);
        Self {
            total: total_inst,
            available: available_inst,
            labels,
        }
    }

    /// Convert to a NodeResources view.
    pub fn to_node_resources(&self) -> NodeResources {
        NodeResources {
            total: self.total.to_resource_set(),
            available: self.available.to_resource_set(),
            load: ResourceSet::new(),
            labels: self.labels.clone(),
            is_draining: false,
            draining_deadline_ms: 0,
        }
    }
}

/// Label constraint for node-label scheduling.
#[derive(Debug, Clone)]
pub struct LabelConstraint {
    pub key: String,
    pub operator: LabelOperator,
    pub values: Vec<String>,
}

/// Label operators matching the protobuf `LabelOperator` oneof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelOperator {
    /// Node label value must be in the provided set.
    In,
    /// Node label value must NOT be in the provided set.
    NotIn,
    /// Node label key must exist (value irrelevant).
    Exists,
    /// Node label key must NOT exist.
    DoesNotExist,
}

/// A set of label constraints that all must match.
#[derive(Debug, Clone, Default)]
pub struct LabelSelector {
    pub constraints: Vec<LabelConstraint>,
}

impl LabelSelector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if labels match all constraints (AND semantics).
    pub fn matches(&self, labels: &HashMap<String, String>) -> bool {
        self.constraints.iter().all(|c| {
            let value = labels.get(&c.key);
            match c.operator {
                LabelOperator::In => {
                    if let Some(v) = value {
                        c.values.iter().any(|allowed| allowed == v)
                    } else {
                        false
                    }
                }
                LabelOperator::NotIn => {
                    if let Some(v) = value {
                        !c.values.iter().any(|disallowed| disallowed == v)
                    } else {
                        true // key not present satisfies NotIn
                    }
                }
                LabelOperator::Exists => value.is_some(),
                LabelOperator::DoesNotExist => value.is_none(),
            }
        })
    }

    pub fn is_empty(&self) -> bool {
        self.constraints.is_empty()
    }
}

/// Scheduling types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchedulingType {
    Hybrid,
    Spread,
    Random,
    NodeAffinity,
    BundlePack,
    BundleSpread,
    BundleStrictPack,
    BundleStrictSpread,
    AffinityWithBundle,
    NodeLabel,
}

/// Scheduling options passed to scheduling policies.
#[derive(Debug, Clone)]
pub struct SchedulingOptions {
    pub scheduling_type: SchedulingType,
    pub spread_threshold: f64,
    pub avoid_local_node: bool,
    pub require_node_available: bool,
    pub avoid_gpu_nodes: bool,
    pub preferred_node_id: Option<String>,
    pub schedule_top_k_absolute: usize,
    pub scheduler_top_k_fraction: f64,
    // Node affinity specific
    pub node_affinity_node_id: Option<String>,
    pub node_affinity_soft: bool,
    pub node_affinity_spill_on_unavailable: bool,
    pub node_affinity_fail_on_unavailable: bool,
    // Label scheduling specific
    pub label_selector: LabelSelector,
    /// Hard label expressions — ALL must be satisfied or scheduling fails.
    pub label_match_expressions_hard: LabelSelector,
    /// Soft label expressions — preferred but not required; increases priority.
    pub label_match_expressions_soft: LabelSelector,
}

impl Default for SchedulingOptions {
    fn default() -> Self {
        Self {
            scheduling_type: SchedulingType::Hybrid,
            spread_threshold: 0.5,
            avoid_local_node: false,
            require_node_available: true,
            avoid_gpu_nodes: false,
            preferred_node_id: None,
            schedule_top_k_absolute: 1,
            scheduler_top_k_fraction: 0.0,
            node_affinity_node_id: None,
            node_affinity_soft: false,
            node_affinity_spill_on_unavailable: false,
            node_affinity_fail_on_unavailable: false,
            label_selector: LabelSelector::new(),
            label_match_expressions_hard: LabelSelector::new(),
            label_match_expressions_soft: LabelSelector::new(),
        }
    }
}

impl SchedulingOptions {
    pub fn hybrid() -> Self {
        Self::default()
    }

    pub fn spread() -> Self {
        Self {
            scheduling_type: SchedulingType::Spread,
            ..Self::default()
        }
    }

    pub fn random() -> Self {
        Self {
            scheduling_type: SchedulingType::Random,
            ..Self::default()
        }
    }

    pub fn node_affinity(
        node_id: String,
        soft: bool,
        spill_on_unavailable: bool,
        fail_on_unavailable: bool,
    ) -> Self {
        Self {
            scheduling_type: SchedulingType::NodeAffinity,
            node_affinity_node_id: Some(node_id),
            node_affinity_soft: soft,
            node_affinity_spill_on_unavailable: spill_on_unavailable,
            node_affinity_fail_on_unavailable: fail_on_unavailable,
            ..Self::default()
        }
    }

    pub fn node_label(selector: LabelSelector) -> Self {
        Self {
            scheduling_type: SchedulingType::NodeLabel,
            label_selector: selector,
            ..Self::default()
        }
    }

    /// Create node-label scheduling options with hard and soft expressions.
    ///
    /// Hard expressions must all be satisfied or scheduling fails.
    /// Soft expressions increase priority when satisfied.
    pub fn node_label_with_expressions(
        hard: LabelSelector,
        soft: LabelSelector,
    ) -> Self {
        Self {
            scheduling_type: SchedulingType::NodeLabel,
            label_match_expressions_hard: hard,
            label_match_expressions_soft: soft,
            ..Self::default()
        }
    }
}

/// Result of a scheduling decision.
#[derive(Debug, Clone)]
pub enum SchedulingResult {
    /// Successfully scheduled to a node.
    Success(Vec<String>),
    /// Failed but may succeed later (retryable).
    Failed,
    /// Infeasible — will never succeed with current cluster.
    Infeasible,
}

// ── helpers ──────────────────────────────────────────────────────────

/// Check if a resource is a unit-instance resource (individual slots).
fn is_unit_instance_resource(name: &str) -> bool {
    matches!(name, CPU | GPU)
}

/// Allocate `amount` from instance slots, returns per-slot allocation.
fn allocate_from_instances(slots: &mut [FixedPoint], amount: FixedPoint) -> Vec<FixedPoint> {
    let mut alloc = vec![FixedPoint::ZERO; slots.len()];
    let mut remaining = amount;

    // First pass: allocate from full-capacity instances
    for (i, slot) in slots.iter_mut().enumerate() {
        if remaining.is_zero() {
            break;
        }
        if *slot >= remaining {
            alloc[i] = remaining;
            *slot -= remaining;
            remaining = FixedPoint::ZERO;
        } else if slot.is_positive() {
            alloc[i] = *slot;
            remaining -= *slot;
            *slot = FixedPoint::ZERO;
        }
    }
    alloc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_resource_instance_set_from_resource_set() {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        rs.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        rs.set("memory".to_string(), FixedPoint::from_f64(8192.0));

        let inst = NodeResourceInstanceSet::from_resource_set(&rs);

        // CPU has 4 unit slots
        assert_eq!(inst.get_instances("CPU").unwrap().len(), 4);
        // GPU has 2 unit slots
        assert_eq!(inst.get_instances("GPU").unwrap().len(), 2);
        // memory has 1 slot
        assert_eq!(inst.get_instances("memory").unwrap().len(), 1);
        assert_eq!(inst.total_available("memory"), FixedPoint::from_f64(8192.0));
    }

    #[test]
    fn test_try_allocate_and_free() {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        rs.set("GPU".to_string(), FixedPoint::from_f64(2.0));

        let mut inst = NodeResourceInstanceSet::from_resource_set(&rs);

        let mut request = ResourceSet::new();
        request.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        request.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let alloc = inst.try_allocate(&request).unwrap();
        assert_eq!(inst.total_available("CPU"), FixedPoint::from_f64(2.0));
        assert_eq!(inst.total_available("GPU"), FixedPoint::from_f64(1.0));

        inst.free(&alloc);
        assert_eq!(inst.total_available("CPU"), FixedPoint::from_f64(4.0));
        assert_eq!(inst.total_available("GPU"), FixedPoint::from_f64(2.0));
    }

    #[test]
    fn test_try_allocate_insufficient() {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(2.0));

        let mut inst = NodeResourceInstanceSet::from_resource_set(&rs);

        let mut request = ResourceSet::new();
        request.set("CPU".to_string(), FixedPoint::from_f64(3.0));

        assert!(inst.try_allocate(&request).is_none());
    }

    #[test]
    fn test_node_resources_utilization() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(2.0));

        let mut nr = NodeResources::new(total);

        // Use 2 of 4 CPUs
        let mut used = ResourceSet::new();
        used.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        nr.available.subtract(&used);

        // CPU util = 2/4 = 0.5, GPU util = 0/2 = 0.0
        let util = nr.critical_resource_utilization();
        assert!((util - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_label_selector_in() {
        let selector = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "zone".to_string(),
                operator: LabelOperator::In,
                values: vec!["us-east-1".to_string(), "us-west-2".to_string()],
            }],
        };

        let mut labels = HashMap::new();
        labels.insert("zone".to_string(), "us-east-1".to_string());
        assert!(selector.matches(&labels));

        labels.insert("zone".to_string(), "eu-west-1".to_string());
        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_label_selector_not_in() {
        let selector = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "zone".to_string(),
                operator: LabelOperator::NotIn,
                values: vec!["us-east-1".to_string()],
            }],
        };

        let mut labels = HashMap::new();
        labels.insert("zone".to_string(), "us-west-2".to_string());
        assert!(selector.matches(&labels));

        // Key not present satisfies NotIn
        let empty_labels = HashMap::new();
        assert!(selector.matches(&empty_labels));
    }

    #[test]
    fn test_label_selector_exists() {
        let selector = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "gpu_type".to_string(),
                operator: LabelOperator::Exists,
                values: vec![],
            }],
        };

        let mut labels = HashMap::new();
        labels.insert("gpu_type".to_string(), "A100".to_string());
        assert!(selector.matches(&labels));

        // Any value satisfies Exists
        labels.insert("gpu_type".to_string(), "".to_string());
        assert!(selector.matches(&labels));

        // Missing key does not satisfy Exists
        let empty_labels = HashMap::new();
        assert!(!selector.matches(&empty_labels));
    }

    #[test]
    fn test_label_selector_does_not_exist() {
        let selector = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "gpu_type".to_string(),
                operator: LabelOperator::DoesNotExist,
                values: vec![],
            }],
        };

        // Key not present satisfies DoesNotExist
        let empty_labels = HashMap::new();
        assert!(selector.matches(&empty_labels));

        // Key present does NOT satisfy DoesNotExist
        let mut labels = HashMap::new();
        labels.insert("gpu_type".to_string(), "A100".to_string());
        assert!(!selector.matches(&labels));
    }

    #[test]
    fn test_label_selector_combined_operators() {
        // Must have "zone" key AND zone NOT IN ("eu-west")
        let selector = LabelSelector {
            constraints: vec![
                LabelConstraint {
                    key: "zone".to_string(),
                    operator: LabelOperator::Exists,
                    values: vec![],
                },
                LabelConstraint {
                    key: "zone".to_string(),
                    operator: LabelOperator::NotIn,
                    values: vec!["eu-west".to_string()],
                },
            ],
        };

        let mut labels = HashMap::new();
        labels.insert("zone".to_string(), "us-east".to_string());
        assert!(selector.matches(&labels));

        labels.insert("zone".to_string(), "eu-west".to_string());
        assert!(!selector.matches(&labels));

        // Missing key: Exists fails
        let empty_labels = HashMap::new();
        assert!(!selector.matches(&empty_labels));
    }

    // ─── Ported from C++ scheduling_policy_test.cc ────────────────────

    /// Port of FeasibleDefinitionTest: object_store_memory makes task
    /// infeasible if node doesn't have it, even if CPU is sufficient.
    #[test]
    fn test_feasible_definition_with_object_store_memory() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let nr = NodeResources::new(total);

        // Request CPU + object_store_memory
        let mut req1 = ResourceSet::new();
        req1.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req1.set("object_store_memory".to_string(), FixedPoint::from_f64(1.0));
        assert!(!nr.is_feasible(&req1));

        // Request only CPU → feasible
        let mut req2 = ResourceSet::new();
        req2.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        assert!(nr.is_feasible(&req2));
    }

    /// Port of AvailableDefinitionTest: available vs feasible distinction.
    #[test]
    fn test_available_definition_with_object_store_memory() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let nr = NodeResources::new(total);

        let mut req1 = ResourceSet::new();
        req1.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req1.set("object_store_memory".to_string(), FixedPoint::from_f64(1.0));
        assert!(!nr.is_available(&req1));

        let mut req2 = ResourceSet::new();
        req2.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        assert!(nr.is_available(&req2));
    }

    /// Port of CriticalResourceUtilizationDefinitionTest: verify max
    /// utilization across multiple resource types.
    #[test]
    fn test_critical_resource_utilization_multi_resource() {
        // CPU: 1/2 used = 50%, Memory: 0.75/1 used = 75%, GPU: 1/2 used = 50%
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        total.set("memory".to_string(), FixedPoint::from_f64(1.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        total.set("object_store_memory".to_string(), FixedPoint::from_f64(100.0));

        let mut nr = NodeResources::new(total);
        // Subtract to match: CPU avail=1, mem avail=0.25, GPU avail=1, osm avail=50
        let mut used = ResourceSet::new();
        used.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        used.set("memory".to_string(), FixedPoint::from_f64(0.75));
        used.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        used.set("object_store_memory".to_string(), FixedPoint::from_f64(50.0));
        nr.available.subtract(&used);

        let util = nr.critical_resource_utilization();
        // Memory has 75% utilization which is the maximum
        assert!((util - 0.75).abs() < 0.01, "expected ~0.75, got {}", util);
    }

    /// Port of has_gpu test: verify GPU detection.
    #[test]
    fn test_node_resources_has_gpu() {
        let mut total_with_gpu = ResourceSet::new();
        total_with_gpu.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        total_with_gpu.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        let nr_gpu = NodeResources::new(total_with_gpu);
        assert!(nr_gpu.has_gpu());

        let mut total_no_gpu = ResourceSet::new();
        total_no_gpu.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let nr_no_gpu = NodeResources::new(total_no_gpu);
        assert!(!nr_no_gpu.has_gpu());
    }

    /// Port of TaskResourceInstances empty check.
    #[test]
    fn test_task_resource_instances_is_empty() {
        let empty = TaskResourceInstances::new();
        assert!(empty.is_empty());

        let mut non_empty = TaskResourceInstances::new();
        non_empty.resources.insert(
            "CPU".to_string(),
            vec![FixedPoint::from_f64(1.0)],
        );
        assert!(!non_empty.is_empty());
    }

    /// Port of TaskResourceInstances to_resource_set.
    #[test]
    fn test_task_resource_instances_to_resource_set() {
        let mut tri = TaskResourceInstances::new();
        tri.resources.insert(
            "CPU".to_string(),
            vec![FixedPoint::from_f64(1.0), FixedPoint::from_f64(1.0), FixedPoint::from_f64(1.0)],
        );
        tri.resources.insert(
            "memory".to_string(),
            vec![FixedPoint::from_f64(4.0)],
        );
        tri.resources.insert(
            "GPU".to_string(),
            vec![FixedPoint::from_f64(1.0), FixedPoint::from_f64(1.0), FixedPoint::from_f64(1.0), FixedPoint::from_f64(1.0), FixedPoint::from_f64(1.0)],
        );

        let rs = tri.to_resource_set();
        assert_eq!(rs.get("CPU"), FixedPoint::from_f64(3.0));
        assert_eq!(rs.get("memory"), FixedPoint::from_f64(4.0));
        assert_eq!(rs.get("GPU"), FixedPoint::from_f64(5.0));
    }

    /// Port of instance allocation failure (partial alloc rollback).
    #[test]
    fn test_allocation_failure_no_leak() {
        let mut rs = ResourceSet::new();
        rs.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        rs.set("custom1".to_string(), FixedPoint::from_f64(4.0));

        let mut inst = NodeResourceInstanceSet::from_resource_set(&rs);

        // Request custom1=3 (ok) + custom5=4 (not present, fails)
        let mut request = ResourceSet::new();
        request.set("custom1".to_string(), FixedPoint::from_f64(3.0));
        request.set("custom5".to_string(), FixedPoint::from_f64(4.0));

        let result = inst.try_allocate(&request);
        assert!(result.is_none());

        // Verify no resources leaked: custom1 should still be fully available
        assert_eq!(inst.total_available("custom1"), FixedPoint::from_f64(4.0));
        assert_eq!(inst.total_available("CPU"), FixedPoint::from_f64(4.0));
    }

    /// Port of NodeResourceInstances to_node_resources conversion.
    #[test]
    fn test_node_resource_instances_to_node_resources() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        let labels = HashMap::from([
            ("zone".to_string(), "us-east".to_string()),
        ]);
        let nri = NodeResourceInstances::new(total, labels);
        let nr = nri.to_node_resources();
        assert_eq!(nr.total.get("CPU"), FixedPoint::from_f64(4.0));
        assert_eq!(nr.total.get("GPU"), FixedPoint::from_f64(2.0));
        assert_eq!(nr.available.get("CPU"), FixedPoint::from_f64(4.0));
        assert_eq!(nr.labels.get("zone").unwrap(), "us-east");
        assert!(!nr.is_draining);
    }
}
