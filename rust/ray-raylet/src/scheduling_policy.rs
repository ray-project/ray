// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Scheduling policies for task placement.
//!
//! Replaces `src/ray/raylet/scheduling/policy/`.

use std::collections::HashMap;

use rand::seq::SliceRandom;
use rand::Rng;
use ray_common::scheduling::ResourceSet;

use crate::scheduling_resources::{NodeResources, SchedulingOptions, SchedulingType, GPU};

/// Trait for single-task scheduling policies.
pub trait SchedulingPolicy: Send + Sync {
    /// Schedule a task: returns the node ID to schedule on, or None.
    fn schedule(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
        local_node_id: &str,
    ) -> Option<String>;
}

/// Trait for bundle (placement group) scheduling policies.
pub trait BundleSchedulingPolicy: Send + Sync {
    /// Schedule a set of bundles. Returns node IDs (one per bundle) on success.
    fn schedule(
        &self,
        requests: &[&ResourceSet],
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
    ) -> BundleSchedulingResult;
}

/// Result of bundle scheduling.
#[derive(Debug, Clone)]
pub enum BundleSchedulingResult {
    Success(Vec<String>),
    Failed,
    Infeasible,
}

// ─── Hybrid Policy ──────────────────────────────────────────────────────

/// Default scheduling policy: pack locally, spread across cluster.
///
/// Algorithm:
/// 1. Filter nodes into available vs feasible-but-unavailable
/// 2. Score by critical resource utilization
/// 3. Select from top-K candidates with randomization
/// 4. Prefer local node when it has lowest utilization
pub struct HybridSchedulingPolicy;

impl SchedulingPolicy for HybridSchedulingPolicy {
    fn schedule(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
        local_node_id: &str,
    ) -> Option<String> {
        let requests_gpu = request.get(GPU).is_positive();
        let avoid_gpu = options.avoid_gpu_nodes && !requests_gpu;

        // Collect candidates
        let mut available: Vec<(String, f64)> = Vec::new();
        let mut feasible_unavailable: Vec<String> = Vec::new();

        for (id, nr) in nodes {
            if nr.is_draining {
                continue;
            }
            if avoid_gpu && nr.has_gpu() {
                continue;
            }
            if options.avoid_local_node && id == local_node_id {
                continue;
            }
            if !nr.is_feasible(request) {
                continue;
            }
            if !options.label_selector.is_empty()
                && !nr.has_required_labels(&options.label_selector)
            {
                continue;
            }
            if nr.is_available(request) {
                let score = nr.critical_resource_utilization();
                // Truncate scores below spread_threshold to 0
                let score = if score < options.spread_threshold {
                    0.0
                } else {
                    score
                };
                available.push((id.clone(), score));
            } else {
                feasible_unavailable.push(id.clone());
            }
        }

        // If avoid_gpu produced no results, retry without the filter
        if available.is_empty() && feasible_unavailable.is_empty() && avoid_gpu {
            return self.schedule_without_gpu_avoidance(request, options, nodes, local_node_id);
        }

        if available.is_empty() {
            // If we require nodes to be available, return None
            if options.require_node_available {
                return None;
            }
            // Otherwise pick from feasible-unavailable
            return feasible_unavailable.first().cloned();
        }

        // Sort by score (lowest utilization first)
        available.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Top-K selection
        let k = std::cmp::max(
            options.schedule_top_k_absolute,
            (available.len() as f64 * options.scheduler_top_k_fraction).ceil() as usize,
        )
        .max(1)
        .min(available.len());

        // Prefer local node if it's in the top candidates with lowest score
        if let Some(preferred) = &options.preferred_node_id {
            if let Some(pos) = available.iter().position(|(id, _)| id == preferred) {
                if pos < k && available[pos].1 <= available[0].1 {
                    return Some(preferred.clone());
                }
            }
        }

        // Random selection from top-K
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..k);
        Some(available[idx].0.clone())
    }
}

impl HybridSchedulingPolicy {
    fn schedule_without_gpu_avoidance(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
        local_node_id: &str,
    ) -> Option<String> {
        let mut opts = options.clone();
        opts.avoid_gpu_nodes = false;
        self.schedule(request, &opts, nodes, local_node_id)
    }
}

// ─── Spread Policy ──────────────────────────────────────────────────────

/// Round-robin scheduling across nodes.
pub struct SpreadSchedulingPolicy {
    next_index: std::sync::atomic::AtomicUsize,
}

impl SpreadSchedulingPolicy {
    pub fn new() -> Self {
        Self {
            next_index: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for SpreadSchedulingPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulingPolicy for SpreadSchedulingPolicy {
    fn schedule(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
        local_node_id: &str,
    ) -> Option<String> {
        let mut sorted_ids: Vec<&String> = nodes.keys().collect();
        sorted_ids.sort();

        let n = sorted_ids.len();
        if n == 0 {
            return None;
        }

        let start = self
            .next_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % n;

        // First pass: available nodes
        for i in 0..n {
            let idx = (start + i) % n;
            let id = sorted_ids[idx];
            if options.avoid_local_node && id == local_node_id {
                continue;
            }
            let nr = &nodes[id];
            if nr.is_draining {
                continue;
            }
            if nr.is_available(request) && nr.is_feasible(request) {
                return Some(id.clone());
            }
        }

        // Second pass: feasible but unavailable
        if !options.require_node_available {
            for i in 0..n {
                let idx = (start + i) % n;
                let id = sorted_ids[idx];
                if options.avoid_local_node && id == local_node_id {
                    continue;
                }
                let nr = &nodes[id];
                if !nr.is_draining && nr.is_feasible(request) {
                    return Some(id.clone());
                }
            }
        }

        None
    }
}

// ─── Random Policy ──────────────────────────────────────────────────────

/// Uniform random scheduling.
pub struct RandomSchedulingPolicy;

impl SchedulingPolicy for RandomSchedulingPolicy {
    fn schedule(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
        local_node_id: &str,
    ) -> Option<String> {
        let mut candidates: Vec<&String> = nodes
            .iter()
            .filter(|(id, nr)| {
                !nr.is_draining
                    && nr.is_feasible(request)
                    && nr.is_available(request)
                    && !(options.avoid_local_node && *id == local_node_id)
            })
            .map(|(id, _)| id)
            .collect();

        let mut rng = rand::thread_rng();
        candidates.shuffle(&mut rng);
        candidates.first().map(|id| (*id).clone())
    }
}

// ─── Node Affinity Policy ───────────────────────────────────────────────

/// Schedule on a specific node (hard or soft affinity).
pub struct NodeAffinitySchedulingPolicy;

impl SchedulingPolicy for NodeAffinitySchedulingPolicy {
    fn schedule(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
        local_node_id: &str,
    ) -> Option<String> {
        let target = options.node_affinity_node_id.as_ref()?;

        if let Some(nr) = nodes.get(target) {
            if nr.is_feasible(request) {
                // Hard affinity: always schedule here (will queue for resources)
                if !options.node_affinity_spill_on_unavailable
                    && !options.node_affinity_fail_on_unavailable
                {
                    return Some(target.clone());
                }
                // Soft: only if resources available
                if nr.is_available(request) {
                    return Some(target.clone());
                }
            }
        }

        // Soft affinity: fallback to hybrid
        if options.node_affinity_soft {
            let hybrid = HybridSchedulingPolicy;
            return hybrid.schedule(request, options, nodes, local_node_id);
        }

        None
    }
}

// ─── Node Label Policy ──────────────────────────────────────────────────

/// Schedule based on node labels.
pub struct NodeLabelSchedulingPolicy;

impl SchedulingPolicy for NodeLabelSchedulingPolicy {
    fn schedule(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
        local_node_id: &str,
    ) -> Option<String> {
        let selector = &options.label_selector;

        let matching: Vec<(&String, &NodeResources)> = nodes
            .iter()
            .filter(|(id, nr)| {
                !nr.is_draining
                    && nr.is_feasible(request)
                    && nr.has_required_labels(selector)
                    && !(options.avoid_local_node && *id == local_node_id)
            })
            .collect();

        // Prefer available nodes
        let available: Vec<&String> = matching
            .iter()
            .filter(|(_, nr)| nr.is_available(request))
            .map(|(id, _)| *id)
            .collect();

        if !available.is_empty() {
            let mut rng = rand::thread_rng();
            let idx = rng.gen_range(0..available.len());
            return Some(available[idx].clone());
        }

        // Fallback to feasible-but-unavailable
        if !options.require_node_available {
            matching.first().map(|(id, _)| (*id).clone())
        } else {
            None
        }
    }
}

// ─── Composite Policy ───────────────────────────────────────────────────

/// Routes to the correct single-task scheduling policy.
pub struct CompositeSchedulingPolicy {
    hybrid: HybridSchedulingPolicy,
    spread: SpreadSchedulingPolicy,
    random: RandomSchedulingPolicy,
    node_affinity: NodeAffinitySchedulingPolicy,
    node_label: NodeLabelSchedulingPolicy,
}

impl CompositeSchedulingPolicy {
    pub fn new() -> Self {
        Self {
            hybrid: HybridSchedulingPolicy,
            spread: SpreadSchedulingPolicy::new(),
            random: RandomSchedulingPolicy,
            node_affinity: NodeAffinitySchedulingPolicy,
            node_label: NodeLabelSchedulingPolicy,
        }
    }
}

impl Default for CompositeSchedulingPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulingPolicy for CompositeSchedulingPolicy {
    fn schedule(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
        local_node_id: &str,
    ) -> Option<String> {
        match options.scheduling_type {
            SchedulingType::Hybrid => self.hybrid.schedule(request, options, nodes, local_node_id),
            SchedulingType::Spread => self.spread.schedule(request, options, nodes, local_node_id),
            SchedulingType::Random => self.random.schedule(request, options, nodes, local_node_id),
            SchedulingType::NodeAffinity => {
                self.node_affinity
                    .schedule(request, options, nodes, local_node_id)
            }
            SchedulingType::NodeLabel => {
                self.node_label
                    .schedule(request, options, nodes, local_node_id)
            }
            _ => self.hybrid.schedule(request, options, nodes, local_node_id),
        }
    }
}

// ─── Bundle Policies ─────────────────────────────────────────────────────

/// Soft pack: try to pack bundles onto few nodes.
pub struct BundlePackSchedulingPolicy;

impl BundleSchedulingPolicy for BundlePackSchedulingPolicy {
    fn schedule(
        &self,
        requests: &[&ResourceSet],
        _options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
    ) -> BundleSchedulingResult {
        if requests.is_empty() {
            return BundleSchedulingResult::Success(vec![]);
        }

        // Check basic feasibility
        for req in requests {
            let feasible = nodes.values().any(|nr| nr.is_feasible(req));
            if !feasible {
                return BundleSchedulingResult::Infeasible;
            }
        }

        // Make a mutable copy of available resources for dry-run
        let mut available: HashMap<String, ResourceSet> = nodes
            .iter()
            .map(|(id, nr)| (id.clone(), nr.available.clone()))
            .collect();

        let mut assignments = Vec::with_capacity(requests.len());

        for req in requests {
            // Find best node (most remaining resources after allocation)
            let best = available
                .iter()
                .filter(|(_, avail)| avail.is_superset_of(req))
                .max_by(|(_, a), (_, b)| {
                    let score_a = remaining_score(a, req);
                    let score_b = remaining_score(b, req);
                    score_a
                        .partial_cmp(&score_b)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(id, _)| id.clone());

            match best {
                Some(node_id) => {
                    if let Some(avail) = available.get_mut(&node_id) {
                        avail.subtract(req);
                    }
                    assignments.push(node_id);
                }
                None => return BundleSchedulingResult::Failed,
            }
        }

        BundleSchedulingResult::Success(assignments)
    }
}

/// Soft spread: try to spread bundles across different nodes.
pub struct BundleSpreadSchedulingPolicy;

impl BundleSchedulingPolicy for BundleSpreadSchedulingPolicy {
    fn schedule(
        &self,
        requests: &[&ResourceSet],
        _options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
    ) -> BundleSchedulingResult {
        if requests.is_empty() {
            return BundleSchedulingResult::Success(vec![]);
        }

        for req in requests {
            let feasible = nodes.values().any(|nr| nr.is_feasible(req));
            if !feasible {
                return BundleSchedulingResult::Infeasible;
            }
        }

        let mut available: HashMap<String, ResourceSet> = nodes
            .iter()
            .map(|(id, nr)| (id.clone(), nr.available.clone()))
            .collect();

        let mut used_nodes: Vec<String> = Vec::new();
        let mut assignments = Vec::with_capacity(requests.len());

        for req in requests {
            // Prefer unused nodes first
            let best = available
                .iter()
                .filter(|(id, avail)| !used_nodes.contains(id) && avail.is_superset_of(req))
                .max_by(|(_, a), (_, b)| {
                    remaining_score(a, req)
                        .partial_cmp(&remaining_score(b, req))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(id, _)| id.clone())
                .or_else(|| {
                    // Fallback: already-used nodes
                    available
                        .iter()
                        .filter(|(_, avail)| avail.is_superset_of(req))
                        .max_by(|(_, a), (_, b)| {
                            remaining_score(a, req)
                                .partial_cmp(&remaining_score(b, req))
                                .unwrap_or(std::cmp::Ordering::Equal)
                        })
                        .map(|(id, _)| id.clone())
                });

            match best {
                Some(node_id) => {
                    if let Some(avail) = available.get_mut(&node_id) {
                        avail.subtract(req);
                    }
                    if !used_nodes.contains(&node_id) {
                        used_nodes.push(node_id.clone());
                    }
                    assignments.push(node_id);
                }
                None => return BundleSchedulingResult::Failed,
            }
        }

        BundleSchedulingResult::Success(assignments)
    }
}

/// Strict pack: all bundles on one node.
pub struct BundleStrictPackSchedulingPolicy;

impl BundleSchedulingPolicy for BundleStrictPackSchedulingPolicy {
    fn schedule(
        &self,
        requests: &[&ResourceSet],
        _options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
    ) -> BundleSchedulingResult {
        if requests.is_empty() {
            return BundleSchedulingResult::Success(vec![]);
        }

        // Aggregate all requirements
        let mut total_request = ResourceSet::new();
        for req in requests {
            total_request.add(req);
        }

        // Find a node that can hold all bundles
        for (id, nr) in nodes {
            if nr.is_available(&total_request) {
                let assignments = vec![id.clone(); requests.len()];
                return BundleSchedulingResult::Success(assignments);
            }
        }

        // Check if any node could ever hold it
        let feasible = nodes.values().any(|nr| nr.is_feasible(&total_request));
        if feasible {
            BundleSchedulingResult::Failed
        } else {
            BundleSchedulingResult::Infeasible
        }
    }
}

/// Strict spread: each bundle on a different node.
pub struct BundleStrictSpreadSchedulingPolicy;

impl BundleSchedulingPolicy for BundleStrictSpreadSchedulingPolicy {
    fn schedule(
        &self,
        requests: &[&ResourceSet],
        _options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
    ) -> BundleSchedulingResult {
        if requests.is_empty() {
            return BundleSchedulingResult::Success(vec![]);
        }

        if requests.len() > nodes.len() {
            return BundleSchedulingResult::Infeasible;
        }

        let mut available: HashMap<String, ResourceSet> = nodes
            .iter()
            .map(|(id, nr)| (id.clone(), nr.available.clone()))
            .collect();

        let mut assignments = Vec::with_capacity(requests.len());
        let mut used: Vec<String> = Vec::new();

        for req in requests {
            let best = available
                .iter()
                .filter(|(id, avail)| !used.contains(id) && avail.is_superset_of(req))
                .max_by(|(_, a), (_, b)| {
                    remaining_score(a, req)
                        .partial_cmp(&remaining_score(b, req))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(id, _)| id.clone());

            match best {
                Some(node_id) => {
                    if let Some(avail) = available.get_mut(&node_id) {
                        avail.subtract(req);
                    }
                    used.push(node_id.clone());
                    assignments.push(node_id);
                }
                None => {
                    // Check if any unused node is feasible
                    let has_feasible = available
                        .iter()
                        .any(|(id, _)| !used.contains(id) && nodes[id].is_feasible(req));
                    return if has_feasible {
                        BundleSchedulingResult::Failed
                    } else {
                        BundleSchedulingResult::Infeasible
                    };
                }
            }
        }

        BundleSchedulingResult::Success(assignments)
    }
}

/// Routes to the correct bundle scheduling policy.
pub struct CompositeBundleSchedulingPolicy;

impl CompositeBundleSchedulingPolicy {
    pub fn schedule(
        &self,
        requests: &[&ResourceSet],
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
    ) -> BundleSchedulingResult {
        match options.scheduling_type {
            SchedulingType::BundlePack => {
                BundlePackSchedulingPolicy.schedule(requests, options, nodes)
            }
            SchedulingType::BundleSpread => {
                BundleSpreadSchedulingPolicy.schedule(requests, options, nodes)
            }
            SchedulingType::BundleStrictPack => {
                BundleStrictPackSchedulingPolicy.schedule(requests, options, nodes)
            }
            SchedulingType::BundleStrictSpread => {
                BundleStrictSpreadSchedulingPolicy.schedule(requests, options, nodes)
            }
            _ => BundleSchedulingResult::Infeasible,
        }
    }
}

/// Score a node by remaining capacity after allocation (higher = more remaining = preferred).
fn remaining_score(available: &ResourceSet, request: &ResourceSet) -> f64 {
    let mut min_remaining = f64::MAX;
    for (name, req_amount) in request.iter() {
        let avail = available.get(name);
        let remaining = (avail - req_amount).to_f64();
        min_remaining = min_remaining.min(remaining);
    }
    if min_remaining == f64::MAX {
        0.0
    } else {
        min_remaining
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ray_common::scheduling::FixedPoint;

    fn make_nodes() -> HashMap<String, NodeResources> {
        let mut nodes = HashMap::new();

        let mut n1_total = ResourceSet::new();
        n1_total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        n1_total.set("GPU".to_string(), FixedPoint::from_f64(2.0));
        nodes.insert("n1".to_string(), NodeResources::new(n1_total));

        let mut n2_total = ResourceSet::new();
        n2_total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        nodes.insert("n2".to_string(), NodeResources::new(n2_total));

        let mut n3_total = ResourceSet::new();
        n3_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        nodes.insert("n3".to_string(), NodeResources::new(n3_total));

        nodes
    }

    #[test]
    fn test_hybrid_policy() {
        let nodes = make_nodes();
        let policy = HybridSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Without preferred_node_id, any feasible node may be chosen (nondeterministic
        // among tied scores). Verify a valid node is returned.
        let result = policy.schedule(&req, &SchedulingOptions::hybrid(), &nodes, "n1");
        assert!(
            result.as_ref().map_or(false, |id| nodes.contains_key(id)),
            "hybrid policy should return a valid node, got {:?}",
            result
        );

        // With preferred_node_id set and sufficient top-k, the local node should be
        // preferred when it has the lowest (tied) utilization score.
        let opts = SchedulingOptions {
            preferred_node_id: Some("n1".to_string()),
            schedule_top_k_absolute: 10,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "n1");
        assert_eq!(
            result,
            Some("n1".to_string()),
            "hybrid policy should prefer local node when preferred_node_id is set with sufficient top-k"
        );
    }

    #[test]
    fn test_hybrid_infeasible() {
        let nodes = make_nodes();
        let policy = HybridSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(100.0));

        let result = policy.schedule(&req, &SchedulingOptions::hybrid(), &nodes, "n1");
        assert!(result.is_none());
    }

    #[test]
    fn test_spread_round_robin() {
        let nodes = make_nodes();
        let policy = SpreadSchedulingPolicy::new();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let opts = SchedulingOptions::spread();

        // Multiple invocations should hit different nodes
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10 {
            if let Some(id) = policy.schedule(&req, &opts, &nodes, "") {
                seen.insert(id);
            }
        }
        // Should have visited multiple nodes
        assert!(seen.len() > 1);
    }

    #[test]
    fn test_node_affinity_hard() {
        let nodes = make_nodes();
        let policy = NodeAffinitySchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let opts = SchedulingOptions::node_affinity("n2".to_string(), false, false, false);
        let result = policy.schedule(&req, &opts, &nodes, "n1");
        assert_eq!(result, Some("n2".to_string()));
    }

    #[test]
    fn test_node_affinity_soft_fallback() {
        let nodes = make_nodes();
        let policy = NodeAffinitySchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Target a non-existent node with soft=true — should fall back to any valid node
        let opts = SchedulingOptions::node_affinity("nonexistent".to_string(), true, true, false);
        let result = policy.schedule(&req, &opts, &nodes, "n1");
        assert!(
            result.as_ref().map_or(false, |id| nodes.contains_key(id)),
            "soft fallback should return a valid node from the cluster, got {:?}",
            result
        );
    }

    #[test]
    fn test_composite_dispatches() {
        let nodes = make_nodes();
        let policy = CompositeSchedulingPolicy::new();

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hybrid — should return a valid node
        let hybrid = policy.schedule(&req, &SchedulingOptions::hybrid(), &nodes, "n1");
        assert!(
            hybrid.as_ref().map_or(false, |id| nodes.contains_key(id)),
            "hybrid should return a valid node, got {:?}",
            hybrid
        );
        // Spread — should return a valid node
        let spread = policy.schedule(&req, &SchedulingOptions::spread(), &nodes, "n1");
        assert!(
            spread.as_ref().map_or(false, |id| nodes.contains_key(id)),
            "spread should return a valid node, got {:?}",
            spread
        );
        // Random — should return a valid node
        let random = policy.schedule(&req, &SchedulingOptions::random(), &nodes, "n1");
        assert!(
            random.as_ref().map_or(false, |id| nodes.contains_key(id)),
            "random should return a valid node, got {:?}",
            random
        );
    }

    #[test]
    fn test_bundle_strict_pack() {
        let nodes = make_nodes();
        let policy = BundleStrictPackSchedulingPolicy;

        let mut r1 = ResourceSet::new();
        r1.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let mut r2 = ResourceSet::new();
        r2.set("CPU".to_string(), FixedPoint::from_f64(3.0));

        let requests: Vec<&ResourceSet> = vec![&r1, &r2];
        let opts = SchedulingOptions {
            scheduling_type: SchedulingType::BundleStrictPack,
            ..SchedulingOptions::default()
        };

        let result = policy.schedule(&requests, &opts, &nodes);
        match result {
            BundleSchedulingResult::Success(assignments) => {
                assert_eq!(assignments.len(), 2);
                // Both on same node (n2 has 8 CPU)
                assert_eq!(assignments[0], assignments[1]);
                assert_eq!(assignments[0], "n2");
            }
            _ => panic!("expected success"),
        }
    }

    #[test]
    fn test_bundle_strict_spread() {
        let nodes = make_nodes();
        let policy = BundleStrictSpreadSchedulingPolicy;

        let mut r1 = ResourceSet::new();
        r1.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let mut r2 = ResourceSet::new();
        r2.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let requests: Vec<&ResourceSet> = vec![&r1, &r2];
        let opts = SchedulingOptions {
            scheduling_type: SchedulingType::BundleStrictSpread,
            ..SchedulingOptions::default()
        };

        let result = policy.schedule(&requests, &opts, &nodes);
        match result {
            BundleSchedulingResult::Success(assignments) => {
                assert_eq!(assignments.len(), 2);
                // Must be different nodes
                assert_ne!(assignments[0], assignments[1]);
            }
            _ => panic!("expected success"),
        }
    }

    #[test]
    fn test_bundle_strict_spread_infeasible() {
        let nodes = make_nodes(); // 3 nodes

        let mut r1 = ResourceSet::new();
        r1.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // 4 bundles but only 3 nodes
        let requests: Vec<&ResourceSet> = vec![&r1, &r1, &r1, &r1];
        let opts = SchedulingOptions {
            scheduling_type: SchedulingType::BundleStrictSpread,
            ..SchedulingOptions::default()
        };

        let result = BundleStrictSpreadSchedulingPolicy.schedule(&requests, &opts, &nodes);
        assert!(
            matches!(result, BundleSchedulingResult::Infeasible),
            "expected Infeasible for 4 bundles on 3 nodes, got {:?}",
            result
        );
    }
}
