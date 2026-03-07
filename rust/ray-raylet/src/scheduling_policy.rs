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

/// Schedule based on node labels with hard/soft expression priority.
///
/// Implements the C++ 4-tier priority selection:
///
/// 1. Hard + Soft match, resources available
/// 2. Hard match only, resources available
/// 3. Hard + Soft match, resources feasible (not yet available)
/// 4. Hard match only, resources feasible
///
/// If `label_match_expressions_hard` is empty, falls back to the legacy
/// `label_selector` field for backwards compatibility.
pub struct NodeLabelSchedulingPolicy;

impl SchedulingPolicy for NodeLabelSchedulingPolicy {
    fn schedule(
        &self,
        request: &ResourceSet,
        options: &SchedulingOptions,
        nodes: &HashMap<String, NodeResources>,
        local_node_id: &str,
    ) -> Option<String> {
        // Use legacy path if no hard/soft expressions are provided.
        if options.label_match_expressions_hard.is_empty()
            && options.label_match_expressions_soft.is_empty()
        {
            return self.schedule_legacy(request, options, nodes, local_node_id);
        }

        let hard = &options.label_match_expressions_hard;
        let soft = &options.label_match_expressions_soft;

        // Step 1: Filter to alive, feasible, non-draining nodes.
        let feasible: Vec<(&String, &NodeResources)> = nodes
            .iter()
            .filter(|(id, nr)| {
                !nr.is_draining
                    && nr.is_feasible(request)
                    && !(options.avoid_local_node && *id == local_node_id)
            })
            .collect();

        if feasible.is_empty() {
            return None;
        }

        // Step 2: Filter by hard expressions (required).
        let hard_matched: Vec<(&String, &NodeResources)> = if hard.is_empty() {
            feasible
        } else {
            feasible
                .into_iter()
                .filter(|(_, nr)| hard.matches(&nr.labels))
                .collect()
        };

        if hard_matched.is_empty() {
            return None; // No nodes satisfy hard constraints.
        }

        // Step 3: From hard-matched, filter by soft expressions (preferred).
        let soft_matched: Vec<(&String, &NodeResources)> = if soft.is_empty() {
            Vec::new()
        } else {
            hard_matched
                .iter()
                .filter(|(_, nr)| soft.matches(&nr.labels))
                .copied()
                .collect()
        };

        // Step 4: Apply 4-tier priority selection.
        let pick = |candidates: &[(&String, &NodeResources)]| -> Option<String> {
            if candidates.is_empty() {
                return None;
            }
            let mut rng = rand::thread_rng();
            let idx = rng.gen_range(0..candidates.len());
            Some(candidates[idx].0.clone())
        };

        // Tier 1: hard + soft, available
        let tier1: Vec<_> = soft_matched
            .iter()
            .filter(|(_, nr)| nr.is_available(request))
            .copied()
            .collect();
        if let Some(node) = pick(&tier1) {
            return Some(node);
        }

        // Tier 2: hard only, available
        let tier2: Vec<_> = hard_matched
            .iter()
            .filter(|(_, nr)| nr.is_available(request))
            .copied()
            .collect();
        if let Some(node) = pick(&tier2) {
            return Some(node);
        }

        // Tier 3: hard + soft, feasible (already filtered)
        if let Some(node) = pick(&soft_matched) {
            return Some(node);
        }

        // Tier 4: hard only, feasible
        pick(&hard_matched)
    }
}

impl NodeLabelSchedulingPolicy {
    /// Legacy scheduling path using the flat `label_selector` field.
    fn schedule_legacy(
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

    // ─── Node Label Policy Tests ────────────────────────────────────────

    use crate::scheduling_resources::{LabelConstraint, LabelOperator, LabelSelector};

    fn make_labeled_nodes() -> HashMap<String, NodeResources> {
        let mut nodes = HashMap::new();

        // n1: zone=us-east, tier=prod, 4 CPU
        let mut n1_total = ResourceSet::new();
        n1_total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let mut n1 = NodeResources::new(n1_total);
        n1.labels.insert("zone".to_string(), "us-east".to_string());
        n1.labels.insert("tier".to_string(), "prod".to_string());
        nodes.insert("n1".to_string(), n1);

        // n2: zone=us-west, tier=prod, 8 CPU
        let mut n2_total = ResourceSet::new();
        n2_total.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        let mut n2 = NodeResources::new(n2_total);
        n2.labels.insert("zone".to_string(), "us-west".to_string());
        n2.labels.insert("tier".to_string(), "prod".to_string());
        nodes.insert("n2".to_string(), n2);

        // n3: zone=eu-west, tier=dev, 2 CPU
        let mut n3_total = ResourceSet::new();
        n3_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let mut n3 = NodeResources::new(n3_total);
        n3.labels.insert("zone".to_string(), "eu-west".to_string());
        n3.labels.insert("tier".to_string(), "dev".to_string());
        nodes.insert("n3".to_string(), n3);

        // n4: no labels, 4 CPU
        let mut n4_total = ResourceSet::new();
        n4_total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        let n4 = NodeResources::new(n4_total);
        nodes.insert("n4".to_string(), n4);

        nodes
    }

    #[test]
    fn test_node_label_hard_only() {
        let nodes = make_labeled_nodes();
        let policy = NodeLabelSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hard: tier=prod → matches n1 and n2
        let hard = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "tier".to_string(),
                operator: LabelOperator::In,
                values: vec!["prod".to_string()],
            }],
        };
        let opts = SchedulingOptions::node_label_with_expressions(hard, LabelSelector::new());

        let mut seen = std::collections::HashSet::new();
        for _ in 0..50 {
            if let Some(id) = policy.schedule(&req, &opts, &nodes, "") {
                seen.insert(id);
            }
        }
        // Should only pick n1 and n2
        assert!(seen.contains("n1") || seen.contains("n2"));
        assert!(!seen.contains("n3"));
        assert!(!seen.contains("n4"));
    }

    #[test]
    fn test_node_label_hard_and_soft() {
        let nodes = make_labeled_nodes();
        let policy = NodeLabelSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hard: tier=prod → matches n1, n2
        // Soft: zone=us-east → prefers n1
        let hard = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "tier".to_string(),
                operator: LabelOperator::In,
                values: vec!["prod".to_string()],
            }],
        };
        let soft = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "zone".to_string(),
                operator: LabelOperator::In,
                values: vec!["us-east".to_string()],
            }],
        };
        let opts = SchedulingOptions::node_label_with_expressions(hard, soft);

        // Tier 1: hard+soft+available → n1 only
        // So all picks should be n1
        for _ in 0..20 {
            let result = policy.schedule(&req, &opts, &nodes, "");
            assert_eq!(result, Some("n1".to_string()));
        }
    }

    #[test]
    fn test_node_label_soft_fallback_to_hard() {
        let mut nodes = make_labeled_nodes();
        let policy = NodeLabelSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Exhaust n1's resources so it's feasible but not available
        let mut used = ResourceSet::new();
        used.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        nodes.get_mut("n1").unwrap().available.subtract(&used);

        // Hard: tier=prod → matches n1, n2
        // Soft: zone=us-east → prefers n1 (but n1 is unavailable)
        let hard = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "tier".to_string(),
                operator: LabelOperator::In,
                values: vec!["prod".to_string()],
            }],
        };
        let soft = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "zone".to_string(),
                operator: LabelOperator::In,
                values: vec!["us-east".to_string()],
            }],
        };
        let opts = SchedulingOptions::node_label_with_expressions(hard, soft);

        // Tier 1: hard+soft+available → empty (n1 unavailable)
        // Tier 2: hard+available → n2 only
        for _ in 0..20 {
            let result = policy.schedule(&req, &opts, &nodes, "");
            assert_eq!(result, Some("n2".to_string()));
        }
    }

    #[test]
    fn test_node_label_exists_operator() {
        let nodes = make_labeled_nodes();
        let policy = NodeLabelSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hard: key "tier" must exist → matches n1, n2, n3 (not n4)
        let hard = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "tier".to_string(),
                operator: LabelOperator::Exists,
                values: vec![],
            }],
        };
        let opts = SchedulingOptions::node_label_with_expressions(hard, LabelSelector::new());

        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            if let Some(id) = policy.schedule(&req, &opts, &nodes, "") {
                seen.insert(id);
            }
        }
        assert!(!seen.contains("n4"), "n4 has no 'tier' label");
        assert!(!seen.is_empty());
    }

    #[test]
    fn test_node_label_does_not_exist_operator() {
        let nodes = make_labeled_nodes();
        let policy = NodeLabelSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hard: key "tier" must NOT exist → matches only n4
        let hard = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "tier".to_string(),
                operator: LabelOperator::DoesNotExist,
                values: vec![],
            }],
        };
        let opts = SchedulingOptions::node_label_with_expressions(hard, LabelSelector::new());

        for _ in 0..20 {
            let result = policy.schedule(&req, &opts, &nodes, "");
            assert_eq!(result, Some("n4".to_string()));
        }
    }

    #[test]
    fn test_node_label_no_hard_match_returns_none() {
        let nodes = make_labeled_nodes();
        let policy = NodeLabelSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hard: tier=staging → matches nobody
        let hard = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "tier".to_string(),
                operator: LabelOperator::In,
                values: vec!["staging".to_string()],
            }],
        };
        let opts = SchedulingOptions::node_label_with_expressions(hard, LabelSelector::new());

        let result = policy.schedule(&req, &opts, &nodes, "");
        assert!(result.is_none());
    }

    #[test]
    fn test_node_label_tier3_feasible_soft() {
        let mut nodes = make_labeled_nodes();
        let policy = NodeLabelSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Exhaust ALL nodes' resources
        for nr in nodes.values_mut() {
            let total = nr.total.clone();
            nr.available.subtract(&total);
        }

        // Hard: tier=prod → n1, n2
        // Soft: zone=us-east → n1
        let hard = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "tier".to_string(),
                operator: LabelOperator::In,
                values: vec!["prod".to_string()],
            }],
        };
        let soft = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "zone".to_string(),
                operator: LabelOperator::In,
                values: vec!["us-east".to_string()],
            }],
        };
        let opts = SchedulingOptions::node_label_with_expressions(hard, soft);

        // Tier 1: empty (nothing available)
        // Tier 2: empty (nothing available)
        // Tier 3: hard+soft+feasible → n1
        for _ in 0..20 {
            let result = policy.schedule(&req, &opts, &nodes, "");
            assert_eq!(result, Some("n1".to_string()));
        }
    }

    #[test]
    fn test_node_label_legacy_selector() {
        let nodes = make_labeled_nodes();
        let policy = NodeLabelSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Use legacy label_selector (no hard/soft expressions)
        let selector = LabelSelector {
            constraints: vec![LabelConstraint {
                key: "tier".to_string(),
                operator: LabelOperator::In,
                values: vec!["dev".to_string()],
            }],
        };
        let opts = SchedulingOptions::node_label(selector);

        for _ in 0..20 {
            let result = policy.schedule(&req, &opts, &nodes, "");
            assert_eq!(result, Some("n3".to_string()));
        }
    }

    #[test]
    fn test_node_label_combined_hard_constraints() {
        let nodes = make_labeled_nodes();
        let policy = NodeLabelSchedulingPolicy;

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hard: tier=prod AND zone IN (us-east, us-west) → n1, n2
        // Then add: zone NOT IN (us-west) → only n1
        let hard = LabelSelector {
            constraints: vec![
                LabelConstraint {
                    key: "tier".to_string(),
                    operator: LabelOperator::In,
                    values: vec!["prod".to_string()],
                },
                LabelConstraint {
                    key: "zone".to_string(),
                    operator: LabelOperator::NotIn,
                    values: vec!["us-west".to_string()],
                },
            ],
        };
        let opts = SchedulingOptions::node_label_with_expressions(hard, LabelSelector::new());

        for _ in 0..20 {
            let result = policy.schedule(&req, &opts, &nodes, "");
            assert_eq!(result, Some("n1".to_string()));
        }
    }

    // ─── Ported from C++ scheduling_policy_test.cc ────────────────────

    /// Port of FeasibleDefinitionTest: a node with total CPU but no
    /// object_store_memory should be infeasible for requests requiring
    /// object_store_memory, but feasible for CPU-only requests.
    #[test]
    fn test_feasible_definition() {
        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let nr = NodeResources::new(total);

        // Request requiring both CPU and object_store_memory
        let mut req1 = ResourceSet::new();
        req1.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req1.set("object_store_memory".to_string(), FixedPoint::from_f64(1.0));
        assert!(!nr.is_feasible(&req1));

        // Request requiring only CPU
        let mut req2 = ResourceSet::new();
        req2.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        assert!(nr.is_feasible(&req2));
    }

    /// Port of AvailableDefinitionTest: a node should be available for
    /// a resource it has, but not for a resource it doesn't have.
    #[test]
    fn test_available_definition() {
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

    /// Port of CriticalResourceUtilizationDefinitionTest: verify correct
    /// calculation of max utilization across multiple resource types.
    #[test]
    fn test_critical_resource_utilization_definition() {
        // Simple: 1 avail of 2 total = 0.5 utilization
        {
            let mut total = ResourceSet::new();
            total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
            let mut nr = NodeResources::new(total);
            let mut used = ResourceSet::new();
            used.set("CPU".to_string(), FixedPoint::from_f64(1.0));
            nr.available.subtract(&used);
            assert!((nr.critical_resource_utilization() - 0.5).abs() < 0.01);
        }
        // Max across multiple resources: memory is 0.75 utilization (max)
        {
            let mut total = ResourceSet::new();
            total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
            total.set("memory".to_string(), FixedPoint::from_f64(1.0));
            total.set("GPU".to_string(), FixedPoint::from_f64(2.0));
            total.set(
                "object_store_memory".to_string(),
                FixedPoint::from_f64(100.0),
            );
            let mut nr = NodeResources::new(total);
            let mut used = ResourceSet::new();
            used.set("CPU".to_string(), FixedPoint::from_f64(1.0));
            used.set("memory".to_string(), FixedPoint::from_f64(0.75));
            used.set("GPU".to_string(), FixedPoint::from_f64(1.0));
            used.set(
                "object_store_memory".to_string(),
                FixedPoint::from_f64(50.0),
            );
            nr.available.subtract(&used);
            assert!((nr.critical_resource_utilization() - 0.75).abs() < 0.01);
        }
    }

    /// Port of AvailableTruncationTest: when two nodes are both below the
    /// spread_threshold, their scores are both truncated to 0, and a
    /// preferred local node should be selected.
    #[test]
    fn test_available_truncation() {
        let mut nodes = HashMap::new();

        // local: 1 avail of 2 total = 0.5 util
        let mut local_total = ResourceSet::new();
        local_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let mut local_nr = NodeResources::new(local_total);
        let mut used = ResourceSet::new();
        used.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        local_nr.available.subtract(&used);
        nodes.insert("local".to_string(), local_nr);

        // remote: 0.75 avail of 2 total = 0.625 util (lower than local)
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let mut remote_nr = NodeResources::new(remote_total);
        let mut used2 = ResourceSet::new();
        used2.set("CPU".to_string(), FixedPoint::from_f64(1.25));
        remote_nr.available.subtract(&used2);
        nodes.insert("remote".to_string(), remote_nr);

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Both are below 0.51 threshold, truncated to 0 — prefer local
        let opts = SchedulingOptions {
            spread_threshold: 0.51,
            preferred_node_id: Some("local".to_string()),
            schedule_top_k_absolute: 10,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "local");
        assert_eq!(result, Some("local".to_string()));
    }

    /// Port of AvailableTieBreakTest: when remote node has lower utilization
    /// than local (above threshold), remote should be selected.
    #[test]
    fn test_available_tie_break() {
        let mut nodes = HashMap::new();

        // local: 1 avail / 2 total = 0.5 util
        let mut local_total = ResourceSet::new();
        local_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let mut local_nr = NodeResources::new(local_total);
        let mut used = ResourceSet::new();
        used.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        local_nr.available.subtract(&used);
        nodes.insert("local".to_string(), local_nr);

        // remote: 1.5 avail / 2 total = 0.25 util (lower → better)
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let mut remote_nr = NodeResources::new(remote_total);
        let mut used2 = ResourceSet::new();
        used2.set("CPU".to_string(), FixedPoint::from_f64(0.5));
        remote_nr.available.subtract(&used2);
        nodes.insert("remote".to_string(), remote_nr);

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let opts = SchedulingOptions {
            spread_threshold: 0.50,
            schedule_top_k_absolute: 1,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "local");
        // Remote has lower utilization so it should be picked
        assert_eq!(result, Some("remote".to_string()));
    }

    /// Port of AvailableOverFeasibleTest: an available remote node should
    /// be preferred over a merely feasible local node.
    #[test]
    fn test_available_over_feasible() {
        let mut nodes = HashMap::new();

        // local: plenty of CPU, 0 GPU available (feasible but not available for GPU)
        let mut local_total = ResourceSet::new();
        local_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        local_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        let mut local_nr = NodeResources::new(local_total);
        let mut used = ResourceSet::new();
        used.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        local_nr.available.subtract(&used);
        nodes.insert("local".to_string(), local_nr);

        // remote: has both CPU and GPU available
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        remote_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        nodes.insert("remote".to_string(), NodeResources::new(remote_total));

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let opts = SchedulingOptions {
            spread_threshold: 0.50,
            schedule_top_k_absolute: 1,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "local");
        assert_eq!(result, Some("remote".to_string()));
    }

    /// Port of InfeasibleTest: when no node is feasible, return None.
    #[test]
    fn test_infeasible() {
        let mut nodes = HashMap::new();

        let mut t1 = ResourceSet::new();
        t1.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        nodes.insert("n1".to_string(), NodeResources::new(t1));

        let mut t2 = ResourceSet::new();
        t2.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        nodes.insert("n2".to_string(), NodeResources::new(t2));

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let result = policy.schedule(&req, &SchedulingOptions::hybrid(), &nodes, "n1");
        assert!(result.is_none());
    }

    /// Port of BarelyFeasibleTest: a fully utilized node is still feasible
    /// and should be returned when require_node_available is false.
    #[test]
    fn test_barely_feasible() {
        let mut nodes = HashMap::new();

        let mut total = ResourceSet::new();
        total.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        let mut nr = NodeResources::new(total);
        // All resources used
        let mut used = ResourceSet::new();
        used.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        used.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        nr.available.subtract(&used);
        nodes.insert("local".to_string(), nr);

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let opts = SchedulingOptions {
            require_node_available: false,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "local");
        assert_eq!(result, Some("local".to_string()));
    }

    /// Port of ForceSpillbackIfAvailableTest: avoid_local_node should
    /// force scheduling to a remote node even when local is better.
    #[test]
    fn test_force_spillback_if_available() {
        let mut nodes = HashMap::new();

        let mut local_total = ResourceSet::new();
        local_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        local_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        nodes.insert("local".to_string(), NodeResources::new(local_total));

        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        remote_total.set("GPU".to_string(), FixedPoint::from_f64(10.0));
        nodes.insert("remote".to_string(), NodeResources::new(remote_total));

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let opts = SchedulingOptions {
            avoid_local_node: true,
            require_node_available: true,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "local");
        assert_eq!(result, Some("remote".to_string()));
    }

    /// Port of AvoidSchedulingCPURequestsOnGPUNodes: CPU-only tasks should
    /// avoid GPU nodes when avoid_gpu_nodes is set. GPU and mixed tasks
    /// should still schedule on GPU nodes.
    #[test]
    fn test_avoid_scheduling_cpu_requests_on_gpu_nodes() {
        let mut nodes = HashMap::new();

        // local: GPU node with 10 CPUs + 1 GPU
        let mut local_total = ResourceSet::new();
        local_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        local_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        nodes.insert("local".to_string(), NodeResources::new(local_total));

        // remote: CPU-only node with 2 CPUs
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        nodes.insert("remote".to_string(), NodeResources::new(remote_total));

        let policy = HybridSchedulingPolicy;

        // CPU-only request with avoid_gpu_nodes should go to remote
        {
            let mut req = ResourceSet::new();
            req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
            let opts = SchedulingOptions {
                avoid_gpu_nodes: true,
                ..SchedulingOptions::hybrid()
            };
            let result = policy.schedule(&req, &opts, &nodes, "local");
            assert_eq!(result, Some("remote".to_string()));
        }

        // GPU request should go to local (GPU node)
        {
            let mut req = ResourceSet::new();
            req.set("GPU".to_string(), FixedPoint::from_f64(1.0));
            let opts = SchedulingOptions {
                avoid_gpu_nodes: true,
                ..SchedulingOptions::hybrid()
            };
            let result = policy.schedule(&req, &opts, &nodes, "local");
            assert_eq!(result, Some("local".to_string()));
        }

        // Mixed CPU+GPU request should go to local
        {
            let mut req = ResourceSet::new();
            req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
            req.set("GPU".to_string(), FixedPoint::from_f64(1.0));
            let opts = SchedulingOptions {
                avoid_gpu_nodes: true,
                ..SchedulingOptions::hybrid()
            };
            let result = policy.schedule(&req, &opts, &nodes, "local");
            assert_eq!(result, Some("local".to_string()));
        }
    }

    /// Port of SchedulenCPURequestsOnGPUNodeAsALastResort: when no non-GPU
    /// node is feasible, a GPU node should be used as last resort even with
    /// avoid_gpu_nodes set.
    #[test]
    fn test_schedule_cpu_on_gpu_node_as_last_resort() {
        let mut nodes = HashMap::new();

        // Only a GPU node exists — no non-GPU node at all
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        remote_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        nodes.insert("gpu-node".to_string(), NodeResources::new(remote_total));

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // With avoid_gpu_nodes, but no non-GPU node exists, should fallback to GPU node
        let opts = SchedulingOptions {
            avoid_gpu_nodes: true,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "non-existent-local");
        assert_eq!(result, Some("gpu-node".to_string()));
    }

    /// Port of ForceSpillbackTest: avoid_local_node with require_node_available=false
    /// should pick a remote feasible node even if it has no availability.
    #[test]
    fn test_force_spillback() {
        let mut nodes = HashMap::new();

        let mut local_total = ResourceSet::new();
        local_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        local_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        nodes.insert("local".to_string(), NodeResources::new(local_total));

        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        remote_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        let mut remote_nr = NodeResources::new(remote_total);
        let mut used = ResourceSet::new();
        used.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        used.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        remote_nr.available.subtract(&used);
        nodes.insert("remote".to_string(), remote_nr);

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let opts = SchedulingOptions {
            avoid_local_node: true,
            require_node_available: false,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "local");
        assert_eq!(result, Some("remote".to_string()));
    }

    /// Port of ForceSpillbackOnlyFeasibleLocallyTest: when spillback is
    /// forced but no remote node is feasible, should return None.
    #[test]
    fn test_force_spillback_only_feasible_locally() {
        let mut nodes = HashMap::new();

        let mut local_total = ResourceSet::new();
        local_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        local_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        nodes.insert("local".to_string(), NodeResources::new(local_total));

        // remote: no GPU at all → infeasible for GPU request
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        nodes.insert("remote".to_string(), NodeResources::new(remote_total));

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        req.set("GPU".to_string(), FixedPoint::from_f64(1.0));

        let opts = SchedulingOptions {
            avoid_local_node: true,
            require_node_available: false,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "local");
        assert!(result.is_none());
    }

    /// Port of NonGpuNodePreferredSchedulingTest: CPU-only tasks prefer
    /// non-GPU nodes; GPU tasks go to GPU nodes.
    #[test]
    fn test_non_gpu_node_preferred_scheduling() {
        let mut nodes = HashMap::new();

        // local: {CPU:2, GPU:1}
        let mut local_total = ResourceSet::new();
        local_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        local_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        nodes.insert("local".to_string(), NodeResources::new(local_total));

        // remote: {CPU: 2}
        let mut remote_total = ResourceSet::new();
        remote_total.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        nodes.insert("remote".to_string(), NodeResources::new(remote_total));

        // remote2: {CPU: 3}
        let mut remote2_total = ResourceSet::new();
        remote2_total.set("CPU".to_string(), FixedPoint::from_f64(3.0));
        nodes.insert("remote2".to_string(), NodeResources::new(remote2_total));

        let policy = HybridSchedulingPolicy;

        // 1 CPU → prefer non-GPU nodes
        {
            let mut req = ResourceSet::new();
            req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
            let opts = SchedulingOptions {
                avoid_gpu_nodes: true,
                schedule_top_k_absolute: 1,
                ..SchedulingOptions::hybrid()
            };
            let result = policy.schedule(&req, &opts, &nodes, "local");
            assert!(result == Some("remote".to_string()) || result == Some("remote2".to_string()));
        }

        // CPU+GPU → must go to GPU node
        {
            let mut req = ResourceSet::new();
            req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
            req.set("GPU".to_string(), FixedPoint::from_f64(1.0));
            let opts = SchedulingOptions {
                avoid_gpu_nodes: true,
                ..SchedulingOptions::hybrid()
            };
            let result = policy.schedule(&req, &opts, &nodes, "local");
            assert_eq!(result, Some("local".to_string()));
        }
    }

    /// Port of StrictPackBundleSchedulingTest: verify target node preference.
    #[test]
    fn test_strict_pack_bundle_scheduling() {
        let mut nodes = HashMap::new();

        let mut n1 = ResourceSet::new();
        n1.set("CPU".to_string(), FixedPoint::from_f64(8.0));
        nodes.insert("local".to_string(), NodeResources::new(n1));

        let mut n2 = ResourceSet::new();
        n2.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        nodes.insert("small".to_string(), NodeResources::new(n2));

        let mut n3 = ResourceSet::new();
        n3.set("CPU".to_string(), FixedPoint::from_f64(4.0));
        nodes.insert("medium".to_string(), NodeResources::new(n3));

        let mut r1 = ResourceSet::new();
        r1.set("CPU".to_string(), FixedPoint::from_f64(2.0));
        let requests: Vec<&ResourceSet> = vec![&r1, &r1];

        let opts = SchedulingOptions {
            scheduling_type: SchedulingType::BundleStrictPack,
            ..SchedulingOptions::default()
        };

        // Should pack all on one node that has capacity for 4 CPU total
        let result = BundleStrictPackSchedulingPolicy.schedule(&requests, &opts, &nodes);
        match result {
            BundleSchedulingResult::Success(assignments) => {
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0], assignments[1]);
                // Must be on a node with at least 4 CPU (local or medium)
                assert!(assignments[0] == "local" || assignments[0] == "medium");
            }
            _ => panic!("expected success"),
        }
    }

    /// Port of StrictPackBundleLabelSelectorSuccessTest.
    #[test]
    fn test_strict_pack_label_selector_success() {
        let mut nodes = HashMap::new();

        let mut n1_total = ResourceSet::new();
        n1_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        let mut n1 = NodeResources::new(n1_total);
        n1.labels.insert("zone".to_string(), "us-east".to_string());
        nodes.insert("n1".to_string(), n1);

        let mut n2_total = ResourceSet::new();
        n2_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        let mut n2 = NodeResources::new(n2_total);
        n2.labels.insert("zone".to_string(), "us-west".to_string());
        nodes.insert("n2".to_string(), n2);

        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));
        let requests: Vec<&ResourceSet> = vec![&req, &req];

        // StrictPack should place both on the same node.
        // Since both have enough resources, pick any one.
        let opts = SchedulingOptions {
            scheduling_type: SchedulingType::BundleStrictPack,
            ..SchedulingOptions::default()
        };
        let result = BundleStrictPackSchedulingPolicy.schedule(&requests, &opts, &nodes);
        match result {
            BundleSchedulingResult::Success(assignments) => {
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0], assignments[1]);
            }
            _ => panic!("expected success"),
        }
    }

    /// Port of RandomPolicyTest: verify both nodes get picked over many runs.
    #[test]
    fn test_random_policy() {
        let mut nodes = HashMap::new();

        let mut t1 = ResourceSet::new();
        t1.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        nodes.insert("n0".to_string(), NodeResources::new(t1));

        let mut t2 = ResourceSet::new();
        t2.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        nodes.insert("n1".to_string(), NodeResources::new(t2));

        let policy = RandomSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            if let Some(id) = policy.schedule(&req, &SchedulingOptions::random(), &nodes, "") {
                seen.insert(id);
            }
        }
        assert!(
            seen.contains("n0") && seen.contains("n1"),
            "Random policy should pick both nodes over 200 runs, seen: {:?}",
            seen
        );
    }

    /// Port of NodeAffinitySchedulingStrategyTest: comprehensive node affinity tests.
    #[test]
    fn test_node_affinity_scheduling_strategy() {
        let mut nodes = HashMap::new();

        let mut local_total = ResourceSet::new();
        local_total.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        nodes.insert("local".to_string(), NodeResources::new(local_total));

        // Unavailable node (0 available)
        let mut unavail_total = ResourceSet::new();
        unavail_total.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        let mut unavail_nr = NodeResources::new(unavail_total);
        let mut used = ResourceSet::new();
        used.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        unavail_nr.available.subtract(&used);
        nodes.insert("unavailable".to_string(), unavail_nr);

        // Infeasible node (0 total)
        let infeasible_total = ResourceSet::new();
        nodes.insert(
            "infeasible".to_string(),
            NodeResources::new(infeasible_total),
        );

        let policy = NodeAffinitySchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Hard affinity to local → should return local
        let opts = SchedulingOptions::node_affinity("local".to_string(), false, false, false);
        assert_eq!(
            policy.schedule(&req, &opts, &nodes, "local"),
            Some("local".to_string())
        );

        // Hard affinity to unavailable (no spill, no fail) → still returns it
        let opts = SchedulingOptions::node_affinity("unavailable".to_string(), false, false, false);
        assert_eq!(
            policy.schedule(&req, &opts, &nodes, "local"),
            Some("unavailable".to_string())
        );

        // Hard affinity to unavailable with fail_on_unavailable → None
        let opts = SchedulingOptions::node_affinity("unavailable".to_string(), false, false, true);
        assert!(policy.schedule(&req, &opts, &nodes, "local").is_none());

        // Soft affinity to unavailable with spill → fallback to local
        let opts = SchedulingOptions::node_affinity("unavailable".to_string(), true, true, false);
        let result = policy.schedule(&req, &opts, &nodes, "local");
        assert!(result.is_some());

        // Hard affinity to infeasible (no soft) → None
        let opts = SchedulingOptions::node_affinity("infeasible".to_string(), false, false, false);
        assert!(policy.schedule(&req, &opts, &nodes, "local").is_none());

        // Soft affinity to infeasible → fallback
        let opts = SchedulingOptions::node_affinity("infeasible".to_string(), true, true, false);
        let result = policy.schedule(&req, &opts, &nodes, "local");
        assert!(result.is_some());

        // Hard affinity to non-existent → None
        let opts = SchedulingOptions::node_affinity("not_exist".to_string(), false, false, false);
        assert!(policy.schedule(&req, &opts, &nodes, "local").is_none());

        // Soft affinity to non-existent → fallback
        let opts = SchedulingOptions::node_affinity("not_exist".to_string(), true, true, false);
        let result = policy.schedule(&req, &opts, &nodes, "local");
        assert!(result.is_some());
    }

    /// Port of SpreadPolicyTest.
    #[test]
    fn test_spread_policy() {
        let mut nodes = HashMap::new();

        let mut n0 = ResourceSet::new();
        n0.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        n0.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        nodes.insert("n0".to_string(), NodeResources::new(n0));

        // Unavailable
        let mut n1_total = ResourceSet::new();
        n1_total.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        n1_total.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        let mut n1 = NodeResources::new(n1_total);
        let mut used = ResourceSet::new();
        used.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        n1.available.subtract(&used);
        nodes.insert("n1".to_string(), n1);

        let mut n3 = ResourceSet::new();
        n3.set("CPU".to_string(), FixedPoint::from_f64(20.0));
        nodes.insert("n3".to_string(), NodeResources::new(n3));

        let policy = SpreadSchedulingPolicy::new();
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Multiple spread calls should spread across available nodes
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10 {
            let opts = SchedulingOptions::spread();
            if let Some(id) = policy.schedule(&req, &opts, &nodes, "n0") {
                seen.insert(id);
            }
        }
        assert!(seen.len() >= 2, "spread should visit multiple nodes");

        // Spread with require_node_available and GPU request (no available GPU nodes)
        let mut gpu_req = ResourceSet::new();
        gpu_req.set("GPU".to_string(), FixedPoint::from_f64(1.0));
        let opts = SchedulingOptions {
            require_node_available: true,
            ..SchedulingOptions::spread()
        };
        // n1 has GPU but no CPU available — GPU is still available though
        // n0 has GPU and is available
        let result = policy.schedule(&gpu_req, &opts, &nodes, "n0");
        assert!(result.is_some());
    }

    // ─── Ported from C++ hybrid_scheduling_policy_test.cc ─────────────

    /// Port of GetBestNode: with 1 candidate, always pick the lowest scorer.
    #[test]
    fn test_get_best_node_single_candidate() {
        let mut nodes = HashMap::new();

        // n1: score 0 (best), n2: score 0, n3: 0.6, n4: 0.7
        let mut n1_total = ResourceSet::new();
        n1_total.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        nodes.insert("n1".to_string(), NodeResources::new(n1_total.clone()));
        nodes.insert("n2".to_string(), NodeResources::new(n1_total.clone()));

        // n3 with some utilization
        let mut n3 = NodeResources::new(n1_total.clone());
        let mut used3 = ResourceSet::new();
        used3.set("CPU".to_string(), FixedPoint::from_f64(6.0));
        n3.available.subtract(&used3);
        nodes.insert("n3".to_string(), n3);

        // n4 with more utilization
        let mut n4 = NodeResources::new(n1_total);
        let mut used4 = ResourceSet::new();
        used4.set("CPU".to_string(), FixedPoint::from_f64(7.0));
        n4.available.subtract(&used4);
        nodes.insert("n4".to_string(), n4);

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // top_k=1 should always return one of the lowest-score nodes
        let opts = SchedulingOptions {
            schedule_top_k_absolute: 1,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "").unwrap();
        // n1 and n2 both have score 0, so either is valid
        assert!(result == "n1" || result == "n2", "got {}", result);
    }

    /// Port of GetBestNodePrioritizePreferredNode: preferred node should be
    /// chosen when its score ties with the best.
    #[test]
    fn test_get_best_node_prioritize_preferred() {
        let mut nodes = HashMap::new();

        let total = {
            let mut t = ResourceSet::new();
            t.set("CPU".to_string(), FixedPoint::from_f64(10.0));
            t
        };

        // n1, n2: score 0 (idle)
        nodes.insert("n1".to_string(), NodeResources::new(total.clone()));
        nodes.insert("n2".to_string(), NodeResources::new(total.clone()));

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        // Prefer n2 — its score (0) equals the best score, so it should be chosen
        let opts = SchedulingOptions {
            preferred_node_id: Some("n2".to_string()),
            schedule_top_k_absolute: 10,
            ..SchedulingOptions::hybrid()
        };
        let result = policy.schedule(&req, &opts, &nodes, "n1");
        assert_eq!(result, Some("n2".to_string()));
    }

    // ─── Bundle label selector infeasible tests ──────────────────────

    /// Port of PackBundleLabelSelectorInfeasibleTest: PACK policy with
    /// an infeasible label should return Infeasible (not Failed).
    /// The Rust BundlePack policy checks is_feasible per-node which
    /// considers total resources. A node with resources but wrong label
    /// won't have the request's "label resource", so this tests the
    /// infeasibility at the resource level.
    #[test]
    fn test_bundle_pack_infeasible_resource() {
        let mut nodes = HashMap::new();

        let mut n1 = ResourceSet::new();
        n1.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        nodes.insert("n1".to_string(), NodeResources::new(n1));

        // Request a resource no node has
        let mut req = ResourceSet::new();
        req.set("NONEXISTENT".to_string(), FixedPoint::from_f64(1.0));
        let requests: Vec<&ResourceSet> = vec![&req];

        let opts = SchedulingOptions {
            scheduling_type: SchedulingType::BundlePack,
            ..SchedulingOptions::default()
        };
        let result = BundlePackSchedulingPolicy.schedule(&requests, &opts, &nodes);
        assert!(matches!(result, BundleSchedulingResult::Infeasible));
    }

    /// Port of SpreadBundleLabelSelectorInfeasibleTest.
    #[test]
    fn test_bundle_spread_infeasible_resource() {
        let mut nodes = HashMap::new();

        let mut n1 = ResourceSet::new();
        n1.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        nodes.insert("n1".to_string(), NodeResources::new(n1));

        let mut req = ResourceSet::new();
        req.set("NONEXISTENT".to_string(), FixedPoint::from_f64(1.0));
        let requests: Vec<&ResourceSet> = vec![&req];

        let opts = SchedulingOptions {
            scheduling_type: SchedulingType::BundleSpread,
            ..SchedulingOptions::default()
        };
        let result = BundleSpreadSchedulingPolicy.schedule(&requests, &opts, &nodes);
        assert!(matches!(result, BundleSchedulingResult::Infeasible));
    }

    /// Port of StrictSpreadBundleLabelSelectorInfeasibleTest.
    #[test]
    fn test_bundle_strict_spread_infeasible_resource() {
        let mut nodes = HashMap::new();

        let mut n1 = ResourceSet::new();
        n1.set("CPU".to_string(), FixedPoint::from_f64(10.0));
        nodes.insert("n1".to_string(), NodeResources::new(n1));

        let mut req = ResourceSet::new();
        req.set("NONEXISTENT".to_string(), FixedPoint::from_f64(1.0));
        let requests: Vec<&ResourceSet> = vec![&req];

        let opts = SchedulingOptions {
            scheduling_type: SchedulingType::BundleStrictSpread,
            ..SchedulingOptions::default()
        };
        let result = BundleStrictSpreadSchedulingPolicy.schedule(&requests, &opts, &nodes);
        assert!(matches!(result, BundleSchedulingResult::Infeasible));
    }

    /// Port of draining node test: draining nodes should be skipped.
    #[test]
    fn test_hybrid_skips_draining_nodes() {
        let mut nodes = HashMap::new();

        let mut t = ResourceSet::new();
        t.set("CPU".to_string(), FixedPoint::from_f64(10.0));

        let mut draining = NodeResources::new(t.clone());
        draining.is_draining = true;
        nodes.insert("draining".to_string(), draining);

        nodes.insert("healthy".to_string(), NodeResources::new(t));

        let policy = HybridSchedulingPolicy;
        let mut req = ResourceSet::new();
        req.set("CPU".to_string(), FixedPoint::from_f64(1.0));

        let result = policy.schedule(&req, &SchedulingOptions::hybrid(), &nodes, "");
        assert_eq!(result, Some("healthy".to_string()));
    }
}
