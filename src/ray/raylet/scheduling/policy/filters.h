// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <vector>

#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/raylet/scheduling/policy/scheduling_interface.h"

namespace ray {
namespace raylet_scheduling_policy {

// TODO
// Old implementation:
// 1. Match hard&soft and available nodes.
// 2. Match hard and available nodes.
// 3. Match hard&soft and feasible nodes.
// 4. Match hard and feasible nodes.
// 5. finally, random.
//
// New implementation:
// Define: OrNot()
// hard . feasible . OrNot(available . OrNot(soft)) . OrNot(soft) $ Random
//
// This is kinda weird. Maybe we should not focus on the calculus this much and write
// regular code.

// Hard filter: returns nodes that match the requirement.
// Returns empty if no node matches the requirement.
class NodeLabelHardSchedulingFilter : public ISchedulingFilter {
  NodeLabelHardSchedulingFilter(const rpc::LabelMatchExpressions &expressions)
      : expressions_(expressions) {}

  std::vector<scheduling::NodeID> Filter(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) const override;

 private:
  const rpc::LabelMatchExpressions expressions_;
};

// Optional filter.
// If some node matches the requirement, returns those nodes.
// If no node matches the requirement, returns all nodes.
class OptionalSchedulingFilter : public ISchedulingFilter {
  // `filter` must outlive this OptionalSchedulingFilter.
  OptionalSchedulingFilter(const ISchedulingFilter &filter) : filter_(filter) {}

  std::vector<scheduling::NodeID> Filter(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) const override {
    if (candidate_nodes.empty()) {
      return candidate_nodes;
    }
    auto filtered_nodes =
        filter_.Filter(all_nodes_view, candidate_nodes, resource_request);
    if (filtered_nodes.empty()) {
      return candidate_nodes;
    }
    return filtered_nodes;
  }

 private:
  const ISchedulingFilter &filter_;
};

class AvailableSchedulingFilter : public ISchedulingFilter {
 public:
  AvailableSchedulingFilter() {}

  std::vector<scheduling::NodeID> Filter(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) const override {
    std::vector<scheduling::NodeID> filtered_nodes;
    for (const auto &node_id : candidate_nodes) {
      if (all_nodes_view.at(node_id).GetLocalView().IsAvailable(resource_request)) {
        filtered_nodes.push_back(node_id);
      }
    }
    return filtered_nodes;
  }
};

// This is used as a "backup" for the AvailableSchedulingFilter. i.e. if there's no
// available resources, but the node is feasible, and there's no other available nodes, we
// may schedule to it.
class AliveAndFeasibleSchedulingFilter : public ISchedulingFilter {
 public:
  AliveAndFeasibleSchedulingFilter(std::function<bool(scheduling::NodeID)> is_node_alive)
      : is_node_alive_(is_node_alive) {}

  std::vector<scheduling::NodeID> Filter(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) const override {
    std::vector<scheduling::NodeID> filtered_nodes;
    for (const auto &node_id : candidate_nodes) {
      const auto &node_resources = all_nodes_view.at(node_id).GetLocalView();
      const auto &raylet_id = scheduling::NodeID(
          NodeID::FromHex(node_resources.labels.at(kLabelKeyRayletID)).Binary());
      if (is_node_alive_(raylet_id) && node_resources.IsFeasible(resource_request)) {
        filtered_nodes.push_back(node_id);
      }
    }
    return filtered_nodes;
  }

 private:
  std::function<bool(scheduling::NodeID)> is_node_alive_;
};

class AvoidNodeIdSchedulingFilter : public ISchedulingFilter {
 public:
  AvoidNodeIdSchedulingFilter(scheduling::NodeID node_id) : node_id_(node_id) {}

  std::vector<scheduling::NodeID> Filter(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) const override {
    std::vector<scheduling::NodeID> filtered_nodes;
    for (const auto &node_id : candidate_nodes) {
      if (node_id != node_id_) {
        filtered_nodes.push_back(node_id);
      }
    }
    return filtered_nodes;
  }

 private:
  const scheduling::NodeID node_id_;
};

}  // namespace raylet_scheduling_policy
}  // namespace ray