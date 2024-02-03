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

class FirstSchedulingFinalizer : public ISchedulingFinalizer {
 public:
  FirstSchedulingFinalizer() = default;

  scheduling::NodeID Finalize(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) override {
    if (candidate_nodes.empty()) {
      return scheduling::NodeID::Nil();
    }
    return candidate_nodes[0];
  }
};

// Stateful finalizer that selects a node from a list of candidate nodes.
class RandomSchedulingFinalizer : public ISchedulingFinalizer {
  RandomSchedulingFinalizer() {}

  scheduling::NodeID Finalize(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) override {
    if (candidate_nodes.empty()) {
      return scheduling::NodeID::Nil();
    }
    std::uniform_int_distribution<int> distribution(0, candidate_nodes.size() - 1);
    int idx = distribution(gen_);
    return candidate_nodes[idx];
  }

 private:
  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
};

class HybridSchedulingFinalizer : public ISchedulingFinalizer {
 public:
  HybridSchedulingFinalizer() = default;

  scheduling::NodeID Finalize(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) override {
    RAY_CHECK(false) << "coming soon";
  }
};

// A stateful finalizer that selects a node from a list of candidate nodes in a round
// robin fashion.
// It's not very fair, since each request may have a different set of candidate nodes.
class RoundRobinSchedulingFinalizer : public ISchedulingFinalizer {
 public:
  RoundRobinSchedulingFinalizer() = default;

  scheduling::NodeID Finalize(
      const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
      const std::vector<scheduling::NodeID> &candidate_nodes,
      const ResourceRequest &resource_request) override {
    if (candidate_nodes.empty()) {
      return scheduling::NodeID::Nil();
    }
    const size_t i = index_;
    index_++;
    return candidate_nodes[i % candidate_nodes.size()];
  }

 private:
  size_t index_ = 0;
};

}  // namespace raylet_scheduling_policy
}  // namespace ray