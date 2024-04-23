// Copyright 2023 The Ray Authors.
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

#include "ray/raylet/scheduling/policy/scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

// Label based node affinity scheduling strategy
class NodeLabelSchedulingPolicy : public ISchedulingPolicy {
 public:
  NodeLabelSchedulingPolicy(scheduling::NodeID local_node_id,
                            const absl::flat_hash_map<scheduling::NodeID, Node> &nodes,
                            std::function<bool(scheduling::NodeID)> is_node_alive)
      : local_node_id_(local_node_id),
        nodes_(nodes),
        is_node_alive_(is_node_alive),
        gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

  scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                              SchedulingOptions options) override;

 private:
  bool IsNodeMatchLabelExpression(const Node &node,
                                  const rpc::LabelMatchExpression &expression) const;

  bool IsNodeLabelKeyExists(const Node &node, const std::string &key) const;

  bool IsNodeLabelInValues(const Node &node,
                           const std::string &key,
                           const absl::flat_hash_set<std::string> &values) const;

  absl::flat_hash_map<scheduling::NodeID, const Node *> SelectAvailableNodes(
      const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes,
      const ResourceRequest &resource_request) const;

  absl::flat_hash_map<scheduling::NodeID, const Node *> SelectFeasibleNodes(
      const ResourceRequest &resource_request) const;

  absl::flat_hash_map<scheduling::NodeID, const Node *>
  FilterNodesByLabelMatchExpressions(
      const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes,
      const rpc::LabelMatchExpressions &expressions) const;

  scheduling::NodeID SelectBestNode(
      const absl::flat_hash_map<scheduling::NodeID, const Node *> &hard_match_nodes,
      const absl::flat_hash_map<scheduling::NodeID, const Node *>
          &hard_and_soft_match_nodes,
      const ResourceRequest &resource_request);

  scheduling::NodeID SelectRandomNode(
      const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes);

  const scheduling::NodeID local_node_id_;
  const absl::flat_hash_map<scheduling::NodeID, Node> &nodes_;
  std::function<bool(scheduling::NodeID)> is_node_alive_;
  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
