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

#include "ray/raylet/scheduling/policy/node_label_scheduling_policy.h"

#include "ray/raylet/scheduling/policy/scheduling_context.h"

namespace ray {
namespace raylet_scheduling_policy {

scheduling::NodeID NodeLabelSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type == SchedulingType::NODE_LABEL);
  auto context =
      dynamic_cast<const NodeLabelSchedulingContext *>(options.scheduling_context.get());
  const auto &scheduling_strategy = context->GetSchedulingStrategy();
  RAY_CHECK(scheduling_strategy.has_node_label_scheduling_strategy());
  const auto &node_label_scheduling_strategy =
      scheduling_strategy.node_label_scheduling_strategy();
  // 1. Select feasible nodes
  auto hard_match_nodes = SelectFeasibleNodes(resource_request);
  if (hard_match_nodes.empty()) {
    return scheduling::NodeID::Nil();
  }
  // 2. Filter by hard expressions.
  if (node_label_scheduling_strategy.hard().expressions().size() > 0) {
    hard_match_nodes = FilterNodesByLabelMatchExpressions(
        hard_match_nodes, node_label_scheduling_strategy.hard());
    if (hard_match_nodes.empty()) {
      return scheduling::NodeID::Nil();
    }
  }

  // 3. Filter by soft expressions.
  absl::flat_hash_map<scheduling::NodeID, const Node *> hard_and_soft_match_nodes;
  const auto &soft_expressions = node_label_scheduling_strategy.soft();
  if (soft_expressions.expressions().size() > 0) {
    hard_and_soft_match_nodes =
        FilterNodesByLabelMatchExpressions(hard_match_nodes, soft_expressions);
  }

  return SelectBestNode(hard_match_nodes, hard_and_soft_match_nodes, resource_request);
}

scheduling::NodeID NodeLabelSchedulingPolicy::SelectBestNode(
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &hard_match_nodes,
    const absl::flat_hash_map<scheduling::NodeID, const Node *>
        &hard_and_soft_match_nodes,
    const ResourceRequest &resource_request) {
  // 1. Match hard&soft and available nodes.
  if (!hard_and_soft_match_nodes.empty()) {
    auto available_soft_nodes =
        SelectAvailableNodes(hard_and_soft_match_nodes, resource_request);
    if (!available_soft_nodes.empty()) {
      return SelectRandomNode(available_soft_nodes);
    }
  }

  // 2. Match hard and available nodes.
  auto available_nodes = SelectAvailableNodes(hard_match_nodes, resource_request);
  if (!available_nodes.empty()) {
    return SelectRandomNode(available_nodes);
  }

  // 3. Match hard&soft and feasible nodes.
  if (!hard_and_soft_match_nodes.empty()) {
    return SelectRandomNode(hard_and_soft_match_nodes);
  }
  // 4. Match hard and feasible nodes.
  return SelectRandomNode(hard_match_nodes);
}

scheduling::NodeID NodeLabelSchedulingPolicy::SelectRandomNode(
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes) {
  std::uniform_int_distribution<int> distribution(0, candidate_nodes.size() - 1);
  int node_index = distribution(gen_);
  auto it = candidate_nodes.begin();
  std::advance(it, node_index);
  return it->first;
}

absl::flat_hash_map<scheduling::NodeID, const Node *>
NodeLabelSchedulingPolicy::FilterNodesByLabelMatchExpressions(
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes,
    const rpc::LabelMatchExpressions &expressions) const {
  auto match_nodes = candidate_nodes;
  for (const auto &expression : expressions.expressions()) {
    if (match_nodes.empty()) {
      return match_nodes;
    }

    for (auto iter = match_nodes.begin(); iter != match_nodes.end(); iter++) {
      if (!IsNodeMatchLabelExpression(*(iter->second), expression)) {
        match_nodes.erase(iter);
      }
    }
  }
  return match_nodes;
}

bool NodeLabelSchedulingPolicy::IsNodeMatchLabelExpression(
    const Node &node, const rpc::LabelMatchExpression &expression) const {
  const auto &key = expression.key();
  const auto &match_operator = expression.operator_();
  if (match_operator.has_label_exists()) {
    if (IsNodeLabelKeyExists(node, key)) {
      return true;
    }
  } else if (match_operator.has_label_does_not_exist()) {
    if (!IsNodeLabelKeyExists(node, key)) {
      return true;
    }
  } else if (match_operator.has_label_in()) {
    absl::flat_hash_set<std::string> values;
    for (const auto &value : match_operator.label_in().values()) {
      values.insert(value);
    }
    if (IsNodeLabelInValues(node, key, values)) {
      return true;
    }
  } else if (match_operator.has_label_not_in()) {
    absl::flat_hash_set<std::string> values;
    for (const auto &value : match_operator.label_not_in().values()) {
      values.insert(value);
    }
    if (!IsNodeLabelInValues(node, key, values)) {
      return true;
    }
  } else {
    RAY_CHECK(false) << "Node label match operator type must be one of `label_in`、"
                        "`label_not_in`、`label_exists` or `label_does_not_exist`.";
  }
  return false;
}

bool NodeLabelSchedulingPolicy::IsNodeLabelKeyExists(const Node &node,
                                                     const std::string &key) const {
  return node.GetLocalView().labels.contains(key);
}

bool NodeLabelSchedulingPolicy::IsNodeLabelInValues(
    const Node &node,
    const std::string &key,
    const absl::flat_hash_set<std::string> &values) const {
  const auto &node_labels = node.GetLocalView().labels;
  if (!node_labels.contains(key)) {
    return false;
  }

  return values.contains(node_labels.at(key));
}

absl::flat_hash_map<scheduling::NodeID, const Node *>
NodeLabelSchedulingPolicy::SelectFeasibleNodes(
    const ResourceRequest &resource_request) const {
  absl::flat_hash_map<scheduling::NodeID, const Node *> candidate_nodes;
  for (const auto &pair : nodes_) {
    const auto &node_id = pair.first;
    const auto &node_resources = pair.second.GetLocalView();
    if (is_node_alive_(node_id) && node_resources.IsFeasible(resource_request)) {
      candidate_nodes.emplace(node_id, &pair.second);
    }
  }
  return candidate_nodes;
}

absl::flat_hash_map<scheduling::NodeID, const Node *>
NodeLabelSchedulingPolicy::SelectAvailableNodes(
    const absl::flat_hash_map<scheduling::NodeID, const Node *> &candidate_nodes,
    const ResourceRequest &resource_request) const {
  absl::flat_hash_map<scheduling::NodeID, const Node *> available_nodes;
  for (const auto &pair : candidate_nodes) {
    const auto &node_id = pair.first;
    const auto &node_resources = pair.second->GetLocalView();
    if (node_resources.IsAvailable(resource_request)) {
      available_nodes.emplace(node_id, pair.second);
    }
  }
  return available_nodes;
}
}  // namespace raylet_scheduling_policy
}  // namespace ray
