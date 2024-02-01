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

#include "ray/raylet/scheduling/policy/filters.h"

namespace ray {
namespace raylet_scheduling_policy {

// TODO: kill the NodelLabelSchedulingPolicy and expose these pure functions.
namespace {

// Returns elements in `candidate_nodes` that match all `expressions`.
std::vector<scheduling::NodeID> FilterNodesByLabelMatchExpressions(
    const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
    const std::vector<scheduling::NodeID> &candidate_nodes,
    const rpc::LabelMatchExpressions &expressions) {
  std::vector<scheduling::NodeID> match_nodes;
  for (const auto &node_id : candidate_nodes) {
    const auto &node = all_nodes_view.at(node_id);
    bool node_matches = std::all_of(expressions.expressions().begin(),
                                    expressions.expressions().end(),
                                    [&node](const auto &expression) {
                                      return IsNodeMatchLabelExpression(node, expression);
                                    });
    if (node_matches) {
      match_nodes.push_back(node_id);
    }
  }
  return match_nodes;
}

bool IsNodeMatchLabelExpression(const Node &node,
                                const rpc::LabelMatchExpression &expression) {
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

bool IsNodeLabelKeyExists(const Node &node, const std::string &key) {
  return node.GetLocalView().labels.contains(key);
}

bool IsNodeLabelInValues(const Node &node,
                         const std::string &key,
                         const absl::flat_hash_set<std::string> &values) {
  const auto &node_labels = node.GetLocalView().labels;
  if (!node_labels.contains(key)) {
    return false;
  }

  return values.contains(node_labels.at(key));
}
}  // namespace

std::vector<scheduling::NodeID> NodeLabelHardSchedulingFilter::Filter(
    const absl::flat_hash_map<scheduling::NodeID, Node> &all_nodes_view,
    const std::vector<scheduling::NodeID> &candidate_nodes,
    const ResourceRequest &resource_request) {
  std::vector<scheduling::NodeID> hard_match_nodes;
  return FilterNodesByLabelMatchExpressions(
      all_nodes_view, candidate_nodes, expressions_);
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
