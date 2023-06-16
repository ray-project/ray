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

#include "ray/raylet/scheduling/node_labels_index.h"

using json = nlohmann::json;

namespace ray {

void NodeLabelsIndex::AddLabels(
    const scheduling::NodeID &node_id,
    const absl::flat_hash_map<std::string, std::string> &labels) {
  if (node_to_labels_.contains(node_id)) {
    RAY_LOG(DEBUG) << "The node id " << node_id
                   << " already exists and will overwrite the original node labels.";
    RemoveLabels(node_id);
  }

  for (const auto &[key, value] : labels) {
    label_to_nodes_[key][value].insert(node_id);
  }

  node_to_labels_[node_id] = labels;
}

void NodeLabelsIndex::RemoveLabels(const scheduling::NodeID &node_id) {
  auto iter = node_to_labels_.find(node_id);
  if (iter == node_to_labels_.end()) {
    return;
  }
  for (const auto &[key, value] : iter->second) {
    auto &node_id_set = label_to_nodes_[key][value];
    node_id_set.erase(node_id);
    if (node_id_set.empty()) {
      label_to_nodes_[key].erase(value);
      if (label_to_nodes_[key].empty()) {
        label_to_nodes_.erase(key);
      }
    }
  }
  node_to_labels_.erase(iter);
}

absl::flat_hash_set<scheduling::NodeID> NodeLabelsIndex::GetNodesByKeyAndValue(
    const std::string &key, const absl::flat_hash_set<std::string> &values) const {
  absl::flat_hash_set<scheduling::NodeID> node_ids;
  auto iter = label_to_nodes_.find(key);
  if (iter == label_to_nodes_.end()) {
    return {};
  }

  const auto &value_to_nodes = iter->second;
  for (const auto &value : values) {
    auto node_set_iter = value_to_nodes.find(value);
    if (node_set_iter != value_to_nodes.end()) {
      node_ids.insert(node_set_iter->second.begin(), node_set_iter->second.end());
    }
  }
  return node_ids;
}

absl::flat_hash_set<scheduling::NodeID> NodeLabelsIndex::GetNodesByKey(
    const std::string &key) const {
  absl::flat_hash_set<scheduling::NodeID> node_ids;
  auto iter = label_to_nodes_.find(key);
  if (iter == label_to_nodes_.end()) {
    return {};
  }

  for (const auto &[value, node_id_set] : iter->second) {
    node_ids.insert(node_id_set.begin(), node_id_set.end());
  }
  return node_ids;
}

std::string NodeLabelsIndex::DebugString() const {
  json object;
  object["label_to_nodes"] = GetLabelToNodeJsonObject(label_to_nodes_);
  return object.dump();
}

nlohmann::json NodeLabelsIndex::GetLabelToNodeJsonObject(
    const LabelToNodes &label_to_nodes) const {
  json object;
  for (const auto &[key, value_map] : label_to_nodes) {
    for (const auto &[value, node_map] : value_map) {
      for (const auto &node_id : node_map) {
        object[key][value].push_back(node_id.ToInt());
      }
    }
  }
  return object;
}
}  // namespace ray
