// Copyright 2017 The Ray Authors.
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

#include <iostream>
#include <sstream>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "nlohmann/json.hpp"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/util/logging.h"

namespace ray {
using LabelToNodes = absl::flat_hash_map<
    std::string,
    absl::flat_hash_map<std::string, absl::flat_hash_set<scheduling::NodeID>>>;

class NodeLabelsIndex {
 public:
  void AddLabels(const scheduling::NodeID &node_id,
                 const absl::flat_hash_map<std::string, std::string> &labels);

  void RemoveLabels(const scheduling::NodeID &node_id);

  absl::flat_hash_set<scheduling::NodeID> GetNodesByKeyAndValue(
      const std::string &key, const absl::flat_hash_set<std::string> &values) const;

  absl::flat_hash_set<scheduling::NodeID> GetNodesByKey(const std::string &key) const;

  std::string DebugString() const;

 private:
  nlohmann::json GetLabelToNodeJsonObject(const LabelToNodes &label_to_nodes) const;

  // <label_key, <lable_value, [node_id]>>
  LabelToNodes label_to_nodes_;

  // <node_id, <label_key, label_value>>
  absl::flat_hash_map<scheduling::NodeID, absl::flat_hash_map<std::string, std::string>>
      node_to_labels_;
};
}  // end namespace ray
