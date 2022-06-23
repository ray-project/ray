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

#include "ray/core_worker/lease_policy.h"

namespace ray {
namespace core {

std::pair<rpc::Address, bool> LocalityAwareLeasePolicy::GetBestNodeForTask(
    const TaskSpecification &spec) {
  if (spec.GetMessage().scheduling_strategy().scheduling_strategy_case() ==
      rpc::SchedulingStrategy::SchedulingStrategyCase::kSpreadSchedulingStrategy) {
    // The explicit spread scheduling strategy
    // has higher priority than locality aware scheduling.
    return std::make_pair(fallback_rpc_address_, false);
  }

  if (spec.IsNodeAffinitySchedulingStrategy()) {
    // The explicit node affinity scheduling strategy
    // has higher priority than locality aware scheduling.
    if (auto addr = node_addr_factory_(spec.GetNodeAffinitySchedulingStrategyNodeId())) {
      return std::make_pair(addr.value(), false);
    }
    return std::make_pair(fallback_rpc_address_, false);
  }

  // Pick node based on locality.
  if (auto node_id = GetBestNodeIdForTask(spec)) {
    if (auto addr = node_addr_factory_(node_id.value())) {
      return std::make_pair(addr.value(), true);
    }
  }
  return std::make_pair(fallback_rpc_address_, false);
}

/// Criteria for "best" node: The node with the most object bytes (from object_ids) local.
absl::optional<NodeID> LocalityAwareLeasePolicy::GetBestNodeIdForTask(
    const TaskSpecification &spec) {
  const auto object_ids = spec.GetDependencyIds();
  // Number of object bytes (from object_ids) that a given node has local.
  absl::flat_hash_map<NodeID, uint64_t> bytes_local_table;
  uint64_t max_bytes = 0;
  absl::optional<NodeID> max_bytes_node;
  // Finds the node with the maximum number of object bytes local.
  for (const ObjectID &object_id : object_ids) {
    if (auto locality_data = locality_data_provider_->GetLocalityData(object_id)) {
      for (const NodeID &node_id : locality_data->nodes_containing_object) {
        auto &bytes = bytes_local_table[node_id];
        bytes += locality_data->object_size;
        // Update max, if needed.
        if (bytes > max_bytes) {
          max_bytes = bytes;
          max_bytes_node = node_id;
        }
      }
    } else {
      RAY_LOG(WARNING) << "No locality data available for object " << object_id
                       << ", won't be included in locality cost";
    }
  }
  return max_bytes_node;
}

std::pair<rpc::Address, bool> LocalLeasePolicy::GetBestNodeForTask(
    const TaskSpecification &spec) {
  // Always return the local node.
  return std::make_pair(local_node_rpc_address_, false);
}

}  // namespace core
}  // namespace ray
