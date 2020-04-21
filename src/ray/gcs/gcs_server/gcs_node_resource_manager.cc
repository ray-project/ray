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

#include "gcs_node_resource_manager.h"

namespace ray {
namespace gcs {

void GcsNodeResourceManager::UpdateNodeResources(
    const ClientID &node_id, const NodeInfoAccessor::ResourceMap &resources) {
  absl::MutexLock lock(&mutex_);
  nodes_resource_cache_[node_id].insert(resources.begin(), resources.end());
}

void GcsNodeResourceManager::DeleteNodeResources(
    const ClientID &node_id, const std::vector<std::string> &resource_names) {
  absl::MutexLock lock(&mutex_);
  if (nodes_resource_cache_.count(node_id)) {
    for (auto &resource_name : resource_names) {
      nodes_resource_cache_[node_id].erase(resource_name);
    }
  }
}

boost::optional<NodeInfoAccessor::ResourceMap> GcsNodeResourceManager::GetNodeResources(
    const ClientID &node_id, const std::vector<std::string> &resource_names) {
  absl::MutexLock lock(&mutex_);
  if (nodes_resource_cache_.count(node_id)) {
    NodeInfoAccessor::ResourceMap result;
    for (auto &resource_name : resource_names) {
      auto it = nodes_resource_cache_[node_id].find(resource_name);
      if (it == nodes_resource_cache_[node_id].end()) {
        return boost::none;
      }
      result[resource_name] = it->second;
    }
    return std::move(result);
  }
  return boost::none;
}

}  // namespace gcs
}  // namespace ray
