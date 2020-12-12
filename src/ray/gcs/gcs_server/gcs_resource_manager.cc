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

#include "ray/gcs/gcs_server/gcs_resource_manager.h"

namespace ray {
namespace gcs {

const absl::flat_hash_map<NodeID, ResourceSet> &GcsResourceManager::GetClusterResources()
    const {
  return cluster_resources_;
}

void GcsResourceManager::UpdateResources(const NodeID &node_id,
                                         const ResourceSet &resources) {
  cluster_resources_[node_id] = resources;
}

void GcsResourceManager::RemoveResources(const NodeID &node_id) {
  cluster_resources_.erase(node_id);
}

bool GcsResourceManager::AcquireResources(const NodeID &node_id,
                                          const ResourceSet &required_resources) {
  auto iter = cluster_resources_.find(node_id);
  RAY_CHECK(iter != cluster_resources_.end()) << "Node " << node_id << " not exist.";
  if (!required_resources.IsSubset(iter->second)) {
    return false;
  }
  iter->second.SubtractResourcesStrict(required_resources);
  return true;
}

bool GcsResourceManager::ReleaseResources(const NodeID &node_id,
                                          const ResourceSet &acquired_resources) {
  auto iter = cluster_resources_.find(node_id);
  if (iter != cluster_resources_.end()) {
    iter->second.AddResources(acquired_resources);
  }
  // If node dead, we will not find the node. This is a normal scenario, so it returns
  // true.
  return true;
}

}  // namespace gcs
}  // namespace ray
