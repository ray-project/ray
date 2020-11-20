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

GcsResourceManager::GcsResourceManager(GcsNodeManager &gcs_node_manager) {
  gcs_node_manager.AddNodeResourceChangeListener(
      [this](const rpc::HeartbeatTableData &heartbeat) {
        auto node_id = NodeID::FromBinary(heartbeat.client_id());
        cluster_resources_[node_id] =
            ResourceSet(MapFromProtobuf(heartbeat.resources_available()));
      });
}

const absl::flat_hash_map<NodeID, ResourceSet> &GcsResourceManager::GetClusterResources()
    const {
  return cluster_resources_;
}

bool GcsResourceManager::AcquireResource(const NodeID &node_id,
                                         const ResourceSet &required_resources) {
  auto iter = cluster_resources_.find(node_id);
  if (iter == cluster_resources_.end()) {
    RAY_LOG(INFO) << "Node " << node_id << " not exist.";
    return false;
  }
  if (!required_resources.IsSubset(iter->second)) {
    RAY_LOG(INFO) << "Attempt to acquire unknown resource: "
                  << required_resources.ToString()
                  << ", node resource is: " << iter->second.ToString();
    return false;
  }
  iter->second.SubtractResourcesStrict(required_resources);
  return true;
}

bool GcsResourceManager::ReleaseResource(const NodeID &node_id,
                                         const ResourceSet &acquired_resources) {
  auto iter = cluster_resources_.find(node_id);
  if (iter == cluster_resources_.end()) {
    RAY_LOG(INFO) << "Node " << node_id << " not exist.";
    return false;
  }
  iter->second.AddResources(acquired_resources);
  return true;
}

}  // namespace gcs
}  // namespace ray
