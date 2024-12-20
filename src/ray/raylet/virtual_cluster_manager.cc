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
// limitations under the License

#include "ray/raylet/virtual_cluster_manager.h"

namespace ray {

namespace raylet {

//////////////////////// VirtualClusterManager ////////////////////////
bool VirtualClusterManager::UpdateVirtualCluster(
    rpc::VirtualClusterTableData virtual_cluster_data) {
  RAY_LOG(INFO) << "Virtual cluster updated: " << virtual_cluster_data.id();
  if (virtual_cluster_data.mode() != rpc::AllocationMode::MIXED) {
    RAY_LOG(WARNING) << "The virtual cluster mode is not MIXED, ignore it.";
    return false;
  }

  const auto &virtual_cluster_id = virtual_cluster_data.id();
  auto it = virtual_clusters_.find(virtual_cluster_id);
  if (it == virtual_clusters_.end()) {
    virtual_clusters_[virtual_cluster_id] = std::move(virtual_cluster_data);
  } else {
    if (it->second.revision() > virtual_cluster_data.revision()) {
      RAY_LOG(WARNING)
          << "The revision of the received virtual cluster is outdated, ignore it.";
      return false;
    }

    if (virtual_cluster_data.is_removed()) {
      virtual_clusters_.erase(it);
      return true;
    }

    it->second = std::move(virtual_cluster_data);
  }
  return true;
}

bool VirtualClusterManager::ContainsVirtualCluster(
    const std::string &virtual_cluster_id) const {
  return virtual_clusters_.find(virtual_cluster_id) != virtual_clusters_.end();
}

bool VirtualClusterManager::ContainsNodeInstance(const std::string &virtual_cluster_id,
                                                 const NodeID &node_id) const {
  auto it = virtual_clusters_.find(virtual_cluster_id);
  if (it == virtual_clusters_.end()) {
    return false;
  }
  const auto &virtual_cluster_data = it->second;
  RAY_CHECK(virtual_cluster_data.mode() == rpc::AllocationMode::MIXED);

  const auto &node_instances = virtual_cluster_data.node_instances();
  return node_instances.find(node_id.Hex()) != node_instances.end();
}

}  // namespace raylet
}  // namespace ray
