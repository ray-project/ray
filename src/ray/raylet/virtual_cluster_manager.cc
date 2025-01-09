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
  if (virtual_cluster_data.divisible()) {
    RAY_LOG(WARNING) << "Virtual cluster " << virtual_cluster_data.id()
                     << " is divisible, "
                     << "ignore it.";
    return false;
  }

  // The virtual cluster id of the input data.
  const auto &input_virtual_cluster_id = virtual_cluster_data.id();

  if (virtual_cluster_data.is_removed()) {
    if (local_virtual_cluster_id_ == input_virtual_cluster_id) {
      local_virtual_cluster_id_.clear();
      // The virtual cluster is removed, we have to clean up
      // the local tasks (it is a no-op in most cases).
      local_node_cleanup_fn_();
    }
    virtual_clusters_.erase(input_virtual_cluster_id);
  } else {
    // Whether the local node in the input data.
    bool local_node_in_input_virtual_cluster =
        virtual_cluster_data.node_instances().contains(local_node_instance_id_);

    // The local node is removed from its current virtual cluster.
    if (local_virtual_cluster_id_ == input_virtual_cluster_id &&
        !local_node_in_input_virtual_cluster) {
      local_virtual_cluster_id_.clear();
      // Clean up the local tasks (it is a no-op in most cases).
      local_node_cleanup_fn_();
    } else if (local_virtual_cluster_id_ != input_virtual_cluster_id &&
               local_node_in_input_virtual_cluster) {  // The local node is added to a new
                                                       // virtual cluster.
      local_virtual_cluster_id_ = input_virtual_cluster_id;
      // There are chances that the pub message (removing the local node from a virtual
      // cluster) was lost in the past, so we also have to clean up when adding the local
      // node to a new virtual cluster (it is a no-op in most cases).
      local_node_cleanup_fn_();
    }

    virtual_clusters_[input_virtual_cluster_id] = std::move(virtual_cluster_data);
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
  RAY_CHECK(!virtual_cluster_data.divisible());

  const auto &node_instances = virtual_cluster_data.node_instances();
  return node_instances.find(node_id.Hex()) != node_instances.end();
}

}  // namespace raylet
}  // namespace ray
