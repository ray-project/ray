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

#include "ray/common/id.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {

namespace raylet {

class VirtualClusterManager {
 public:
  VirtualClusterManager(const NodeID &node_id,
                        std::function<void()> local_node_cleanup_fn)
      : local_node_instance_id_(node_id.Hex()),
        local_node_cleanup_fn_(local_node_cleanup_fn) {}

  /// Update the virtual cluster.
  ///
  /// \param virtual_cluster_data The virtual cluster data.
  bool UpdateVirtualCluster(rpc::VirtualClusterTableData virtual_cluster_data);

  /// Check if the virtual cluster exists.
  ///
  /// \param virtual_cluster_id The virtual cluster id.
  /// \return Whether the virtual cluster exists.
  bool ContainsVirtualCluster(const std::string &virtual_cluster_id) const;

  /// Check if the virtual cluster contains the node instance.
  ///
  /// \param virtual_cluster_id The virtual cluster id.
  /// \param node_id The node instance id.
  /// \return Whether the virtual cluster contains the node instance.
  bool ContainsNodeInstance(const std::string &virtual_cluster_id,
                            const NodeID &node_id) const;

 private:
  /// The virtual clusters.
  absl::flat_hash_map<std::string, rpc::VirtualClusterTableData> virtual_clusters_;
  /// The local node instance id.
  std::string local_node_instance_id_;
  std::function<void()> local_node_cleanup_fn_;
  /// The (indivisible) virtual cluster to which the local node belongs.
  std::string local_virtual_cluster_id_;
};

}  // namespace raylet
}  // end namespace ray
