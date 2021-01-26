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

#include "ray/common/task/scheduling_resources.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler_interface.h"

namespace ray {
class OldClusterResourceScheduler : public ClusterResourceSchedulerInterface {
 public:
  explicit OldClusterResourceScheduler(
      const NodeID &self_node_id, ResourceIdSet &local_available_resources,
      std::unordered_map<NodeID, SchedulingResources> &cluster_resource_map,
      std::shared_ptr<SchedulingResources> last_heartbeat_resources);

  /// Remove node from the cluster data structure. This happens
  /// when a node fails or it is removed from the cluster.
  ///
  /// \param node_id_string ID of the node to be removed.
  bool RemoveNode(const std::string &node_id_string) override;

  /// Update node resources. This hanppens when a node resource usage udpated.
  ///
  /// \param node_id_string ID of the node which resoruces need to be udpated.
  /// \param resource_data The node resource data.
  bool UpdateNode(const std::string &node_id_string,
                  const rpc::ResourcesData &resource_data) override;

  /// \param node_name: Node whose resource we want to update.
  /// \param resource_name: Resource which we want to update.
  /// \param resource_total: New capacity of the resource.
  void UpdateResourceCapacity(const std::string &node_id_string,
                              const std::string &resource_name,
                              double resource_total) override;

  /// Delete a given resource from a given node.
  ///
  /// \param node_name: Node whose resource we want to delete.
  /// \param resource_name: Resource we want to delete
  void DeleteResource(const std::string &node_id_string,
                      const std::string &resource_name) override;

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending raylet <-> gcs heartbeats. In particular, this should fill in
  /// resources_available and resources_total.
  ///
  /// \param Output parameter. `resources_available` and `resources_total` are the only
  /// fields used.
  void FillResourceUsage(std::shared_ptr<rpc::ResourcesData> data) override;

  /// Return local resources in human-readable string form.
  std::string GetLocalResourceViewString() const override;

 private:
  std::string self_node_id_string_;
  ResourceIdSet &local_available_resources_;
  std::unordered_map<NodeID, SchedulingResources> &cluster_resource_map_;
  std::shared_ptr<SchedulingResources> last_heartbeat_resources_;
};
}  // namespace ray
