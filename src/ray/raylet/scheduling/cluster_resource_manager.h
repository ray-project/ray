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

#include <gtest/gtest_prod.h>

#include <iostream>
#include <sstream>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/raylet/scheduling/cluster_resource_data.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler_interface.h"
#include "ray/raylet/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/local_resource_manager.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/raylet/scheduling/scheduling_policy.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

using rpc::HeartbeatTableData;

/// Class encapsulating the cluster resources and the logic to assign
/// tasks to nodes based on the task's constraints and the available
/// resources at those nodes.
class ClusterResourceManager {
 public:
  ClusterResourceManager() {}
  /// Constructor initializing the resources associated with the local node.
  ///
  /// \param local_node_id: ID of local node,
  /// \param local_node_resources: The total and the available resources associated
  /// with the local node.
  ClusterResourceManager(int64_t local_node_id,
                           const NodeResources &local_node_resources,
                           gcs::GcsClient &gcs_client);
  ClusterResourceManager(
      const std::string &local_node_id,
      const absl::flat_hash_map<std::string, double> &local_node_resources,
      gcs::GcsClient &gcs_client,
      std::function<int64_t(void)> get_used_object_store_memory = nullptr,
      std::function<bool(void)> get_pull_manager_at_capacity = nullptr);

  // Mapping from predefined resource indexes to resource strings
  std::string GetResourceNameFromIndex(int64_t res_idx);

  /// Update node resources. This hanppens when a node resource usage udpated.
  ///
  /// \param node_id_string ID of the node which resoruces need to be udpated.
  /// \param resource_data The node resource data.
  bool UpdateNode(const std::string &node_id_string,
                  const rpc::ResourcesData &resource_data) override;

  /// Remove node from the cluster data structure. This happens
  /// when a node fails or it is removed from the cluster.
  ///
  /// \param node_id_string ID of the node to be removed.
  bool RemoveNode(const std::string &node_id_string) override;

  /// Get number of nodes in the cluster.
  int64_t NumNodes() const;

  /// Temporarily get the StringIDMap.
  const StringIdMap &GetStringIdMap() const;

  /// Update total capacity of a given resource of a given node.
  ///
  /// \param node_name: Node whose resource we want to update.
  /// \param resource_name: Resource which we want to update.
  /// \param resource_total: New capacity of the resource.
  void UpdateResourceCapacity(const std::string &node_name,
                              const std::string &resource_name,
                              double resource_total) override;

  /// Delete a given resource from a given node.
  ///
  /// \param node_name: Node whose resource we want to delete.
  /// \param resource_name: Resource we want to delete
  void DeleteResource(const std::string &node_name,
                      const std::string &resource_name) override;

  /// Return local resources in human-readable string form.
  std::string GetNodeResourceViewString(const std::string &node_name) const override;

  /// Subtract the resources required by a given resource request (resource_request) from
  /// a given remote node.
  ///
  /// \param node_id Remote node whose resources we allocate.
  /// \param resource_request Task for which we allocate resources.
  /// \return True if remote node has enough resources to satisfy the resource request.
  /// False otherwise.
  bool AllocateRemoteTaskResources(
      const std::string &node_id,
      const absl::flat_hash_map<std::string, double> &task_resources);

  /// Return human-readable string for this scheduler state.
  std::string DebugString() const;

  LocalResourceManager &GetLocalResourceManager() { return *local_resource_manager_; }

  const NodeResources &GetNodeResources(const std::string &node_name) const;

  /// Decrease the available resources of a node when a resource request is
  /// scheduled on the given node.
  ///
  /// \param node_id: ID of node on which request is being scheduled.
  /// \param resource_request: resource request being scheduled.
  ///
  /// \return true, if resource_request can be indeed scheduled on the node,
  /// and false otherwise.
  bool SubtractRemoteNodeAvailableResources(int64_t node_id,
                                            const ResourceRequest &resource_request);
 private:

  /// Add a new node or overwrite the resources of an existing node.
  ///
  /// \param node_id: Node ID.
  /// \param node_resources: Up to date total and available resources of the node.
  void AddOrUpdateNode(int64_t node_id, const NodeResources &node_resources);

  void AddOrUpdateNode(
      const std::string &node_id,
      const absl::flat_hash_map<std::string, double> &resource_map_total,
      const absl::flat_hash_map<std::string, double> &resource_map_available);

  /// Remove node from the cluster data structure. This happens
  /// when a node fails or it is removed from the cluster.
  ///
  /// \param node_id ID of the node to be removed.
  bool RemoveNode(int64_t node_id);

  /// Return resources associated to the given node_id in ret_resources.
  /// If node_id not found, return false; otherwise return true.
  bool GetNodeResources(int64_t node_id, NodeResources *ret_resources) const;

  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  absl::flat_hash_map<int64_t, Node> nodes_;
  /// Resources of local node.
  std::unique_ptr<LocalResourceManager> local_resource_manager_;
  /// Keep the mapping between node and resource IDs in string representation
  /// to integer representation. Used for improving map performance.
  StringIdMap string_to_int_map_;

  friend class ClusterResourceManagerTest;
  FRIEND_TEST(ClusterResourceManagerTest, SchedulingDeleteClusterNodeTest);
  FRIEND_TEST(ClusterResourceManagerTest, SchedulingModifyClusterNodeTest);
  FRIEND_TEST(ClusterResourceManagerTest, SchedulingUpdateAvailableResourcesTest);
  FRIEND_TEST(ClusterResourceManagerTest, SchedulingAddOrUpdateNodeTest);
  FRIEND_TEST(ClusterResourceManagerTest, SpreadSchedulingStrategyTest);
  FRIEND_TEST(ClusterResourceManagerTest, SchedulingResourceRequestTest);
  FRIEND_TEST(ClusterResourceManagerTest, SchedulingUpdateTotalResourcesTest);
  FRIEND_TEST(ClusterResourceManagerTest,
              UpdateLocalAvailableResourcesFromResourceInstancesTest);
  FRIEND_TEST(ClusterResourceManagerTest, ResourceUsageReportTest);
  FRIEND_TEST(ClusterResourceManagerTest, DeadNodeTest);
  FRIEND_TEST(ClusterResourceManagerTest, TestAlwaysSpillInfeasibleTask);
  FRIEND_TEST(ClusterResourceManagerTest, ObjectStoreMemoryUsageTest);
  FRIEND_TEST(ClusterResourceManagerTest, AvailableResourceInstancesOpsTest);
  FRIEND_TEST(ClusterResourceManagerTest, DirtyLocalViewTest);
  FRIEND_TEST(ClusterResourceManagerTest, DynamicResourceTest);
  FRIEND_TEST(ClusterTaskManagerTestWithGPUsAtHead, RleaseAndReturnWorkerCpuResources);
  FRIEND_TEST(ClusterResourceManagerTest, TestForceSpillback);
};

}  // end namespace ray
