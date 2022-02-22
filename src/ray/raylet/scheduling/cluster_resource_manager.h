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
#include "ray/raylet/scheduling/cluster_resource_data.h"
#include "ray/raylet/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/local_resource_manager.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace raylet {
class ClusterTaskManagerTest;
}

/// Class manages the resources view of the entire cluster.
/// This class is not thread safe.
class ClusterResourceManager {
 public:
  explicit ClusterResourceManager(StringIdMap &string_to_int_map);

  /// Get the resource view of the cluster.
  const absl::flat_hash_map<int64_t, Node> &GetResourceView() const;

  // Mapping from predefined resource indexes to resource strings
  std::string GetResourceNameFromIndex(int64_t res_idx);

  /// Update node resources. This hanppens when a node resource usage udpated.
  ///
  /// \param node_id_string ID of the node which resoruces need to be udpated.
  /// \param resource_data The node resource data.
  bool UpdateNode(const std::string &node_id_string,
                  const rpc::ResourcesData &resource_data);

  /// Remove node from the cluster data structure. This happens
  /// when a node fails or it is removed from the cluster.
  ///
  /// \param node_id_string ID of the node to be removed.
  bool RemoveNode(const std::string &node_id_string);

  /// Get number of nodes in the cluster.
  int64_t NumNodes() const;

  /// Update total capacity of a given resource of a given node.
  ///
  /// \param node_name: Node whose resource we want to update.
  /// \param resource_name: Resource which we want to update.
  /// \param resource_total: New capacity of the resource.
  void UpdateResourceCapacity(const std::string &node_name,
                              const std::string &resource_name, double resource_total);

  /// Delete a given resource from a given node.
  ///
  /// \param node_name: Node whose resource we want to delete.
  /// \param resource_name: Resource we want to delete
  void DeleteResource(const std::string &node_name, const std::string &resource_name);

  /// Return local resources in human-readable string form.
  std::string GetNodeResourceViewString(const std::string &node_name) const;

  /// Get local resource.
  const NodeResources &GetNodeResources(const std::string &node_name) const;

  /// Subtract available resource from a given node.
  //// Return false if such node doesn't exist.
  bool SubtractNodeAvailableResources(int64_t node_id,
                                      const ResourceRequest &resource_request);

 private:
  friend class ClusterResourceScheduler;

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
  /// Keep the mapping between node and resource IDs in string representation
  /// to integer representation. Used for improving map performance.
  StringIdMap &string_to_int_map_;

  friend class ClusterResourceSchedulerTest;
  friend class raylet::ClusterTaskManagerTest;
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingDeleteClusterNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingModifyClusterNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingUpdateAvailableResourcesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingAddOrUpdateNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SpreadSchedulingStrategyTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingResourceRequestTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingUpdateTotalResourcesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest,
              UpdateLocalAvailableResourcesFromResourceInstancesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, ResourceUsageReportTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, DeadNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TestAlwaysSpillInfeasibleTask);
  FRIEND_TEST(ClusterResourceSchedulerTest, ObjectStoreMemoryUsageTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, AvailableResourceInstancesOpsTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, DirtyLocalViewTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, DynamicResourceTest);
  FRIEND_TEST(ClusterTaskManagerTestWithGPUsAtHead, RleaseAndReturnWorkerCpuResources);
  FRIEND_TEST(ClusterResourceSchedulerTest, TestForceSpillback);
};

}  // end namespace ray
