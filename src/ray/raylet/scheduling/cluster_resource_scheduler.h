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
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/raylet/scheduling/scheduling_policy.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

using rpc::HeartbeatTableData;

/// Class encapsulating the cluster resources and the logic to assign
/// tasks to nodes based on the task's constraints and the available
/// resources at those nodes.
class ClusterResourceScheduler : public ClusterResourceSchedulerInterface {
 public:
  ClusterResourceScheduler() {}
  /// Constructor initializing the resources associated with the local node.
  ///
  /// \param local_node_id: ID of local node,
  /// \param local_node_resources: The total and the available resources associated
  /// with the local node.
  ClusterResourceScheduler(int64_t local_node_id,
                           const NodeResources &local_node_resources,
                           gcs::GcsClient &gcs_client);
  ClusterResourceScheduler(
      const std::string &local_node_id,
      const absl::flat_hash_map<std::string, double> &local_node_resources,
      gcs::GcsClient &gcs_client,
      std::function<int64_t(void)> get_used_object_store_memory = nullptr,
      std::function<bool(void)> get_pull_manager_at_capacity = nullptr);

  // Mapping from predefined resource indexes to resource strings
  std::string GetResourceNameFromIndex(int64_t res_idx);

  void AddOrUpdateNode(
      const std::string &node_id,
      const absl::flat_hash_map<std::string, double> &resource_map_total,
      const absl::flat_hash_map<std::string, double> &resource_map_available);

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

  ///  Find a node in the cluster on which we can schedule a given resource request.
  ///  In hybrid mode, see `scheduling_policy.h` for a description of the policy.
  ///
  ///  \param resource_request: Task to be scheduled.
  ///  \param scheduling_strategy: Strategy about how to schedule this task.
  ///  \param actor_creation: True if this is an actor creation task.
  ///  \param force_spillback: For non-actor creation requests, pick a remote
  ///  feasible node. If this is false, then the task may be scheduled to the
  ///  local node.
  ///  \param violations: The number of soft constraint violations associated
  ///                     with the node returned by this function (assuming
  ///                     a node that can schedule resource_request is found).
  ///  \param is_infeasible[in]: It is set true if the task is not schedulable because it
  ///  is infeasible.
  ///
  ///  \return -1, if no node can schedule the current request; otherwise,
  ///          return the ID of a node that can schedule the resource request.
  int64_t GetBestSchedulableNode(const ResourceRequest &resource_request,
                                 const rpc::SchedulingStrategy &scheduling_strategy,
                                 bool actor_creation, bool force_spillback,
                                 int64_t *violations, bool *is_infeasible);

  /// Similar to
  ///    int64_t GetBestSchedulableNode(...)
  /// but the return value is different:
  /// \return "", if no node can schedule the current request; otherwise,
  ///          return the ID in string format of a node that can schedule the
  //           resource request.
  std::string GetBestSchedulableNode(
      const absl::flat_hash_map<std::string, double> &resource_request,
      const rpc::SchedulingStrategy &scheduling_strategy,
      bool requires_object_store_memory, bool actor_creation, bool force_spillback,
      int64_t *violations, bool *is_infeasible);

  /// Get local node resources.
  const NodeResources &GetLocalNodeResources() const;

  /// Get number of nodes in the cluster.
  int64_t NumNodes() const;

  /// Temporarily get the StringIDMap.
  const StringIdMap &GetStringIdMap() const;

  /// Add a local resource that is available.
  ///
  /// \param resource_name: Resource which we want to update.
  /// \param resource_total: New capacity of the resource.
  void AddLocalResourceInstances(const std::string &resource_name,
                                 const std::vector<FixedPoint> &instances);

  /// Check whether the available resources are empty.
  ///
  /// \param resource_name: Resource which we want to check.
  bool IsAvailableResourceEmpty(const std::string &resource_name);

  /// Update total capacity of a given resource of a given node.
  ///
  /// \param node_name: Node whose resource we want to update.
  /// \param resource_name: Resource which we want to update.
  /// \param resource_total: New capacity of the resource.
  void UpdateResourceCapacity(const std::string &node_name,
                              const std::string &resource_name,
                              double resource_total) override;

  /// Delete a given resource from the local node.
  ///
  /// \param resource_name: Resource we want to delete
  void DeleteLocalResource(const std::string &resource_name);

  /// Delete a given resource from a given node.
  ///
  /// \param node_name: Node whose resource we want to delete.
  /// \param resource_name: Resource we want to delete
  void DeleteResource(const std::string &node_name,
                      const std::string &resource_name) override;

  /// Return local resources.
  NodeResourceInstances GetLocalResources() const { return local_resources_; };

  /// Return local resources in human-readable string form.
  std::string GetLocalResourceViewString() const override;

  /// Allocate local resources to satisfy a given request (resource_request).
  ///
  /// \param resource_request: Resources requested by a task.
  /// \param task_allocation: Local resources allocated to satsify resource_request
  /// demand.
  ///
  /// \return true, if allocation successful. If false, the caller needs to free the
  /// allocated resources, i.e., task_allocation.
  bool AllocateTaskResourceInstances(
      const ResourceRequest &resource_request,
      std::shared_ptr<TaskResourceInstances> task_allocation);

  /// Free resources which were allocated with a task. The freed resources are
  /// added back to the node's local available resources.
  ///
  /// \param task_allocation: Task's resources to be freed.
  void FreeTaskResourceInstances(std::shared_ptr<TaskResourceInstances> task_allocation);

  /// Increase the available CPU instances of this node.
  ///
  /// \param cpu_instances CPU instances to be added to available cpus.
  ///
  /// \return Overflow capacities of CPU instances after adding CPU
  /// capacities in cpu_instances.
  std::vector<double> AddCPUResourceInstances(std::vector<double> &cpu_instances);

  /// Decrease the available CPU instances of this node.
  ///
  /// \param cpu_instances CPU instances to be removed from available cpus.
  /// \param allow_going_negative Allow the values to go negative (disable underflow).
  ///
  /// \return Underflow capacities of CPU instances after subtracting CPU
  /// capacities in cpu_instances.
  std::vector<double> SubtractCPUResourceInstances(std::vector<double> &cpu_instances,
                                                   bool allow_going_negative = false);

  /// Increase the available GPU instances of this node.
  ///
  /// \param gpu_instances GPU instances to be added to available gpus.
  ///
  /// \return Overflow capacities of GPU instances after adding GPU
  /// capacities in gpu_instances.
  std::vector<double> AddGPUResourceInstances(std::vector<double> &gpu_instances);

  /// Decrease the available GPU instances of this node.
  ///
  /// \param gpu_instances GPU instances to be removed from available gpus.
  ///
  /// \return Underflow capacities of GPU instances after subtracting GPU
  /// capacities in gpu_instances.
  std::vector<double> SubtractGPUResourceInstances(std::vector<double> &gpu_instances);

  /// Subtract the resources required by a given resource request (resource_request) from
  /// the local node. This function also updates the local node resources at the instance
  /// granularity.
  ///
  /// \param resource_request Task for which we allocate resources.
  /// \param task_allocation Resources allocated to the task at instance granularity.
  /// This is a return parameter.
  ///
  /// \return True if local node has enough resources to satisfy the resource request.
  /// False otherwise.
  bool AllocateLocalTaskResources(
      const absl::flat_hash_map<std::string, double> &task_resources,
      std::shared_ptr<TaskResourceInstances> task_allocation);

  bool AllocateLocalTaskResources(const ResourceRequest &resource_request,
                                  std::shared_ptr<TaskResourceInstances> task_allocation);

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

  void ReleaseWorkerResources(std::shared_ptr<TaskResourceInstances> task_allocation);

  /// Update the available resources of the local node given
  /// the available instances of each resource of the local node.
  /// Basically, this means computing the available resources
  /// by adding up the available quantities of each instance of that
  /// resources.
  ///
  /// Example: Assume the local node has four GPU instances with the
  /// following availabilities: 0.2, 0.3, 0.1, 1. Then the total GPU
  // resources availabile at that node is 0.2 + 0.3 + 0.1 + 1. = 1.6
  void UpdateLocalAvailableResourcesFromResourceInstances();

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending resource usage of raylet to gcs. In particular, this should fill in
  /// resources_available and resources_total.
  ///
  /// \param Output parameter. `resources_available` and `resources_total` are the only
  /// fields used.
  void FillResourceUsage(rpc::ResourcesData &resources_data) override;

  /// Populate a UpdateResourcesRequest. This is inteneded to update the
  /// resource totals on a node when a custom resource is created or deleted
  /// (e.g. during the placement group lifecycle).
  ///
  /// \param resource_map_filter When returning the resource map, the returned result will
  /// only contain the keys in the filter. Note that only the key of the map is used.
  /// \return The total resource capacity of the node.
  ray::gcs::NodeResourceInfoAccessor::ResourceMap GetResourceTotals(
      const absl::flat_hash_map<std::string, double> &resource_map_filter) const override;

  /// Update last report resources local cache from gcs cache,
  /// this is needed when gcs fo.
  ///
  /// \param gcs_resources: The remote cache from gcs.
  void UpdateLastResourceUsage(
      const std::shared_ptr<SchedulingResources> gcs_resources) override;

  double GetLocalAvailableCpus() const override;

  /// Serialize task resource instances to json string.
  ///
  /// \param task_allocation Allocated resource instances for a task.
  /// \return The task resource instances json string
  std::string SerializedTaskResourceInstances(
      std::shared_ptr<TaskResourceInstances> task_allocation) const;

  /// Return human-readable string for this scheduler state.
  std::string DebugString() const;

  /// Get the number of cpus on this node.
  uint64_t GetNumCpus() const;

  /// Check whether a task request is schedulable on a the local node. A node is
  /// schedulable if it has the available resources needed to execute the task.
  ///
  /// \param shape The resource demand's shape.
  bool IsLocallySchedulable(const absl::flat_hash_map<std::string, double> &shape);

 private:
  /// Create instances for each resource associated with the local node, given
  /// the node's resources.
  ///
  /// \param local_resources: Total resources of the node.
  void InitLocalResources(const NodeResources &local_resources);

  /// Initialize the instances of a given resource given the resource's total capacity.
  /// If unit_instances is true we split the resources in unit-size instances. For
  /// example, if total = 10, then we create 10 instances, each with caoacity 1.
  /// Otherwise, we create a single instance of capacity equal to the resource's capacity.
  ///
  /// \param total: Total resource capacity.
  /// \param unit_instances: If true, we split the resource in unit-size instances.
  /// If false, we create a single instance of capacity "total".
  /// \param instance_list: The list of capacities this resource instances.
  void InitResourceInstances(FixedPoint total, bool unit_instances,
                             ResourceInstanceCapacities *instance_list);

  bool NodeAlive(int64_t node_id) const;

  /// Init the information about which resources are unit_instance.
  void InitResourceUnitInstanceInfo();

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

  /// Add a new node or overwrite the resources of an existing node.
  ///
  /// \param node_id: Node ID.
  /// \param node_resources: Up to date total and available resources of the node.
  void AddOrUpdateNode(int64_t node_id, const NodeResources &node_resources);

  /// Remove node from the cluster data structure. This happens
  /// when a node fails or it is removed from the cluster.
  ///
  /// \param node_id ID of the node to be removed.
  bool RemoveNode(int64_t node_id);

  /// Check whether a resource request can be scheduled given a node.
  ///
  ///  \param resource_request: Resource request to be scheduled.
  ///  \param node_id: ID of the node.
  ///  \param resources: Node's resources. (Note: Technically, this is
  ///     redundant, as we can get the node's resources from nodes_
  ///     using node_id. However, typically both node_id and resources
  ///     are available when we call this function, and this way we avoid
  ///     a map find call which could be expensive.)
  ///
  ///  \return: Whether the request can be scheduled.
  bool IsSchedulable(const ResourceRequest &resource_request, int64_t node_id,
                     const NodeResources &resources) const;

  /// Return resources associated to the given node_id in ret_resources.
  /// If node_id not found, return false; otherwise return true.
  bool GetNodeResources(int64_t node_id, NodeResources *ret_resources) const;

  /// Increase the available capacities of the instances of a given resource.
  ///
  /// \param available A list of available capacities for resource's instances.
  /// \param resource_instances List of the resource instances being updated.
  ///
  /// \return Overflow capacities of "resource_instances" after adding instance
  /// capacities in "available", i.e.,
  /// min(available + resource_instances.available, resource_instances.total)
  std::vector<FixedPoint> AddAvailableResourceInstances(
      std::vector<FixedPoint> available, ResourceInstanceCapacities *resource_instances);

  /// Decrease the available capacities of the instances of a given resource.
  ///
  /// \param free A list of capacities for resource's instances to be freed.
  /// \param resource_instances List of the resource instances being updated.
  /// \param allow_going_negative Allow the values to go negative (disable underflow).
  /// \return Underflow of "resource_instances" after subtracting instance
  /// capacities in "available", i.e.,.
  /// max(available - reasource_instances.available, 0)
  std::vector<FixedPoint> SubtractAvailableResourceInstances(
      std::vector<FixedPoint> available, ResourceInstanceCapacities *resource_instances,
      bool allow_going_negative = false);

  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  absl::flat_hash_map<int64_t, Node> nodes_;
  /// Identifier of local node.
  int64_t local_node_id_;
  /// The scheduling policy to use.
  std::unique_ptr<raylet_scheduling_policy::SchedulingPolicy> scheduling_policy_;
  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
  /// Resources of local node.
  NodeResourceInstances local_resources_;
  /// Keep the mapping between node and resource IDs in string representation
  /// to integer representation. Used for improving map performance.
  StringIdMap string_to_int_map_;
  /// Cached resources, used to compare with newest one in light heartbeat mode.
  std::unique_ptr<NodeResources> last_report_resources_;
  /// Function to get used object store memory.
  std::function<int64_t(void)> get_used_object_store_memory_;
  /// Function to get whether the pull manager is at capacity.
  std::function<bool(void)> get_pull_manager_at_capacity_;

  /// Gcs client. It's not owned by this class.
  gcs::GcsClient *gcs_client_;

  // Specify predefine resources that consists of unit-size instances.
  std::unordered_set<int64_t> predefined_unit_instance_resources_{};

  // Specify custom resources that consists of unit-size instances.
  std::unordered_set<int64_t> custom_unit_instance_resources_{};

  friend class ClusterResourceSchedulerTest;
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingDeleteClusterNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingModifyClusterNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingUpdateAvailableResourcesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingAddOrUpdateNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingResourceRequestTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingUpdateTotalResourcesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest,
              UpdateLocalAvailableResourcesFromResourceInstancesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, ResourceUsageReportTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, ObjectStoreMemoryUsageTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, AvailableResourceInstancesOpsTest);
};

}  // end namespace ray
