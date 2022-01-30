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
#include "ray/raylet/scheduling/fixed_point.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

/// Class manages the resources of the local node.
/// It is responsible for allocating/deallocating resources for (task) resource request;
/// it also supports creating a new resource or delete an existing resource.
/// Whenever the resouce changes, it notifies the subscriber of the change.
/// This class is not thread safe.
class LocalResourceManager {
 public:
  LocalResourceManager(
      int64_t local_node_id, StringIdMap &resource_name_to_id,
      const NodeResources &node_resources,
      std::function<int64_t(void)> get_used_object_store_memory,
      std::function<bool(void)> get_pull_manager_at_capacity,
      std::function<void(const NodeResources &)> resource_change_subscriber);

  int64_t GetNodeId() const { return local_node_id_; }

  /// Add a local resource that is available.
  ///
  /// \param resource_name: Resource which we want to update.
  /// \param resource_total: New capacity of the resource.
  void AddLocalResourceInstances(const std::string &resource_name,
                                 const std::vector<FixedPoint> &instances);

  /// Delete a given resource from the local node.
  ///
  /// \param resource_name: Resource we want to delete
  void DeleteLocalResource(const std::string &resource_name);

  /// Check whether the available resources are empty.
  ///
  /// \param resource_name: Resource which we want to check.
  bool IsAvailableResourceEmpty(const std::string &resource_name);

  /// Return local resources.
  NodeResourceInstances GetLocalResources() const { return local_resources_; }

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

  void ReleaseWorkerResources(std::shared_ptr<TaskResourceInstances> task_allocation);

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending resource usage of raylet to gcs. In particular, this should fill in
  /// resources_available and resources_total.
  ///
  /// \param Output parameter. `resources_available` and `resources_total` are the only
  /// fields used.
  void FillResourceUsage(rpc::ResourcesData &resources_data);

  /// Populate a UpdateResourcesRequest. This is inteneded to update the
  /// resource totals on a node when a custom resource is created or deleted
  /// (e.g. during the placement group lifecycle).
  ///
  /// \param resource_map_filter When returning the resource map, the returned result will
  /// only contain the keys in the filter. Note that only the key of the map is used.
  /// \return The total resource capacity of the node.
  ray::gcs::NodeResourceInfoAccessor::ResourceMap GetResourceTotals(
      const absl::flat_hash_map<std::string, double> &resource_map_filter) const;

  double GetLocalAvailableCpus() const;

  /// Return human-readable string for this scheduler state.
  std::string DebugString() const;

  /// Get the number of cpus on this node.
  uint64_t GetNumCpus() const;

  /// Serialize task resource instances to json string.
  ///
  /// \param task_allocation Allocated resource instances for a task.
  /// \return The task resource instances json string
  std::string SerializedTaskResourceInstances(
      std::shared_ptr<TaskResourceInstances> task_allocation) const;

  /// Replace the local resources by the provided value.
  ///
  /// \param replacement: the new value.
  void ResetLastReportResourceUsage(const SchedulingResources &replacement);

  /// Check whether the specific resource exists or not in local node.
  ///
  /// \param resource_name: the specific resource name.
  ///
  /// \return true, if exist. otherwise, false.
  bool ResourcesExist(const std::string &resource_name);

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

  /// Init the information about which resources are unit_instance.
  void InitResourceUnitInstanceInfo();

  /// Notify the subscriber that the local resouces has changed.
  void OnResourceChanged();

  /// Increase the available capacities of the instances of a given resource.
  ///
  /// \param available A list of available capacities for resource's instances.
  /// \param resource_instances List of the resource instances being updated.
  ///
  /// \return Overflow capacities of "resource_instances" after adding instance
  /// capacities in "available", i.e.,
  /// min(available + resource_instances.available, resource_instances.total)
  std::vector<FixedPoint> AddAvailableResourceInstances(
      std::vector<FixedPoint> available,
      ResourceInstanceCapacities *resource_instances) const;

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
      bool allow_going_negative = false) const;

  /// Allocate enough capacity across the instances of a resource to satisfy "demand".
  /// If resource has multiple unit-capacity instances, we consider two cases.
  ///
  /// 1) If the constraint is hard, allocate full unit-capacity instances until
  /// demand becomes fractional, and then satisfy the fractional demand using the
  /// instance with the smallest available capacity that can satisfy the fractional
  /// demand. For example, assume a resource conisting of 4 instances, with available
  /// capacities: (1., 1., .7, 0.5) and deman of 1.2. Then we allocate one full
  /// instance and then allocate 0.2 of the 0.5 instance (as this is the instance
  /// with the smalest available capacity that can satisfy the remaining demand of 0.2).
  /// As a result remaining available capacities will be (0., 1., .7, .3).
  /// Thus, if the constraint is hard, we will allocate a bunch of full instances and
  /// at most a fractional instance.
  ///
  /// 2) If the constraint is soft, we can allocate multiple fractional resources,
  /// and even overallocate the resource. For example, in the previous case, if we
  /// have a demand of 1.8, we can allocate one full instance, the 0.5 instance, and
  /// 0.3 from the 0.7 instance. Furthermore, if the demand is 3.5, then we allocate
  /// all instances, and return success (true), despite the fact that the total
  /// available capacity of the rwsource is 3.2 (= 1. + 1. + .7 + .5), which is less
  /// than the demand, 3.5. In this case, the remaining available resource is
  /// (0., 0., 0., 0.)
  ///
  /// \param demand: The resource amount to be allocated.
  /// \param available: List of available capacities of the instances of the resource.
  /// \param allocation: List of instance capacities allocated to satisfy the demand.
  /// This is a return parameter.
  ///
  /// \return true, if allocation successful. In this case, the sum of the elements in
  /// "allocation" is equal to "demand".

  bool AllocateResourceInstances(FixedPoint demand, std::vector<FixedPoint> &available,
                                 std::vector<FixedPoint> *allocation) const;

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

  /// Identifier of local node.
  int64_t local_node_id_;
  /// Keep the mapping between node and resource IDs in string representation
  /// to integer representation. Used for improving map performance.
  StringIdMap &resource_name_to_id_;
  /// Resources of local node.
  NodeResourceInstances local_resources_;
  /// Cached resources, used to compare with newest one in light heartbeat mode.
  std::unique_ptr<NodeResources> last_report_resources_;
  /// Function to get used object store memory.
  std::function<int64_t(void)> get_used_object_store_memory_;
  /// Function to get whether the pull manager is at capacity.
  std::function<bool(void)> get_pull_manager_at_capacity_;
  /// Subscribes to resource changes.
  std::function<void(const NodeResources &)> resource_change_subscriber_;

  // Specify predefine resources that consists of unit-size instances.
  std::unordered_set<int64_t> predefined_unit_instance_resources_{};

  // Specify custom resources that consists of unit-size instances.
  std::unordered_set<int64_t> custom_unit_instance_resources_{};

  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingUpdateTotalResourcesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, AvailableResourceInstancesOpsTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstancesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstancesAllocationFailureTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstancesTest2);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstanceWithHardRequestTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstanceWithoutCpuUnitTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, CustomResourceInstanceTest);
};

int GetPredefinedResourceIndex(const std::string &resource_name);

}  // end namespace ray
