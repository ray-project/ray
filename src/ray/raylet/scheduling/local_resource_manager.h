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
#include "ray/common/bundle_spec.h"
#include "ray/common/ray_syncer/ray_syncer.h"
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
class LocalResourceManager : public syncer::ReporterInterface {
 public:
  LocalResourceManager(
      scheduling::NodeID local_node_id,
      const NodeResources &node_resources,
      std::function<int64_t(void)> get_used_object_store_memory,
      std::function<bool(void)> get_pull_manager_at_capacity,
      std::function<void(const NodeResources &)> resource_change_subscriber);

  scheduling::NodeID GetNodeId() const { return local_node_id_; }

  /// Add a local resource that is available.
  ///
  /// \param resource_id: Resource which we want to update.
  /// \param resource_total: New capacity of the resource.
  void AddLocalResourceInstances(scheduling::ResourceID resource_id,
                                 const std::vector<FixedPoint> &instances);

  /// Delete a given resource from the local node.
  ///
  /// \param resource_id: Resource we want to delete
  void DeleteLocalResource(scheduling::ResourceID resource_id);

  /// Check whether the available resources are empty.
  ///
  /// \param resource_id: Resource which we want to check.
  bool IsAvailableResourceEmpty(scheduling::ResourceID resource_id) const;

  /// Return local resources.
  NodeResourceInstances GetLocalResources() const { return local_resources_; }

  /// Increase the available resource instances of this node.
  ///
  /// \param resource_id id of the resource.
  /// \param instances instances to be added to available resources.
  ///
  /// \return Overflow capacities of resource instances after adding the resources.
  std::vector<double> AddResourceInstances(scheduling::ResourceID resource_id,
                                           const std::vector<double> &cpu_instances);

  /// Decrease the available resource instances of this node.
  ///
  /// \param resource_id id of the resource.
  /// \param instances instances to be removed from available resources.
  /// \param allow_going_negative Allow the values to go negative (disable underflow).
  ///
  /// \return Underflow capacities of reousrce instances after subtracting the resources.
  std::vector<double> SubtractResourceInstances(scheduling::ResourceID resource_id,
                                                const std::vector<double> &instances,
                                                bool allow_going_negative = false);

  /// Subtract the resources required by a given resource request (resource_request) from
  /// the local node. This function also updates the local node resources at the instance
  /// granularity.
  ///
  /// \param task_resources Task for which we allocate resources.
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

  /// Replace the local resources by the provided value.
  ///
  /// \param replacement: the new value.
  void ResetLastReportResourceUsage(const NodeResources &replacement);

  /// Check whether the specific resource exists or not in local node.
  ///
  /// \param resource_name: the specific resource name.
  ///
  /// \return true, if exist. otherwise, false.
  bool ResourcesExist(scheduling::ResourceID resource_id) const;

  std::optional<syncer::RaySyncMessage> CreateSyncMessage(
      int64_t after_version, syncer::MessageType message_type) const override;

  /// Record the metrics.
  void RecordMetrics() const;

 private:
  struct ResourceUsage {
    double avail;
    double used;
    // TODO(sang): Add PG avail & PG used.
  };

  /// Return the resource usage map for each resource.
  absl::flat_hash_map<std::string, LocalResourceManager::ResourceUsage>
  GetResourceUsageMap() const;

  /// Notify the subscriber that the local resouces has changed.
  void OnResourceChanged();

  /// Increase the available capacities of the instances of a given resource.
  ///
  /// \param available A list of available capacities for resource's instances.
  /// \param local_total Local total resource instances.
  /// \param local_available Local available resource instances being updated.
  ///
  /// \return Overflow capacities of "local_available" after adding instance
  /// capacities in "available", i.e.,
  /// min(available + local_available, local_total)
  std::vector<FixedPoint> AddAvailableResourceInstances(
      const std::vector<FixedPoint> &available,
      const std::vector<FixedPoint> &local_total,
      std::vector<FixedPoint> &local_available) const;

  /// Decrease the available capacities of the instances of a given resource.
  ///
  /// \param free A list of capacities for resource's instances to be freed.
  /// \param local_available Local available resource instances being updated.
  /// \param allow_going_negative Allow the values to go negative (disable underflow).
  ///
  /// \return Underflow of "local_available" after subtracting instance
  /// capacities in "available", i.e.,.
  /// max(available - reasource_instances.available, 0)
  std::vector<FixedPoint> SubtractAvailableResourceInstances(
      const std::vector<FixedPoint> &available,
      std::vector<FixedPoint> &local_available,
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

  bool AllocateResourceInstances(FixedPoint demand,
                                 std::vector<FixedPoint> &available,
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

  void UpdateAvailableObjectStoreMemResource();

  /// Identifier of local node.
  scheduling::NodeID local_node_id_;
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

  // Version of this resource. It will incr by one whenever the state changed.
  int64_t version_ = 0;

  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingUpdateTotalResourcesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, AvailableResourceInstancesOpsTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstancesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstancesAllocationFailureTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstancesTest2);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstanceWithHardRequestTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstanceWithoutCpuUnitTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, CustomResourceInstanceTest);

  friend class LocalResourceManagerTest;
  FRIEND_TEST(LocalResourceManagerTest, BasicGetResourceUsageMapTest);
};

}  // end namespace ray
