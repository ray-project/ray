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

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/common/scheduling/fixed_point.h"
#include "ray/ray_syncer/ray_syncer.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

/// Encapsulates non-resource artifacts that evidence work when present.
enum WorkFootprint {
  NODE_WORKERS = 1,
};

// Represents artifacts of a node that can be busy or idle.
// Resources are schedulable, such as gpu or cpu.
// WorkFootprints are not, such as leased workers on a node.
using WorkArtifact = std::variant<WorkFootprint, scheduling::ResourceID>;

using rpc::autoscaler::DrainNodeReason;

/// Class manages the resources of the local node.
/// It is responsible for allocating/deallocating resources for (task) resource request;
/// it also supports creating a new resource or delete an existing resource.
/// Whenever the resource changes, it notifies the subscriber of the change.
/// This class is not thread safe.
class LocalResourceManager : public syncer::ReporterInterface {
 public:
  LocalResourceManager(
      scheduling::NodeID local_node_id,
      const NodeResources &node_resources,
      std::function<int64_t(void)> get_used_object_store_memory,
      std::function<bool(void)> get_pull_manager_at_capacity,
      std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
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
  void AddResourceInstances(scheduling::ResourceID resource_id,
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

  // Removes idle time for a WorkFootprint, thereby marking it busy.
  void SetBusyFootprint(WorkFootprint item);
  // Sets the idle time for a WorkFootprint to now.
  void SetIdleFootprint(WorkFootprint item);

  double GetLocalAvailableCpus() const;

  /// Return human-readable string for this scheduler state.
  std::string DebugString() const;

  /// Get the number of cpus on this node.
  uint64_t GetNumCpus() const;

  /// Check whether the specific resource exists or not in local node.
  ///
  /// \param resource_name: the specific resource name.
  ///
  /// \return true, if exist. otherwise, false.
  bool ResourcesExist(scheduling::ResourceID resource_id) const;

  std::optional<syncer::RaySyncMessage> CreateSyncMessage(
      int64_t after_version, syncer::MessageType message_type) const override;

  void PopulateResourceViewSyncMessage(
      syncer::ResourceViewSyncMessage &resource_view_sync_message) const;

  /// Record the metrics.
  void RecordMetrics() const;

  bool IsLocalNodeIdle() const { return GetResourceIdleTime() != absl::nullopt; }

  /// Change the local node to the draining state.
  /// After that, no new tasks can be scheduled onto the local node.
  void SetLocalNodeDraining(const rpc::DrainRayletRequest &drain_request);

  bool IsLocalNodeDraining() const { return drain_request_.has_value(); }

  /// Get the local drain request.
  std::optional<rpc::DrainRayletRequest> GetLocalDrainRequest() const {
    return drain_request_;
  }

  /// Generate node death info from existing drain request.
  rpc::NodeDeathInfo DeathInfoFromDrainRequest();

 private:
  struct ResourceUsage {
    double avail;
    double used;
    // TODO(sang): Add PG avail & PG used.
  };

  /// Return the resource usage map for each resource.
  absl::flat_hash_map<std::string, LocalResourceManager::ResourceUsage>
  GetResourceUsageMap() const;

  /// Notify the subscriber that the local resouces or state has changed.
  void OnResourceOrStateChanged();

  /// Convert local resources to NodeResources.
  NodeResources ToNodeResources() const;

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
  /// \param record_idle_resource: Whether to record the idle resource. This is false
  ///   when the resource was allocated partially so its idle state is actually not
  ///   affected.
  void FreeTaskResourceInstances(std::shared_ptr<TaskResourceInstances> task_allocation,
                                 bool record_idle_resource = true);

  void UpdateAvailableObjectStoreMemResource();

  void SetResourceIdle(const scheduling::ResourceID &resource_id);

  void SetResourceNonIdle(const scheduling::ResourceID &resource_id);

  std::optional<absl::Time> GetResourceIdleTime() const;

  /// Get the draining deadline if node is in draining state.
  ///
  /// \return The draining deadline if node is in draining state, otherwise -1.
  int64_t GetDrainingDeadline() const {
    return drain_request_.has_value() ? drain_request_->deadline_timestamp_ms() : -1;
  }
  /// Identifier of local node.
  scheduling::NodeID local_node_id_;
  /// Resources of local node.
  NodeResourceInstances local_resources_;

  /// A map storing when the resource was last idle.
  absl::flat_hash_map<WorkArtifact, std::optional<absl::Time>> last_idle_times_;
  /// Function to get used object store memory.
  std::function<int64_t(void)> get_used_object_store_memory_;
  /// Function to get whether the pull manager is at capacity.
  std::function<bool(void)> get_pull_manager_at_capacity_;
  /// Function to shutdown the raylet gracefully.
  std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully_;

  /// Subscribes to resource changes.
  std::function<void(const NodeResources &)> resource_change_subscriber_;

  // Version of this resource. It will incr by one whenever the state changed.
  int64_t version_ = 0;

  /// The draining request this node received.
  std::optional<rpc::DrainRayletRequest> drain_request_;

  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingUpdateTotalResourcesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, AvailableResourceInstancesOpsTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstancesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstancesAllocationFailureTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstancesTest2);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstanceWithHardRequestTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskResourceInstanceWithoutCpuUnitTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, CustomResourceInstanceTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, TaskGPUResourceInstancesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, ObjectStoreMemoryUsageTest);

  friend class LocalResourceManagerTest;
  FRIEND_TEST(LocalResourceManagerTest, BasicGetResourceUsageMapTest);
  FRIEND_TEST(LocalResourceManagerTest, IdleResourceTimeTest);
  FRIEND_TEST(LocalResourceManagerTest, ObjectStoreMemoryDrainingTest);
};

}  // end namespace ray
