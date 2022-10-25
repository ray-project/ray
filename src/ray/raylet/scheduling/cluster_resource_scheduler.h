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
#include "ray/raylet/scheduling/cluster_resource_data.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/internal.h"
#include "ray/raylet/scheduling/local_resource_manager.h"
#include "ray/raylet/scheduling/policy/composite_scheduling_policy.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

using raylet_scheduling_policy::SchedulingOptions;
using raylet_scheduling_policy::SchedulingResult;
using rpc::HeartbeatTableData;

/// Class encapsulating the cluster resources and the logic to assign
/// tasks to nodes based on the task's constraints and the available
/// resources at those nodes.
class ClusterResourceScheduler {
 public:
  /// Constructor initializing the resources associated with the local node.
  ///
  /// \param local_node_id: ID of local node,
  /// \param local_node_resources: The total and the available resources associated
  /// with the local node.
  /// \param is_node_available_fn: Function to determine whether a node is available.
  /// \param is_local_node_with_raylet: Whether there is a raylet on the local node.
  ClusterResourceScheduler(scheduling::NodeID local_node_id,
                           const NodeResources &local_node_resources,
                           std::function<bool(scheduling::NodeID)> is_node_available_fn,
                           bool is_local_node_with_raylet = true);

  ClusterResourceScheduler(
      scheduling::NodeID local_node_id,
      const absl::flat_hash_map<std::string, double> &local_node_resources,
      std::function<bool(scheduling::NodeID)> is_node_available_fn,
      std::function<int64_t(void)> get_used_object_store_memory = nullptr,
      std::function<bool(void)> get_pull_manager_at_capacity = nullptr);

  /// Schedule the specified resources to the cluster nodes.
  ///
  /// \param resource_request_list The resource request list we're attempting to schedule.
  /// \param options: scheduling options.
  /// \param context: The context of current scheduling. Each policy can
  /// correspond to a different type of context.
  /// \return `SchedulingResult`, including the
  /// selected nodes if schedule successful, otherwise, it will return an empty vector and
  /// a flag to indicate whether this request can be retry or not.
  SchedulingResult Schedule(
      const std::vector<const ResourceRequest *> &resource_request_list,
      SchedulingOptions options);

  ///  Find a node in the cluster on which we can schedule a given resource request.
  ///  In hybrid mode, see `scheduling_policy.h` for a description of the policy.
  ///
  ///  \param task_spec: Task/Actor to be scheduled.
  ///  \param prioritize_local_node: true if we want to try out local node first.
  ///  \param exclude_local_node: true if we want to avoid local node. This will cancel
  ///  prioritize_local_node if set to true.
  ///  \param requires_object_store_memory: take object store memory usage as part of
  ///  scheduling decision.
  ///  \param is_infeasible[out]: It is set
  ///  true if the task is not schedulable because it is infeasible.
  ///
  ///  \return emptry string, if no node can schedule the current request; otherwise,
  ///          return the string name of a node that can schedule the resource request.
  scheduling::NodeID GetBestSchedulableNode(const TaskSpecification &task_spec,
                                            bool prioritize_local_node,
                                            bool exclude_local_node,
                                            bool requires_object_store_memory,
                                            bool *is_infeasible);

  /// Subtract the resources required by a given resource request (resource_request) from
  /// a given remote node.
  ///
  /// \param node_id Remote node whose resources we allocate.
  /// \param resource_request Task for which we allocate resources.
  /// \return True if remote node has enough resources to satisfy the resource request.
  /// False otherwise.
  bool AllocateRemoteTaskResources(
      scheduling::NodeID node_id,
      const absl::flat_hash_map<std::string, double> &resource_request);

  /// Return human-readable string for this scheduler state.
  std::string DebugString() const;

  /// Check whether a task request is schedulable on a given node. A node is
  /// schedulable if it has the available resources needed to execute the task.
  ///
  /// \param node_name Name of the node.
  /// \param shape The resource demand's shape.
  bool IsSchedulableOnNode(scheduling::NodeID node_id,
                           const absl::flat_hash_map<std::string, double> &shape,
                           bool requires_object_store_memory);

  LocalResourceManager &GetLocalResourceManager() { return *local_resource_manager_; }
  ClusterResourceManager &GetClusterResourceManager() {
    return *cluster_resource_manager_;
  }

  bool IsLocalNodeWithRaylet() { return is_local_node_with_raylet_; }

 private:
  void Init(const NodeResources &local_node_resources,
            std::function<int64_t(void)> get_used_object_store_memory,
            std::function<bool(void)> get_pull_manager_at_capacity);

  bool NodeAlive(scheduling::NodeID node_id) const;

  /// Decrease the available resources of a node when a resource request is
  /// scheduled on the given node.
  ///
  /// \param node_id: ID of node on which request is being scheduled.
  /// \param resource_request: resource request being scheduled.
  ///
  /// \return true, if resource_request can be indeed scheduled on the node,
  /// and false otherwise.
  bool SubtractRemoteNodeAvailableResources(scheduling::NodeID node_id,
                                            const ResourceRequest &resource_request);

  /// Check whether a resource request can be scheduled given a node.
  ///
  ///  \param resource_request: Resource request to be scheduled.
  ///  \param node_id: ID of the node.
  ///
  ///  \return: Whether the request can be scheduled.
  bool IsSchedulable(const ResourceRequest &resource_request,
                     scheduling::NodeID node_id) const;

  ///  Find a node in the cluster on which we can schedule a given resource request.
  ///  In hybrid mode, see `scheduling_policy.h` for a description of the policy.
  ///
  ///  \param resource_request: Task to be scheduled.
  ///  \param scheduling_strategy: Strategy about how to schedule this task.
  ///  \param actor_creation: True if this is an actor creation task.
  ///  \param force_spillback: True if we want to avoid local node.
  ///  \param violations: The number of soft constraint violations associated
  ///                     with the node returned by this function (assuming
  ///                     a node that can schedule resource_request is found).
  ///  \param is_infeasible[in]: It is set true if the task is not schedulable because it
  ///  is infeasible.
  ///
  ///  \return -1, if no node can schedule the current request; otherwise,
  ///          return the ID of a node that can schedule the resource request.
  scheduling::NodeID GetBestSchedulableNode(
      const ResourceRequest &resource_request,
      const rpc::SchedulingStrategy &scheduling_strategy,
      bool actor_creation,
      bool force_spillback,
      int64_t *violations,
      bool *is_infeasible);

  /// Similar to
  ///    int64_t GetBestSchedulableNode(...)
  /// but the return value is different:
  /// \return "", if no node can schedule the current request; otherwise,
  ///          return the ID in string format of a node that can schedule the
  //           resource request.
  scheduling::NodeID GetBestSchedulableNode(
      const absl::flat_hash_map<std::string, double> &resource_request,
      const rpc::SchedulingStrategy &scheduling_strategy,
      bool requires_object_store_memory,
      bool actor_creation,
      bool force_spillback,
      int64_t *violations,
      bool *is_infeasible);

  /// Judging whether it affinity with placement group bundle
  bool IsAffinityWithBundleSchedule(const rpc::SchedulingStrategy &scheduling_strategy);
  /// Identifier of local node.
  scheduling::NodeID local_node_id_;
  /// Callback to check if node is available.
  std::function<bool(scheduling::NodeID)> is_node_available_fn_;
  /// Resources of local node.
  std::unique_ptr<LocalResourceManager> local_resource_manager_;
  /// Resources of the entire cluster.
  std::unique_ptr<ClusterResourceManager> cluster_resource_manager_;
  /// The scheduling policy to use.
  std::unique_ptr<raylet_scheduling_policy::ISchedulingPolicy> scheduling_policy_;
  /// The bundle scheduling policy to use.
  std::unique_ptr<raylet_scheduling_policy::IBundleSchedulingPolicy>
      bundle_scheduling_policy_;
  /// Whether there is a raylet on the local node.
  bool is_local_node_with_raylet_ = true;

  friend class ClusterResourceSchedulerTest;
  FRIEND_TEST(ClusterResourceSchedulerTest, PopulatePredefinedResources);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingDeleteClusterNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingModifyClusterNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingUpdateAvailableResourcesTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingAddOrUpdateNodeTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, NodeAffinitySchedulingStrategyTest);
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
  FRIEND_TEST(ClusterResourceSchedulerTest, AffinityWithBundleScheduleTest);
};

}  // end namespace ray
