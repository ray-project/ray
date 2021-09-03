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

#include <iostream>
#include <sstream>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/accessor.h"
#include "ray/raylet/scheduling/cluster_resource_data.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler_interface.h"
#include "ray/raylet/scheduling/fixed_point.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

using rpc::HeartbeatTableData;

/// Class encapsulating the cluster resources and the logic to assign
/// tasks to nodes based on the task's constraints and the available
/// resources at those nodes.
class ClusterResourceScheduler : public ClusterResourceSchedulerInterface {
 public:
  ClusterResourceScheduler(void);
  /// Constructor initializing the resources associated with the local node.
  ///
  /// \param local_node_id: ID of local node,
  /// \param local_node_resources: The total and the available resources associated
  /// with the local node.
  ClusterResourceScheduler(int64_t local_node_id,
                           const NodeResources &local_node_resources);
  ClusterResourceScheduler(
      const std::string &local_node_id,
      const std::unordered_map<std::string, double> &local_node_resources,
      std::function<int64_t(void)> get_used_object_store_memory = nullptr,
      std::function<bool(void)> get_pull_manager_at_capacity = nullptr);

  // Mapping from predefined resource indexes to resource strings
  std::string GetResourceNameFromIndex(int64_t res_idx);

  /// Add a new node or overwrite the resources of an existing node.
  ///
  /// \param node_id: Node ID.
  /// \param node_resources: Up to date total and available resources of the node.
  void AddOrUpdateNode(int64_t node_id, const NodeResources &node_resources);
  void AddOrUpdateNode(
      const std::string &node_id,
      const std::unordered_map<std::string, double> &resource_map_total,
      const std::unordered_map<std::string, double> &resource_map_available);

  /// Update node resources. This hanppens when a node resource usage udpated.
  ///
  /// \param node_id_string ID of the node which resoruces need to be udpated.
  /// \param resource_data The node resource data.
  bool UpdateNode(const std::string &node_id_string,
                  const rpc::ResourcesData &resource_data) override;

  /// Remove node from the cluster data structure. This happens
  /// when a node fails or it is removed from the cluster.
  ///
  /// \param ID of the node to be removed.
  bool RemoveNode(int64_t node_id);
  bool RemoveNode(const std::string &node_id_string) override;

  /// Check whether a resource request is feasible on a given node. A node is
  /// feasible if it has the total resources needed to eventually execute the
  /// task, even if those resources are currently allocated.
  ///
  /// \param resource_request Resource request to be scheduled.
  /// \param resources Node's resources.
  bool IsFeasible(const ResourceRequest &resource_request,
                  const NodeResources &resources) const;

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
  ///  \return: -1, if the request cannot be scheduled. This happens when at
  ///           least a hard constraints is violated.
  ///           >= 0, the number soft constraint violations. If 0, no
  ///           constraint is violated.
  int64_t IsSchedulable(const ResourceRequest &resource_request, int64_t node_id,
                        const NodeResources &resources) const;

  ///  Find a node in the cluster on which we can schedule a given resource request.
  ///
  ///  Ignoring soft constraints, this policy prioritizes nodes in the
  ///  following order:
  ///
  ///  1. Local node if resources available.
  ///  2. Any remote node if resources available.
  ///  3. If the local node is not feasible, any remote node if feasible.
  ///
  ///  If soft constraints are specified, then this policy will prioritize:
  ///  1. Local node if resources available and does not violate soft
  ///     constraints.
  ///  2. Any remote node if resources available and does not violate soft
  ///     constraints.
  ///  3. Out of all the nodes, including the local node, pick the one that
  ///     has resources available and violates the fewest soft constraints.
  ///  4. If the local node is not feasible, any remote node if feasible.
  ///
  ///  If no node can meet any of these, returns -1, in which case the caller
  ///  should queue the task and try again once resource availability has been
  ///  updated.
  ///
  ///  \param resource_request: Task to be scheduled.
  ///  \param actor_creation: True if this is an actor creation task.
  ///  \param force_spillback For non-actor creation requests, pick a remote
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
  int64_t GetBestSchedulableNodeSimpleBinPack(const ResourceRequest &resource_request,
                                              bool actor_creation, bool force_spillback,
                                              int64_t *violations, bool *is_infeasible);

  ///  Find a node in the cluster on which we can schedule a given resource request.
  ///  In hybrid mode, see `scheduling_policy.h` for a description of the policy.
  ///  In legacy mode, see `GetBestSchedulableNodeLegacy` for a description of the policy.
  ///
  ///  \param resource_request: Task to be scheduled.
  ///  \param actor_creation: True if this is an actor creation task.
  ///  \param force_spillback For non-actor creation requests, pick a remote
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
                                 bool actor_creation, bool force_spillback,
                                 int64_t *violations, bool *is_infeasible);

  /// Similar to
  ///    int64_t GetBestSchedulableNode(const ResourceRequest &resource_request, int64_t
  ///    *violations)
  /// but the return value is different:
  /// \return "", if no node can schedule the current request; otherwise,
  ///          return the ID in string format of a node that can schedule the
  //           resource request.
  std::string GetBestSchedulableNode(
      const std::unordered_map<std::string, double> &resource_request,
      bool requires_object_store_memory, bool actor_creation, bool force_spillback,
      int64_t *violations, bool *is_infeasible);

  /// Return resources associated to the given node_id in ret_resources.
  /// If node_id not found, return false; otherwise return true.
  bool GetNodeResources(int64_t node_id, NodeResources *ret_resources) const;

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
  NodeResourceInstances GetLocalResources() { return local_resources_; };

  /// Return local resources in human-readable string form.
  std::string GetLocalResourceViewString() const override;

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
                                 std::vector<FixedPoint> *allocation);

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
      const std::unordered_map<std::string, double> &task_resources,
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
      const std::unordered_map<std::string, double> &task_resources);

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

  /// \return The total resource capacity of the node.
  ray::gcs::NodeResourceInfoAccessor::ResourceMap GetResourceTotals() const override;

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

 private:
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

  /// Use the hybrid spillback policy.
  const bool hybrid_spillback_;
  /// The threshold at which to switch from packing to spreading.
  const float spread_threshold_;
  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  absl::flat_hash_map<int64_t, Node> nodes_;
  /// Identifier of local node.
  int64_t local_node_id_;
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

  // Specify predefine resources that consists of unit-size instances.
  std::unordered_set<int64_t> predefined_unit_instance_resources_{};

  // Specify custom resources that consists of unit-size instances.
  std::unordered_set<int64_t> custom_unit_instance_resources_{};
};

}  // end namespace ray
