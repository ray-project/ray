#ifndef RAY_COMMON_SCHEDULING_SCHEDULING_H
#define RAY_COMMON_SCHEDULING_SCHEDULING_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/util/logging.h"

#include <iostream>
#include <sstream>
#include <vector>

/// List of predefined resources.
enum PredefinedResources { CPU, MEM, GPU, TPU, PredefinedResources_MAX };
// Specify resources that consists of unit-size instances.
static std::unordered_set<int64_t> UnitInstanceResources{CPU, GPU, TPU};

// Helper function to compare two vectors with double values.
bool EqualVectors(const std::vector<double> &v1, const std::vector<double> &v2);

struct ResourceCapacity {
  double total;
  double available;
};

/// Capacities of each instance of a resource.
struct ResourceInstanceCapacities {
  std::vector<double> total;
  std::vector<double> available;
};

struct ResourceRequest {
  /// Amount of resource being requested.
  double demand;
  /// Specify whether the request is soft or hard.
  /// If hard, the entire request is denied if the demand exceeds the resource
  /// availability. Otherwise, the request can be still be granted.
  /// Prefernces are given to the nodes with the lowest number of violations.
  bool soft;
};

/// Resource request, including resource ID. This is used for custom resources.
struct ResourceRequestWithId : ResourceRequest {
  /// Resource ID.
  int64_t id;
};

class TaskRequest {
 public:
  /// List of predefined resources required by the task.
  std::vector<ResourceRequest> predefined_resources;
  /// List of custom resources required by the tasl.
  std::vector<ResourceRequestWithId> custom_resources;
  /// List of placement hints. A placement hint is a node on which
  /// we desire to run this task. This is a soft constraint in that
  /// the task will run on a different node in the cluster, if none of the
  /// nodes in this list can schedule this task.
  absl::flat_hash_set<int64_t> placement_hints;
  /// Returns human-readable string for this task request.
  std::string DebugString();
};

// Task request specifying instances for each resource.
class TaskResourceInstances {
 public:
  /// The list of instances of each predifined resource allocated to a task.
  std::vector<std::vector<double>> predefined_resources;
  /// The list of instances of each custom resource allocated to a task.
  absl::flat_hash_map<int64_t, std::vector<double>> custom_resources;
  bool operator==(const TaskResourceInstances &other);
  /// Get CPU instances only.
  std::vector<double> GetCPUInstances() { return this->predefined_resources[CPU]; };
  /// Returns human-readable string for these resources.
  std::string DebugString();
};

/// Total and available capacities of each resource of a node.
class NodeResources {
 public:
  /// Available and total capacities for predefined resources.
  std::vector<ResourceCapacity> predefined_resources;
  /// Map containing custom resources. The key of each entry represents the
  /// custom resource ID.
  absl::flat_hash_map<int64_t, ResourceCapacity> custom_resources;
  /// Returns if this equals another node resources.
  bool operator==(const NodeResources &other);
  /// Returns human-readable string for these resources.
  std::string DebugString();
};

/// Total and available capacities of each resource instance.
/// This is used to describe the resources of the local node.
class NodeResourceInstances {
 public:
  /// Available and total capacities for each instance of a predefined resource.
  std::vector<ResourceInstanceCapacities> predefined_resources;
  /// Map containing custom resources. The key of each entry represents the
  /// custom resource ID.
  absl::flat_hash_map<int64_t, ResourceInstanceCapacities> custom_resources;
  /// Extract available resource instances.
  TaskResourceInstances GetAvailableResourceInstances();
  /// Returns if this equals another node resources.
  bool operator==(const NodeResourceInstances &other);
  /// Returns human-readable string for these resources.
  std::string DebugString();
};

/// Class encapsulating the cluster resources and the logic to assign
/// tasks to nodes based on the task's constraints and the available
/// resources at those nodes.
class ClusterResourceScheduler {
  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  absl::flat_hash_map<int64_t, NodeResources> nodes_;
  /// Identifier of local node.
  int64_t local_node_id_;
  /// Resources of local node.
  NodeResourceInstances local_resources_;
  /// Keep the mapping between node and resource IDs in string representation
  /// to integer representation. Used for improving map performance.
  StringIdMap string_to_int_map_;

  /// Set predefined resources.
  ///
  /// \param[in] new_resources: New predefined resources.
  /// \param[out] old_resources: Predefined resources to be updated.
  void SetPredefinedResources(const NodeResources &new_resources,
                              NodeResources *old_resources);
  /// Set custom resources.
  ///
  /// \param[in] new_resources: New custom resources.
  /// \param[out] old_resources: Custom resources to be updated.
  void SetCustomResources(
      const absl::flat_hash_map<int64_t, ResourceCapacity> &new_custom_resources,
      absl::flat_hash_map<int64_t, ResourceCapacity> *old_custom_resources);

 public:
  ClusterResourceScheduler(void){};

  /// Constructor initializing the resources associated with the local node.
  ///
  /// \param local_node_id: ID of local node,
  /// \param local_node_resources: The total and the available resources associated
  /// with the local node.
  ClusterResourceScheduler(int64_t local_node_id,
                           const NodeResources &local_node_resources);
  ClusterResourceScheduler(
      const std::string &local_node_id,
      const std::unordered_map<std::string, double> &local_node_resources);

  /// Add a new node or overwrite the resources of an existing node.
  ///
  /// \param node_id: Node ID.
  /// \param node_resources: Up to date total and available resources of the node.
  void AddOrUpdateNode(int64_t node_id, const NodeResources &node_resources);
  void AddOrUpdateNode(
      const std::string &node_id,
      const std::unordered_map<std::string, double> &resource_map_total,
      const std::unordered_map<std::string, double> &resource_map_available);

  /// Remove node from the cluster data structure. This happens
  /// when a node fails or it is removed from the cluster.
  ///
  /// \param ID of the node to be removed.
  bool RemoveNode(int64_t node_id);

  /// Check whether a task request can be scheduled given a node.
  ///
  ///  \param task_req: Task request to be scheduled.
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
  int64_t IsSchedulable(const TaskRequest &task_req, int64_t node_id,
                        const NodeResources &resources);

  ///  Find a node in the cluster on which we can schedule a given task request.
  ///
  ///  First, this function checks whether the local node can schedule
  ///  the request without violating any constraints. If yes, it returns the
  ///  ID of the local node.
  ///
  ///  If not, this function checks whether there is another node in the cluster
  ///  that satisfies all request's constraints (both soft and hard).
  ///
  ///  If no such node exists, the function checks whether there are nodes
  ///  that satisfy all the request's hard constraints, but might violate some
  ///  soft constraints. Among these nodes, it returns a node which violates
  ///  the least number of soft constraints.
  ///
  ///  Finally, if no such node exists, return -1.
  ///
  ///  \param task_request: Task to be scheduled.
  ///  \param violations: The number of soft constraint violations associated
  ///                     with the node returned by this function (assuming
  ///                     a node that can schedule task_req is found).
  ///
  ///  \return -1, if no node can schedule the current request; otherwise,
  ///          return the ID of a node that can schedule the task request.
  int64_t GetBestSchedulableNode(const TaskRequest &task_request, int64_t *violations);

  /// Similar to
  ///    int64_t GetBestSchedulableNode(const TaskRequest &task_request, int64_t
  ///    *violations)
  /// but the return value is different:
  /// \return "", if no node can schedule the current request; otherwise,
  ///          return the ID in string format of a node that can schedule the
  //           task request.
  std::string GetBestSchedulableNode(
      const std::unordered_map<std::string, double> &task_request, int64_t *violations);

  /// Decrease the available resources of a node when a task request is
  /// scheduled on the given node.
  ///
  /// \param node_id: ID of node on which request is being scheduled.
  /// \param task_req: task request being scheduled.
  ///
  /// \return true, if task_req can be indeed scheduled on the node,
  /// and false otherwise.
  bool SubtractNodeAvailableResources(int64_t node_id, const TaskRequest &task_request);
  bool SubtractNodeAvailableResources(
      const std::string &node_id,
      const std::unordered_map<std::string, double> &task_request);

  /// Increase available resources of a node when a worker has finished
  /// a task.
  ///
  /// \param node_id: ID of node on which request is being scheduled.
  /// \param task_request: resource requests of the task finishing execution.
  ///
  /// \return true, if task_req can be indeed scheduled on the node,
  /// and false otherwise.
  bool AddNodeAvailableResources(int64_t node_id, const TaskRequest &task_request);
  bool AddNodeAvailableResources(
      const std::string &node_id,
      const std::unordered_map<std::string, double> &task_request);

  /// Return resources associated to the given node_id in ret_resources.
  /// If node_id not found, return false; otherwise return true.
  bool GetNodeResources(int64_t node_id, NodeResources *ret_resources);

  /// Get number of nodes in the cluster.
  int64_t NumNodes();

  /// Convert a map of resources to a TaskRequest data structure.
  void ResourceMapToTaskRequest(
      const std::unordered_map<std::string, double> &resource_map,
      TaskRequest *task_request);

  /// Convert a map of resources to a TaskRequest data structure.
  void ResourceMapToNodeResources(
      const std::unordered_map<std::string, double> &resource_map_total,
      const std::unordered_map<std::string, double> &resource_map_available,
      NodeResources *node_resources);

  /// Update total capacity of resource resource_name at node client_id.
  void UpdateResourceCapacity(const std::string &client_id,
                              const std::string &resource_name, int64_t resource_total);

  /// Delete resource resource_name from node cleint_id_string.
  void DeleteResource(const std::string &client_id_string,
                      const std::string &resource_name);

  /// Return local resources.
  NodeResourceInstances GetLocalResources() { return local_resources_; };

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
  void InitResourceInstances(double total, bool unit_instances,
                             ResourceInstanceCapacities *instance_list);

  /// Allocate enough capacity across the instances of a resource to satisfy "demand".
  /// If resource has multiple unit-capacity instance, we consider two cases.
  ///
  /// 1) If the constraint is hard, allocate full unit-capacity instances until
  /// demand becomes fractional, and then satisfy the fractional deman using the
  /// instance with the smallest available capacity that can satisfy the fractional
  /// demand. For example, assume a resource conisting of 4 instances, with available
  /// capacities: (1., 1., .7, 0.5) and deman of 1.2. Then we allocate one full
  /// instance and then allocate 0.2 of the 0.5 instance (as this is the instance
  /// with the smalest available capacity that can satisfy the remaining demand of 0.2).
  /// As a result remaining available capacities will be (0., 1., .7, .2).
  /// Thus, if the constraint is hard, we will allocate at most a fractional resource.
  ///
  /// 2) If the constraint is soft, we can allocate multiple fractional resources,
  /// and even overallocate the resource. For example, in the previous case, if we
  /// have a demand of 1.8, we can allocate one full instance, the 0.5 instance, and
  /// 0.1 from the 0.7 instance. Furthermore, if the demand is 3.5, then we allocate
  /// all instances, and return success (true), despite the fact that the total
  /// available capacity of the rwsource is 3.2 (= 1. + 1. + .7 + .5), which is less
  /// than the demand, 3.5.
  ///
  /// \param demand: The resource amount to be allocated.
  /// \param soft: Specifies whether this demand has soft or hard constraints.
  /// \param available: List of available capacities of the instances of the resource.
  /// \param allocation: List of instance capacities allocated to satisfy the demand.
  /// This is a return parameter.
  ///
  /// \return true, if allocation successful. In this case, the sum of the elements in
  /// "allocation" is equal to "demand".
  bool AllocateResourceInstances(double demand, bool soft, std::vector<double> &available,
                                 std::vector<double> *allocation);

  /// Allocate local resources to satisfy a given request (task_req).
  ///
  /// \param task_req: Resources requested by a task.
  /// \param task_allocation: Local resources allocated to satsify task_req demand.
  /// This is an output argument.
  ///
  /// \return true, if allocation successful. If false, the caller needs to free the
  /// allocated resources, i.e., task_allocation.
  bool AllocateTaskResourceInstances(const TaskRequest &task_req,
                                     TaskResourceInstances *task_allocation);

  /// Free resources which were allocated with a task. The freed resources are
  /// added back to the node's local available resources.
  ///
  /// \param task_allocation: Task's resources to be freed.
  void FreeTaskResourceInstances(TaskResourceInstances &task_allocation);

  /// Increase the available capacities of the instances of a given resource.
  ///
  /// \param available A list of available capacities for resource's instances.
  /// \param resource_instances List of the resource instances being updated.
  void AddAvailableResourceInstances(std::vector<double> available,
                                     ResourceInstanceCapacities *resource_instances);

  /// Decrease the available capacities of the instances of a given resource.
  ///
  /// \param free A list of capacities for resource's instances to be freed.
  /// \param resource_instances List of the resource instances being updated.
  void SubtractAvailableResourceInstances(std::vector<double> free,
                                          ResourceInstanceCapacities *resource_instances);

  /// Increase the available CPU instances of this node.
  ///
  /// \param cpu_instances CPU instances to be added to available cpus.
  void AddCPUResourceInstances(std::vector<double> &cpu_instances);

  /// Decrease the available cpu instances of this node.
  ///
  /// \param cpu_instances Cpu instances to be removed from available cpus.
  void SubtractCPUResourceInstances(std::vector<double> &cpu_instances);

  /// Return human-readable string for this scheduler state.
  std::string DebugString();
};

#endif  // RAY_COMMON_SCHEDULING_SCHEDULING_H
