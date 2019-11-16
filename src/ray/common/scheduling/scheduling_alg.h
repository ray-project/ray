#ifndef RAY_COMMON_SCHEDULING_SCHEDULING_H
#define RAY_COMMON_SCHEDULING_SCHEDULING_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>

/// Maximum number of predefined resources.
#define NUM_PREDIFINED_RESOURCES 4
/// List of predefined resources.
/// The number of elements in enum should be equal to NUM_PREDIFINED_RESOURCES.
enum PredefinedResources {CPU = 0, MEM = 1, GPU = 2, TPU = 3};

/// Total and available capacity of a resource instance.
struct ResourceCapacity {
  int64_t total;
  int64_t available;
};

/// Resource request.
struct ResourceReq {
  /// Amount of resource being requested.
  int64_t demand;
  /// Specify whether the request is soft or hard.
  /// If hard, the entire request is denied if the demand exceeds the resource
  /// availability. Otherwise, the request can be still be granted.
  bool soft;
};

/// Resource request, including resource ID. This is used for custom resources.
struct ResourceReqWithId {
  /// Resource ID.
  int64_t id;
  /// Resource request.
  ResourceReq req;
};


/// All resources associated with a node.
struct NodeResources {
  /// Available and total capacities for predefined resources.
  std::vector<ResourceCapacity> capacities;
  /// Map containing custom resources. The key of each entry represents the
  /// custom resource ID.
  std::unordered_map<int64_t, ResourceCapacity> custom_resources;
};

/// Task request.
struct TaskReq {
  /// List of predefined resources.
  std::vector<ResourceReq> predefined_resources;
  /// List of custom resources.
  std::vector<ResourceReqWithId> custom_resources;
  /// List of placement hints. A placement hint is a node on which
  /// we desire to run this task. This is a soft constraint in that
  /// the task will run on a different node in the cluster, if none of the
  /// nodes in this list can schedule this task.
  std::unordered_set<int64_t> placement_hints;
};

/// Class encapsulating the cluster resources and the logic to assign
/// tasks to nodes based on the task's constraints and the available
/// resources at those nodes.
class ClusterResources {
  /// List of nodes in the clusters and their resources organized as a map.
  /// The key of the map is the node ID.
  std::unordered_map<int64_t, NodeResources> nodes_;
  /// ID of local node.
  int64_t local_node_id_;

public:
  ClusterResources(void) {};

  /// Constructor initializing the resources associated with the local node.
  ///
  /// \param local_node_id: ID of local node,
  /// \param node_resources: The total and the available resources associated
  /// with the local node.
  ClusterResources(int64_t local_node_id, const NodeResources &node_resources);

  /// Add a new node, or update the resources of an existing node.
  ///
  /// \param node_id: Node ID.
  /// \param node_resources: Up to date total and available resources of the node.
  void add(int64_t node_id, const NodeResources &node_resources);

  /// Remove node from the cluster data structure. This happens
  /// when a node fails or it is removed from the cluster.
  ///
  /// \param ID of the node to be removed.
  bool remove(int64_t node_id);

   /// Check whether a task request can be scheduled given a node's resources.
   ///
   ///  \param task_req: Task request to be scheduled.
   ///  \param nr: Node's resources.
   ///
   ///  \return: -1, if the request cannot be scheduled. This happens when at
   ///           least a hard constraints is violated.
   ///           >= 0, the number soft constraint violations. If 0, no
   ///           constraint is violatede.
  int64_t isSchedulable(const TaskReq &task_req, const NodeResources &nr);

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
   ///  \param task_req: Task to be scheduled.
   ///  \param violations: The number of soft constraint violations associated
   ///                     with the node returned by this function (assuming
   ///                     a node that can schedule task_req is found).
   ///
   ///  \return -1, if no node can schedule the current request; otherwise,
   ///          return the ID of a node that can schedule the task request.
  int64_t getSchedulableNode(const TaskReq &task_req, int64_t *violations);

  /// Update the available resources of a node when a task request is
  /// scheduled on the given node.
  ///
  /// \param node_id: ID of node on which request is being scheduled.
  /// \param task_req: task request being scheduled.
  ///
  /// \return true, if task_req can be indeed scheduled on the node,
  /// and false otherwise.
  bool updateAvailableResources(int64_t node_id, const TaskReq &task_req);

  /// Return a pointer to the resources associated to the given node.
  NodeResources* getNodeResources(int64_t node_id);

  /// Get number of nodes in the cluster.
  int64_t count();
};

#endif // RAY_COMMON_SCHEDULING_SCHEDULING_H
