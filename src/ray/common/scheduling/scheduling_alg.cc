#include "scheduling_alg.h"
#include <iostream>
#include <stdlib.h>
#include <chrono>

using namespace std;

ClusterResources::ClusterResources(int64_t local_node_id, const NodeResources &local_node_resources) {
  local_node_id_ = local_node_id;
  AddOrUpdateNode(local_node_id, local_node_resources);
}

void ClusterResources::AddOrUpdateNode(int64_t node_id, const NodeResources &node_resources) {

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // This node is new. Add it to the map.
    nodes_.emplace(node_id, node_resources);
  } else {
    // This node exists. Update its resources.
    NodeResources &nr = it->second;

    // First, update its predefined resources.
    for (int i = 0; i < NUM_PREDIFINED_RESOURCES; i++) {
      nr.capacities[i].total = node_resources.capacities[i].total;
      nr.capacities[i].available = node_resources.capacities[i].available;
    }

    // Then, delete existing custom resources and replace them with the new ones.
    nr.custom_resources.clear();

    auto mcr = node_resources.custom_resources;
    for (auto it = mcr.begin(); it != mcr.end(); ++it) {
      nr.custom_resources.insert(*it);
    }
  }
}

bool ClusterResources::RemoveNode(int64_t node_id) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    /// Node not found.
    return false;
  } else {
    /// Remobe node.
    it->second.custom_resources.clear();
    nodes_.erase(it);
    return true;
  }
}

int64_t ClusterResources::IsSchedulable(const TaskReq &task_req, const NodeResources &nr) {
  int violations = 0;

  /// First, check predefined resources.
  for (int i = 0; i < NUM_PREDIFINED_RESOURCES; i++) {
    if (task_req.predefined_resources[i].demand > nr.capacities[i].available) {
      if (task_req.predefined_resources[i].soft) {
        // A soft constraint has been violated.
        violations++;
      } else {
        // A hard constraint has been violated.
        return -1;
      }
    }
  }

  for (int i = 0; i < task_req.custom_resources.size(); i++) {
    auto it = nr.custom_resources.find(task_req.custom_resources[i].id);

    if (it == nr.custom_resources.end()) {
      /// Requested resource doesn't exist at this node.
      if (task_req.custom_resources[i].req.soft) {
        violations++;
      } else {
        return -1;
      }
    } else {
      if (task_req.custom_resources[i].req.demand > it->second.available) {
        /// Resource constraint is violated.
        if (task_req.custom_resources[i].req.soft) {
          violations++;
        } else {
          return -1;
        }
      }
    }
  }
  return violations;
}

int64_t ClusterResources::GetSchedulableNode(const TaskReq &task_req, int64_t *violations) {
  /// Min number of violations across all nodes that can schedule the request.
  int64_t min_violations = INT_MAX;
  /// Node associated to min_violations.
  int64_t best_node = -1;
  *violations = 0;

  /// Check whether local node is schedulable.
  auto it = nodes_.find(local_node_id_);
  if (it != nodes_.end()) {
    int64_t v = IsSchedulable(task_req, it->second);
    if (v == 0) {
      auto it_p = task_req.placement_hints.find(local_node_id_);
      if (it_p != task_req.placement_hints.end()) {
        return local_node_id_;
      }
    }
  }

  /// Check whether any node in the request placement_hints, satisfes
  /// all resource constraints of the request.
  for (auto it_p = task_req.placement_hints.begin();
       it_p != task_req.placement_hints.end(); ++it_p) {
    auto it = nodes_.find(*it_p);
    if (it != nodes_.end()) {
      int64_t v = IsSchedulable(task_req, it->second);
      if (v == 0) {
        return it->first;
      }
    }
  }

  int num_placement_hints = task_req.placement_hints.size();

  for (auto it = nodes_.begin(); it != nodes_.end(); ++it) {
    /// Return -1 if node not schedulable. otherwise return the number
    /// of soft constraint violations.
    int64_t v = IsSchedulable(task_req, it->second);

    /// If a hard constraint has been violated, ignnore this node.
    if (v == -1) {
      break;
    }
    // check wether placement_hints are satisfied
    if (num_placement_hints > 0) {
      auto it_p = task_req.placement_hints.find(it->first);
      if (it_p == task_req.placement_hints.end()) {
        /// Node not found in the placemen_hints list, so
        /// record this a soft constraint violation.
        v++;
      }
    }
    // Update the node with the smallest number of soft constraints violated.
    if (min_violations > v) {
      min_violations = v;
      best_node = it->first;
    }
    if (v == 0) {
      *violations = 0;
      return best_node;
    }
  }
  *violations = min_violations;
  return best_node;
}

bool ClusterResources::UpdateNodeAvailableResources(int64_t node_id, const TaskReq &task_req) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }
  NodeResources &nr = it->second;

  /// Just double check this node can still schedule the task request.
  if (IsSchedulable(task_req, nr) == -1) {
    return false;
  }

  for (int i = 0; i < NUM_PREDIFINED_RESOURCES; i++) {
    if (task_req.predefined_resources[i].demand > 0) {
      nr.capacities[i].available -= task_req.predefined_resources[i].demand;
      if (nr.capacities[i].available < 0) {
        nr.capacities[i].available = 0;
      }
    }
  }

  for (int i = 0; i < task_req.custom_resources.size(); i++) {
    auto it = nr.custom_resources.find(task_req.custom_resources[i].id);
    if (it != nr.custom_resources.end()) {
      it->second.available -= task_req.custom_resources[i].req.demand;
      if (it->second.available < 0) {
        it->second.available = 0;
      }
    }
  }

  return true;
}

NodeResources* ClusterResources::GetNodeResources(int64_t node_id) {
  auto it = nodes_.find(node_id);
  if (it != nodes_.end()) {
    return &it->second;
  } else {
    return NULL;
  }
}

/// Get number of nodes in the cluster.
int64_t ClusterResources::Count() {
  return nodes_.size();
}
