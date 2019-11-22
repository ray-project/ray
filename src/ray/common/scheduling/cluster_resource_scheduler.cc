#include "cluster_resource_scheduler.h"

ClusterResourceScheduler::ClusterResourceScheduler(
    int64_t local_node_id, const NodeResources &local_node_resources)
    : local_node_id_(local_node_id) {
  AddOrUpdateNode(local_node_id_, local_node_resources);
}

void ClusterResourceScheduler::SetPredefinedResources(const NodeResources &new_resources,
                                                      NodeResources *old_resources) {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    old_resources->capacities[i].total = new_resources.capacities[i].total;
    old_resources->capacities[i].available = new_resources.capacities[i].available;
  }
}

void ClusterResourceScheduler::SetCustomResources(
    const absl::flat_hash_map<int64_t, ResourceCapacity> &new_custom_resources,
    absl::flat_hash_map<int64_t, ResourceCapacity> *old_custom_resources) {
  old_custom_resources->clear();
  for (auto &elem : new_custom_resources) {
    old_custom_resources->insert(elem);
  }
}

void ClusterResourceScheduler::AddOrUpdateNode(int64_t node_id,
                                               const NodeResources &node_resources) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // This node is new, so add it to the map.
    nodes_.emplace(node_id, node_resources);
  } else {
    // This node exists, so update its resources.
    NodeResources &resources = it->second;
    SetPredefinedResources(node_resources, &resources);
    SetCustomResources(node_resources.custom_resources, &resources.custom_resources);
  }
}

bool ClusterResourceScheduler::RemoveNode(int64_t node_id) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // Node not found.
    return false;
  } else {
    it->second.custom_resources.clear();
    nodes_.erase(it);
    return true;
  }
}

int64_t ClusterResourceScheduler::IsSchedulable(const TaskRequest &task_req,
                                                int64_t node_id,
                                                const NodeResources &resources) {
  int violations = 0;

  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (task_req.predefined_resources[i].demand > resources.capacities[i].available) {
      if (task_req.predefined_resources[i].soft) {
        // A soft constraint has been violated.
        violations++;
      } else {
        // A hard constraint has been violated.
        return -1;
      }
    }
  }

  for (size_t i = 0; i < task_req.custom_resources.size(); i++) {
    auto it = resources.custom_resources.find(task_req.custom_resources[i].id);

    if (it == resources.custom_resources.end()) {
      // Requested resource doesn't exist at this node.
      if (task_req.custom_resources[i].req.soft) {
        violations++;
      } else {
        return -1;
      }
    } else {
      if (task_req.custom_resources[i].req.demand > it->second.available) {
        // Resource constraint is violated.
        if (task_req.custom_resources[i].req.soft) {
          violations++;
        } else {
          return -1;
        }
      }
    }
  }

  if (task_req.placement_hints.size() > 0) {
    auto it_p = task_req.placement_hints.find(node_id);
    if (it_p == task_req.placement_hints.end()) {
      // Node not found in the placement_hints list, so
      // record this a soft constraint violation.
      violations++;
    }
  }

  return violations;
}

int64_t ClusterResourceScheduler::GetBestSchedulableNode(const TaskRequest &task_req,
                                                         int64_t *total_violations) {
  // Min number of violations across all nodes that can schedule the request.
  int64_t min_violations = INT_MAX;
  // Node associated to min_violations.
  int64_t best_node = -1;
  *total_violations = 0;

  // Check whether local node is schedulable. We return immediately
  // the local node only if there are zero violations.
  auto it = nodes_.find(local_node_id_);
  if (it != nodes_.end()) {
    if (IsSchedulable(task_req, it->first, it->second) == 0) {
      return local_node_id_;
    }
  }

  // Check whether any node in the request placement_hints, satisfes
  // all resource constraints of the request.
  for (auto it_p = task_req.placement_hints.begin();
       it_p != task_req.placement_hints.end(); ++it_p) {
    auto it = nodes_.find(*it_p);
    if (it != nodes_.end()) {
      if (IsSchedulable(task_req, it->first, it->second) == 0) {
        return it->first;
      }
    }
  }

  for (auto it = nodes_.begin(); it != nodes_.end(); ++it) {
    // Return -1 if node not schedulable. otherwise return the number
    // of soft constraint violations.
    int64_t violations;

    if ((violations = IsSchedulable(task_req, it->first, it->second)) == -1) {
      break;
    }
    // Update the node with the smallest number of soft constraints violated.
    if (min_violations > violations) {
      min_violations = violations;
      best_node = it->first;
    }
    if (violations == 0) {
      *total_violations = 0;
      return best_node;
    }
  }
  *total_violations = min_violations;
  return best_node;
}

bool ClusterResourceScheduler::SubtractNodeAvailableResources(
    int64_t node_id, const TaskRequest &task_req) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }
  NodeResources &resources = it->second;

  // Just double check this node can still schedule the task request.
  if (IsSchedulable(task_req, local_node_id_, resources) == -1) {
    return false;
  }

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    resources.capacities[i].available =
        std::max(static_cast<int64_t>(0), resources.capacities[i].available -
                                              task_req.predefined_resources[i].demand);
  }

  for (size_t i = 0; i < task_req.custom_resources.size(); i++) {
    auto it = resources.custom_resources.find(task_req.custom_resources[i].id);
    if (it != resources.custom_resources.end()) {
      it->second.available =
          std::max(static_cast<int64_t>(0),
                   it->second.available - task_req.custom_resources[i].req.demand);
    }
  }
  return true;
}

bool ClusterResourceScheduler::GetNodeResources(int64_t node_id,
                                                NodeResources *ret_resources) {
  auto it = nodes_.find(node_id);
  if (it != nodes_.end()) {
    *ret_resources = it->second;
    return true;
  } else {
    return false;
  }
}

int64_t ClusterResourceScheduler::NumNodes() { return nodes_.size(); }
