#include "cluster_resource_scheduler.h"

std::string NodeResources::DebugString() {
  std::stringstream buffer;
  buffer << "  node predefined resources {";
  for (size_t i = 0; i < this->capacities.size(); i++) {
    buffer << "(" << this->capacities[i].total << ":" << this->capacities[i].available
           << ") ";
  }
  buffer << "}" << std::endl;

  buffer << "  node custom resources {";
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    buffer << it->first << ":(" << it->second.total << ":" << it->second.available
           << ") ";
  }
  buffer << "}" << std::endl;
  return buffer.str();
}

std::string TaskRequest::DebugString() {
  std::stringstream buffer;
  buffer << std::endl << "  request predefined resources {";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << "(" << this->predefined_resources[i].demand << ":"
           << this->predefined_resources[i].soft << ") ";
  }
  buffer << "}" << std::endl;

  buffer << "  request custom resources {";
  for (size_t i = 0; i < this->custom_resources.size(); i++) {
    buffer << this->custom_resources[i].id << ":"
           << "(" << this->custom_resources[i].req.demand << ":"
           << this->custom_resources[i].req.soft << ") ";
  }
  buffer << "}" << std::endl;
  return buffer.str();
}

bool NodeResources::operator==(const NodeResources &other) {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (this->capacities[i].total != other.capacities[i].total) {
      return false;
    }
    if (this->capacities[i].available != other.capacities[i].available) {
      return false;
    }
  }

  if (this->custom_resources.size() != other.custom_resources.size()) {
    return true;
  }

  for (auto it1 = this->custom_resources.begin(); it1 != this->custom_resources.end();
       ++it1) {
    auto it2 = other.custom_resources.find(it1->first);
    if (it2 == other.custom_resources.end()) {
      return false;
    }
    if (it1->second.total != it2->second.total) {
      return false;
    }
    if (it1->second.available != it2->second.available) {
      return false;
    }
  }
  return true;
}

ClusterResourceScheduler::ClusterResourceScheduler(
    int64_t local_node_id, const NodeResources &local_node_resources)
    : local_node_id_(local_node_id) {
  AddOrUpdateNode(local_node_id_, local_node_resources);
}

ClusterResourceScheduler::ClusterResourceScheduler(
    const std::string &local_node_id,
    const std::unordered_map<std::string, double> &local_node_resources) {
  local_node_id_ = string_to_int_map_.Insert(local_node_id);
  AddOrUpdateNode(local_node_id, local_node_resources, local_node_resources);
}

void ClusterResourceScheduler::AddOrUpdateNode(
    const std::string &node_id,
    const std::unordered_map<std::string, double> &resources_total,
    const std::unordered_map<std::string, double> &resources_available) {
  NodeResources node_resources;
  ResourceMapToNodeResources(resources_total, resources_available, &node_resources);
  AddOrUpdateNode(string_to_int_map_.Insert(node_id), node_resources);
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
    string_to_int_map_.Remove(node_id);
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
      continue;
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

std::string ClusterResourceScheduler::GetBestSchedulableNode(
    const std::unordered_map<std::string, double> &task_resources,
    int64_t *total_violations) {
  TaskRequest task_request;
  ResourceMapToTaskRequest(task_resources, &task_request);
  int64_t node_id = GetBestSchedulableNode(task_request, total_violations);

  std::string id_string;
  if (node_id == -1) {
    return "";
  }
  return string_to_int_map_.Get(node_id);
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

bool ClusterResourceScheduler::SubtractNodeAvailableResources(
    const std::string &node_id,
    const std::unordered_map<std::string, double> &resource_map) {
  TaskRequest task_request;
  ResourceMapToTaskRequest(resource_map, &task_request);
  return SubtractNodeAvailableResources(string_to_int_map_.Get(node_id), task_request);
}

bool ClusterResourceScheduler::AddNodeAvailableResources(int64_t node_id,
                                                         const TaskRequest &task_req) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }
  NodeResources &resources = it->second;

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    resources.capacities[i].available =
        resources.capacities[i].available + task_req.predefined_resources[i].demand;
  }

  for (size_t i = 0; i < task_req.custom_resources.size(); i++) {
    auto it = resources.custom_resources.find(task_req.custom_resources[i].id);
    if (it != resources.custom_resources.end()) {
      it->second.available =
          it->second.available + task_req.custom_resources[i].req.demand;
    }
  }
  return true;
}

bool ClusterResourceScheduler::AddNodeAvailableResources(
    const std::string &node_id,
    const std::unordered_map<std::string, double> &resource_map) {
  TaskRequest task_request;
  ResourceMapToTaskRequest(resource_map, &task_request);
  return AddNodeAvailableResources(string_to_int_map_.Get(node_id), task_request);
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

void ClusterResourceScheduler::ResourceMapToNodeResources(
    const std::unordered_map<std::string, double> &resource_map_total,
    const std::unordered_map<std::string, double> &resource_map_available,
    NodeResources *node_resources) {
  node_resources->capacities.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    node_resources->capacities[i].total = node_resources->capacities[i].available = 0;
  }

  for (auto it = resource_map_total.begin(); it != resource_map_total.end(); ++it) {
    ResourceCapacity resource_capacity;
    resource_capacity.total = (int64_t)it->second;
    auto it2 = resource_map_available.find(it->first);
    if (it2 == resource_map_available.end()) {
      resource_capacity.available = 0;
    } else {
      resource_capacity.available = (int64_t)it2->second;
    }
    if (it->first == ray::kCPU_ResourceLabel) {
      node_resources->capacities[CPU] = resource_capacity;
    } else if (it->first == ray::kGPU_ResourceLabel) {
      node_resources->capacities[GPU] = resource_capacity;
    } else if (it->first == ray::kTPU_ResourceLabel) {
      node_resources->capacities[TPU] = resource_capacity;
    } else if (it->first == ray::kMemory_ResourceLabel) {
      node_resources->capacities[MEM] = resource_capacity;
    } else {
      // This is a custom resource.
      node_resources->custom_resources.emplace(string_to_int_map_.Insert(it->first),
                                               resource_capacity);
    }
  }
}

void ClusterResourceScheduler::ResourceMapToTaskRequest(
    const std::unordered_map<std::string, double> &resource_map,
    TaskRequest *task_request) {
  size_t i = 0;

  task_request->predefined_resources.resize(PredefinedResources_MAX);
  task_request->custom_resources.resize(resource_map.size());
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    task_request->predefined_resources[0].demand = 0;
    task_request->predefined_resources[0].soft = false;
  }

  for (auto it = resource_map.begin(); it != resource_map.end(); ++it) {
    if (it->first == ray::kCPU_ResourceLabel) {
      task_request->predefined_resources[CPU].demand = it->second;
    } else if (it->first == ray::kGPU_ResourceLabel) {
      task_request->predefined_resources[GPU].demand = it->second;
    } else if (it->first == ray::kTPU_ResourceLabel) {
      task_request->predefined_resources[TPU].demand = it->second;
    } else if (it->first == ray::kMemory_ResourceLabel) {
      task_request->predefined_resources[MEM].demand = it->second;
    } else {
      task_request->custom_resources[i].id = string_to_int_map_.Insert(it->first);
      task_request->custom_resources[i].req.demand = it->second;
      task_request->custom_resources[i].req.soft = false;
      i++;
    }
  }
  task_request->custom_resources.resize(i);
}

void ClusterResourceScheduler::UpdateResourceCapacity(const std::string &client_id_string,
                                                      const std::string &resource_name,
                                                      int64_t resource_total) {
  int64_t client_id = string_to_int_map_.Get(client_id_string);
  auto it = nodes_.find(client_id);
  if (it == nodes_.end()) {
    return;
  }

  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)GPU;
  } else if (resource_name == ray::kTPU_ResourceLabel) {
    idx = (int)TPU;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)MEM;
  };
  if (idx != -1) {
    int64_t diff_capacity = resource_total - it->second.capacities[idx].total;
    it->second.capacities[idx].total += diff_capacity;
    it->second.capacities[idx].available += diff_capacity;
    if (it->second.capacities[idx].available < 0) {
      it->second.capacities[idx].available = 0;
    }
    if (it->second.capacities[idx].total < 0) {
      it->second.capacities[idx].total = 0;
    }
  } else {
    int64_t resource_id = string_to_int_map_.Insert(resource_name);
    auto itr = it->second.custom_resources.find(resource_id);
    if (itr != it->second.custom_resources.end()) {
      int64_t diff_capacity = resource_total - itr->second.total;
      itr->second.total += diff_capacity;
      itr->second.available += diff_capacity;
      if (itr->second.available < 0) {
        itr->second.available = 0;
      }
      if (itr->second.total < 0) {
        itr->second.total = 0;
      }
    }
    ResourceCapacity resource_capacity;
    resource_capacity.total = resource_capacity.available = resource_total;
    it->second.custom_resources.emplace(resource_id, resource_capacity);
  }
}

void ClusterResourceScheduler::DeleteResource(const std::string &client_id_string,
                                              const std::string &resource_name) {
  int64_t client_id = string_to_int_map_.Get(client_id_string);
  auto it = nodes_.find(client_id);
  if (it == nodes_.end()) {
    return;
  }

  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)GPU;
  } else if (resource_name == ray::kTPU_ResourceLabel) {
    idx = (int)TPU;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)MEM;
  };
  if (idx != -1) {
    it->second.capacities[idx].total = 0;
  } else {
    int64_t resource_id = string_to_int_map_.Get(resource_name);
    auto itr = it->second.custom_resources.find(resource_id);
    if (itr != it->second.custom_resources.end()) {
      string_to_int_map_.Remove(resource_id);
      it->second.custom_resources.erase(itr);
    }
  }
}

std::string ClusterResourceScheduler::DebugString(void) {
  std::stringstream buffer;
  buffer << std::endl << "local node id: " << local_node_id_ << std::endl;
  for (auto it = nodes_.begin(); it != nodes_.end(); ++it) {
    buffer << "node id: " << it->first << std::endl;
    buffer << it->second.DebugString();
  }
  return buffer.str();
}
