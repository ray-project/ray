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

#include "cluster_resource_scheduler.h"

bool NodeResources::operator==(const NodeResources &other) {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (this->predefined_resources[i].total != other.predefined_resources[i].total) {
      return false;
    }
    if (this->predefined_resources[i].available !=
        other.predefined_resources[i].available) {
      return false;
    }
  }

  if (this->custom_resources.size() != other.custom_resources.size()) {
    return false;
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

std::string NodeResources::DebugString() {
  std::stringstream buffer;
  buffer << "  node predefined resources {";
  for (size_t i = 0; i < static_cast<size_t>(this->predefined_resources.size()); i++) {
    buffer << "(" << this->predefined_resources[i].total << ":"
           << this->predefined_resources[i].available << ") ";
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

std::string VectorToString(std::vector<double> &vector) {
  std::stringstream buffer;

  buffer << "[";
  for (size_t i = 0; i < vector.size(); i++) {
    buffer << vector[i];
    if (i < vector.size() - 1) {
      buffer << ", ";
    }
  }
  buffer << "]";
  return buffer.str();
}

bool NodeResourceInstances::operator==(const NodeResourceInstances &other) {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (!EqualVectors(this->predefined_resources[i].total,
                      other.predefined_resources[i].total)) {
      return false;
    }
    if (!EqualVectors(this->predefined_resources[i].available,
                      other.predefined_resources[i].available)) {
      return false;
    }
  }

  if (this->custom_resources.size() != other.custom_resources.size()) {
    return false;
  }

  for (auto it1 = this->custom_resources.begin(); it1 != this->custom_resources.end();
       ++it1) {
    auto it2 = other.custom_resources.find(it1->first);
    if (it2 == other.custom_resources.end()) {
      return false;
    }
    if (!EqualVectors(it1->second.total, it2->second.total)) {
      return false;
    }
    if (!EqualVectors(it1->second.available, it2->second.available)) {
      return false;
    }
  }
  return true;
}

std::string NodeResourceInstances::DebugString() {
  std::stringstream buffer;
  buffer << "  node predefined resources {";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << "(" << VectorToString(predefined_resources[i].total) << ":"
           << VectorToString(this->predefined_resources[i].available) << ") ";
  }
  buffer << "}" << std::endl;

  buffer << "  node custom resources {";
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    buffer << it->first << ":(" << VectorToString(it->second.total) << ":"
           << VectorToString(it->second.available) << ") ";
  }
  buffer << "}" << std::endl;
  return buffer.str();
};

TaskResourceInstances NodeResourceInstances::GetAvailableResourceInstances() {
  TaskResourceInstances task_resources;
  task_resources.predefined_resources.resize(PredefinedResources_MAX);

  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    task_resources.predefined_resources[i] = this->predefined_resources[i].available;
  }

  for (const auto it : this->custom_resources) {
    task_resources.custom_resources.emplace(it.first, it.second.available);
  }

  return task_resources;
};

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
           << "(" << this->custom_resources[i].demand << ":"
           << this->custom_resources[i].soft << ") ";
  }
  buffer << "}" << std::endl;
  return buffer.str();
}

std::string TaskResourceInstances::DebugString() {
  std::stringstream buffer;
  buffer << std::endl << "  task allocation: P {";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << VectorToString(this->predefined_resources[i]);
  }
  buffer << "}";

  buffer << "  C {";
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    buffer << it->first << ":" << VectorToString(it->second) << ", ";
  }

  buffer << "}" << std::endl;
  return buffer.str();
}

bool EqualVectors(const std::vector<double> &v1, const std::vector<double> &v2) {
  return (v1.size() == v2.size() && std::equal(v1.begin(), v1.end(), v2.begin()));
}

bool TaskResourceInstances::operator==(const TaskResourceInstances &other) {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (!EqualVectors(this->predefined_resources[i], other.predefined_resources[i])) {
      return false;
    }
  }

  if (this->custom_resources.size() != other.custom_resources.size()) {
    return false;
  }

  for (auto it1 = this->custom_resources.begin(); it1 != this->custom_resources.end();
       ++it1) {
    auto it2 = other.custom_resources.find(it1->first);
    if (it2 == other.custom_resources.end()) {
      return false;
    }
    if (!EqualVectors(it1->second, it2->second)) {
      return false;
    }
  }
  return true;
}

ClusterResourceScheduler::ClusterResourceScheduler(
    int64_t local_node_id, const NodeResources &local_node_resources)
    : local_node_id_(local_node_id) {
  AddOrUpdateNode(local_node_id_, local_node_resources);
  InitLocalResources(local_node_resources);
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
    old_resources->predefined_resources[i].total =
        new_resources.predefined_resources[i].total;
    old_resources->predefined_resources[i].available =
        new_resources.predefined_resources[i].available;
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
    if (task_req.predefined_resources[i].demand >
        resources.predefined_resources[i].available) {
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
      if (task_req.custom_resources[i].soft) {
        violations++;
      } else {
        return -1;
      }
    } else {
      if (task_req.custom_resources[i].demand > it->second.available) {
        // Resource constraint is violated.
        if (task_req.custom_resources[i].soft) {
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
  if (IsSchedulable(task_req, node_id, resources) == -1) {
    return false;
  }

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    resources.predefined_resources[i].available =
        std::max(0., resources.predefined_resources[i].available -
                         task_req.predefined_resources[i].demand);
  }

  for (size_t i = 0; i < task_req.custom_resources.size(); i++) {
    auto it = resources.custom_resources.find(task_req.custom_resources[i].id);
    if (it != resources.custom_resources.end()) {
      it->second.available =
          std::max(0., it->second.available - task_req.custom_resources[i].demand);
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
    resources.predefined_resources[i].available =
        std::min(resources.predefined_resources[i].available +
                     task_req.predefined_resources[i].demand,
                 resources.predefined_resources[i].total);
  }

  for (size_t i = 0; i < task_req.custom_resources.size(); i++) {
    auto it = resources.custom_resources.find(task_req.custom_resources[i].id);
    if (it != resources.custom_resources.end()) {
      it->second.available = std::min(
          it->second.available + task_req.custom_resources[i].demand, it->second.total);
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
  node_resources->predefined_resources.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    node_resources->predefined_resources[i].total =
        node_resources->predefined_resources[i].available = 0;
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
      node_resources->predefined_resources[CPU] = resource_capacity;
    } else if (it->first == ray::kGPU_ResourceLabel) {
      node_resources->predefined_resources[GPU] = resource_capacity;
    } else if (it->first == ray::kTPU_ResourceLabel) {
      node_resources->predefined_resources[TPU] = resource_capacity;
    } else if (it->first == ray::kMemory_ResourceLabel) {
      node_resources->predefined_resources[MEM] = resource_capacity;
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
      task_request->custom_resources[i].demand = it->second;
      task_request->custom_resources[i].soft = false;
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
    int64_t diff_capacity = resource_total - it->second.predefined_resources[idx].total;
    it->second.predefined_resources[idx].total += diff_capacity;
    it->second.predefined_resources[idx].available += diff_capacity;
    if (it->second.predefined_resources[idx].available < 0) {
      it->second.predefined_resources[idx].available = 0;
    }
    if (it->second.predefined_resources[idx].total < 0) {
      it->second.predefined_resources[idx].total = 0;
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
    it->second.predefined_resources[idx].total = 0;
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

void ClusterResourceScheduler::InitResourceInstances(
    double total, bool unit_instances,
    ResourceInstanceCapacities *instance_list /* return */) {
  if (unit_instances) {
    size_t num_instances = static_cast<size_t>(total);
    instance_list->total.resize(num_instances);
    instance_list->available.resize(num_instances);
    for (size_t i = 0; i < num_instances; i++) {
      instance_list->total[i] = instance_list->available[i] = 1.0;
    };
  } else {
    instance_list->total.resize(1);
    instance_list->available.resize(1);
    instance_list->total[0] = instance_list->available[0] = total;
  }
}

void ClusterResourceScheduler::InitLocalResources(const NodeResources &node_resources) {
  local_resources_.predefined_resources.resize(PredefinedResources_MAX);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (node_resources.predefined_resources[i].total > 0) {
      InitResourceInstances(
          node_resources.predefined_resources[i].total,
          (UnitInstanceResources.find(i) != UnitInstanceResources.end()),
          &local_resources_.predefined_resources[i]);
    }
  }

  if (node_resources.custom_resources.size() == 0) {
    return;
  }

  for (auto it = node_resources.custom_resources.begin();
       it != node_resources.custom_resources.end(); ++it) {
    if (it->second.total > 0) {
      ResourceInstanceCapacities instance_list;
      InitResourceInstances(it->second.total, false, &instance_list);
      local_resources_.custom_resources.emplace(it->first, instance_list);
    }
  }
}

void ClusterResourceScheduler::AddAvailableResourceInstances(
    std::vector<double> available,
    ResourceInstanceCapacities *resource_instances /* return */) {
  for (size_t i = 0; i < available.size(); i++) {
    resource_instances->available[i] = std::min(
        resource_instances->available[i] + available[i], resource_instances->total[i]);
  }
}

void ClusterResourceScheduler::SubtractAvailableResourceInstances(
    std::vector<double> available,
    ResourceInstanceCapacities *resource_instances /* return */) {
  RAY_CHECK(available.size() == resource_instances->available.size());

  for (size_t i = 0; i < available.size(); i++) {
    resource_instances->available[i] =
        std::max(resource_instances->available[i] - available[i], 0.);
  }
}

bool ClusterResourceScheduler::AllocateResourceInstances(
    double demand, bool soft, std::vector<double> &available,
    std::vector<double> *allocation /* return */) {
  allocation->resize(available.size());
  double remaining_demand = demand;

  if (available.size() == 1) {
    // This resource has just an instance.
    if (available[0] >= remaining_demand) {
      available[0] -= remaining_demand;
      (*allocation)[0] = remaining_demand;
      return true;
    } else {
      if (soft) {
        available[0] = 0;
        return true;
      }
      // Not enough capacity.
      return false;
    }
  }

  // If resources has multiple instances, each instance has total capacity of 1.
  //
  // If this resource constraint is hard, as long as remaining_demand is greater than 1.,
  // allocate full unit-capacity instances until the remaining_demand becomes fractional.
  // Then try to find the best fit for the fractional remaining_resources. Best fist means
  // allocating the resource instance with the smallest available capacity greater than
  // remaining_demand
  //
  // If resource constraint is soft, allocate as many full unit-capacity resources and
  // then distribute remaining_demand across remaining instances. Note that in case we can
  // overallocate this resource.
  if (remaining_demand >= 1.) {
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] == 1.) {
        // Allocate a full unit-capacity instance.
        (*allocation)[i] = 1.;
        available[i] = 0;
        remaining_demand -= 1.;
      }
      if (remaining_demand < 1.) {
        break;
      }
    }
  }

  if (soft) {
    // Just get as many resources as available.
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] >= remaining_demand) {
        available[i] -= remaining_demand;
        (*allocation)[i] = remaining_demand;
        return true;
      } else {
        (*allocation)[i] = available[i];
        remaining_demand -= available[i];
        available[i] = 0;
      }
    }
    return true;
  }

  if (remaining_demand >= 1.) {
    // Cannot satisfy a demand greater than one if no unit caapcity resource is available.
    return false;
  }

  // Remaining demand is fractional. Find the best fit, if exists.
  if (remaining_demand > 0.) {
    int64_t idx_best_fit = -1;
    double available_best_fit = 1.;
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] >= remaining_demand) {
        if (idx_best_fit == -1 ||
            (available[i] - remaining_demand < available_best_fit)) {
          available_best_fit = available[i] - remaining_demand;
          idx_best_fit = static_cast<int64_t>(i);
        }
      }
    }
    if (idx_best_fit == -1) {
      return false;
    } else {
      (*allocation)[idx_best_fit] = remaining_demand;
      available[idx_best_fit] -= remaining_demand;
    }
  }
  return true;
}

bool ClusterResourceScheduler::AllocateTaskResourceInstances(
    const TaskRequest &task_req, TaskResourceInstances *task_allocation /* return */) {
  auto it = nodes_.find(local_node_id_);
  if (it == nodes_.end()) {
    return false;
  }

  // Just double check this node can still schedule the task request.
  if (IsSchedulable(task_req, local_node_id_, it->second) == -1) {
    return false;
  }

  task_allocation->predefined_resources.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (task_req.predefined_resources[i].demand > 0) {
      if (!AllocateResourceInstances(task_req.predefined_resources[i].demand,
                                     task_req.predefined_resources[i].soft,
                                     local_resources_.predefined_resources[i].available,
                                     &task_allocation->predefined_resources[i])) {
        // Allocation failed. Restore node's local resources by freeing the resources
        // of the failed allocation.
        FreeTaskResourceInstances(*task_allocation);
        return false;
      }
    }
  }

  for (size_t i = 0; i < task_req.custom_resources.size(); i++) {
    auto it = local_resources_.custom_resources.find(task_req.custom_resources[i].id);
    if (it != local_resources_.custom_resources.end()) {
      if (task_req.custom_resources[i].demand > 0) {
        std::vector<double> allocation;
        bool success = AllocateResourceInstances(task_req.custom_resources[i].demand,
                                                 task_req.custom_resources[i].soft,
                                                 it->second.available, &allocation);
        // Even if allocation failed we need to remember partial allocations to correctly
        // free resources.
        task_allocation->custom_resources.emplace(it->first, allocation);
        if (!success) {
          // Allocation failed. Restore node's local resources by freeing the resources
          // of the failed allocation.
          FreeTaskResourceInstances(*task_allocation);
          return false;
        }
      }
    } else {
      return false;
    }
  }
  return true;
}

void ClusterResourceScheduler::FreeTaskResourceInstances(
    TaskResourceInstances &task_allocation) {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    AddAvailableResourceInstances(task_allocation.predefined_resources[i],
                                  &local_resources_.predefined_resources[i]);
  }

  for (auto it = task_allocation.custom_resources.begin();
       it != task_allocation.custom_resources.end(); it++) {
    auto it_local = local_resources_.custom_resources.find(it->first);
    if (it_local != local_resources_.custom_resources.end()) {
      AddAvailableResourceInstances(it->second, &it_local->second);
    }
  }
}

void ClusterResourceScheduler::AddCPUResourceInstances(
    std::vector<double> &cpu_instances) {
  AddAvailableResourceInstances(cpu_instances,
                                &local_resources_.predefined_resources[CPU]);
}

void ClusterResourceScheduler::SubtractCPUResourceInstances(
    std::vector<double> &cpu_instances) {
  SubtractAvailableResourceInstances(cpu_instances,
                                     &local_resources_.predefined_resources[CPU]);
}
