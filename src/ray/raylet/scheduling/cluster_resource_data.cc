#include "ray/raylet/scheduling/cluster_resource_data.h"
#include "ray/common/task/scheduling_resources.h"

const std::string resource_labels[] = {
    ray::kCPU_ResourceLabel, ray::kMemory_ResourceLabel, ray::kGPU_ResourceLabel,
    ray::kObjectStoreMemory_ResourceLabel};

std::unordered_map<std::string, PredefinedResources>
create_predefined_resource_name_enum_map() {
  std::unordered_map<std::string, PredefinedResources> map;
  map[ray::kCPU_ResourceLabel] = PredefinedResources::CPU;
  map[ray::kMemory_ResourceLabel] = PredefinedResources::MEM;
  map[ray::kGPU_ResourceLabel] = PredefinedResources::GPU;
  map[ray::kObjectStoreMemory_ResourceLabel] = PredefinedResources::OBJECT_STORE_MEM;
  return map;
}
const std::unordered_map<std::string, PredefinedResources>
    predefined_resource_name_enum_map = create_predefined_resource_name_enum_map();

const std::string ResourceEnumToString(PredefinedResources resource) {
  // TODO (Alex): We should replace this with a protobuf enum.
  RAY_CHECK(resource < PredefinedResources_MAX)
      << "Something went wrong. Please file a bug report with this stack "
         "trace: https://github.com/ray-project/ray/issues/new.";
  std::string label = resource_labels[resource];
  return label;
}

std::string VectorToString(const std::vector<FixedPoint> &vector) {
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

std::string UnorderedMapToString(const std::unordered_map<std::string, double> &map) {
  std::stringstream buffer;

  buffer << "[";
  for (auto it = map.begin(); it != map.end(); ++it) {
    buffer << "(" << it->first << ":" << it->second << ")";
  }
  buffer << "]";
  return buffer.str();
}

/// Convert a vector of doubles to a vector of resource units.
std::vector<FixedPoint> VectorDoubleToVectorFixedPoint(
    const std::vector<double> &vector) {
  std::vector<FixedPoint> vector_fp(vector.size());
  for (size_t i = 0; i < vector.size(); i++) {
    vector_fp[i] = vector[i];
  }
  return vector_fp;
}

/// Convert a vector of resource units to a vector of doubles.
std::vector<double> VectorFixedPointToVectorDouble(
    const std::vector<FixedPoint> &vector_fp) {
  std::vector<double> vector(vector_fp.size());
  for (size_t i = 0; i < vector_fp.size(); i++) {
    vector[i] = FixedPoint(vector_fp[i]).Double();
  }
  return vector;
}

/// Convert a map of resources to a TaskRequest data structure.
TaskRequest ResourceMapToTaskRequest(
    StringIdMap &string_to_int_map,
    const std::unordered_map<std::string, double> &resource_map) {
  TaskRequest task_request;

  task_request.predefined_resources.resize(PredefinedResources_MAX);

  for (auto const &resource : resource_map) {
    if (resource.first == ray::kCPU_ResourceLabel) {
      task_request.predefined_resources[CPU] = resource.second;
    } else if (resource.first == ray::kGPU_ResourceLabel) {
      task_request.predefined_resources[GPU] = resource.second;
    } else if (resource.first == ray::kObjectStoreMemory_ResourceLabel) {
      task_request.predefined_resources[OBJECT_STORE_MEM] = resource.second;
    } else if (resource.first == ray::kMemory_ResourceLabel) {
      task_request.predefined_resources[MEM] = resource.second;
    } else {
      int64_t id = string_to_int_map.Insert(resource.first);
      task_request.custom_resources[id] = resource.second;
    }
  }

  return task_request;
}

const std::vector<FixedPoint> &TaskResourceInstances::Get(
    const std::string &resource_name, const StringIdMap &string_id_map) const {
  if (ray::kCPU_ResourceLabel == resource_name) {
    return predefined_resources[CPU];
  } else if (ray::kGPU_ResourceLabel == resource_name) {
    return predefined_resources[GPU];
  } else if (ray::kObjectStoreMemory_ResourceLabel == resource_name) {
    return predefined_resources[OBJECT_STORE_MEM];
  } else if (ray::kMemory_ResourceLabel == resource_name) {
    return predefined_resources[MEM];
  } else {
    int64_t resource_id = string_id_map.Get(resource_name);
    auto it = custom_resources.find(resource_id);
    RAY_CHECK(it != custom_resources.end());
    return it->second;
  }
}

TaskRequest TaskResourceInstances::ToTaskRequest() const {
  TaskRequest task_req;
  task_req.predefined_resources.resize(PredefinedResources_MAX);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    task_req.predefined_resources[i] = 0;
    for (auto predefined_resource_instance : this->predefined_resources[i]) {
      task_req.predefined_resources[i] += predefined_resource_instance;
    }
  }

  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    task_req.custom_resources[it->first] = 0;
    for (size_t j = 0; j < it->second.size(); j++) {
      task_req.custom_resources[it->first] += it->second[j];
    }
  }
  return task_req;
}

/// Convert a map of resources to a TaskRequest data structure.
///
/// \param string_to_int_map: Map between names and ids maintained by the
/// \param resource_map_total: Total capacities of resources we want to convert.
/// \param resource_map_available: Available capacities of resources we want to convert.
///
/// \request Conversion result to a TaskRequest data structure.
NodeResources ResourceMapToNodeResources(
    StringIdMap &string_to_int_map,
    const std::unordered_map<std::string, double> &resource_map_total,
    const std::unordered_map<std::string, double> &resource_map_available) {
  NodeResources node_resources;
  node_resources.predefined_resources.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    node_resources.predefined_resources[i].total =
        node_resources.predefined_resources[i].available = 0;
  }

  for (auto const &resource : resource_map_total) {
    ResourceCapacity resource_capacity;
    resource_capacity.total = resource.second;
    auto it = resource_map_available.find(resource.first);
    if (it == resource_map_available.end()) {
      resource_capacity.available = 0;
    } else {
      resource_capacity.available = it->second;
    }
    if (resource.first == ray::kCPU_ResourceLabel) {
      node_resources.predefined_resources[CPU] = resource_capacity;
    } else if (resource.first == ray::kGPU_ResourceLabel) {
      node_resources.predefined_resources[GPU] = resource_capacity;
    } else if (resource.first == ray::kObjectStoreMemory_ResourceLabel) {
      node_resources.predefined_resources[OBJECT_STORE_MEM] = resource_capacity;
    } else if (resource.first == ray::kMemory_ResourceLabel) {
      node_resources.predefined_resources[MEM] = resource_capacity;
    } else {
      // This is a custom resource.
      node_resources.custom_resources.emplace(string_to_int_map.Insert(resource.first),
                                              resource_capacity);
    }
  }
  return node_resources;
}

float NodeResources::CalculateCriticalResourceUtilization() const {
  float highest = 0;
  for (const auto &i : {CPU, MEM, OBJECT_STORE_MEM}) {
    if (i >= this->predefined_resources.size()) {
      continue;
    }
    const auto &capacity = this->predefined_resources[i];
    if (capacity.total == 0) {
      continue;
    }

    float utilization = 1 - (capacity.available.Double() / capacity.total.Double());
    if (utilization > highest) {
      highest = utilization;
    }
  }
  return highest;
}

bool NodeResources::IsAvailable(const TaskRequest &task_req) const {
  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (i >= this->predefined_resources.size()) {
      if (task_req.predefined_resources[i] != 0) {
        return false;
      }
      continue;
    }

    const auto &resource = this->predefined_resources[i].available;
    const auto &demand = task_req.predefined_resources[i];

    if (resource < demand) {
      return false;
    }
  }

  // Now check custom resources.
  for (const auto &task_req_custom_resource : task_req.custom_resources) {
    auto it = this->custom_resources.find(task_req_custom_resource.first);
    if (it == this->custom_resources.end()) {
      return false;
    } else if (task_req_custom_resource.second > it->second.available) {
      return false;
    }
  }
  return true;
}

bool NodeResources::IsFeasible(const TaskRequest &task_req) const {
  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (i >= this->predefined_resources.size()) {
      if (task_req.predefined_resources[i] != 0) {
        return false;
      }
      continue;
    }
    const auto &resource = this->predefined_resources[i].total;
    const auto &demand = task_req.predefined_resources[i];

    if (resource < demand) {
      return false;
    }
  }

  // Now check custom resources.
  for (const auto &task_req_custom_resource : task_req.custom_resources) {
    auto it = this->custom_resources.find(task_req_custom_resource.first);
    if (it == this->custom_resources.end()) {
      return false;
    } else if (task_req_custom_resource.second > it->second.total) {
      return false;
    }
  }
  return true;
}

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

bool NodeResources::operator!=(const NodeResources &other) { return !(*this == other); }

std::string NodeResources::DebugString(StringIdMap string_to_in_map) const {
  std::stringstream buffer;
  buffer << " {\n";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << "\t";
    switch (i) {
    case CPU:
      buffer << "CPU: ";
      break;
    case MEM:
      buffer << "MEM: ";
      break;
    case GPU:
      buffer << "GPU: ";
      break;
    case OBJECT_STORE_MEM:
      buffer << "OBJECT_STORE_MEM: ";
      break;
    default:
      RAY_CHECK(false) << "This should never happen.";
      break;
    }
    buffer << "(" << this->predefined_resources[i].total << ":"
           << this->predefined_resources[i].available << ")\n";
  }
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    buffer << "\t" << string_to_in_map.Get(it->first) << ":(" << it->second.total << ":"
           << it->second.available << ")\n";
  }
  buffer << "}" << std::endl;
  return buffer.str();
}

const std::string format_resource(std::string resource_name, double quantity) {
  if (resource_name == "object_store_memory" || resource_name == "memory") {
    return std::to_string(quantity / (1024 * 1024 * 1024)) + " GiB";
  }
  return std::to_string(quantity);
}

std::string NodeResources::DictString(StringIdMap string_to_in_map) const {
  std::stringstream buffer;
  bool first = true;
  buffer << "{";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    if (this->predefined_resources[i].total <= 0) {
      continue;
    }
    if (first) {
      first = false;
    } else {
      buffer << ", ";
    }
    std::string name = "";
    switch (i) {
    case CPU:
      name = "CPU";
      break;
    case MEM:
      name = "memory";
      break;
    case GPU:
      name = "GPU";
      break;
    case OBJECT_STORE_MEM:
      name = "object_store_memory";
      break;
    default:
      RAY_CHECK(false) << "This should never happen.";
      break;
    }
    buffer << format_resource(name, this->predefined_resources[i].available.Double())
           << "/";
    buffer << format_resource(name, this->predefined_resources[i].total.Double());
    buffer << " " << name;
  }
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    auto name = string_to_in_map.Get(it->first);
    buffer << ", " << format_resource(name, it->second.available.Double()) << "/"
           << format_resource(name, it->second.total.Double());
    buffer << " " << name;
  }
  buffer << "}" << std::endl;
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

void TaskResourceInstances::ClearCPUInstances() {
  if (predefined_resources.size() >= CPU) {
    predefined_resources[CPU].clear();
    predefined_resources[CPU].clear();
  }
}

std::string NodeResourceInstances::DebugString(StringIdMap string_to_int_map) const {
  std::stringstream buffer;
  buffer << "{\n";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << "\t";
    switch (i) {
    case CPU:
      buffer << "CPU: ";
      break;
    case MEM:
      buffer << "MEM: ";
      break;
    case GPU:
      buffer << "GPU: ";
      break;
    case OBJECT_STORE_MEM:
      buffer << "OBJECT_STORE_MEM: ";
      break;
    default:
      RAY_CHECK(false) << "This should never happen.";
      break;
    }
    buffer << "(" << VectorToString(predefined_resources[i].total) << ":"
           << VectorToString(this->predefined_resources[i].available) << ")\n";
  }
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    buffer << "\t" << it->first << ":(" << VectorToString(it->second.total) << ":"
           << VectorToString(it->second.available) << ")\n";
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

  for (const auto &it : this->custom_resources) {
    task_resources.custom_resources.emplace(it.first, it.second.available);
  }

  return task_resources;
};

bool TaskRequest::IsEmpty() const {
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    if (this->predefined_resources[i] != 0) {
      return false;
    }
  }
  for (auto &it : custom_resources) {
    if (it.second != 0) {
      return false;
    }
  }
  return true;
}

std::string TaskRequest::DebugString() const {
  std::stringstream buffer;
  buffer << " {";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << "(" << this->predefined_resources[i] << ") ";
  }
  buffer << "}";

  buffer << "  [";
  for (auto &it : this->custom_resources) {
    buffer << it.first << ":"
           << "(" << it.second << ") ";
  }
  buffer << "]" << std::endl;
  return buffer.str();
}

bool TaskResourceInstances::IsEmpty() const {
  // Check whether all resource instances of a task are zero.
  for (const auto &predefined_resource : predefined_resources) {
    for (const auto &predefined_resource_instance : predefined_resource) {
      if (predefined_resource_instance != 0) {
        return false;
      }
    }
  }

  for (const auto &custom_resource : custom_resources) {
    for (const auto &custom_resource_instances : custom_resource.second) {
      if (custom_resource_instances != 0) {
        return false;
      }
    }
  }
  return true;
}

std::string TaskResourceInstances::DebugString() const {
  std::stringstream buffer;
  buffer << std::endl << "  Allocation: {";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << VectorToString(this->predefined_resources[i]);
  }
  buffer << "}";

  buffer << "  [";
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    buffer << it->first << ":" << VectorToString(it->second) << ", ";
  }

  buffer << "]" << std::endl;
  return buffer.str();
}

bool EqualVectors(const std::vector<FixedPoint> &v1, const std::vector<FixedPoint> &v2) {
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

/// ResourceSet class implementation
void ResourceSet::FillByResourceMap(
    const std::unordered_map<std::string, FixedPoint> &resource_map) {
  predefined_resources_.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    predefined_resources_[i] = 0;
  }
  for (auto const &resource : resource_map) {
    auto iter = predefined_resource_name_enum_map.find(resource.first);
    if (iter != predefined_resource_name_enum_map.end()) {
      predefined_resources_[iter->second] = resource.second;
    } else {
      int64_t id = string_id_map_.Insert(resource.first);
      custom_resources_[id] = resource.second;
    }
  }
}

ResourceSet::ResourceSet() { FillByResourceMap({}); }

ResourceSet::ResourceSet(
    const std::unordered_map<std::string, FixedPoint> &resource_map) {
  FillByResourceMap(resource_map);
}

ResourceSet::ResourceSet(const std::unordered_map<std::string, double> &resource_map) {
  std::unordered_map<std::string, FixedPoint> resource_map_fixed_point;
  for (auto &pair : resource_map) {
    resource_map_fixed_point[pair.first] = pair.second;
  }
  FillByResourceMap(resource_map_fixed_point);
}

ResourceSet::ResourceSet(const std::vector<std::string> &resource_labels,
                         const std::vector<double> resource_capacity) {
  std::unordered_map<std::string, FixedPoint> resource_map;
  for (size_t i = 0; i < resource_labels.size(); ++i) {
    resource_map[resource_labels[i]] = resource_capacity[i];
  }
  FillByResourceMap(resource_map);
}

bool ResourceSet::operator==(const ResourceSet &rhs) const {
  return (this->IsSubset(rhs) && rhs.IsSubset(*this));
}

/// Test whether this ResourceSet is precisely equal to the other ResourceSet.
bool ResourceSet::IsEqual(const ResourceSet &rhs) const {
  return (this->IsSubset(rhs) && rhs.IsSubset(*this));
}

FixedPoint ResourceSet::GetPredefinedResource(PredefinedResources resource) const {
  return predefined_resources_[resource];
}

FixedPoint ResourceSet::GetPredefinedResource(int resource) const {
  return predefined_resources_[static_cast<PredefinedResources>(resource)];
}

FixedPoint ResourceSet::GetCustomResource(int64_t resource_id) const {
  auto iter = custom_resources_.find(resource_id);
  if (iter != custom_resources_.end()) {
    return iter->second;
  }
  return 0;
}

bool ResourceSet::IsSubset(const ResourceSet &other) const {
  // Check to make sure all keys of this are in other.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    const FixedPoint &lhs_quantity = predefined_resources_[i];
    const FixedPoint &rhs_quantity = other.GetPredefinedResource(i);
    if (lhs_quantity > rhs_quantity) {
      // Resource found in rhs, but lhs capacity exceeds rhs capacity.
      return false;
    }
  }
  for (const auto &resource_pair : custom_resources_) {
    const int64_t resource_id = resource_pair.first;
    const FixedPoint &lhs_quantity = resource_pair.second;
    const FixedPoint &rhs_quantity = other.GetCustomResource(resource_id);
    if (lhs_quantity > rhs_quantity) {
      // Resource found in rhs, but lhs capacity exceeds rhs capacity.
      return false;
    }
  }
  return true;
}

bool ResourceSet::IsSuperset(const ResourceSet &other) const {
  return other.IsSubset(*this);
}

void ResourceSet::AddOrUpdateResource(const std::string &resource_name,
                                      const FixedPoint &capacity) {
  if (capacity <= 0) {
    return;
  }

  auto iter = predefined_resource_name_enum_map.find(resource_name);
  if (iter != predefined_resource_name_enum_map.end()) {
    predefined_resources_[iter->second] = capacity;
  } else {
    int64_t id = string_id_map_.Insert(resource_name);
    custom_resources_[id] = capacity;
  }
}

bool ResourceSet::DeleteResource(const std::string &resource_name) {
  auto iter = predefined_resource_name_enum_map.find(resource_name);
  if (iter != predefined_resource_name_enum_map.end()) {
    predefined_resources_[iter->second] = 0;
    return true;
  } else {
    int64_t id = string_id_map_.Get(resource_name);
    if (id == -1) {
      return false;
    }
    custom_resources_.erase(id);
    return true;
  }
}

const std::unordered_map<int64_t, FixedPoint> &ResourceSet::GetCustomResourceAmountMap()
    const {
  return custom_resources_;
}

// Add a set of resources to the current set of resources subject to upper limits on
// capacity from the total_resource set
void ResourceSet::AddResourcesCapacityConstrained(const ResourceSet &other,
                                                  const ResourceSet &total_resources) {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    // ResourceSet must have predefind resources
    const FixedPoint &total_capacity = total_resources.GetPredefinedResource(i);
    const FixedPoint &to_add_resource_capacity = other.GetPredefinedResource(i);
    if (total_capacity > 0) {
      predefined_resources_[i] =
          std::min(predefined_resources_[i] + to_add_resource_capacity, total_capacity);
    } else {
      // Resource does not exist in the total map, it probably got deleted from the total.
      // Don't panic, do nothing and simply continue.
      RAY_LOG(DEBUG) << "[AddResourcesCapacityConstrained] Predefined resource " << i
                     << " not found in the total resource map. It probably got deleted, "
                        "not adding back to resource_capacity_.";
    }
  }
  for (const auto &resource_pair : other.GetCustomResourceAmountMap()) {
    const int64_t to_add_resource_id = resource_pair.first;
    const FixedPoint &to_add_resource_capacity = resource_pair.second;
    if (total_resources.GetCustomResource(to_add_resource_id) != 0) {
      // If resource exists in total map, add to the local capacity map.
      // If the new capacity is less than the total capacity, set the new capacity to
      // the local capacity (capping to the total).
      const FixedPoint &total_capacity =
          total_resources.GetCustomResource(to_add_resource_id);
      custom_resources_[to_add_resource_id] =
          std::min(custom_resources_[to_add_resource_id] + to_add_resource_capacity,
                   total_capacity);
    } else {
      // Resource does not exist in the total map, it probably got deleted from the total.
      // Don't panic, do nothing and simply continue.
      RAY_LOG(DEBUG) << "[AddResourcesCapacityConstrained] Resource "
                     << to_add_resource_id
                     << " not found in the total resource map. It probably got deleted, "
                        "not adding back to resource_capacity_.";
    }
  }
}

// Perform an outer join.
void ResourceSet::AddResources(const ResourceSet &other) {
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    predefined_resources_[i] += other.GetPredefinedResource(i);
  }
  for (const auto &resource_pair : other.GetCustomResourceAmountMap()) {
    const int64_t resource_id = resource_pair.first;
    const FixedPoint &resource_capacity = resource_pair.second;
    custom_resources_[resource_id] += resource_capacity;
  }
}

void ResourceSet::SubtractResources(const ResourceSet &other) {
  // Subtract the resources, make sure none goes below zero and delete any if new capacity
  // is zero.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    predefined_resources_[i] = std::max(
        predefined_resources_[i] - other.GetPredefinedResource(i), FixedPoint(0));
  }
  for (const auto &resource_pair : other.GetCustomResourceAmountMap()) {
    const int64_t resource_id = resource_pair.first;
    const FixedPoint &resource_capacity = resource_pair.second;
    if (custom_resources_.count(resource_id) == 1) {
      custom_resources_[resource_id] -= resource_capacity;
    }
    if (custom_resources_[resource_id] <= 0) {
      custom_resources_.erase(resource_id);
    }
  }
}

void ResourceSet::SubtractResourcesStrict(const ResourceSet &other) {
  // Subtract the resources, make sure none goes below zero and delete any if new capacity
  // is zero.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    predefined_resources_[i] -= other.GetPredefinedResource(i);
    RAY_CHECK(predefined_resources_[i] >= 0)
        << "Capacity of resource after subtraction is negative, "
        << predefined_resources_[i].Double() << ".";
  }
  for (const auto &resource_pair : other.GetCustomResourceAmountMap()) {
    const int64_t resource_id = resource_pair.first;
    const FixedPoint &resource_capacity = resource_pair.second;
    RAY_CHECK(custom_resources_.count(resource_id) == 1)
        << "Attempt to acquire unknown resource: " << resource_id << " capacity "
        << resource_capacity.Double();
    custom_resources_[resource_id] -= resource_capacity;

    // Ensure that quantity is positive. Note, we have to have the check before
    // erasing the object to make sure that it doesn't get added back.
    RAY_CHECK(custom_resources_[resource_id] >= 0)
        << "Capacity of resource after subtraction is negative, "
        << custom_resources_[resource_id].Double() << ".";

    if (custom_resources_[resource_id] == 0) {
      custom_resources_.erase(resource_id);
    }
  }
}

FixedPoint ResourceSet::GetResource(const std::string &resource_name) const {
  auto iter = predefined_resource_name_enum_map.find(resource_name);
  if (iter != predefined_resource_name_enum_map.end()) {
    return predefined_resources_[iter->second];
  } else {
    int64_t id = string_id_map_.Get(resource_name);
    if (id == -1) {
      return 0;
    }
    return custom_resources_.at(id);
  }
}

void ResourceSet::SetPredefinedResources(PredefinedResources resource,
                                         FixedPoint quantity) {
  predefined_resources_[resource] = quantity;
}

const ResourceSet ResourceSet::GetNumCpus() const {
  ResourceSet cpu_resource_set;
  const FixedPoint cpu_quantity = GetPredefinedResource(CPU);
  if (cpu_quantity > 0) {
    cpu_resource_set.SetPredefinedResources(CPU, cpu_quantity);
  }
  return cpu_resource_set;
}

bool ResourceSet::IsEmpty() const {
  // Check whether the capacity of each resource type is zero. Exit early if not.
  for (auto &it : predefined_resources_) {
    if (it != 0) {
      return false;
    }
  }
  return custom_resources_.empty();
}

const std::unordered_map<std::string, double> ResourceSet::GetResourceMap() const {
  std::unordered_map<std::string, double> result;

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (predefined_resources_[i] <= 0) {
      continue;
    }
    result[resource_labels[i]] = predefined_resources_[i].Double();
  }
  for (const auto &resource_pair : custom_resources_) {
    result[string_id_map_.Get(resource_pair.first)] = resource_pair.second.Double();
  }
  return result;
}

const std::unordered_map<std::string, FixedPoint> ResourceSet::GetResourceAmountMap()
    const {
  std::unordered_map<std::string, FixedPoint> result;

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (predefined_resources_[i] <= 0) {
      continue;
    }
    result[resource_labels[i]] = predefined_resources_[i];
  }
  for (const auto &resource_pair : custom_resources_) {
    result[string_id_map_.Get(resource_pair.first)] = resource_pair.second;
  }
  return result;
}

const std::string ResourceSet::ToString() const {
  std::string return_string = "";

  // Convert the first element to a string.
  double resource_amount = predefined_resources_[0].Double();
  return_string += ", {" + resource_labels[0] + ": " +
                   format_resource(resource_labels[0], resource_amount) + "}";

  // Add the remaining elements to the string (along with a comma).
  for (size_t i = 1; i < PredefinedResources_MAX; i++) {
    double resource_amount = predefined_resources_[i].Double();
    return_string += ", {" + resource_labels[i] + ": " +
                     format_resource(resource_labels[i], resource_amount) + "}";
  }

  for (auto it = custom_resources_.begin(); it != custom_resources_.end(); ++it) {
    double resource_amount = (it->second).Double();
    return_string += ", {" + string_id_map_.Get(it->first) + ": " +
                     format_resource(string_id_map_.Get(it->first), resource_amount) +
                     "}";
  }

  return return_string;
}
