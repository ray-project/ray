#include "ray/raylet/scheduling/cluster_resource_data.h"

const std::string resource_labels[] = {ray::kCPU_ResourceLabel,
                                       ray::kMemory_ResourceLabel,
                                       ray::kGPU_ResourceLabel, ray::kTPU_ResourceLabel};

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
  size_t i = 0;

  TaskRequest task_request;

  task_request.predefined_resources.resize(PredefinedResources_MAX);
  task_request.custom_resources.resize(resource_map.size());
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    task_request.predefined_resources[0].demand = 0;
    task_request.predefined_resources[0].soft = false;
  }

  for (auto const &resource : resource_map) {
    if (resource.first == ray::kCPU_ResourceLabel) {
      task_request.predefined_resources[CPU].demand = resource.second;
    } else if (resource.first == ray::kGPU_ResourceLabel) {
      task_request.predefined_resources[GPU].demand = resource.second;
    } else if (resource.first == ray::kTPU_ResourceLabel) {
      task_request.predefined_resources[TPU].demand = resource.second;
    } else if (resource.first == ray::kMemory_ResourceLabel) {
      task_request.predefined_resources[MEM].demand = resource.second;
    } else {
      string_to_int_map.Insert(resource.first);
      task_request.custom_resources[i].id = string_to_int_map.Get(resource.first);
      task_request.custom_resources[i].demand = resource.second;
      task_request.custom_resources[i].soft = false;
      i++;
    }
  }
  task_request.custom_resources.resize(i);

  return task_request;
}

TaskRequest TaskResourceInstances::ToTaskRequest() const {
  TaskRequest task_req;
  task_req.predefined_resources.resize(PredefinedResources_MAX);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    task_req.predefined_resources[i].demand = 0;
    for (auto predefined_resource_instance : this->predefined_resources[i]) {
      task_req.predefined_resources[i].demand += predefined_resource_instance;
    }
  }

  task_req.custom_resources.resize(this->custom_resources.size());
  size_t i = 0;
  for (auto it = this->custom_resources.begin(); it != this->custom_resources.end();
       ++it) {
    task_req.custom_resources[i].id = it->first;
    task_req.custom_resources[i].soft = false;
    task_req.custom_resources[i].demand = 0;
    for (size_t j = 0; j < it->second.size(); j++) {
      task_req.custom_resources[i].demand += it->second[j];
    }
    i++;
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
    } else if (resource.first == ray::kTPU_ResourceLabel) {
      node_resources.predefined_resources[TPU] = resource_capacity;
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
    case TPU:
      buffer << "TPU: ";
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
    // Convert to 50MiB chunks and then to GiB
    return std::to_string(quantity * (50 * 1024 * 1024) / (1024 * 1024 * 1024)) + " GiB";
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
    case TPU:
      name = "TPU";
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
    case TPU:
      buffer << "TPU: ";
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
    if (this->predefined_resources[i].demand != 0) {
      return false;
    }
  }
  for (size_t i = 0; i < this->custom_resources.size(); i++) {
    if (this->custom_resources[i].demand != 0) {
      return false;
    }
  }
  return true;
}

std::string TaskRequest::DebugString() const {
  std::stringstream buffer;
  buffer << " {";
  for (size_t i = 0; i < this->predefined_resources.size(); i++) {
    buffer << "(" << this->predefined_resources[i].demand << ":"
           << this->predefined_resources[i].soft << ") ";
  }
  buffer << "}";

  buffer << "  [";
  for (size_t i = 0; i < this->custom_resources.size(); i++) {
    buffer << this->custom_resources[i].id << ":"
           << "(" << this->custom_resources[i].demand << ":"
           << this->custom_resources[i].soft << ") ";
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
