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

#include "ray/raylet/scheduling/local_resource_manager.h"

#include <boost/algorithm/string.hpp>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"

namespace ray {

LocalResourceManager::LocalResourceManager(
    int64_t local_node_id, StringIdMap &resource_name_to_id,
    const NodeResources &node_resources,
    std::function<int64_t(void)> get_used_object_store_memory,
    std::function<bool(void)> get_pull_manager_at_capacity,
    std::function<void(const NodeResources &)> resource_change_subscriber)
    : local_node_id_(local_node_id),
      resource_name_to_id_(resource_name_to_id),
      get_used_object_store_memory_(get_used_object_store_memory),
      get_pull_manager_at_capacity_(get_pull_manager_at_capacity),
      resource_change_subscriber_(resource_change_subscriber) {
  InitResourceUnitInstanceInfo();
  InitLocalResources(node_resources);
  RAY_LOG(DEBUG) << "local resources: "
                 << local_resources_.DebugString(resource_name_to_id);
}

void LocalResourceManager::InitResourceUnitInstanceInfo() {
  std::string predefined_unit_instance_resources =
      RayConfig::instance().predefined_unit_instance_resources();
  if (!predefined_unit_instance_resources.empty()) {
    std::vector<std::string> results;
    boost::split(results, predefined_unit_instance_resources, boost::is_any_of(","));
    for (std::string &result : results) {
      PredefinedResources resource = ResourceStringToEnum(result);
      RAY_CHECK(resource < PredefinedResources_MAX)
          << "Failed to parse predefined resource";
      predefined_unit_instance_resources_.emplace(resource);
    }
  }
  std::string custom_unit_instance_resources =
      RayConfig::instance().custom_unit_instance_resources();
  if (!custom_unit_instance_resources.empty()) {
    std::vector<std::string> results;
    boost::split(results, custom_unit_instance_resources, boost::is_any_of(","));
    for (std::string &result : results) {
      int64_t resource_id = resource_name_to_id_.Insert(result);
      custom_unit_instance_resources_.emplace(resource_id);
    }
  }
}

void LocalResourceManager::AddLocalResourceInstances(
    const std::string &resource_name, const std::vector<FixedPoint> &instances) {
  ResourceInstanceCapacities *node_instances;
  local_resources_.predefined_resources.resize(PredefinedResources_MAX);
  if (kCPU_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[CPU];
  } else if (kGPU_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[GPU];
  } else if (kObjectStoreMemory_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[OBJECT_STORE_MEM];
  } else if (kMemory_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[MEM];
  } else {
    resource_name_to_id_.Insert(resource_name);
    int64_t resource_id = resource_name_to_id_.Get(resource_name);
    node_instances = &local_resources_.custom_resources[resource_id];
  }

  if (node_instances->total.size() < instances.size()) {
    node_instances->total.resize(instances.size());
    node_instances->available.resize(instances.size());
  }

  for (size_t i = 0; i < instances.size(); i++) {
    node_instances->available[i] += instances[i];
    node_instances->total[i] += instances[i];
  }
  OnResourceChanged();
}

void LocalResourceManager::DeleteLocalResource(const std::string &resource_name) {
  int idx = GetPredefinedResourceIndex(resource_name);
  if (idx != -1) {
    for (auto &total : local_resources_.predefined_resources[idx].total) {
      total = 0;
    }
    for (auto &available : local_resources_.predefined_resources[idx].available) {
      available = 0;
    }
  } else {
    int64_t resource_id = resource_name_to_id_.Get(resource_name);
    auto c_itr = local_resources_.custom_resources.find(resource_id);
    if (c_itr != local_resources_.custom_resources.end()) {
      local_resources_.custom_resources[resource_id].total.clear();
      local_resources_.custom_resources[resource_id].available.clear();
      local_resources_.custom_resources.erase(c_itr);
    }
  }
  OnResourceChanged();
}

bool LocalResourceManager::IsAvailableResourceEmpty(const std::string &resource_name) {
  int idx = GetPredefinedResourceIndex(resource_name);

  if (idx != -1) {
    return FixedPoint::Sum(local_resources_.predefined_resources[idx].available) <= 0;
  }
  resource_name_to_id_.Insert(resource_name);
  int64_t resource_id = resource_name_to_id_.Get(resource_name);
  auto itr = local_resources_.custom_resources.find(resource_id);
  if (itr != local_resources_.custom_resources.end()) {
    return FixedPoint::Sum(itr->second.available) <= 0;
  } else {
    return true;
  }
}

std::string LocalResourceManager::DebugString(void) const {
  std::stringstream buffer;
  buffer << local_resources_.DebugString(resource_name_to_id_);
  return buffer.str();
}

uint64_t LocalResourceManager::GetNumCpus() const {
  return static_cast<uint64_t>(
      FixedPoint::Sum(local_resources_.predefined_resources[CPU].total).Double());
}

void LocalResourceManager::InitResourceInstances(
    FixedPoint total, bool unit_instances, ResourceInstanceCapacities *instance_list) {
  if (unit_instances) {
    size_t num_instances = static_cast<size_t>(total.Double());
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

void LocalResourceManager::InitLocalResources(const NodeResources &node_resources) {
  local_resources_.predefined_resources.resize(PredefinedResources_MAX);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (node_resources.predefined_resources[i].total > 0) {
      // when we enable cpushare, the CPU will not be treat as unit_instance.
      bool is_unit_instance = predefined_unit_instance_resources_.find(i) !=
                              predefined_unit_instance_resources_.end();
      InitResourceInstances(node_resources.predefined_resources[i].total,
                            is_unit_instance, &local_resources_.predefined_resources[i]);
    }
  }

  if (node_resources.custom_resources.size() == 0) {
    return;
  }

  for (auto it = node_resources.custom_resources.begin();
       it != node_resources.custom_resources.end(); ++it) {
    if (it->second.total > 0) {
      bool is_unit_instance = custom_unit_instance_resources_.find(it->first) !=
                              custom_unit_instance_resources_.end();
      ResourceInstanceCapacities instance_list;
      InitResourceInstances(it->second.total, is_unit_instance, &instance_list);
      local_resources_.custom_resources.emplace(it->first, instance_list);
    }
  }
}

std::vector<FixedPoint> LocalResourceManager::AddAvailableResourceInstances(
    std::vector<FixedPoint> available,
    ResourceInstanceCapacities *resource_instances) const {
  std::vector<FixedPoint> overflow(available.size(), 0.);
  for (size_t i = 0; i < available.size(); i++) {
    resource_instances->available[i] = resource_instances->available[i] + available[i];
    if (resource_instances->available[i] > resource_instances->total[i]) {
      overflow[i] = (resource_instances->available[i] - resource_instances->total[i]);
      resource_instances->available[i] = resource_instances->total[i];
    }
  }

  return overflow;
}

std::vector<FixedPoint> LocalResourceManager::SubtractAvailableResourceInstances(
    std::vector<FixedPoint> available, ResourceInstanceCapacities *resource_instances,
    bool allow_going_negative) const {
  RAY_CHECK(available.size() == resource_instances->available.size());

  std::vector<FixedPoint> underflow(available.size(), 0.);
  for (size_t i = 0; i < available.size(); i++) {
    if (resource_instances->available[i] < 0) {
      if (allow_going_negative) {
        resource_instances->available[i] =
            resource_instances->available[i] - available[i];
      } else {
        underflow[i] = available[i];  // No change in the value in this case.
      }
    } else {
      resource_instances->available[i] = resource_instances->available[i] - available[i];
      if (resource_instances->available[i] < 0 && !allow_going_negative) {
        underflow[i] = -resource_instances->available[i];
        resource_instances->available[i] = 0;
      }
    }
  }
  return underflow;
}

bool LocalResourceManager::AllocateResourceInstances(
    FixedPoint demand, std::vector<FixedPoint> &available,
    std::vector<FixedPoint> *allocation) const {
  allocation->resize(available.size());
  FixedPoint remaining_demand = demand;

  if (available.size() == 1) {
    // This resource has just an instance.
    if (available[0] >= remaining_demand) {
      available[0] -= remaining_demand;
      (*allocation)[0] = remaining_demand;
      return true;
    } else {
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

  if (remaining_demand >= 1.) {
    // Cannot satisfy a demand greater than one if no unit capacity resource is available.
    return false;
  }

  // Remaining demand is fractional. Find the best fit, if exists.
  if (remaining_demand > 0.) {
    int64_t idx_best_fit = -1;
    FixedPoint available_best_fit = 1.;
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

bool LocalResourceManager::AllocateTaskResourceInstances(
    const ResourceRequest &resource_request,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  task_allocation->predefined_resources.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (resource_request.predefined_resources[i] > 0) {
      if (!AllocateResourceInstances(resource_request.predefined_resources[i],
                                     local_resources_.predefined_resources[i].available,
                                     &task_allocation->predefined_resources[i])) {
        // Allocation failed. Restore node's local resources by freeing the resources
        // of the failed allocation.
        FreeTaskResourceInstances(task_allocation);
        return false;
      }
    }
  }

  for (const auto &task_req_custom_resource : resource_request.custom_resources) {
    auto it = local_resources_.custom_resources.find(task_req_custom_resource.first);
    if (it != local_resources_.custom_resources.end()) {
      if (task_req_custom_resource.second > 0) {
        std::vector<FixedPoint> allocation;
        bool success = AllocateResourceInstances(task_req_custom_resource.second,
                                                 it->second.available, &allocation);
        // Even if allocation failed we need to remember partial allocations to correctly
        // free resources.
        task_allocation->custom_resources.emplace(it->first, allocation);
        if (!success) {
          // Allocation failed. Restore node's local resources by freeing the resources
          // of the failed allocation.
          FreeTaskResourceInstances(task_allocation);
          return false;
        }
      }
    } else {
      // Allocation failed because the custom resources don't exist in this local node.
      // Restore node's local resources by freeing the resources
      // of the failed allocation.
      FreeTaskResourceInstances(task_allocation);
      return false;
    }
  }
  return true;
}

void LocalResourceManager::FreeTaskResourceInstances(
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    AddAvailableResourceInstances(task_allocation->predefined_resources[i],
                                  &local_resources_.predefined_resources[i]);
  }

  for (const auto &task_allocation_custom_resource : task_allocation->custom_resources) {
    auto it =
        local_resources_.custom_resources.find(task_allocation_custom_resource.first);
    if (it != local_resources_.custom_resources.end()) {
      AddAvailableResourceInstances(task_allocation_custom_resource.second, &it->second);
    }
  }
}

std::vector<double> LocalResourceManager::AddCPUResourceInstances(
    std::vector<double> &cpu_instances) {
  std::vector<FixedPoint> cpu_instances_fp =
      VectorDoubleToVectorFixedPoint(cpu_instances);

  if (cpu_instances.size() == 0) {
    return cpu_instances;  // No overflow.
  }

  auto overflow = AddAvailableResourceInstances(
      cpu_instances_fp, &local_resources_.predefined_resources[CPU]);
  OnResourceChanged();

  return VectorFixedPointToVectorDouble(overflow);
}

std::vector<double> LocalResourceManager::SubtractCPUResourceInstances(
    std::vector<double> &cpu_instances, bool allow_going_negative) {
  std::vector<FixedPoint> cpu_instances_fp =
      VectorDoubleToVectorFixedPoint(cpu_instances);

  if (cpu_instances.size() == 0) {
    return cpu_instances;  // No underflow.
  }

  auto underflow = SubtractAvailableResourceInstances(
      cpu_instances_fp, &local_resources_.predefined_resources[CPU],
      allow_going_negative);
  OnResourceChanged();

  return VectorFixedPointToVectorDouble(underflow);
}

std::vector<double> LocalResourceManager::AddGPUResourceInstances(
    std::vector<double> &gpu_instances) {
  std::vector<FixedPoint> gpu_instances_fp =
      VectorDoubleToVectorFixedPoint(gpu_instances);

  if (gpu_instances.size() == 0) {
    return gpu_instances;  // No overflow.
  }

  auto overflow = AddAvailableResourceInstances(
      gpu_instances_fp, &local_resources_.predefined_resources[GPU]);
  OnResourceChanged();

  return VectorFixedPointToVectorDouble(overflow);
}

std::vector<double> LocalResourceManager::SubtractGPUResourceInstances(
    std::vector<double> &gpu_instances) {
  std::vector<FixedPoint> gpu_instances_fp =
      VectorDoubleToVectorFixedPoint(gpu_instances);

  if (gpu_instances.size() == 0) {
    return gpu_instances;  // No underflow.
  }

  auto underflow = SubtractAvailableResourceInstances(
      gpu_instances_fp, &local_resources_.predefined_resources[GPU]);
  OnResourceChanged();

  return VectorFixedPointToVectorDouble(underflow);
}

bool LocalResourceManager::AllocateLocalTaskResources(
    const ResourceRequest &resource_request,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  if (AllocateTaskResourceInstances(resource_request, task_allocation)) {
    OnResourceChanged();
    return true;
  }
  return false;
}

bool LocalResourceManager::AllocateLocalTaskResources(
    const absl::flat_hash_map<std::string, double> &task_resources,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  // We don't track object store memory demands so no need to allocate them.
  ResourceRequest resource_request = ResourceMapToResourceRequest(
      resource_name_to_id_, task_resources, /*requires_object_store_memory=*/false);
  return AllocateLocalTaskResources(resource_request, task_allocation);
}

void LocalResourceManager::ReleaseWorkerResources(
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  if (task_allocation == nullptr || task_allocation->IsEmpty()) {
    return;
  }
  FreeTaskResourceInstances(task_allocation);
  OnResourceChanged();
}

namespace {

NodeResources ToNodeResources(const NodeResourceInstances &instance) {
  NodeResources node_resources;
  node_resources.predefined_resources.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    node_resources.predefined_resources[i].available = 0;
    node_resources.predefined_resources[i].total = 0;
    for (size_t j = 0; j < instance.predefined_resources[i].available.size(); j++) {
      node_resources.predefined_resources[i].available +=
          instance.predefined_resources[i].available[j];
      node_resources.predefined_resources[i].total +=
          instance.predefined_resources[i].total[j];
    }
  }

  for (auto &custom_resource : instance.custom_resources) {
    int64_t resource_name = custom_resource.first;
    auto &instances = custom_resource.second;

    FixedPoint available = std::accumulate(instances.available.begin(),
                                           instances.available.end(), FixedPoint());
    FixedPoint total =
        std::accumulate(instances.total.begin(), instances.total.end(), FixedPoint());

    node_resources.custom_resources[resource_name].available = available;
    node_resources.custom_resources[resource_name].total = total;
  }
  return node_resources;
}

}  // namespace

void LocalResourceManager::FillResourceUsage(rpc::ResourcesData &resources_data) {
  NodeResources resources = ToNodeResources(local_resources_);

  // Initialize if last report resources is empty.
  if (!last_report_resources_) {
    NodeResources node_resources =
        ResourceMapToNodeResources(resource_name_to_id_, {{}}, {{}});
    last_report_resources_.reset(new NodeResources(node_resources));
  }

  // Automatically report object store usage.
  // XXX: this MUTATES the resources field, which is needed since we are storing
  // it in last_report_resources_.
  if (get_used_object_store_memory_ != nullptr) {
    auto &capacity = resources.predefined_resources[OBJECT_STORE_MEM];
    double used = get_used_object_store_memory_();
    capacity.available = FixedPoint(capacity.total.Double() - used);
  }

  for (int i = 0; i < PredefinedResources_MAX; i++) {
    const auto &label = ResourceEnumToString((PredefinedResources)i);
    const auto &capacity = resources.predefined_resources[i];
    const auto &last_capacity = last_report_resources_->predefined_resources[i];
    // Note: available may be negative, but only report positive to GCS.
    if (capacity.available != last_capacity.available && capacity.available > 0) {
      resources_data.set_resources_available_changed(true);
      (*resources_data.mutable_resources_available())[label] =
          capacity.available.Double();
    }
    if (capacity.total != last_capacity.total) {
      (*resources_data.mutable_resources_total())[label] = capacity.total.Double();
    }
  }
  for (const auto &it : resources.custom_resources) {
    uint64_t custom_id = it.first;
    const auto &capacity = it.second;
    const auto &last_capacity = last_report_resources_->custom_resources[custom_id];
    const auto &label = resource_name_to_id_.Get(custom_id);
    // Note: available may be negative, but only report positive to GCS.
    if (capacity.available != last_capacity.available && capacity.available > 0) {
      resources_data.set_resources_available_changed(true);
      (*resources_data.mutable_resources_available())[label] =
          capacity.available.Double();
    }
    if (capacity.total != last_capacity.total) {
      (*resources_data.mutable_resources_total())[label] = capacity.total.Double();
    }
  }

  if (get_pull_manager_at_capacity_ != nullptr) {
    resources.object_pulls_queued = get_pull_manager_at_capacity_();
    if (last_report_resources_->object_pulls_queued != resources.object_pulls_queued) {
      resources_data.set_object_pulls_queued(resources.object_pulls_queued);
      resources_data.set_resources_available_changed(true);
    }
  }

  if (resources != *last_report_resources_.get()) {
    last_report_resources_.reset(new NodeResources(resources));
  }

  if (!RayConfig::instance().enable_light_weight_resource_report()) {
    resources_data.set_resources_available_changed(true);
  }
}

double LocalResourceManager::GetLocalAvailableCpus() const {
  auto &capacity = local_resources_.predefined_resources[CPU];
  return FixedPoint::Sum(capacity.available).Double();
}

ray::gcs::NodeResourceInfoAccessor::ResourceMap LocalResourceManager::GetResourceTotals(
    const absl::flat_hash_map<std::string, double> &resource_map_filter) const {
  ray::gcs::NodeResourceInfoAccessor::ResourceMap map;
  for (size_t i = 0; i < local_resources_.predefined_resources.size(); i++) {
    std::string resource_name = ResourceEnumToString(static_cast<PredefinedResources>(i));
    double resource_total =
        FixedPoint::Sum(local_resources_.predefined_resources[i].total).Double();
    if (!resource_map_filter.contains(resource_name)) {
      continue;
    }

    if (resource_total > 0) {
      auto data = std::make_shared<rpc::ResourceTableData>();
      data->set_resource_capacity(resource_total);
      map.emplace(resource_name, std::move(data));
    }
  }

  for (auto entry : local_resources_.custom_resources) {
    std::string resource_name = resource_name_to_id_.Get(entry.first);
    double resource_total = FixedPoint::Sum(entry.second.total).Double();
    if (!resource_map_filter.contains(resource_name)) {
      continue;
    }

    if (resource_total > 0) {
      auto data = std::make_shared<rpc::ResourceTableData>();
      data->set_resource_capacity(resource_total);
      map.emplace(resource_name, std::move(data));
    }
  }
  return map;
}

void LocalResourceManager::OnResourceChanged() {
  if (resource_change_subscriber_ == nullptr) {
    return;
  }
  resource_change_subscriber_(ToNodeResources(local_resources_));
}

std::string LocalResourceManager::SerializedTaskResourceInstances(
    std::shared_ptr<TaskResourceInstances> task_allocation) const {
  bool has_added_resource = false;
  std::stringstream buffer;
  buffer << "{";
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    std::vector<FixedPoint> resource = task_allocation->predefined_resources[i];
    if (resource.empty()) {
      continue;
    }
    if (has_added_resource) {
      buffer << ",";
    }
    std::string resource_name = ResourceEnumToString(static_cast<PredefinedResources>(i));
    buffer << "\"" << resource_name << "\":";
    bool is_unit_instance = predefined_unit_instance_resources_.find(i) !=
                            predefined_unit_instance_resources_.end();
    if (!is_unit_instance) {
      buffer << resource[0];
    } else {
      buffer << "[";
      for (size_t i = 0; i < resource.size(); i++) {
        buffer << resource[i];
        if (i < resource.size() - 1) {
          buffer << ", ";
        }
      }
      buffer << "]";
    }
    has_added_resource = true;
  }
  // TODO (chenk008): add custom_resources
  buffer << "}";
  return buffer.str();
}

void LocalResourceManager::ResetLastReportResourceUsage(
    const SchedulingResources &replacement) {
  last_report_resources_ = std::make_unique<NodeResources>(ResourceMapToNodeResources(
      resource_name_to_id_, replacement.GetTotalResources().GetResourceMap(),
      replacement.GetAvailableResources().GetResourceMap()));
}

bool LocalResourceManager::ResourcesExist(const std::string &resource_name) {
  int idx = GetPredefinedResourceIndex(resource_name);
  if (idx != -1) {
    // Return true directly for predefined resources as we always initialize this kind of
    // resources at the beginning.
    return true;
  } else {
    int64_t resource_id = resource_name_to_id_.Get(resource_name);
    const auto &it = local_resources_.custom_resources.find(resource_id);
    return it != local_resources_.custom_resources.end();
  }
}

int GetPredefinedResourceIndex(const std::string &resource_name) {
  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)ray::CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)ray::GPU;
  } else if (resource_name == ray::kObjectStoreMemory_ResourceLabel) {
    idx = (int)ray::OBJECT_STORE_MEM;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)ray::MEM;
  };
  return idx;
}

}  // namespace ray
