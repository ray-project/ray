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
    scheduling::NodeID local_node_id,
    const NodeResources &node_resources,
    std::function<int64_t(void)> get_used_object_store_memory,
    std::function<bool(void)> get_pull_manager_at_capacity,
    std::function<void(const NodeResources &)> resource_change_subscriber)
    : local_node_id_(local_node_id),
      get_used_object_store_memory_(get_used_object_store_memory),
      get_pull_manager_at_capacity_(get_pull_manager_at_capacity),
      resource_change_subscriber_(resource_change_subscriber) {
  local_resources_.available = TaskResourceInstances(node_resources.available);
  local_resources_.total = TaskResourceInstances(node_resources.total);
  RAY_LOG(DEBUG) << "local resources: " << local_resources_.DebugString();
}

void LocalResourceManager::AddLocalResourceInstances(
    scheduling::ResourceID resource_id, const std::vector<FixedPoint> &instances) {
  local_resources_.available.Add(resource_id, instances);
  local_resources_.total.Add(resource_id, instances);
  OnResourceChanged();
}

void LocalResourceManager::DeleteLocalResource(scheduling::ResourceID resource_id) {
  local_resources_.available.Remove(resource_id);
  local_resources_.total.Remove(resource_id);
  OnResourceChanged();
}

bool LocalResourceManager::IsAvailableResourceEmpty(
    scheduling::ResourceID resource_id) const {
  return local_resources_.available.Sum(resource_id) <= 0;
}

std::string LocalResourceManager::DebugString(void) const {
  std::stringstream buffer;
  buffer << local_resources_.DebugString();
  return buffer.str();
}

uint64_t LocalResourceManager::GetNumCpus() const {
  return static_cast<uint64_t>(local_resources_.total.Sum(ResourceID::CPU()).Double());
}

std::vector<FixedPoint> LocalResourceManager::AddAvailableResourceInstances(
    const std::vector<FixedPoint> &available,
    const std::vector<FixedPoint> &local_total,
    std::vector<FixedPoint> &local_available) const {
  RAY_CHECK(available.size() == local_available.size())
      << available.size() << ", " << local_available.size();
  std::vector<FixedPoint> overflow(available.size(), 0.);
  for (size_t i = 0; i < available.size(); i++) {
    local_available[i] = local_available[i] + available[i];
    if (local_available[i] > local_total[i]) {
      overflow[i] = (local_available[i] - local_total[i]);
      local_available[i] = local_total[i];
    }
  }

  return overflow;
}

std::vector<FixedPoint> LocalResourceManager::SubtractAvailableResourceInstances(
    const std::vector<FixedPoint> &available,
    std::vector<FixedPoint> &local_available,
    bool allow_going_negative) const {
  RAY_CHECK(available.size() == local_available.size());

  std::vector<FixedPoint> underflow(available.size(), 0.);
  for (size_t i = 0; i < available.size(); i++) {
    if (local_available[i] < 0) {
      if (allow_going_negative) {
        local_available[i] = local_available[i] - available[i];
      } else {
        underflow[i] = available[i];  // No change in the value in this case.
      }
    } else {
      local_available[i] = local_available[i] - available[i];
      if (local_available[i] < 0 && !allow_going_negative) {
        underflow[i] = -local_available[i];
        local_available[i] = 0;
      }
    }
  }
  return underflow;
}

bool LocalResourceManager::AllocateResourceInstances(
    FixedPoint demand,
    std::vector<FixedPoint> &available,
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
  for (auto &resource_id : resource_request.ResourceIds()) {
    bool success = true;
    if (!local_resources_.available.Has(resource_id)) {
      success = false;
    } else {
      auto demand = resource_request.Get(resource_id);
      auto &available = local_resources_.available.GetMutable(resource_id);
      std::vector<FixedPoint> allocation;
      success = AllocateResourceInstances(demand, available, &allocation);
      // Even if allocation failed we need to remember partial allocations to correctly
      // free resources.
      task_allocation->Set(resource_id, allocation);
    }
    if (!success) {
      // Allocation failed. Restore node's local resources by freeing the resources
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
  for (auto &resource_id : task_allocation->ResourceIds()) {
    if (local_resources_.total.Has(resource_id)) {
      AddAvailableResourceInstances(task_allocation->Get(resource_id),
                                    local_resources_.total.GetMutable(resource_id),
                                    local_resources_.available.GetMutable(resource_id));
    }
  }
}

std::vector<double> LocalResourceManager::AddResourceInstances(
    scheduling::ResourceID resource_id, const std::vector<double> &resource_instances) {
  std::vector<FixedPoint> resource_instances_fp =
      FixedPointVectorFromDouble(resource_instances);

  if (resource_instances.size() == 0) {
    return resource_instances;  // No overflow.
  }

  auto overflow =
      AddAvailableResourceInstances(resource_instances_fp,
                                    local_resources_.total.GetMutable(resource_id),
                                    local_resources_.available.GetMutable(resource_id));
  OnResourceChanged();

  return FixedPointVectorToDouble(overflow);
}

std::vector<double> LocalResourceManager::SubtractResourceInstances(
    scheduling::ResourceID resource_id,
    const std::vector<double> &resource_instances,
    bool allow_going_negative) {
  std::vector<FixedPoint> resource_instances_fp =
      FixedPointVectorFromDouble(resource_instances);

  if (resource_instances.size() == 0) {
    return resource_instances;  // No underflow.
  }

  auto underflow = SubtractAvailableResourceInstances(
      resource_instances_fp,
      local_resources_.available.GetMutable(resource_id),
      allow_going_negative);
  OnResourceChanged();

  return FixedPointVectorToDouble(underflow);
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
      task_resources, /*requires_object_store_memory=*/false);
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
  node_resources.available = instance.available.ToResourceRequest();
  node_resources.total = instance.total.ToResourceRequest();
  return node_resources;
}

}  // namespace

void LocalResourceManager::UpdateAvailableObjectStoreMemResource() {
  // Update local object store usage and report to other raylets.
  if (get_used_object_store_memory_ == nullptr) {
    return;
  }

  auto &total_instances = local_resources_.total.Get(ResourceID::ObjectStoreMemory());
  RAY_CHECK_EQ(total_instances.size(), 1u);
  const double used = get_used_object_store_memory_();
  const double total = total_instances[0].Double();
  auto new_available =
      std::vector<FixedPoint>{FixedPoint(total >= used ? total - used : 0.0)};
  if (new_available != local_resources_.available.Get(ResourceID::ObjectStoreMemory())) {
    local_resources_.available.Set(ResourceID::ObjectStoreMemory(),
                                   std::move(new_available));
    OnResourceChanged();
  }
}

void LocalResourceManager::FillResourceUsage(rpc::ResourcesData &resources_data) {
  UpdateAvailableObjectStoreMemResource();

  NodeResources resources = ToNodeResources(local_resources_);

  // Initialize if last report resources is empty.
  if (!last_report_resources_) {
    NodeResources node_resources = ResourceMapToNodeResources({{}}, {{}});
    last_report_resources_.reset(new NodeResources(node_resources));
  }

  for (auto entry : resources.total.ToMap()) {
    auto resource_id = entry.first;
    auto label = ResourceID(resource_id).Binary();
    auto total = entry.second;
    auto available = resources.available.Get(resource_id);
    auto last_total = last_report_resources_->total.Get(resource_id);
    auto last_available = last_report_resources_->available.Get(resource_id);

    // Note: available may be negative, but only report positive to GCS.
    if (available != last_available && available > 0) {
      resources_data.set_resources_available_changed(true);
      (*resources_data.mutable_resources_available())[label] = available.Double();
    }
    if (total != last_total) {
      (*resources_data.mutable_resources_total())[label] = total.Double();
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
  return local_resources_.available.Sum(ResourceID::CPU()).Double();
}

std::optional<syncer::RaySyncMessage> LocalResourceManager::CreateSyncMessage(
    int64_t after_version, syncer::MessageType message_type) const {
  RAY_CHECK(message_type == syncer::MessageType::RESOURCE_VIEW);
  // We check the memory inside version, so version is not a const function.
  // Ideally, we need to move the memory check somewhere else.
  // TODO(iycheng): Make version as a const function.
  const_cast<LocalResourceManager *>(this)->UpdateAvailableObjectStoreMemResource();

  if (version_ <= after_version) {
    return std::nullopt;
  }

  syncer::RaySyncMessage msg;
  rpc::ResourcesData resources_data;

  resources_data.set_node_id(local_node_id_.Binary());

  NodeResources resources = ToNodeResources(local_resources_);

  for (auto entry : resources.total.ToMap()) {
    auto resource_id = entry.first;
    auto label = ResourceID(resource_id).Binary();
    auto total = entry.second;
    auto available = resources.available.Get(resource_id);

    resources_data.set_resources_available_changed(true);
    (*resources_data.mutable_resources_available())[label] = available.Double();
    (*resources_data.mutable_resources_total())[label] = total.Double();
  }

  if (get_pull_manager_at_capacity_ != nullptr) {
    resources.object_pulls_queued = get_pull_manager_at_capacity_();
    resources_data.set_object_pulls_queued(resources.object_pulls_queued);
    resources_data.set_resources_available_changed(true);
  }

  resources_data.set_resources_available_changed(true);

  msg.set_node_id(local_node_id_.Binary());
  msg.set_version(version_);
  msg.set_message_type(message_type);
  std::string serialized_msg;
  RAY_CHECK(resources_data.SerializeToString(&serialized_msg));
  msg.set_sync_message(std::move(serialized_msg));
  return std::make_optional(std::move(msg));
}

ray::gcs::NodeResourceInfoAccessor::ResourceMap LocalResourceManager::GetResourceTotals(
    const absl::flat_hash_map<std::string, double> &resource_map_filter) const {
  ray::gcs::NodeResourceInfoAccessor::ResourceMap map;
  for (auto &resource_id : local_resources_.total.ResourceIds()) {
    auto resource_name = resource_id.Binary();
    if (!resource_map_filter.contains(resource_name)) {
      continue;
    }
    auto resource_total = local_resources_.total.Sum(resource_id);
    if (resource_total > 0) {
      auto data = std::make_shared<rpc::ResourceTableData>();
      data->set_resource_capacity(resource_total.Double());
      map.emplace(resource_name, std::move(data));
    }
  }
  return map;
}

void LocalResourceManager::OnResourceChanged() {
  ++version_;
  if (resource_change_subscriber_ == nullptr) {
    return;
  }
  resource_change_subscriber_(ToNodeResources(local_resources_));
}

void LocalResourceManager::ResetLastReportResourceUsage(
    const NodeResources &replacement) {
  last_report_resources_.reset(new NodeResources(replacement));
}

bool LocalResourceManager::ResourcesExist(scheduling::ResourceID resource_id) const {
  return local_resources_.total.Has(resource_id);
}

}  // namespace ray
