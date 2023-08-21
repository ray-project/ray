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
#include <csignal>

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/raylet/raylet_util.h"

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
  RAY_CHECK(node_resources.total == node_resources.available);
  local_resources_.available = NodeResourceInstanceSet(node_resources.total);
  local_resources_.total = NodeResourceInstanceSet(node_resources.total);
  local_resources_.labels = node_resources.labels;
  const auto now = absl::Now();
  for (const auto &resource_id : node_resources.total.ExplicitResourceIds()) {
    resources_last_idle_time_[resource_id] = now;
  }
  RAY_LOG(DEBUG) << "local resources: " << local_resources_.DebugString();
}

void LocalResourceManager::AddLocalResourceInstances(
    scheduling::ResourceID resource_id, const std::vector<FixedPoint> &instances) {
  local_resources_.available.Add(resource_id, instances);
  local_resources_.total.Add(resource_id, instances);
  SetResourceIdle(resource_id);
  OnResourceOrStateChanged();
}

void LocalResourceManager::DeleteLocalResource(scheduling::ResourceID resource_id) {
  local_resources_.available.Remove(resource_id);
  local_resources_.total.Remove(resource_id);
  resources_last_idle_time_.erase(resource_id);
  OnResourceOrStateChanged();
}

bool LocalResourceManager::IsAvailableResourceEmpty(
    scheduling::ResourceID resource_id) const {
  return local_resources_.available.Sum(resource_id) <= 0;
}

std::string LocalResourceManager::DebugString(void) const {
  std::stringstream buffer;
  buffer << local_resources_.DebugString();
  buffer << " is_draining: " << IsLocalNodeDraining();
  buffer << " is_idle: " << IsLocalNodeIdle();
  return buffer.str();
}

uint64_t LocalResourceManager::GetNumCpus() const {
  return static_cast<uint64_t>(local_resources_.total.Sum(ResourceID::CPU()).Double());
}

bool LocalResourceManager::AllocateTaskResourceInstances(
    const ResourceRequest &resource_request,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  auto allocation =
      local_resources_.available.TryAllocate(resource_request.GetResourceSet());
  if (allocation) {
    *task_allocation = TaskResourceInstances(*allocation);
    for (auto &resource_id : resource_request.ResourceIds()) {
      SetResourceNonIdle(resource_id);
    }
    return true;
  } else {
    return false;
  }
}

void LocalResourceManager::FreeTaskResourceInstances(
    std::shared_ptr<TaskResourceInstances> task_allocation, bool record_idle_resource) {
  RAY_CHECK(task_allocation != nullptr);
  for (auto &resource_id : task_allocation->ResourceIds()) {
    if (!local_resources_.total.Has(resource_id)) {
      continue;
    }
    local_resources_.available.Free(resource_id, task_allocation->Get(resource_id));
    const auto &available = local_resources_.available.Get(resource_id);
    const auto &total = local_resources_.total.Get(resource_id);
    bool is_idle = true;
    for (size_t i = 0; i < total.size(); ++i) {
      RAY_CHECK_GE(total[i], available[i]);
      is_idle = is_idle && (available[i] == total[i]);
    }

    if (record_idle_resource && is_idle) {
      SetResourceIdle(resource_id);
    }
  }
}

void LocalResourceManager::AddResourceInstances(
    scheduling::ResourceID resource_id, const std::vector<double> &resource_instances) {
  std::vector<FixedPoint> resource_instances_fp =
      FixedPointVectorFromDouble(resource_instances);

  if (resource_instances.size() == 0) {
    return;
  }

  local_resources_.available.Free(resource_id, resource_instances_fp);
  const auto &available = local_resources_.available.Get(resource_id);
  const auto &total = local_resources_.total.Get(resource_id);
  bool is_idle = true;
  for (size_t i = 0; i < total.size(); ++i) {
    RAY_CHECK_GE(total[i], available[i]);
    is_idle = is_idle && (available[i] == total[i]);
  }

  if (is_idle) {
    SetResourceIdle(resource_id);
  }

  OnResourceOrStateChanged();
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

  auto underflow = local_resources_.available.Subtract(
      resource_id, resource_instances_fp, allow_going_negative);

  // If there's any non 0 instance delta to be subtracted, the source should be marked as
  // non-idle.
  for (const auto &to_subtract_instance : resource_instances_fp) {
    if (to_subtract_instance > 0) {
      SetResourceNonIdle(resource_id);
      break;
    }
  }
  OnResourceOrStateChanged();

  return FixedPointVectorToDouble(underflow);
}

void LocalResourceManager::SetResourceNonIdle(const scheduling::ResourceID &resource_id) {
  if (resource_id.IsImplicitResource()) {
    return;
  }
  resources_last_idle_time_[resource_id] = absl::nullopt;
}

void LocalResourceManager::SetResourceIdle(const scheduling::ResourceID &resource_id) {
  if (resource_id.IsImplicitResource()) {
    return;
  }
  resources_last_idle_time_[resource_id] = absl::Now();
}

absl::optional<absl::Time> LocalResourceManager::GetResourceIdleTime() const {
  // If all the resources are idle.
  absl::Time all_idle_time = absl::InfinitePast();

  for (const auto &iter : resources_last_idle_time_) {
    const auto &idle_time_or_busy = iter.second;

    if (idle_time_or_busy == absl::nullopt) {
      // One resource is busy, entire resources should be considered non-idle.
      return absl::nullopt;
    }

    // Update the all resource idle time to be the most recent idle time.
    all_idle_time = std::max(all_idle_time, idle_time_or_busy.value());
  }
  return all_idle_time;
}

bool LocalResourceManager::AllocateLocalTaskResources(
    const ResourceRequest &resource_request,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  if (AllocateTaskResourceInstances(resource_request, task_allocation)) {
    OnResourceOrStateChanged();
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
  OnResourceOrStateChanged();
}

NodeResources LocalResourceManager::ToNodeResources() const {
  NodeResources node_resources;
  node_resources.available = local_resources_.available.ToNodeResourceSet();
  node_resources.total = local_resources_.total.ToNodeResourceSet();
  node_resources.labels = local_resources_.labels;
  node_resources.is_draining = is_local_node_draining_;
  return node_resources;
}

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

    // This is more of a discrete approximate of the last idle object store memory usage.
    // TODO(rickyx): in order to know exactly when object store becomes idle/busy, we
    // would need to plumb the info out of the object store directly.
    if (used == 0.0) {
      // Set it to idle as of now.
      RAY_LOG(INFO) << "Object store memory is idle.";
      resources_last_idle_time_[ResourceID::ObjectStoreMemory()] = absl::Now();
    } else {
      // Clear the idle info since we know it's being used.
      RAY_LOG(INFO) << "Object store memory is not idle.";
      resources_last_idle_time_[ResourceID::ObjectStoreMemory()] = absl::nullopt;
    }

    OnResourceOrStateChanged();
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

  NodeResources resources = ToNodeResources();

  auto total = resources.total.GetResourceMap();
  resources_data.mutable_resources_total()->insert(total.begin(), total.end());

  auto available = resources.available.GetResourceMap();
  resources_data.mutable_resources_available()->insert(available.begin(),
                                                       available.end());

  if (get_pull_manager_at_capacity_ != nullptr) {
    resources.object_pulls_queued = get_pull_manager_at_capacity_();
    resources_data.set_object_pulls_queued(resources.object_pulls_queued);
  }

  const auto now = absl::Now();
  resources_data.set_idle_duration_ms(
      absl::ToInt64Milliseconds(now - GetResourceIdleTime().value_or(now)));

  resources_data.set_is_draining(IsLocalNodeDraining());

  msg.set_node_id(local_node_id_.Binary());
  msg.set_version(version_);
  msg.set_message_type(message_type);
  std::string serialized_msg;
  RAY_CHECK(resources_data.SerializeToString(&serialized_msg));
  msg.set_sync_message(std::move(serialized_msg));
  return std::make_optional(std::move(msg));
}

void LocalResourceManager::OnResourceOrStateChanged() {
  if (IsLocalNodeDraining() && IsLocalNodeIdle()) {
    // The node is drained.
    RAY_LOG(INFO) << "The node is drained, exiting...";
    raylet::ShutdownRayletGracefully();
  }

  ++version_;
  if (resource_change_subscriber_ == nullptr) {
    return;
  }
  resource_change_subscriber_(ToNodeResources());
}

void LocalResourceManager::ResetLastReportResourceUsage(
    const NodeResources &replacement) {
  last_report_resources_.reset(new NodeResources(replacement));
}

bool LocalResourceManager::ResourcesExist(scheduling::ResourceID resource_id) const {
  return !local_resources_.total.Get(resource_id).empty();
}

absl::flat_hash_map<std::string, LocalResourceManager::ResourceUsage>
LocalResourceManager::GetResourceUsageMap() const {
  const auto &local_resources = GetLocalResources();
  const auto avail_map = local_resources.GetAvailableResourceInstances()
                             .ToNodeResourceSet()
                             .GetResourceMap();
  const auto total_map =
      local_resources.GetTotalResourceInstances().ToNodeResourceSet().GetResourceMap();

  absl::flat_hash_map<std::string, ResourceUsage> resource_usage_map;
  for (const auto &it : total_map) {
    const auto &resource = it.first;
    auto total = it.second;
    auto avail_it = avail_map.find(resource);
    double avail = avail_it == avail_map.end() ? 0 : avail_it->second;

    // Ignore the node IP resource. It is useless to track because it is
    // for the affinity purpose.
    std::string prefix("node:");
    if (resource.compare(0, prefix.size(), prefix) == 0) {
      continue;
    }

    // TODO(sang): Right now, we just skip pg resource.
    // Process pg resources properly.
    const auto &data = ParsePgFormattedResource(
        resource, /*for_wildcard_resource*/ true, /*for_indexed_resource*/ true);
    if (data) {
      continue;
    }

    resource_usage_map[resource].avail = avail;
    resource_usage_map[resource].used = total - avail;
  }

  return resource_usage_map;
}

void LocalResourceManager::RecordMetrics() const {
  for (auto &[resource, resource_usage] : GetResourceUsageMap()) {
    ray::stats::STATS_resources.Record(resource_usage.avail,
                                       {{"State", "AVAILABLE"}, {"Name", resource}});
    ray::stats::STATS_resources.Record(resource_usage.used,
                                       {{"State", "USED"}, {"Name", resource}});
  }
}

void LocalResourceManager::SetLocalNodeDraining() {
  is_local_node_draining_ = true;
  OnResourceOrStateChanged();
}

}  // namespace ray
