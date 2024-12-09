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

namespace ray {

LocalResourceManager::LocalResourceManager(
    scheduling::NodeID local_node_id,
    const NodeResources &node_resources,
    std::function<int64_t(void)> get_used_object_store_memory,
    std::function<bool(void)> get_pull_manager_at_capacity,
    std::function<void(const rpc::NodeDeathInfo &)> shutdown_raylet_gracefully,
    std::function<void(const NodeResources &)> resource_change_subscriber)
    : local_node_id_(local_node_id),
      get_used_object_store_memory_(get_used_object_store_memory),
      get_pull_manager_at_capacity_(get_pull_manager_at_capacity),
      shutdown_raylet_gracefully_(shutdown_raylet_gracefully),
      resource_change_subscriber_(resource_change_subscriber) {
  RAY_CHECK(node_resources.total == node_resources.available);
  local_resources_.available = NodeResourceInstanceSet(node_resources.total);
  local_resources_.total = NodeResourceInstanceSet(node_resources.total);
  local_resources_.labels = node_resources.labels;
  const auto now = absl::Now();
  for (const auto &resource_id : node_resources.total.ExplicitResourceIds()) {
    last_idle_times_[resource_id] = now;
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
  last_idle_times_.erase(resource_id);
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
    for (const auto &resource_id : resource_request.ResourceIds()) {
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
void LocalResourceManager::SetBusyFootprint(WorkFootprint item) {
  auto prev = last_idle_times_.find(item);
  if (prev != last_idle_times_.end() && !prev->second.has_value()) {
    return;
  }
  last_idle_times_[item] = absl::nullopt;
  OnResourceOrStateChanged();
}

void LocalResourceManager::SetIdleFootprint(WorkFootprint item) {
  auto prev = last_idle_times_.find(item);
  bool state_change = prev == last_idle_times_.end() || !prev->second.has_value();

  last_idle_times_[item] = absl::Now();
  if (state_change) {
    OnResourceOrStateChanged();
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
  // Implicit resources are not used by users directly
  // and don't affect idleness.
  if (resource_id.IsImplicitResource()) {
    return;
  }
  last_idle_times_[resource_id] = absl::nullopt;
}

void LocalResourceManager::SetResourceIdle(const scheduling::ResourceID &resource_id) {
  if (resource_id.IsImplicitResource()) {
    return;
  }
  last_idle_times_[resource_id] = absl::Now();
}

absl::optional<absl::Time> LocalResourceManager::GetResourceIdleTime() const {
  // If all the resources are idle.
  absl::Time all_idle_time = absl::InfinitePast();

  for (const auto &iter : last_idle_times_) {
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
  node_resources.is_draining = IsLocalNodeDraining();
  node_resources.draining_deadline_timestamp_ms = GetDrainingDeadline();
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
      last_idle_times_[ResourceID::ObjectStoreMemory()] = absl::Now();
    } else {
      // Clear the idle info since we know it's being used.
      RAY_LOG(INFO) << "Object store memory is not idle.";
      last_idle_times_[ResourceID::ObjectStoreMemory()] = absl::nullopt;
    }

    OnResourceOrStateChanged();
  }
}

double LocalResourceManager::GetLocalAvailableCpus() const {
  return local_resources_.available.Sum(ResourceID::CPU()).Double();
}

void LocalResourceManager::PopulateResourceViewSyncMessage(
    syncer::ResourceViewSyncMessage &resource_view_sync_message) const {
  NodeResources resources = ToNodeResources();

  auto total = resources.total.GetResourceMap();
  resource_view_sync_message.mutable_resources_total()->insert(total.begin(),
                                                               total.end());

  for (const auto &[resource_name, available] : resources.available.GetResourceMap()) {
    // Resource availability can be negative locally but treat it as 0
    // when we broadcast to others since other parts of the
    // system assume resource availability cannot be negative and
    // there is no difference between negative and zero from other nodes
    // and gcs's point of view.
    (*resource_view_sync_message.mutable_resources_available())[resource_name] =
        std::max(available, 0.0);
  }

  if (get_pull_manager_at_capacity_ != nullptr) {
    resources.object_pulls_queued = get_pull_manager_at_capacity_();
    resource_view_sync_message.set_object_pulls_queued(resources.object_pulls_queued);
  }

  auto idle_time = GetResourceIdleTime();
  if (idle_time.has_value()) {
    // We round up the idle duration to the nearest millisecond such that the idle
    // reporting would be correct even if it's less than 1 millisecond.
    const auto now = absl::Now();
    resource_view_sync_message.set_idle_duration_ms(std::max(
        static_cast<int64_t>(1), absl::ToInt64Milliseconds(now - idle_time.value())));
  }

  resource_view_sync_message.set_is_draining(IsLocalNodeDraining());
  resource_view_sync_message.set_draining_deadline_timestamp_ms(GetDrainingDeadline());

  for (const auto &iter : last_idle_times_) {
    if (iter.second == absl::nullopt) {
      // If it is a WorkFootprint
      if (iter.first.index() == 0) {
        switch (std::get<WorkFootprint>(iter.first)) {
        case WorkFootprint::NODE_WORKERS:
          resource_view_sync_message.add_node_activity("Busy workers on node.");
          break;
        default:
          UNREACHABLE;
        }
        // If it is a ResourceID
      } else {
        std::stringstream out;
        out << "Resource: " << std::get<ResourceID>(iter.first).Binary()
            << " currently in use.";
        resource_view_sync_message.add_node_activity(out.str());
      }
    }
  }
}

std::optional<syncer::RaySyncMessage> LocalResourceManager::CreateSyncMessage(
    int64_t after_version, syncer::MessageType message_type) const {
  RAY_CHECK_EQ(message_type, syncer::MessageType::RESOURCE_VIEW);
  // We check the memory inside version, so version is not a const function.
  // Ideally, we need to move the memory check somewhere else.
  // TODO(iycheng): Make version as a const function.
  const_cast<LocalResourceManager *>(this)->UpdateAvailableObjectStoreMemResource();

  if (version_ <= after_version) {
    return std::nullopt;
  }

  syncer::RaySyncMessage msg;
  syncer::ResourceViewSyncMessage resource_view_sync_message;
  PopulateResourceViewSyncMessage(resource_view_sync_message);

  msg.set_node_id(local_node_id_.Binary());
  msg.set_version(version_);
  msg.set_message_type(message_type);
  std::string serialized_msg;
  RAY_CHECK(resource_view_sync_message.SerializeToString(&serialized_msg));
  msg.set_sync_message(std::move(serialized_msg));
  return std::make_optional(std::move(msg));
}

void LocalResourceManager::OnResourceOrStateChanged() {
  if (IsLocalNodeDraining() && IsLocalNodeIdle()) {
    RAY_LOG(INFO) << "The node is drained, continue to shut down raylet...";
    rpc::NodeDeathInfo node_death_info = DeathInfoFromDrainRequest();
    shutdown_raylet_gracefully_(std::move(node_death_info));
  }

  ++version_;
  if (resource_change_subscriber_ == nullptr) {
    return;
  }
  resource_change_subscriber_(ToNodeResources());
}

rpc::NodeDeathInfo LocalResourceManager::DeathInfoFromDrainRequest() {
  rpc::NodeDeathInfo death_info;
  RAY_CHECK(drain_request_.has_value());
  if (drain_request_->reason() ==
      rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_IDLE_TERMINATION) {
    death_info.set_reason(rpc::NodeDeathInfo::AUTOSCALER_DRAIN_IDLE);
    death_info.set_reason_message(drain_request_->reason_message());
  } else {
    RAY_CHECK_EQ(drain_request_->reason(),
                 rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION);
    death_info.set_reason(rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
    death_info.set_reason_message(drain_request_->reason_message());
  }
  return death_info;
}

bool LocalResourceManager::ResourcesExist(scheduling::ResourceID resource_id) const {
  return local_resources_.total.Has(resource_id);
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

void LocalResourceManager::SetLocalNodeDraining(
    const rpc::DrainRayletRequest &drain_request) {
  drain_request_ = std::make_optional(drain_request);
  OnResourceOrStateChanged();
}

}  // namespace ray
