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

#include "ray/gcs/gcs_server/gcs_resource_manager.h"

#include "ray/common/ray_config.h"
#include "ray/stats/metric_defs.h"

namespace ray {
namespace gcs {

GcsResourceManager::GcsResourceManager(
    instrumented_io_context &main_io_service, std::shared_ptr<GcsPublisher> gcs_publisher,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
    : periodical_runner_(main_io_service),
      gcs_publisher_(gcs_publisher),
      gcs_table_storage_(gcs_table_storage) {}

void GcsResourceManager::HandleGetResources(const rpc::GetResourcesRequest &request,
                                            rpc::GetResourcesReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    rpc::ResourceTableData resource_table_data;
    const auto &node_resources = iter->second->GetLocalView();
    for (size_t i = 0; i < node_resources.predefined_resources.size(); ++i) {
      const auto &resource_value = node_resources.predefined_resources[i].total;
      if (resource_value <= 0) {
        continue;
      }

      const auto &resource_name = scheduling::ResourceID(i).Binary();
      resource_table_data.set_resource_capacity(resource_value.Double());
      (*reply->mutable_resources()).insert({resource_name, resource_table_data});
    }
    for (const auto &entry : node_resources.custom_resources) {
      const auto &resource_name = scheduling::ResourceID(entry.first).Binary();
      const auto &resource_value = entry.second.total;
      resource_table_data.set_resource_capacity(resource_value.Double());
      (*reply->mutable_resources()).insert({resource_name, resource_table_data});
    }
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_RESOURCES_REQUEST];
}

void GcsResourceManager::UpdateResources(
    const NodeID &node_id, absl::flat_hash_map<std::string, double> changed_resources) {
  RAY_LOG(DEBUG) << "Updating resources, node id = " << node_id;
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    // Update `cluster_scheduling_resources_`.

    auto node_resources = iter->second->GetMutableLocalView();
    for (const auto &[name, capacity] : changed_resources) {
      UpdateResourceCapacity(node_resources, name, capacity);
    }

    // Update gcs storage.
    rpc::ResourceMap resource_map;
    for (size_t i = 0; i < node_resources->predefined_resources.size(); ++i) {
      const auto &resource_value = node_resources->predefined_resources[i].total;
      if (resource_value <= 0) {
        continue;
      }

      const auto &resource_name = scheduling::ResourceID(i).Binary();
      (*resource_map.mutable_items())[resource_name].set_resource_capacity(
          resource_value.Double());
    }
    for (const auto &[id, capacity] : node_resources->custom_resources) {
      const auto &resource_name = scheduling::ResourceID(id).Binary();
      const auto &resource_value = capacity.total;
      (*resource_map.mutable_items())[resource_name].set_resource_capacity(
          resource_value.Double());
    }

    auto start = absl::GetCurrentTimeNanos();
    auto on_done = [node_id, start](const Status &status) {
      auto end = absl::GetCurrentTimeNanos();
      ray::stats::STATS_gcs_new_resource_creation_latency_ms.Record(
          absl::Nanoseconds(end - start) / absl::Milliseconds(1));
      RAY_CHECK_OK(status);
      RAY_LOG(DEBUG) << "Finished updating resources, node id = " << node_id;
    };

    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, resource_map, on_done));
  } else {
    RAY_LOG(ERROR) << "Failed to update resources as node " << node_id
                   << " is not registered.";
  }
}

void GcsResourceManager::DeleteResources(const NodeID &node_id,
                                         std::vector<std::string> resource_names) {
  RAY_LOG(DEBUG) << "Deleting node resources, node id = " << node_id;
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    auto node_resources = iter->second->GetMutableLocalView();
    // Update `cluster_scheduling_resources_`.
    DeleteResources(node_resources, resource_names);

    // Update gcs storage.
    rpc::ResourceMap resource_map;
    for (size_t i = 0; i < node_resources->predefined_resources.size(); ++i) {
      const auto &resource_name = scheduling::ResourceID(i).Binary();
      if (std::find(resource_names.begin(), resource_names.end(), resource_name) !=
          resource_names.end()) {
        continue;
      }

      const auto &resource_value = node_resources->predefined_resources[i].total;
      if (resource_value <= 0) {
        continue;
      }

      (*resource_map.mutable_items())[resource_name].set_resource_capacity(
          resource_value.Double());
    }
    for (const auto &entry : node_resources->custom_resources) {
      const auto &resource_name = scheduling::ResourceID(entry.first).Binary();
      if (std::find(resource_names.begin(), resource_names.end(), resource_name) !=
          resource_names.end()) {
        continue;
      }

      const auto &resource_value = entry.second.total;
      if (resource_value <= 0) {
        continue;
      }

      (*resource_map.mutable_items())[resource_name].set_resource_capacity(
          resource_value.Double());
    }

    auto on_done = [](const Status &status) { RAY_CHECK_OK(status); };
    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, resource_map, on_done));
  } else {
    RAY_LOG(DEBUG) << "Finished deleting node resources, node id = " << node_id;
  }
}

void GcsResourceManager::HandleGetAllAvailableResources(
    const rpc::GetAllAvailableResourcesRequest &request,
    rpc::GetAllAvailableResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  for (const auto &node_resources_entry : cluster_scheduling_resources_) {
    const auto &node_id = node_resources_entry.first;
    const auto &node_resources = node_resources_entry.second->GetLocalView();
    rpc::AvailableResources resource;
    resource.set_node_id(node_id.Binary());

    for (size_t i = 0; i < node_resources.predefined_resources.size(); ++i) {
      const auto &resource_value = node_resources.predefined_resources[i].available;
      if (resource_value <= 0) {
        continue;
      }

      const auto &resource_name = scheduling::ResourceID(i).Binary();
      resource.mutable_resources_available()->insert(
          {resource_name, resource_value.Double()});
    }
    for (const auto &entry : node_resources.custom_resources) {
      const auto &resource_value = entry.second.available;
      if (resource_value <= 0) {
        continue;
      }

      const auto &resource_name = scheduling::ResourceID(entry.first).Binary();
      resource.mutable_resources_available()->insert(
          {resource_name, resource_value.Double()});
    }
    reply->add_resources_list()->CopyFrom(resource);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST];
}

void GcsResourceManager::UpdateFromResourceReport(const rpc::ResourcesData &data) {
  NodeID node_id = NodeID::FromBinary(data.node_id());
  if (RayConfig::instance().gcs_actor_scheduling_enabled()) {
    UpdateNodeNormalTaskResources(node_id, data);
  } else {
    if (node_resource_usages_.count(node_id) == 0 || data.resources_available_changed()) {
      SetAvailableResources(node_id, MapFromProtobuf(data.resources_available()));
    }
  }

  UpdateNodeResourceUsage(node_id, data);
}

void GcsResourceManager::HandleReportResourceUsage(
    const rpc::ReportResourceUsageRequest &request, rpc::ReportResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  UpdateFromResourceReport(request.resources());

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::HandleGetAllResourceUsage(
    const rpc::GetAllResourceUsageRequest &request, rpc::GetAllResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (!node_resource_usages_.empty()) {
    auto batch = std::make_shared<rpc::ResourceUsageBatchData>();
    std::unordered_map<google::protobuf::Map<std::string, double>, rpc::ResourceDemand>
        aggregate_load;
    for (const auto &usage : node_resource_usages_) {
      // Aggregate the load reported by each raylet.
      auto load = usage.second.resource_load_by_shape();
      for (const auto &demand : load.resource_demands()) {
        auto &aggregate_demand = aggregate_load[demand.shape()];
        aggregate_demand.set_num_ready_requests_queued(
            aggregate_demand.num_ready_requests_queued() +
            demand.num_ready_requests_queued());
        aggregate_demand.set_num_infeasible_requests_queued(
            aggregate_demand.num_infeasible_requests_queued() +
            demand.num_infeasible_requests_queued());
        aggregate_demand.set_backlog_size(aggregate_demand.backlog_size() +
                                          demand.backlog_size());
      }

      batch->add_batch()->CopyFrom(usage.second);
    }

    for (const auto &demand : aggregate_load) {
      auto demand_proto = batch->mutable_resource_load_by_shape()->add_resource_demands();
      demand_proto->CopyFrom(demand.second);
      for (const auto &resource_pair : demand.first) {
        (*demand_proto->mutable_shape())[resource_pair.first] = resource_pair.second;
      }
    }

    // Update placement group load to heartbeat batch.
    // This is updated only one per second.
    if (placement_group_load_.has_value()) {
      auto placement_group_load = placement_group_load_.value();
      auto placement_group_load_proto = batch->mutable_placement_group_load();
      placement_group_load_proto->CopyFrom(*placement_group_load.get());
    }
    reply->mutable_resource_usage_data()->CopyFrom(*batch);
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::UpdateNodeResourceUsage(const NodeID &node_id,
                                                 const rpc::ResourcesData &resources) {
  auto iter = node_resource_usages_.find(node_id);
  if (iter == node_resource_usages_.end()) {
    auto resources_data = std::make_shared<rpc::ResourcesData>();
    resources_data->CopyFrom(resources);
    node_resource_usages_[node_id] = *resources_data;
  } else {
    if (resources.resources_total_size() > 0) {
      (*iter->second.mutable_resources_total()) = resources.resources_total();
    }
    if (resources.resources_available_changed()) {
      (*iter->second.mutable_resources_available()) = resources.resources_available();
    }
    if (resources.resource_load_changed()) {
      (*iter->second.mutable_resource_load()) = resources.resource_load();
    }
    if (resources.resources_normal_task_changed()) {
      (*iter->second.mutable_resources_normal_task()) = resources.resources_normal_task();
    }
    (*iter->second.mutable_resource_load_by_shape()) = resources.resource_load_by_shape();
    iter->second.set_cluster_full_of_actors_detected(
        resources.cluster_full_of_actors_detected());
  }
}

void GcsResourceManager::Initialize(const GcsInitData &gcs_init_data) {
  const auto &nodes = gcs_init_data.Nodes();
  for (const auto &entry : nodes) {
    if (entry.second.state() == rpc::GcsNodeInfo::ALIVE) {
      OnNodeAdd(entry.second);
    }
  }

  const auto &cluster_resources = gcs_init_data.ClusterResources();
  for (const auto &entry : cluster_resources) {
    const auto &iter = cluster_scheduling_resources_.find(entry.first);
    if (iter != cluster_scheduling_resources_.end()) {
      auto node_resources = iter->second->GetMutableLocalView();
      for (const auto &resource : entry.second.items()) {
        UpdateResourceCapacity(node_resources, resource.first,
                               resource.second.resource_capacity());
      }
    }
  }
}

const absl::flat_hash_map<NodeID, std::shared_ptr<Node>>
    &GcsResourceManager::GetClusterResources() const {
  return cluster_scheduling_resources_;
}

void GcsResourceManager::SetAvailableResources(
    const NodeID &node_id, const absl::flat_hash_map<std::string, double> &resource_map) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    auto resources = ResourceMapToResourceRequest(resource_map,
                                                  /*requires_object_store_memory=*/false);
    auto node_resources = iter->second->GetMutableLocalView();
    for (size_t i = 0; i < node_resources->predefined_resources.size(); ++i) {
      node_resources->predefined_resources[i].available =
          resources.predefined_resources[i];
    }
    for (auto &entry : node_resources->custom_resources) {
      auto it = resources.custom_resources.find(entry.first);
      if (it != resources.custom_resources.end()) {
        entry.second.available = it->second;
      } else {
        entry.second.available = 0.;
      }
    }
  } else {
    RAY_LOG(WARNING)
        << "Skip the setting of available resources of node " << node_id
        << " as it does not exist, maybe it is not registered yet or is already dead.";
  }
}

void GcsResourceManager::DeleteResources(NodeResources *node_resources,
                                         const std::vector<std::string> &resource_names) {
  for (const auto &resource_name : resource_names) {
    auto resource_id = scheduling::ResourceID(resource_name).ToInt();
    if (resource_id == -1) {
      continue;
    }

    if (resource_id >= 0 && resource_id < PredefinedResources_MAX) {
      node_resources->predefined_resources[resource_id].total = 0;
      node_resources->predefined_resources[resource_id].available = 0;
    } else {
      node_resources->custom_resources.erase(resource_id);
    }
  }
}

void GcsResourceManager::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  auto node_id = NodeID::FromBinary(node.node_id());
  if (!cluster_scheduling_resources_.contains(node_id)) {
    absl::flat_hash_map<std::string, double> resource_mapping(
        node.resources_total().begin(), node.resources_total().end());
    // Update the cluster scheduling resources as new node is added.
    cluster_scheduling_resources_.emplace(
        node_id, std::make_shared<Node>(
                     ResourceMapToNodeResources(resource_mapping, resource_mapping)));
  }
}

void GcsResourceManager::OnNodeDead(const NodeID &node_id) {
  node_resource_usages_.erase(node_id);
  cluster_scheduling_resources_.erase(node_id);
  latest_resources_normal_task_timestamp_.erase(node_id);
}

bool GcsResourceManager::AcquireResources(const NodeID &node_id,
                                          const ResourceRequest &required_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    auto node_resources = iter->second->GetMutableLocalView();
    if (!node_resources->IsAvailable(required_resources)) {
      return false;
    }

    for (size_t i = 0; i < required_resources.predefined_resources.size(); ++i) {
      node_resources->predefined_resources[i].available -=
          required_resources.predefined_resources[i];
    }
    for (auto &entry : required_resources.custom_resources) {
      node_resources->custom_resources[entry.first].available -= entry.second;
    }
  }
  // If node dead, we will not find the node. This is a normal scenario, so it returns
  // true.
  return true;
}

bool GcsResourceManager::ReleaseResources(const NodeID &node_id,
                                          const ResourceRequest &acquired_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    auto node_resources = iter->second->GetMutableLocalView();
    RAY_CHECK(acquired_resources.predefined_resources.size() <=
              node_resources->predefined_resources.size());

    for (size_t i = 0; i < acquired_resources.predefined_resources.size(); ++i) {
      node_resources->predefined_resources[i].available +=
          acquired_resources.predefined_resources[i];
      node_resources->predefined_resources[i].available =
          std::min(node_resources->predefined_resources[i].available,
                   node_resources->predefined_resources[i].total);
    }
    for (auto &entry : acquired_resources.custom_resources) {
      auto it = node_resources->custom_resources.find(entry.first);
      if (it != node_resources->custom_resources.end()) {
        it->second.available += entry.second;
        it->second.available = std::min(it->second.available, it->second.total);
      }
    }
  }
  // If node dead, we will not find the node. This is a normal scenario, so it returns
  // true.
  return true;
}

void GcsResourceManager::UpdatePlacementGroupLoad(
    const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load) {
  placement_group_load_ = absl::make_optional(placement_group_load);
}

std::string GcsResourceManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsResourceManager: "
         << "\n- GetResources request count: "
         << counts_[CountType::GET_RESOURCES_REQUEST]
         << "\n- GetAllAvailableResources request count"
         << counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST]
         << "\n- UpdateResources request count: "
         << counts_[CountType::UPDATE_RESOURCES_REQUEST]
         << "\n- DeleteResources request count: "
         << counts_[CountType::DELETE_RESOURCES_REQUEST]
         << "\n- ReportResourceUsage request count: "
         << counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST]
         << "\n- GetAllResourceUsage request count: "
         << counts_[CountType::GET_ALL_RESOURCE_USAGE_REQUEST];
  return stream.str();
}

void GcsResourceManager::AddResourcesChangedListener(std::function<void()> listener) {
  RAY_CHECK(listener != nullptr);
  resources_changed_listeners_.emplace_back(std::move(listener));
}

void GcsResourceManager::UpdateNodeNormalTaskResources(
    const NodeID &node_id, const rpc::ResourcesData &heartbeat) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter == cluster_scheduling_resources_.end()) {
    return;
  }

  auto normal_task_resources =
      ResourceMapToResourceRequest(MapFromProtobuf(heartbeat.resources_normal_task()),
                                   /*requires_object_store_memory=*/false);
  auto &local_normal_task_resources =
      iter->second->GetMutableLocalView()->normal_task_resources;
  if (heartbeat.resources_normal_task_changed() &&
      heartbeat.resources_normal_task_timestamp() >
          latest_resources_normal_task_timestamp_[node_id] &&
      local_normal_task_resources != normal_task_resources) {
    local_normal_task_resources.predefined_resources.resize(PredefinedResources_MAX);
    for (size_t i = 0; i < PredefinedResources_MAX; ++i) {
      local_normal_task_resources.predefined_resources[i] =
          normal_task_resources.predefined_resources[i];
    }
    local_normal_task_resources.custom_resources = normal_task_resources.custom_resources;
    latest_resources_normal_task_timestamp_[node_id] =
        heartbeat.resources_normal_task_timestamp();
    for (const auto &listener : resources_changed_listeners_) {
      listener();
    }
  }
}

std::string GcsResourceManager::ToString() const {
  std::ostringstream ostr;
  const int indent = 0;
  std::string indent_0(indent + 0 * 2, ' ');
  std::string indent_1(indent + 1 * 2, ' ');
  ostr << "{\n";
  for (const auto &entry : cluster_scheduling_resources_) {
    ostr << indent_1 << entry.first << " : " << entry.second->GetLocalView().DebugString()
         << ",\n";
  }
  ostr << indent_0 << "}\n";
  return ostr.str();
}

void GcsResourceManager::UpdateResourceCapacity(NodeResources *node_resources,
                                                const std::string &resource_name,
                                                double capacity) {
  auto idx = scheduling::ResourceID(resource_name).ToInt();
  if (idx == -1) {
    return;
  }

  FixedPoint resource_total_fp(capacity);
  if (idx >= 0 && idx < PredefinedResources_MAX) {
    auto diff_capacity =
        resource_total_fp - node_resources->predefined_resources[idx].total;
    node_resources->predefined_resources[idx].total += diff_capacity;
    node_resources->predefined_resources[idx].available += diff_capacity;
    if (node_resources->predefined_resources[idx].available < 0) {
      node_resources->predefined_resources[idx].available = 0;
    }
    if (node_resources->predefined_resources[idx].total < 0) {
      node_resources->predefined_resources[idx].total = 0;
    }
  } else {
    auto itr = node_resources->custom_resources.find(idx);
    if (itr != node_resources->custom_resources.end()) {
      auto diff_capacity = resource_total_fp - itr->second.total;
      itr->second.total += diff_capacity;
      itr->second.available += diff_capacity;
      if (itr->second.available < 0) {
        itr->second.available = 0;
      }
      if (itr->second.total < 0) {
        itr->second.total = 0;
      }
    } else {
      ResourceCapacity resource_capacity;
      resource_capacity.total = resource_capacity.available = resource_total_fp;
      node_resources->custom_resources.emplace(idx, resource_capacity);
    }
  }
}

}  // namespace gcs
}  // namespace ray
