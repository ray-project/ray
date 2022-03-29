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
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    ClusterResourceManager &cluster_resource_manager)
    : gcs_table_storage_(gcs_table_storage),
      cluster_resource_manager_(cluster_resource_manager) {}

void GcsResourceManager::HandleGetResources(const rpc::GetResourcesRequest &request,
                                            rpc::GetResourcesReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  scheduling::NodeID node_id(request.node_id());
  const auto &resource_view = cluster_resource_manager_.GetResourceView();
  auto iter = resource_view.find(node_id);
  if (iter != resource_view.end()) {
    rpc::ResourceTableData resource_table_data;
    const auto &node_resources = iter->second.GetLocalView();

    for (const auto &resource_id : node_resources.total.ResourceIds()) {
      const auto &resource_value = node_resources.total.Get(resource_id);
      const auto &resource_name = resource_id.Binary();
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

  scheduling::NodeID scheduling_node_id(node_id.Binary());
  if (cluster_resource_manager_.ContainsNode(scheduling_node_id)) {
    // Update `cluster_resource_manager_`.
    for (const auto &[name, capacity] : changed_resources) {
      cluster_resource_manager_.UpdateResourceCapacity(
          scheduling_node_id, scheduling::ResourceID(name), capacity);
    }

    const auto &node_resources =
        cluster_resource_manager_.GetNodeResources(scheduling_node_id);
    // Update gcs storage.
    rpc::ResourceMap resource_map;
    for (const auto &resource_id : node_resources.total.ResourceIds()) {
      const auto &resource_value = node_resources.total.Get(resource_id);
      const auto &resource_name = resource_id.Binary();
      (*resource_map.mutable_items())[resource_name].set_resource_capacity(
          resource_value.Double());
    }

    for (const auto &listener : resources_changed_listeners_) {
      listener();
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
  scheduling::NodeID scheduling_node_id(node_id.Binary());
  if (cluster_resource_manager_.ContainsNode(scheduling_node_id)) {
    // Update `cluster_resource_manager_`.
    for (const auto &resource_name : resource_names) {
      cluster_resource_manager_.DeleteResource(scheduling_node_id,
                                               scheduling::ResourceID(resource_name));
    }

    const auto &node_resources =
        cluster_resource_manager_.GetNodeResources(scheduling_node_id);
    // Update gcs storage.
    rpc::ResourceMap resource_map;
    for (const auto &resource_id : node_resources.total.ResourceIds()) {
      const auto &resource_name = resource_id.Binary();
      if (std::find(resource_names.begin(), resource_names.end(), resource_name) !=
          resource_names.end()) {
        continue;
      }
      const auto &resource_value = node_resources.total.Get(resource_id);
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
  for (const auto &node_resources_entry : cluster_resource_manager_.GetResourceView()) {
    const auto &node_id = node_resources_entry.first;
    const auto &node_resources = node_resources_entry.second.GetLocalView();
    rpc::AvailableResources resource;
    resource.set_node_id(node_id.Binary());

    for (const auto &resource_id : node_resources.available.ResourceIds()) {
      const auto &resource_value = node_resources.available.Get(resource_id);
      const auto &resource_name = resource_id.Binary();
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
    if (!cluster_resource_manager_.UpdateNodeAvailableResourcesIfExist(
            scheduling::NodeID(node_id.Binary()), data)) {
      RAY_LOG(INFO)
          << "[UpdateFromResourceReport]: received resource usage from unknown node id "
          << node_id;
    }
  }

  UpdateNodeResourceUsage(node_id, data);
}

void GcsResourceManager::HandleReportResourceUsage(
    const rpc::ReportResourceUsageRequest &request,
    rpc::ReportResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  UpdateFromResourceReport(request.resources());

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::HandleGetAllResourceUsage(
    const rpc::GetAllResourceUsageRequest &request,
    rpc::GetAllResourceUsageReply *reply,
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
  for (const auto &entry : gcs_init_data.Nodes()) {
    if (entry.second.state() == rpc::GcsNodeInfo::ALIVE) {
      OnNodeAdd(entry.second);
    }
  }

  for (const auto &entry : gcs_init_data.ClusterResources()) {
    scheduling::NodeID node_id(entry.first.Binary());
    for (const auto &resource : entry.second.items()) {
      cluster_resource_manager_.UpdateResourceCapacity(
          node_id,
          scheduling::ResourceID(resource.first),
          resource.second.resource_capacity());
    }
  }
}

void GcsResourceManager::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  if (!node.resources_total().empty()) {
    scheduling::NodeID node_id(node.node_id());
    for (const auto &entry : node.resources_total()) {
      cluster_resource_manager_.UpdateResourceCapacity(
          node_id, scheduling::ResourceID(entry.first), entry.second);
    }
  } else {
    RAY_LOG(WARNING) << "The registered node " << NodeID::FromBinary(node.node_id())
                     << " doesn't set the total resources.";
  }
}

void GcsResourceManager::OnNodeDead(const NodeID &node_id) {
  node_resource_usages_.erase(node_id);
  cluster_resource_manager_.RemoveNode(scheduling::NodeID(node_id.Binary()));
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
  if (cluster_resource_manager_.UpdateNodeNormalTaskResources(
          scheduling::NodeID(node_id.Binary()), heartbeat)) {
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
  for (const auto &entry : cluster_resource_manager_.GetResourceView()) {
    ostr << indent_1 << entry.first << " : " << entry.second.GetLocalView().DebugString()
         << ",\n";
  }
  ostr << indent_0 << "}\n";
  return ostr.str();
}

}  // namespace gcs
}  // namespace ray
