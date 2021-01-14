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
#include "ray/stats/stats.h"

namespace ray {
namespace gcs {

GcsResourceManager::GcsResourceManager(
    boost::asio::io_service &main_io_service, std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
    : resource_timer_(main_io_service),
      gcs_pub_sub_(gcs_pub_sub),
      gcs_table_storage_(gcs_table_storage) {
  SendBatchedResourceUsage();
}

void GcsResourceManager::HandleGetResources(const rpc::GetResourcesRequest &request,
                                            rpc::GetResourcesReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    const auto &resource_map = iter->second.GetTotalResources().GetResourceMap();
    rpc::ResourceTableData resource_table_data;
    for (const auto &resource : resource_map) {
      resource_table_data.set_resource_capacity(resource.second);
      (*reply->mutable_resources())[resource.first] = resource_table_data;
    }
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleUpdateResources(
    const rpc::UpdateResourcesRequest &request, rpc::UpdateResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Updating resources, node id = " << node_id;
  auto changed_resources = std::make_shared<std::unordered_map<std::string, double>>();
  for (const auto &entry : request.resources()) {
    changed_resources->emplace(entry.first, entry.second.resource_capacity());
  }

  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    // Update `cluster_scheduling_resources_`.
    SchedulingResources &scheduling_resources = iter->second;
    for (const auto &entry : *changed_resources) {
      scheduling_resources.UpdateResourceCapacity(entry.first, entry.second);
    }

    // Update gcs storage.
    rpc::ResourceMap resource_map;
    for (const auto &entry : iter->second.GetTotalResources().GetResourceMap()) {
      (*resource_map.mutable_items())[entry.first].set_resource_capacity(entry.second);
    }
    for (const auto &entry : *changed_resources) {
      (*resource_map.mutable_items())[entry.first].set_resource_capacity(entry.second);
    }

    auto on_done = [this, node_id, changed_resources, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      node_resource_change.mutable_updated_resources()->insert(changed_resources->begin(),
                                                               changed_resources->end());
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Hex(),
                                         node_resource_change.SerializeAsString(),
                                         nullptr));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      RAY_LOG(DEBUG) << "Finished updating resources, node id = " << node_id;
    };

    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, resource_map, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid("Node is not exist."));
    RAY_LOG(ERROR) << "Failed to update resources as node " << node_id
                   << " is not registered.";
  }
  ++counts_[CountType::UPDATE_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleDeleteResources(
    const rpc::DeleteResourcesRequest &request, rpc::DeleteResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Deleting node resources, node id = " << node_id;
  auto resource_names = VectorFromProtobuf(request.resource_name_list());
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    // Update `cluster_scheduling_resources_`.
    for (const auto &resource_name : resource_names) {
      iter->second.DeleteResource(resource_name);
    }

    // Update gcs storage.
    rpc::ResourceMap resource_map;
    auto resources = iter->second.GetTotalResources().GetResourceMap();
    for (const auto &resource_name : resource_names) {
      resources.erase(resource_name);
    }
    for (const auto &entry : resources) {
      (*resource_map.mutable_items())[entry.first].set_resource_capacity(entry.second);
    }

    auto on_done = [this, node_id, resource_names, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      for (const auto &resource_name : resource_names) {
        node_resource_change.add_deleted_resources(resource_name);
      }
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Hex(),
                                         node_resource_change.SerializeAsString(),
                                         nullptr));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    };
    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, resource_map, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    RAY_LOG(DEBUG) << "Finished deleting node resources, node id = " << node_id;
  }
  ++counts_[CountType::DELETE_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleGetAllAvailableResources(
    const rpc::GetAllAvailableResourcesRequest &request,
    rpc::GetAllAvailableResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  for (const auto &iter : cluster_scheduling_resources_) {
    rpc::AvailableResources resource;
    resource.set_node_id(iter.first.Binary());
    for (const auto &res : iter.second.GetAvailableResources().GetResourceAmountMap()) {
      (*resource.mutable_resources_available())[res.first] = res.second.ToDouble();
    }
    reply->add_resources_list()->CopyFrom(resource);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleReportResourceUsage(
    const rpc::ReportResourceUsageRequest &request, rpc::ReportResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.resources().node_id());
  auto resources_data = std::make_shared<rpc::ResourcesData>();
  resources_data->CopyFrom(request.resources());

  // We use `node_resource_usages_` to filter out the nodes that report resource
  // information for the first time. `UpdateNodeResourceUsage` will modify
  // `node_resource_usages_`, so we need to do it before `UpdateNodeResourceUsage`.
  if (node_resource_usages_.count(node_id) == 0 ||
      resources_data->resources_available_changed()) {
    const auto &resource_changed = MapFromProtobuf(resources_data->resources_available());
    SetAvailableResources(node_id, ResourceSet(resource_changed));
  }

  UpdateNodeResourceUsage(node_id, request);

  if (resources_data->should_global_gc() || resources_data->resources_total_size() > 0 ||
      resources_data->resources_available_changed() ||
      resources_data->resource_load_changed()) {
    resources_buffer_[node_id] = *resources_data;
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::HandleGetAllResourceUsage(
    const rpc::GetAllResourceUsageRequest &request, rpc::GetAllResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (!node_resource_usages_.empty()) {
    auto batch = std::make_shared<rpc::ResourceUsageBatchData>();
    absl::flat_hash_map<ResourceSet, rpc::ResourceDemand> aggregate_load;
    for (const auto &usage : node_resource_usages_) {
      // Aggregate the load reported by each raylet.
      auto load = usage.second.resource_load_by_shape();
      for (const auto &demand : load.resource_demands()) {
        auto scheduling_key = ResourceSet(MapFromProtobuf(demand.shape()));
        auto &aggregate_demand = aggregate_load[scheduling_key];
        aggregate_demand.set_num_ready_requests_queued(
            aggregate_demand.num_ready_requests_queued() +
            demand.num_ready_requests_queued());
        aggregate_demand.set_num_infeasible_requests_queued(
            aggregate_demand.num_infeasible_requests_queued() +
            demand.num_infeasible_requests_queued());
        if (RayConfig::instance().report_worker_backlog()) {
          aggregate_demand.set_backlog_size(aggregate_demand.backlog_size() +
                                            demand.backlog_size());
        }
      }

      batch->add_batch()->CopyFrom(usage.second);
    }

    for (const auto &demand : aggregate_load) {
      auto demand_proto = batch->mutable_resource_load_by_shape()->add_resource_demands();
      demand_proto->CopyFrom(demand.second);
      for (const auto &resource_pair : demand.first.GetResourceMap()) {
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

void GcsResourceManager::UpdateNodeResourceUsage(
    const NodeID node_id, const rpc::ReportResourceUsageRequest &request) {
  auto iter = node_resource_usages_.find(node_id);
  if (iter == node_resource_usages_.end()) {
    auto resources_data = std::make_shared<rpc::ResourcesData>();
    resources_data->CopyFrom(request.resources());
    node_resource_usages_[node_id] = *resources_data;
  } else {
    if (request.resources().resources_total_size() > 0) {
      (*iter->second.mutable_resources_total()) = request.resources().resources_total();
    }
    if (request.resources().resources_available_changed()) {
      (*iter->second.mutable_resources_available()) =
          request.resources().resources_available();
    }
    if (request.resources().resource_load_changed()) {
      (*iter->second.mutable_resource_load()) = request.resources().resource_load();
    }
    (*iter->second.mutable_resource_load_by_shape()) =
        request.resources().resource_load_by_shape();
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
      for (const auto &resource : entry.second.items()) {
        iter->second.UpdateResourceCapacity(resource.first,
                                            resource.second.resource_capacity());
      }
    }
  }
}

const absl::flat_hash_map<NodeID, SchedulingResources>
    &GcsResourceManager::GetClusterResources() const {
  return cluster_scheduling_resources_;
}

void GcsResourceManager::SetAvailableResources(const NodeID &node_id,
                                               const ResourceSet &resources) {
  cluster_scheduling_resources_[node_id].SetAvailableResources(ResourceSet(resources));
}

void GcsResourceManager::UpdateResourceCapacity(
    const NodeID &node_id,
    const std::unordered_map<std::string, double> &changed_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    SchedulingResources &scheduling_resources = iter->second;
    for (const auto &entry : changed_resources) {
      scheduling_resources.UpdateResourceCapacity(entry.first, entry.second);
    }
  } else {
    cluster_scheduling_resources_.emplace(
        node_id, SchedulingResources(ResourceSet(changed_resources)));
  }
}

void GcsResourceManager::DeleteResources(
    const NodeID &node_id, const std::vector<std::string> &deleted_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    for (const auto &resource_name : deleted_resources) {
      iter->second.DeleteResource(resource_name);
    }
  }
}

void GcsResourceManager::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  auto node_id = NodeID::FromBinary(node.node_id());
  if (!cluster_scheduling_resources_.contains(node_id)) {
    cluster_scheduling_resources_.emplace(node_id, SchedulingResources());
  }
}

void GcsResourceManager::OnNodeDead(const NodeID &node_id) {
  resources_buffer_.erase(node_id);
  node_resource_usages_.erase(node_id);
  cluster_scheduling_resources_.erase(node_id);
}

bool GcsResourceManager::AcquireResources(const NodeID &node_id,
                                          const ResourceSet &required_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    if (!required_resources.IsSubset(iter->second.GetAvailableResources())) {
      return false;
    }
    iter->second.Acquire(required_resources);
  }
  // If node dead, we will not find the node. This is a normal scenario, so it returns
  // true.
  return true;
}

bool GcsResourceManager::ReleaseResources(const NodeID &node_id,
                                          const ResourceSet &acquired_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    iter->second.Release(acquired_resources);
  }
  // If node dead, we will not find the node. This is a normal scenario, so it returns
  // true.
  return true;
}

void GcsResourceManager::SendBatchedResourceUsage() {
  if (!resources_buffer_.empty()) {
    auto batch = std::make_shared<rpc::ResourceUsageBatchData>();
    for (auto &resources : resources_buffer_) {
      batch->add_batch()->Swap(&resources.second);
    }
    stats::OutboundHeartbeatSizeKB.Record((double)(batch->ByteSizeLong() / 1024.0));

    RAY_CHECK_OK(gcs_pub_sub_->Publish(RESOURCES_BATCH_CHANNEL, "",
                                       batch->SerializeAsString(), nullptr));
    resources_buffer_.clear();
  }

  auto resources_period = boost::posix_time::milliseconds(
      RayConfig::instance().raylet_report_resources_period_milliseconds());
  resource_timer_.expires_from_now(resources_period);
  resource_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      // `operation_aborted` is set when `resource_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Sending batched resource usage failed with error: "
                      << error.message();
    SendBatchedResourceUsage();
  });
}

void GcsResourceManager::UpdatePlacementGroupLoad(
    const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load) {
  placement_group_load_ = absl::make_optional(placement_group_load);
}

std::string GcsResourceManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsResourceManager: {GetResources request count: "
         << counts_[CountType::GET_RESOURCES_REQUEST]
         << ", GetAllAvailableResources request count"
         << counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST]
         << ", UpdateResources request count: "
         << counts_[CountType::UPDATE_RESOURCES_REQUEST]
         << ", DeleteResources request count: "
         << counts_[CountType::DELETE_RESOURCES_REQUEST]
         << ", ReportResourceUsage request count: "
         << counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST]
         << ", GetAllResourceUsage request count: "
         << counts_[CountType::GET_ALL_RESOURCE_USAGE_REQUEST] << "}";
  return stream.str();
}

}  // namespace gcs
}  // namespace ray
