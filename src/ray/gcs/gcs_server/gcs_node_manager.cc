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

#include "ray/gcs/gcs_server/gcs_node_manager.h"

#include "ray/common/ray_config.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

//////////////////////////////////////////////////////////////////////////////////////////
GcsNodeManager::GcsNodeManager(
    boost::asio::io_service &main_io_service, std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager)
    : resource_timer_(main_io_service),
      light_report_resource_usage_enabled_(
          RayConfig::instance().light_report_resource_usage_enabled()),
      gcs_pub_sub_(gcs_pub_sub),
      gcs_table_storage_(gcs_table_storage),
      gcs_resource_manager_(gcs_resource_manager) {
  SendBatchedResourceUsage();
}

void GcsNodeManager::HandleRegisterNode(const rpc::RegisterNodeRequest &request,
                                        rpc::RegisterNodeReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_info().node_id());
  RAY_LOG(INFO) << "Registering node info, node id = " << node_id
                << ", address = " << request.node_info().node_manager_address();
  AddNode(std::make_shared<rpc::GcsNodeInfo>(request.node_info()));
  auto on_done = [this, node_id, request, reply,
                  send_reply_callback](const Status &status) {
    RAY_CHECK_OK(status);
    RAY_LOG(INFO) << "Finished registering node info, node id = " << node_id
                  << ", address = " << request.node_info().node_manager_address();
    RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_CHANNEL, node_id.Hex(),
                                       request.node_info().SerializeAsString(), nullptr));
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(
      gcs_table_storage_->NodeTable().Put(node_id, request.node_info(), on_done));
  ++counts_[CountType::REGISTER_NODE_REQUEST];
}

void GcsNodeManager::HandleUnregisterNode(const rpc::UnregisterNodeRequest &request,
                                          rpc::UnregisterNodeReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(INFO) << "Unregistering node info, node id = " << node_id;
  if (auto node = RemoveNode(node_id, /* is_intended = */ true)) {
    node->set_state(rpc::GcsNodeInfo::DEAD);
    node->set_timestamp(current_sys_time_ms());
    AddDeadNodeToCache(node);

    auto on_done = [this, node_id, node, reply,
                    send_reply_callback](const Status &status) {
      auto on_done = [this, node_id, node, reply,
                      send_reply_callback](const Status &status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_CHANNEL, node_id.Hex(),
                                           node->SerializeAsString(), nullptr));
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        RAY_LOG(INFO) << "Finished unregistering node info, node id = " << node_id;
      };
      RAY_CHECK_OK(gcs_table_storage_->NodeResourceTable().Delete(node_id, on_done));
    };
    // Update node state to DEAD instead of deleting it.
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(node_id, *node, on_done));
  }
  ++counts_[CountType::UNREGISTER_NODE_REQUEST];
}

void GcsNodeManager::HandleGetAllNodeInfo(const rpc::GetAllNodeInfoRequest &request,
                                          rpc::GetAllNodeInfoReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  for (const auto &entry : alive_nodes_) {
    reply->add_node_info_list()->CopyFrom(*entry.second);
  }
  for (const auto &entry : dead_nodes_) {
    reply->add_node_info_list()->CopyFrom(*entry.second);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_NODE_INFO_REQUEST];
}

void GcsNodeManager::HandleReportResourceUsage(
    const rpc::ReportResourceUsageRequest &request, rpc::ReportResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.resources().node_id());
  auto resources_data = std::make_shared<rpc::ResourcesData>();
  resources_data->CopyFrom(request.resources());

  UpdateNodeResourceUsage(node_id, request);

  // Update node realtime resources.
  UpdateNodeRealtimeResources(node_id, *resources_data);

  if (!light_report_resource_usage_enabled_ || resources_data->should_global_gc() ||
      resources_data->resources_total_size() > 0 ||
      resources_data->resources_available_changed() ||
      resources_data->resource_load_changed()) {
    resources_buffer_[node_id] = *resources_data;
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST];
}

void GcsNodeManager::HandleGetResources(const rpc::GetResourcesRequest &request,
                                        rpc::GetResourcesReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  auto iter = cluster_resources_.find(node_id);
  if (iter != cluster_resources_.end()) {
    for (auto &resource : iter->second.items()) {
      (*reply->mutable_resources())[resource.first] = resource.second;
    }
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_RESOURCES_REQUEST];
}

void GcsNodeManager::HandleUpdateResources(const rpc::UpdateResourcesRequest &request,
                                           rpc::UpdateResourcesReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Updating resources, node id = " << node_id;
  auto iter = cluster_resources_.find(node_id);
  auto to_be_updated_resources = request.resources();
  if (iter != cluster_resources_.end()) {
    for (auto &entry : to_be_updated_resources) {
      (*iter->second.mutable_items())[entry.first] = entry.second;
    }
    auto on_done = [this, node_id, to_be_updated_resources, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      for (auto &it : to_be_updated_resources) {
        (*node_resource_change.mutable_updated_resources())[it.first] =
            it.second.resource_capacity();
      }
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Hex(),
                                         node_resource_change.SerializeAsString(),
                                         nullptr));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      RAY_LOG(DEBUG) << "Finished updating resources, node id = " << node_id;
    };

    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, iter->second, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid("Node is not exist."));
    RAY_LOG(ERROR) << "Failed to update resources as node " << node_id
                   << " is not registered.";
  }
  ++counts_[CountType::UPDATE_RESOURCES_REQUEST];
}

void GcsNodeManager::HandleDeleteResources(const rpc::DeleteResourcesRequest &request,
                                           rpc::DeleteResourcesReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Deleting node resources, node id = " << node_id;
  auto resource_names = VectorFromProtobuf(request.resource_name_list());
  auto iter = cluster_resources_.find(node_id);
  if (iter != cluster_resources_.end()) {
    for (auto &resource_name : resource_names) {
      RAY_IGNORE_EXPR(iter->second.mutable_items()->erase(resource_name));
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
        gcs_table_storage_->NodeResourceTable().Put(node_id, iter->second, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    RAY_LOG(DEBUG) << "Finished deleting node resources, node id = " << node_id;
  }
  ++counts_[CountType::DELETE_RESOURCES_REQUEST];
}

void GcsNodeManager::HandleSetInternalConfig(const rpc::SetInternalConfigRequest &request,
                                             rpc::SetInternalConfigReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  auto on_done = [reply, send_reply_callback, request](const Status &status) {
    RAY_LOG(DEBUG) << "Set internal config: " << request.config().DebugString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(gcs_table_storage_->InternalConfigTable().Put(UniqueID::Nil(),
                                                             request.config(), on_done));
  ++counts_[CountType::SET_INTERNAL_CONFIG_REQUEST];
}

void GcsNodeManager::HandleGetInternalConfig(const rpc::GetInternalConfigRequest &request,
                                             rpc::GetInternalConfigReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  auto get_system_config = [reply, send_reply_callback](
                               const ray::Status &status,
                               const boost::optional<rpc::StoredConfig> &config) {
    if (config.has_value()) {
      reply->mutable_config()->CopyFrom(config.get());
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(
      gcs_table_storage_->InternalConfigTable().Get(UniqueID::Nil(), get_system_config));
  ++counts_[CountType::GET_INTERNAL_CONFIG_REQUEST];
}

void GcsNodeManager::HandleGetAllAvailableResources(
    const rpc::GetAllAvailableResourcesRequest &request,
    rpc::GetAllAvailableResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  for (const auto &iter : gcs_resource_manager_->GetClusterResources()) {
    rpc::AvailableResources resource;
    resource.set_node_id(iter.first.Binary());
    for (const auto &res : iter.second.GetResourceAmountMap()) {
      (*resource.mutable_resources_available())[res.first] = res.second.ToDouble();
    }
    reply->add_resources_list()->CopyFrom(resource);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST];
}

void GcsNodeManager::HandleGetAllResourceUsage(
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

void GcsNodeManager::UpdateNodeResourceUsage(
    const NodeID node_id, const rpc::ReportResourceUsageRequest &request) {
  auto iter = node_resource_usages_.find(node_id);
  if (!light_report_resource_usage_enabled_ || iter == node_resource_usages_.end()) {
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

absl::optional<std::shared_ptr<rpc::GcsNodeInfo>> GcsNodeManager::GetAliveNode(
    const ray::NodeID &node_id) const {
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    return {};
  }

  return iter->second;
}

void GcsNodeManager::AddNode(std::shared_ptr<rpc::GcsNodeInfo> node) {
  auto node_id = NodeID::FromBinary(node->node_id());
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    alive_nodes_.emplace(node_id, node);
    // Add an empty resources for this node.
    RAY_CHECK(cluster_resources_.emplace(node_id, rpc::ResourceMap()).second);

    // Notify all listeners.
    for (auto &listener : node_added_listeners_) {
      listener(node);
    }
  }
}

std::shared_ptr<rpc::GcsNodeInfo> GcsNodeManager::RemoveNode(
    const ray::NodeID &node_id, bool is_intended /*= false*/) {
  RAY_LOG(INFO) << "Removing node, node id = " << node_id;
  std::shared_ptr<rpc::GcsNodeInfo> removed_node;
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    removed_node = std::move(iter->second);
    // Record stats that there's a new removed node.
    stats::NodeFailureTotal.Record(1);
    // Remove from alive nodes.
    alive_nodes_.erase(iter);
    // Remove from cluster resources.
    cluster_resources_.erase(node_id);
    resources_buffer_.erase(node_id);
    if (!is_intended) {
      // Broadcast a warning to all of the drivers indicating that the node
      // has been marked as dead.
      // TODO(rkn): Define this constant somewhere else.
      std::string type = "node_removed";
      std::ostringstream error_message;
      error_message << "The node with node id " << node_id
                    << " has been marked dead because the detector"
                    << " has missed too many heartbeats from it. This can happen when a "
                       "raylet crashes unexpectedly or has lagging heartbeats.";
      auto error_data_ptr =
          gcs::CreateErrorTableData(type, error_message.str(), current_time_ms());
      RAY_CHECK_OK(gcs_pub_sub_->Publish(ERROR_INFO_CHANNEL, node_id.Hex(),
                                         error_data_ptr->SerializeAsString(), nullptr));
    }

    // Notify all listeners.
    for (auto &listener : node_removed_listeners_) {
      listener(removed_node);
    }
  }
  return removed_node;
}

void GcsNodeManager::OnNodeFailure(const NodeID &node_id) {
  if (auto node = RemoveNode(node_id, /* is_intended = */ false)) {
    node->set_state(rpc::GcsNodeInfo::DEAD);
    node->set_timestamp(current_sys_time_ms());
    AddDeadNodeToCache(node);
    auto on_done = [this, node_id, node](const Status &status) {
      auto on_done = [this, node_id, node](const Status &status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_CHANNEL, node_id.Hex(),
                                           node->SerializeAsString(), nullptr));
      };
      RAY_CHECK_OK(gcs_table_storage_->NodeResourceTable().Delete(node_id, on_done));
    };
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(node_id, *node, on_done));
  }
}

void GcsNodeManager::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &item : gcs_init_data.Nodes()) {
    if (item.second.state() == rpc::GcsNodeInfo::ALIVE) {
      AddNode(std::make_shared<rpc::GcsNodeInfo>(item.second));
    } else if (item.second.state() == rpc::GcsNodeInfo::DEAD) {
      dead_nodes_.emplace(item.first, std::make_shared<rpc::GcsNodeInfo>(item.second));
      sorted_dead_node_list_.emplace_back(item.first, item.second.timestamp());
    }
  }
  sorted_dead_node_list_.sort(
      [](const std::pair<NodeID, int64_t> &left,
         const std::pair<NodeID, int64_t> &right) { return left.second < right.second; });

  for (auto &entry : gcs_init_data.ClusterResources()) {
    if (alive_nodes_.count(entry.first)) {
      cluster_resources_[entry.first] = entry.second;
    }
  }
}

void GcsNodeManager::UpdateNodeRealtimeResources(
    const NodeID &node_id, const rpc::ResourcesData &resource_data) {
  if (!light_report_resource_usage_enabled_ ||
      gcs_resource_manager_->GetClusterResources().count(node_id) == 0 ||
      resource_data.resources_available_changed()) {
    gcs_resource_manager_->UpdateResources(
        node_id, ResourceSet(MapFromProtobuf(resource_data.resources_available())));
  }
}

void GcsNodeManager::UpdatePlacementGroupLoad(
    const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load) {
  placement_group_load_ = absl::make_optional(placement_group_load);
}

void GcsNodeManager::AddDeadNodeToCache(std::shared_ptr<rpc::GcsNodeInfo> node) {
  if (dead_nodes_.size() >= RayConfig::instance().maximum_gcs_dead_node_cached_count()) {
    const auto &node_id = sorted_dead_node_list_.begin()->first;
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Delete(node_id, nullptr));
    dead_nodes_.erase(sorted_dead_node_list_.begin()->first);
    sorted_dead_node_list_.erase(sorted_dead_node_list_.begin());
  }
  auto node_id = NodeID::FromBinary(node->node_id());
  dead_nodes_.emplace(node_id, node);
  sorted_dead_node_list_.emplace_back(node_id, node->timestamp());
}

void GcsNodeManager::SendBatchedResourceUsage() {
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

std::string GcsNodeManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsNodeManager: {RegisterNode request count: "
         << counts_[CountType::REGISTER_NODE_REQUEST]
         << ", UnregisterNode request count: "
         << counts_[CountType::UNREGISTER_NODE_REQUEST]
         << ", GetAllNodeInfo request count: "
         << counts_[CountType::GET_ALL_NODE_INFO_REQUEST]
         << ", ReportResourceUsage request count: "
         << counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST]
         << ", GetHeartbeat request count: " << counts_[CountType::GET_HEARTBEAT_REQUEST]
         << ", GetAllResourceUsage request count: "
         << counts_[CountType::GET_ALL_RESOURCE_USAGE_REQUEST]
         << ", GetResources request count: " << counts_[CountType::GET_RESOURCES_REQUEST]
         << ", UpdateResources request count: "
         << counts_[CountType::UPDATE_RESOURCES_REQUEST]
         << ", DeleteResources request count: "
         << counts_[CountType::DELETE_RESOURCES_REQUEST]
         << ", SetInternalConfig request count: "
         << counts_[CountType::SET_INTERNAL_CONFIG_REQUEST]
         << ", GetInternalConfig request count: "
         << counts_[CountType::GET_INTERNAL_CONFIG_REQUEST] << "}";
  return stream.str();
}

}  // namespace gcs
}  // namespace ray
