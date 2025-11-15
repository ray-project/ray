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

#include "ray/gcs/gcs_resource_manager.h"

#include <memory>
#include <string>
#include <utility>

#include "ray/common/ray_config.h"
#include "ray/gcs/state_util.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

GcsResourceManager::GcsResourceManager(instrumented_io_context &io_context,
                                       ClusterResourceManager &cluster_resource_manager,
                                       GcsNodeManager &gcs_node_manager,
                                       NodeID local_node_id,
                                       raylet::ClusterLeaseManager *cluster_lease_manager)
    : io_context_(io_context),
      cluster_resource_manager_(cluster_resource_manager),
      gcs_node_manager_(gcs_node_manager),
      local_node_id_(std::move(local_node_id)),
      cluster_lease_manager_(cluster_lease_manager) {}

void GcsResourceManager::ConsumeSyncMessage(
    std::shared_ptr<const rpc::syncer::RaySyncMessage> message) {
  // ConsumeSyncMessage is called by ray_syncer which might not run
  // in a dedicated thread for performance.
  // GcsResourceManager is a module always run in the main thread, so we just
  // delegate the work to the main thread for thread safety.
  // Ideally, all public api in GcsResourceManager need to be put into this
  // io context for thread safety.
  io_context_.dispatch(
      [this, message]() {
        if (message->message_type() == syncer::MessageType::COMMANDS) {
          syncer::CommandsSyncMessage commands_sync_message;
          commands_sync_message.ParseFromString(message->sync_message());
          UpdateClusterFullOfActorsDetected(
              NodeID::FromBinary(message->node_id()),
              commands_sync_message.cluster_full_of_actors_detected());
        } else if (message->message_type() == syncer::MessageType::RESOURCE_VIEW) {
          syncer::ResourceViewSyncMessage resource_view_sync_message;
          resource_view_sync_message.ParseFromString(message->sync_message());
          UpdateFromResourceView(NodeID::FromBinary(message->node_id()),
                                 resource_view_sync_message);
        } else {
          RAY_LOG(FATAL) << "Unsupported message type: " << message->message_type();
        }
      },
      "GcsResourceManager::Update");
}

void GcsResourceManager::HandleGetDrainingNodes(
    rpc::GetDrainingNodesRequest request,
    rpc::GetDrainingNodesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto local_scheduling_node_id = scheduling::NodeID(local_node_id_.Binary());
  for (const auto &node_resources_entry : cluster_resource_manager_.GetResourceView()) {
    if (node_resources_entry.first == local_scheduling_node_id) {
      continue;
    }
    const auto &node_resources = node_resources_entry.second.GetLocalView();
    if (node_resources.is_draining) {
      auto draining_node = reply->add_draining_nodes();
      draining_node->set_node_id(node_resources_entry.first.Binary());
      draining_node->set_draining_deadline_timestamp_ms(
          node_resources.draining_deadline_timestamp_ms);
    }
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsResourceManager::HandleGetAllAvailableResources(
    rpc::GetAllAvailableResourcesRequest request,
    rpc::GetAllAvailableResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto local_scheduling_node_id = scheduling::NodeID(local_node_id_.Binary());
  for (const auto &node_resources_entry : cluster_resource_manager_.GetResourceView()) {
    if (node_resources_entry.first == local_scheduling_node_id) {
      continue;
    }
    rpc::AvailableResources resource;
    resource.set_node_id(node_resources_entry.first.Binary());
    const auto &node_resources = node_resources_entry.second.GetLocalView();
    const auto node_id = NodeID::FromBinary(node_resources_entry.first.Binary());
    bool using_resource_reports = RayConfig::instance().gcs_actor_scheduling_enabled() &&
                                  node_resource_usages_.contains(node_id);
    for (const auto &resource_id : node_resources.available.ExplicitResourceIds()) {
      const auto &resource_name = resource_id.Binary();
      // Because gcs scheduler does not directly update the available resources of
      // `cluster_resource_manager_`, use the record from resource reports (stored in
      // `node_resource_usages_`) instead.
      if (using_resource_reports) {
        auto resource_iter =
            node_resource_usages_[node_id].resources_available().find(resource_name);
        if (resource_iter != node_resource_usages_[node_id].resources_available().end()) {
          resource.mutable_resources_available()->insert(
              {resource_name, resource_iter->second});
        }
      } else {
        const auto &resource_value = node_resources.available.Get(resource_id);
        resource.mutable_resources_available()->insert(
            {resource_name, resource_value.Double()});
      }
    }
    reply->add_resources_list()->CopyFrom(resource);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleGetAllTotalResources(
    rpc::GetAllTotalResourcesRequest request,
    rpc::GetAllTotalResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto local_scheduling_node_id = scheduling::NodeID(local_node_id_.Binary());
  for (const auto &node_resources_entry : cluster_resource_manager_.GetResourceView()) {
    if (node_resources_entry.first == local_scheduling_node_id) {
      continue;
    }
    rpc::TotalResources resource;
    resource.set_node_id(node_resources_entry.first.Binary());
    const auto &node_resources = node_resources_entry.second.GetLocalView();
    for (const auto &resource_id : node_resources.total.ExplicitResourceIds()) {
      const auto &resource_name = resource_id.Binary();
      const auto &resource_value = node_resources.total.Get(resource_id);
      resource.mutable_resources_total()->insert(
          {resource_name, resource_value.Double()});
    }
    reply->add_resources_list()->CopyFrom(resource);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_All_TOTAL_RESOURCES_REQUEST];
}

void GcsResourceManager::UpdateFromResourceView(
    const NodeID &node_id,
    const syncer::ResourceViewSyncMessage &resource_view_sync_message) {
  // When gcs detects task pending, we may receive an local update. But it can be ignored
  // here because gcs' syncer has already broadcast it.
  if (node_id == local_node_id_) {
    return;
  }
  if (RayConfig::instance().gcs_actor_scheduling_enabled()) {
    // TODO(jjyao) This is currently an no-op and is broken.
    // UpdateNodeNormalTaskResources(node_id, data);
  } else {
    // We will only update the node's resources if it's from resource view reports.
    if (!cluster_resource_manager_.UpdateNode(scheduling::NodeID(node_id.Binary()),
                                              resource_view_sync_message)) {
      RAY_LOG(INFO)
          << "[UpdateFromResourceView]: received resource usage from unknown node id "
          << node_id;
    }
  }
  UpdateNodeResourceUsage(node_id, resource_view_sync_message);
}

void GcsResourceManager::UpdateResourceLoads(const rpc::ResourcesData &data) {
  NodeID node_id = NodeID::FromBinary(data.node_id());
  auto iter = node_resource_usages_.find(node_id);
  if (iter == node_resource_usages_.end()) {
    // It will happen when the node has been deleted or hasn't been added.
    return;
  }
  (*iter->second.mutable_resource_load()) = data.resource_load();
  (*iter->second.mutable_resource_load_by_shape()) = data.resource_load_by_shape();
}

const absl::flat_hash_map<NodeID, rpc::ResourcesData>
    &GcsResourceManager::NodeResourceReportView() const {
  return node_resource_usages_;
}

void GcsResourceManager::HandleGetAllResourceUsage(
    rpc::GetAllResourceUsageRequest request,
    rpc::GetAllResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (!node_resource_usages_.empty()) {
    rpc::ResourceUsageBatchData batch;
    absl::flat_hash_map<ResourceDemandKey, rpc::ResourceDemand> aggregate_load;

    for (const auto &usage : node_resource_usages_) {
      // Aggregate the load reported by each raylet.
      FillAggregateLoad(usage.second, &aggregate_load);
      batch.add_batch()->CopyFrom(usage.second);
    }

    if (cluster_lease_manager_ != nullptr) {
      // Fill the gcs info when gcs actor scheduler is enabled.
      rpc::ResourcesData gcs_resources_data;
      cluster_lease_manager_->FillPendingActorInfo(gcs_resources_data);
      // Aggregate the load (pending actor info) of gcs.
      FillAggregateLoad(gcs_resources_data, &aggregate_load);
      // We only export gcs's pending info without adding the corresponding
      // `ResourcesData` to the `batch` list. So if gcs has detected cluster full of
      // actors, set the dedicated field in reply.
      if (gcs_resources_data.cluster_full_of_actors_detected()) {
        reply->set_cluster_full_of_actors_detected_by_gcs(true);
      }
    }

    for (const auto &demand : aggregate_load) {
      auto demand_proto = batch.mutable_resource_load_by_shape()->add_resource_demands();
      demand_proto->CopyFrom(demand.second);
      for (const auto &resource_pair : demand.first.shape) {
        (*demand_proto->mutable_shape())[resource_pair.first] = resource_pair.second;
      }
      for (auto &selector : demand.first.label_selectors) {
        *demand_proto->add_label_selectors() = std::move(selector);
      }
    }
    // Update placement group load to heartbeat batch.
    // This is updated only one per second.
    if (placement_group_load_.has_value()) {
      auto placement_group_load = placement_group_load_.value();
      auto placement_group_load_proto = batch.mutable_placement_group_load();
      placement_group_load_proto->CopyFrom(*placement_group_load.get());
    }

    reply->mutable_resource_usage_data()->CopyFrom(batch);
  }

  RAY_DCHECK(static_cast<size_t>(reply->resource_usage_data().batch().size()) ==
             num_alive_nodes_)
      << "Number of alive nodes " << num_alive_nodes_
      << " is not equal to number of usage reports "
      << reply->resource_usage_data().batch().size() << " in the autoscaler report.";
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::UpdateClusterFullOfActorsDetected(
    const NodeID &node_id, bool cluster_full_of_actors_detected) {
  auto iter = node_resource_usages_.find(node_id);
  if (iter == node_resource_usages_.end()) {
    return;
  }

  // TODO(rickyx): We should change this to be part of RESOURCE_VIEW.
  // This is being populated from NodeManager as part of COMMANDS
  iter->second.set_cluster_full_of_actors_detected(cluster_full_of_actors_detected);
}

void GcsResourceManager::UpdateNodeResourceUsage(
    const NodeID &node_id,
    const syncer::ResourceViewSyncMessage &resource_view_sync_message) {
  // Note: This may be inconsistent with autoscaler state, which is
  // not reported as often as a Ray Syncer message.
  gcs_node_manager_.UpdateAliveNode(node_id, resource_view_sync_message);

  auto iter = node_resource_usages_.find(node_id);
  if (iter == node_resource_usages_.end()) {
    // It will only happen when the node has been deleted.
    // If the node is not registered to GCS,
    // we are guaranteed that no resource usage will be reported.
    return;
  }
  if (resource_view_sync_message.resources_total_size() > 0) {
    (*iter->second.mutable_resources_total()) =
        resource_view_sync_message.resources_total();
  }

  (*iter->second.mutable_resources_available()) =
      resource_view_sync_message.resources_available();
}

void GcsResourceManager::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &entry : gcs_init_data.Nodes()) {
    if (entry.second.state() == rpc::GcsNodeInfo::ALIVE) {
      OnNodeAdd(entry.second);
    }
  }
}

void GcsResourceManager::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  NodeID node_id = NodeID::FromBinary(node.node_id());
  scheduling::NodeID scheduling_node_id(node_id.Binary());
  if (!node.resources_total().empty()) {
    for (const auto &entry : node.resources_total()) {
      cluster_resource_manager_.UpdateResourceCapacity(
          scheduling_node_id, scheduling::ResourceID(entry.first), entry.second);
    }
  } else {
    RAY_LOG(WARNING).WithField(node_id)
        << "The registered node doesn't set the total resources.";
  }

  absl::flat_hash_map<std::string, std::string> labels(node.labels().begin(),
                                                       node.labels().end());
  cluster_resource_manager_.SetNodeLabels(scheduling_node_id, std::move(labels));

  rpc::ResourcesData data;
  data.set_node_id(node_id.Binary());
  data.set_node_manager_address(node.node_manager_address());
  node_resource_usages_.emplace(node_id, std::move(data));
  num_alive_nodes_++;
}

void GcsResourceManager::OnNodeDead(const NodeID &node_id) {
  node_resource_usages_.erase(node_id);
  cluster_resource_manager_.RemoveNode(scheduling::NodeID(node_id.Binary()));
  num_alive_nodes_--;
}

void GcsResourceManager::UpdatePlacementGroupLoad(
    const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load) {
  RAY_CHECK(placement_group_load != nullptr);
  placement_group_load_ = absl::make_optional(placement_group_load);
}

std::string GcsResourceManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsResourceManager: "
         << "\n- GetAllAvailableResources request count: "
         << counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST]
         << "\n- GetAllTotalResources request count: "
         << counts_[CountType::GET_All_TOTAL_RESOURCES_REQUEST]
         << "\n- GetAllResourceUsage request count: "
         << counts_[CountType::GET_ALL_RESOURCE_USAGE_REQUEST];
  return stream.str();
}

void GcsResourceManager::AddResourcesChangedListener(std::function<void()> &&listener) {
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
