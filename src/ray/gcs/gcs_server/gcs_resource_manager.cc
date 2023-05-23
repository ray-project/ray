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
    instrumented_io_context &io_context,
    ClusterResourceManager &cluster_resource_manager,
    NodeID local_node_id,
    std::shared_ptr<ClusterTaskManager> cluster_task_manager)
    : io_context_(io_context),
      cluster_resource_manager_(cluster_resource_manager),
      local_node_id_(std::move(local_node_id)),
      cluster_task_manager_(std::move(cluster_task_manager)) {}

void GcsResourceManager::ConsumeSyncMessage(
    std::shared_ptr<const syncer::RaySyncMessage> message) {
  // ConsumeSyncMessage is called by ray_syncer which might not run
  // in a dedicated thread for performance.
  // GcsResourceManager is a module always run in the main thread, so we just
  // delegate the work to the main thread for thread safety.
  // Ideally, all public api in GcsResourceManager need to be put into this
  // io context for thread safety.
  io_context_.dispatch(
      [this, message]() {
        rpc::ResourcesData resources;
        resources.ParseFromString(message->sync_message());
        resources.set_node_id(message->node_id());
        UpdateFromResourceReport(resources);
      },
      "GcsResourceManager::Update");
}

void GcsResourceManager::HandleGetResources(rpc::GetResourcesRequest request,
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
    for (const auto &resource_id : node_resources.available.ResourceIds()) {
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

void GcsResourceManager::UpdateFromResourceReport(const rpc::ResourcesData &data) {
  NodeID node_id = NodeID::FromBinary(data.node_id());
  // When gcs detects task pending, we may receive an local update. But it can be ignored
  // here because gcs' syncer has already broadcast it.
  if (node_id == local_node_id_) {
    return;
  }
  if (RayConfig::instance().gcs_actor_scheduling_enabled()) {
    UpdateNodeNormalTaskResources(node_id, data);
  } else {
    // QQ: So this doesn't update total resources, but the below UpdateNodeResourceUsage
    // updates the total resources info in `node_resource_usages_`.
    // Do we ever expect a node to have total resources changed during it's lifetime?
    if (!cluster_resource_manager_.UpdateNodeAvailableResourcesIfExist(
            scheduling::NodeID(node_id.Binary()), data)) {
      RAY_LOG(INFO)
          << "[UpdateFromResourceReport]: received resource usage from unknown node id "
          << node_id;
    }
  }

  UpdateNodeResourceUsage(node_id, data);

  // QQ:
  // NOTE: we only bump the version if the available resources changes for a node.
  // Since that's right now the only thing being used as part of the ClusterState for
  // autoscalng. We should really have a way to unify:
  //  1. cluster resource manager's resource view
  //  2. node_resource_usage_ here...
  if (data.resources_available_changed()) {
    IncrementClusterResourceStateVersion();
  }
}

void GcsResourceManager::UpdateResourceLoads(const rpc::ResourcesData &data) {
  NodeID node_id = NodeID::FromBinary(data.node_id());
  auto iter = node_resource_usages_.find(node_id);
  if (iter == node_resource_usages_.end()) {
    // It will happen when the node has been deleted or hasn't been added.
    return;
  }
  if (data.resource_load_changed()) {
    (*iter->second.mutable_resource_load()) = data.resource_load();
    (*iter->second.mutable_resource_load_by_shape()) = data.resource_load_by_shape();

    IncrementClusterResourceStateVersion();
  }
}

const absl::flat_hash_map<NodeID, rpc::ResourcesData> &
GcsResourceManager::NodeResourceReportView() const {
  return node_resource_usages_;
}

void GcsResourceManager::HandleReportResourceUsage(
    rpc::ReportResourceUsageRequest request,
    rpc::ReportResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  UpdateFromResourceReport(request.resources());

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::FillAggregateLoad(
    const rpc::ResourcesData &resources_data,
    std::unordered_map<google::protobuf::Map<std::string, double>, rpc::ResourceDemand>
        *aggregate_load) {
  auto load = resources_data.resource_load_by_shape();
  for (const auto &demand : load.resource_demands()) {
    auto &aggregate_demand = (*aggregate_load)[demand.shape()];
    aggregate_demand.set_num_ready_requests_queued(
        aggregate_demand.num_ready_requests_queued() +
        demand.num_ready_requests_queued());
    aggregate_demand.set_num_infeasible_requests_queued(
        aggregate_demand.num_infeasible_requests_queued() +
        demand.num_infeasible_requests_queued());
    aggregate_demand.set_backlog_size(aggregate_demand.backlog_size() +
                                      demand.backlog_size());
  }
}

void GcsResourceManager::HandleGetAllResourceUsage(
    rpc::GetAllResourceUsageRequest request,
    rpc::GetAllResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (!node_resource_usages_.empty()) {
    rpc::ResourceUsageBatchData batch;
    std::unordered_map<google::protobuf::Map<std::string, double>, rpc::ResourceDemand>
        aggregate_load;

    for (const auto &usage : node_resource_usages_) {
      // Aggregate the load reported by each raylet.
      FillAggregateLoad(usage.second, &aggregate_load);
      batch.add_batch()->CopyFrom(usage.second);
    }

    if (cluster_task_manager_) {
      // Fill the gcs info when gcs actor scheduler is enabled.
      rpc::ResourcesData gcs_resources_data;
      cluster_task_manager_->FillPendingActorInfo(gcs_resources_data);
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
      for (const auto &resource_pair : demand.first) {
        (*demand_proto->mutable_shape())[resource_pair.first] = resource_pair.second;
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
             num_alive_nodes_);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::UpdateNodeResourceUsage(const NodeID &node_id,
                                                 const rpc::ResourcesData &resources) {
  auto iter = node_resource_usages_.find(node_id);
  if (iter == node_resource_usages_.end()) {
    // It will only happen when the node has been deleted.
    // If the node is not registered to GCS,
    // we are guaranteed that no resource usage will be reported.
    return;
  } else {
    if (resources.resources_total_size() > 0) {
      (*iter->second.mutable_resources_total()) = resources.resources_total();
    }
    if (resources.resources_available_changed()) {
      (*iter->second.mutable_resources_available()) = resources.resources_available();
    }
    if (resources.resources_normal_task_changed()) {
      (*iter->second.mutable_resources_normal_task()) = resources.resources_normal_task();
    }
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
  rpc::ResourcesData data;
  data.set_node_id(node.node_id());
  data.set_node_manager_address(node.node_manager_address());
  node_resource_usages_.emplace(NodeID::FromBinary(node.node_id()), std::move(data));
  num_alive_nodes_++;

  auto instance_id = node.instance_id();
  auto version = IncrementClusterResourceStateVersion();
  // Add a new node state.
  AddNewNodeState(instance_id, node.node_id(), version);
}

void GcsResourceManager::OnNodeDead(const NodeID &node_id) {
  node_resource_usages_.erase(node_id);
  cluster_resource_manager_.RemoveNode(scheduling::NodeID(node_id.Binary()));
  num_alive_nodes_--;
  RemoveNodeState(node_id);
  IncrementClusterResourceStateVersion();
}

void GcsResourceManager::UpdatePlacementGroupLoad(
    const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load) {
  placement_group_load_ = absl::make_optional(placement_group_load);

  IncrementClusterResourceStateVersion();
}

std::string GcsResourceManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsResourceManager: "
         << "\n- GetResources request count: "
         << counts_[CountType::GET_RESOURCES_REQUEST]
         << "\n- GetAllAvailableResources request count"
         << counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST]
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

void GcsResourceManager::HandleGetClusterResourceState(
    rpc::autoscaler::GetClusterResourceStateRequest request,
    rpc::autoscaler::GetClusterResourceStateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // TODO: last_seen_cluster_resource_state_version not needed?
  RAY_CHECK(request.last_seen_cluster_resource_state_version() <=
            cluster_resource_state_version_);

  reply->set_last_seen_autoscaler_state_version(last_seen_autoscaler_state_version_);
  reply->set_cluster_resource_state_version(cluster_resource_state_version_);

  GetNodeStates(reply);
  GetPendingResourceRequests(reply);
  GetPendingGangResourceRequests(reply);
  GetClusterResourceConstraints(reply);

  // QQ: should we use GCS_RPC_SEND_REPLY
  // That would add another field in the reply, and introduce dep of `GcsStatus`.
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsResourceManager::GetPendingGangResourceRequests(
    rpc::autoscaler::GetClusterResourceStateReply *reply) {
  // Get the gang resource requests from the placement group load.
  if (!placement_group_load_.has_value()) {
    return;
  }

  for (const auto &pg_data : placement_group_load_.value()->placement_group_data()) {
    auto gang_resource_req = reply->add_pending_gang_resource_requests();
    if (!(pg_data.state() == rpc::PlacementGroupTableData::PENDING ||
          pg_data.state() == rpc::PlacementGroupTableData::RESCHEDULING)) {
      continue;
    }

    absl::optional<rpc::PlacementConstraint> anti_affinity_constraint = absl::nullopt;
    // TODO: how to handle soft anti-affinity?
    if (pg_data.strategy() == rpc::PlacementStrategy::STRICT_SPREAD) {
      anti_affinity_constraint = rpc::PlacementConstraint();
      anti_affinity_constraint->mutable_anti_affinity()->set_label_name(
          kPlacementGroupAntiAffinityLabelName);
      anti_affinity_constraint->mutable_anti_affinity()->set_label_value(
          pg_data.placement_group_id());
    }

    for (const auto &bundle : pg_data.bundles()) {
      // Add the resources.
      auto resource_req = gang_resource_req->add_requests();
      resource_req->mutable_resources_bundle()->insert(bundle.unit_resources().begin(),
                                                       bundle.unit_resources().end());

      // Add the placement constraint.
      if (anti_affinity_constraint.has_value()) {
        resource_req->add_placement_constraints()->CopyFrom(
            anti_affinity_constraint.value());
      }
    }
  }

  return;
}

void GcsResourceManager::GetClusterResourceConstraints(
    rpc::autoscaler::GetClusterResourceStateReply *reply) {
  // TODO
  return;
}

void GcsResourceManager::GetPendingResourceRequests(
    rpc::autoscaler::GetClusterResourceStateReply *reply) {
  std::unordered_map<google::protobuf::Map<std::string, double>, rpc::ResourceDemand>
      aggregate_load;

  for (const auto &usage : node_resource_usages_) {
    // Aggregate the load reported by each raylet.
    FillAggregateLoad(usage.second, &aggregate_load);
  }

  for (const auto &[shape, demand] : aggregate_load) {
    // QQ: should backlog size to be included here?
    auto num_pending = demand.num_infeasible_requests_queued();
    if (num_pending > 0) {
      auto pending_req = reply->add_pending_resource_requests();
      pending_req->set_count(num_pending);
      auto req = pending_req->mutable_request();
      req->mutable_resources_bundle()->insert(shape.begin(), shape.end());
    }
  }
}

void GcsResourceManager::GetNodeStates(
    rpc::autoscaler::GetClusterResourceStateReply *reply) {
  for (auto &[_node_id, node_state] : node_states_) {
    auto node_state_proto = reply->add_node_states();
    node_state_proto->CopyFrom(node_state);

    // QQ: There's node_resource_usages_ and cluster resource manager for the node
    // resources? Why's seemingly 2 sources of truth?
    //
    // const auto &node_resource_data =
    //     node_resource_usages_.at(NodeID::FromBinary(node_state.node_id()));

    auto const &node_resource_data = cluster_resource_manager_.GetNodeResources(
        scheduling::NodeID(node_state.node_id()));

    // Copy resource available
    const auto &available = node_resource_data.available.ToResourceMap();
    node_state_proto->mutable_available_resources()->insert(available.begin(),
                                                            available.end());

    // Copy total resources
    const auto &total = node_resource_data.total.ToResourceMap();
    node_state_proto->mutable_total_resources()->insert(total.begin(), total.end());

    // TODO: support dynamic labels.
  }
}

void GcsResourceManager::RemoveNodeState(const NodeID &node_id) {
  node_states_.erase(node_id);
}

void GcsResourceManager::AddNewNodeState(const std::string &instance_id,
                                         const std::string &node_id,
                                         int64_t version) {
  rpc::autoscaler::NodeState node_state;
  node_state.set_node_id(node_id);
  node_state.set_instance_id(instance_id);
  node_state.set_status(rpc::autoscaler::NodeState::ALIVE);
  node_state.set_node_state_version(version);
  node_states_.insert({NodeID::FromBinary(node_id), std::move(node_state)});
}

void GcsResourceManager::HandleReportAutoscalingState(
    rpc::autoscaler::ReportAutoscalingStateRequest request,
    rpc::autoscaler::ReportAutoscalingStateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // TODO
}

}  // namespace gcs
}  // namespace ray
