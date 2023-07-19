// Copyright 2023 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_autoscaler_state_manager.h"

#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/pb_util.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"

namespace ray {
namespace gcs {

GcsAutoscalerStateManager::GcsAutoscalerStateManager(
    const std::string &session_name,
    const ClusterResourceManager &cluster_resource_manager,
    const GcsResourceManager &gcs_resource_manager,
    const GcsNodeManager &gcs_node_manager,
    const GcsPlacementGroupManager &gcs_placement_group_manager)
    : session_name_(session_name),
      cluster_resource_manager_(cluster_resource_manager),
      gcs_node_manager_(gcs_node_manager),
      gcs_resource_manager_(gcs_resource_manager),
      gcs_placement_group_manager_(gcs_placement_group_manager),
      last_cluster_resource_state_version_(0),
      last_seen_autoscaler_state_version_(0) {}

void GcsAutoscalerStateManager::HandleGetClusterResourceState(
    rpc::autoscaler::GetClusterResourceStateRequest request,
    rpc::autoscaler::GetClusterResourceStateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.last_seen_cluster_resource_state_version() <=
            last_cluster_resource_state_version_);

  auto state = reply->mutable_cluster_resource_state();
  MakeClusterResourceStateInternal(state);

  // We are not using GCS_RPC_SEND_REPLY like other GCS managers to avoid the client
  // having to parse the gcs status code embedded.
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsAutoscalerStateManager::HandleReportAutoscalingState(
    rpc::autoscaler::ReportAutoscalingStateRequest request,
    rpc::autoscaler::ReportAutoscalingStateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // TODO(rickyx): We should handle the infeasible requests in the future.
  // Right now, this info will only be used for observability, i.e. ray status.

  // Never seen any autoscaling state before - so just takes this.
  if (!autoscaling_state_.has_value()) {
    autoscaling_state_ = std::move(request.autoscaling_state());
    send_reply_callback(ray::Status::OK(), nullptr, nullptr);
    return;
  }

  // We have a state cached. We discard the incoming state if it's older than the
  // cached state.
  if (request.autoscaling_state().autoscaler_state_version() <
      autoscaling_state_->autoscaler_state_version()) {
    RAY_LOG(INFO) << "Received an outdated autoscaling state. "
                  << "Current version: " << autoscaling_state_->autoscaler_state_version()
                  << ", received version: "
                  << request.autoscaling_state().autoscaler_state_version()
                  << ". Discarding incoming request.";
    send_reply_callback(ray::Status::OK(), nullptr, nullptr);
    return;
  }

  // We should overwrite the cache version.
  autoscaling_state_ = std::move(request.autoscaling_state());
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsAutoscalerStateManager::HandleRequestClusterResourceConstraint(
    rpc::autoscaler::RequestClusterResourceConstraintRequest request,
    rpc::autoscaler::RequestClusterResourceConstraintReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  cluster_resource_constraint_ =
      std::move(*request.mutable_cluster_resource_constraint());

  // We are not using GCS_RPC_SEND_REPLY like other GCS managers to avoid the client
  // having to parse the gcs status code embedded.
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsAutoscalerStateManager::HandleGetClusterStatus(
    rpc::autoscaler::GetClusterStatusRequest request,
    rpc::autoscaler::GetClusterStatusReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto ray_resource_state = reply->mutable_cluster_resource_state();
  MakeClusterResourceStateInternal(ray_resource_state);

  if (autoscaling_state_) {
    reply->mutable_autoscaling_state()->CopyFrom(*autoscaling_state_);
  }
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsAutoscalerStateManager::MakeClusterResourceStateInternal(
    rpc::autoscaler::ClusterResourceState *state) {
  state->set_last_seen_autoscaler_state_version(last_seen_autoscaler_state_version_);
  state->set_cluster_resource_state_version(
      IncrementAndGetNextClusterResourceStateVersion());
  state->set_cluster_session_name(session_name_);

  GetNodeStates(state);
  GetPendingResourceRequests(state);
  GetPendingGangResourceRequests(state);
  GetClusterResourceConstraints(state);
}

void GcsAutoscalerStateManager::GetPendingGangResourceRequests(
    rpc::autoscaler::ClusterResourceState *state) {
  // Get the gang resource requests from the placement group load.
  auto placement_group_load = gcs_resource_manager_.GetPlacementGroupLoad();
  if (!placement_group_load) {
    return;
  }

  // Iterate through each placement group load.
  for (const auto &pg_data : placement_group_load->placement_group_data()) {
    auto gang_resource_req = state->add_pending_gang_resource_requests();
    // For each placement group, if it's not pending/rescheduling, skip it since.
    // it's not part of the load.
    RAY_CHECK(pg_data.state() == rpc::PlacementGroupTableData::PENDING ||
              pg_data.state() == rpc::PlacementGroupTableData::RESCHEDULING)
        << "Placement group load should only include pending/rescheduling PGs. ";

    const auto pg_constraint = GenPlacementConstraintForPlacementGroup(
        PlacementGroupID::FromBinary(pg_data.placement_group_id()).Hex(),
        pg_data.strategy());

    // Add the strategy as detail info for the gang resource request.
    gang_resource_req->set_details(FormatPlacementGroupDetails(pg_data));

    // Copy the PG's bundles to the request.
    for (const auto &bundle : pg_data.bundles()) {
      if (!NodeID::FromBinary(bundle.node_id()).IsNil()) {
        // We will be skipping **placed** bundle (which has node id associated with it).
        // This is to avoid double counting the bundles that are already placed when
        // reporting PG related load.
        RAY_CHECK(pg_data.state() == rpc::PlacementGroupTableData::RESCHEDULING);
        // NOTE: This bundle is placed in a PG, this must be a bundle that was lost due
        // to node crashed.
        continue;
      }
      // Add the resources.
      auto resource_req = gang_resource_req->add_requests();
      resource_req->mutable_resources_bundle()->insert(bundle.unit_resources().begin(),
                                                       bundle.unit_resources().end());

      // Add the placement constraint.
      if (pg_constraint.has_value()) {
        resource_req->add_placement_constraints()->CopyFrom(pg_constraint.value());
      }
    }
  }

  return;
}

void GcsAutoscalerStateManager::GetClusterResourceConstraints(
    rpc::autoscaler::ClusterResourceState *state) {
  if (cluster_resource_constraint_.has_value()) {
    state->add_cluster_resource_constraints()->CopyFrom(
        cluster_resource_constraint_.value());
  }
}

void GcsAutoscalerStateManager::GetPendingResourceRequests(
    rpc::autoscaler::ClusterResourceState *state) {
  // TODO(rickyx): We could actually get the load of each node from the cluster resource
  // manager. Need refactoring on the GcsResourceManager.
  // We could then do cluster_resource_manager_GetResourceLoad(), and decouple it
  // from gcs_resource_manager_.
  auto aggregate_load = gcs_resource_manager_.GetAggregatedResourceLoad();
  for (const auto &[shape, demand] : aggregate_load) {
    auto num_pending = demand.num_infeasible_requests_queued() + demand.backlog_size() +
                       demand.num_ready_requests_queued();
    if (num_pending > 0) {
      auto pending_req = state->add_pending_resource_requests();
      pending_req->set_count(num_pending);
      auto req = pending_req->mutable_request();
      req->mutable_resources_bundle()->insert(shape.begin(), shape.end());
    }
  }
}

void GcsAutoscalerStateManager::GetNodeStates(
    rpc::autoscaler::ClusterResourceState *state) {
  auto populate_node_state = [&](const rpc::GcsNodeInfo &gcs_node_info) {
    auto node_state_proto = state->add_node_states();
    node_state_proto->set_node_id(gcs_node_info.node_id());
    node_state_proto->set_instance_id(gcs_node_info.instance_id());
    node_state_proto->set_ray_node_type_name(gcs_node_info.node_type_name());
    node_state_proto->set_node_state_version(last_cluster_resource_state_version_);
    node_state_proto->set_node_ip_address(gcs_node_info.node_manager_address());
    node_state_proto->set_instance_type_name(gcs_node_info.instance_type_name());

    if (gcs_node_info.state() == rpc::GcsNodeInfo::DEAD) {
      node_state_proto->set_status(rpc::autoscaler::NodeStatus::DEAD);
      // We don't need populate other info for a dead node.
      return;
    }

    // THe node is alive. We need to check if the node is idle.
    auto const &node_resource_data = cluster_resource_manager_.GetNodeResources(
        scheduling::NodeID(node_state_proto->node_id()));
    if (node_resource_data.idle_resource_duration_ms > 0) {
      // The node was reported idle.
      node_state_proto->set_status(rpc::autoscaler::NodeStatus::IDLE);

      // We approximate the idle duration by the time since the last idle report
      // plus the idle duration reported by the node:
      //  idle_dur = <idle-dur-reported-by-raylet> + <time-since-gcs-gets-last-report>
      //
      // This is because with lightweight resource update, we don't keep reporting
      // the idle time duration when there's no resource change. We also don't want to
      // use raylet reported idle timestamp since there might be clock skew.
      RAY_CHECK(node_resource_data.last_resource_update_time != absl::nullopt);
      node_state_proto->set_idle_duration_ms(
          node_resource_data.idle_resource_duration_ms +
          absl::ToInt64Milliseconds(
              absl::Now() - node_resource_data.last_resource_update_time.value()));
    } else {
      node_state_proto->set_status(rpc::autoscaler::NodeStatus::RUNNING);
    }

    // Copy resource available
    const auto &available = node_resource_data.available.ToResourceMap();
    node_state_proto->mutable_available_resources()->insert(available.begin(),
                                                            available.end());

    // Copy total resources
    const auto &total = node_resource_data.total.ToResourceMap();
    node_state_proto->mutable_total_resources()->insert(total.begin(), total.end());

    // Add dynamic PG labels.
    const auto &pgs_on_node = gcs_placement_group_manager_.GetBundlesOnNode(
        NodeID::FromBinary(gcs_node_info.node_id()));
    for (const auto &[pg_id, _bundle_indices] : pgs_on_node) {
      node_state_proto->mutable_dynamic_labels()->insert(
          {FormatPlacementGroupLabelName(pg_id.Hex()), ""});
    }
  };

  const auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  std::for_each(alive_nodes.begin(), alive_nodes.end(), [&](const auto &gcs_node_info) {
    populate_node_state(*gcs_node_info.second);
  });

  // This might be large if there are many nodes for a long-running cluster.
  // However, since we don't report resources for a dead node, the data size being
  // reported by dead node should be small.
  // TODO(rickyx): We will need to GC the head nodes in the future.
  // https://github.com/ray-project/ray/issues/35874
  const auto &dead_nodes = gcs_node_manager_.GetAllDeadNodes();
  std::for_each(dead_nodes.begin(), dead_nodes.end(), [&](const auto &gcs_node_info) {
    populate_node_state(*gcs_node_info.second);
  });
}

}  // namespace gcs
}  // namespace ray
