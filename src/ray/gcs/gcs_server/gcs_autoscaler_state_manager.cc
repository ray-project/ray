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
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"

namespace ray {
namespace gcs {

GcsAutoscalerStateManager::GcsAutoscalerStateManager(
    const ClusterResourceManager &cluster_resource_manager,
    const GcsResourceManager &gcs_resource_manager,
    const GcsNodeManager &gcs_node_manager)
    : cluster_resource_manager_(cluster_resource_manager),
      gcs_node_manager_(gcs_node_manager),
      gcs_resource_manager_(gcs_resource_manager),
      last_cluster_resource_state_version_(0),
      last_seen_autoscaler_state_version_(0) {}

void GcsAutoscalerStateManager::HandleGetClusterResourceState(
    rpc::autoscaler::GetClusterResourceStateRequest request,
    rpc::autoscaler::GetClusterResourceStateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.last_seen_cluster_resource_state_version() <=
            last_cluster_resource_state_version_);
  reply->set_last_seen_autoscaler_state_version(last_seen_autoscaler_state_version_);
  reply->set_cluster_resource_state_version(
      IncrementAndGetNextClusterResourceStateVersion());

  GetNodeStates(reply);
  GetPendingResourceRequests(reply);
  // GetPendingGangResourceRequests(reply);
  // GetClusterResourceConstraints(reply);

  // We are not using GCS_RPC_SEND_REPLY like other GCS managers to avoid the client
  // having to parse the gcs status code embedded.
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsAutoscalerStateManager::HandleReportAutoscalingState(
    rpc::autoscaler::ReportAutoscalingStateRequest request,
    rpc::autoscaler::ReportAutoscalingStateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // Unimplemented.
  throw std::runtime_error("Unimplemented");
}

void GcsAutoscalerStateManager::GetPendingGangResourceRequests(
    rpc::autoscaler::GetClusterResourceStateReply *reply) {
  throw std::runtime_error("Unimplemented");
}

void GcsAutoscalerStateManager::GetClusterResourceConstraints(
    rpc::autoscaler::GetClusterResourceStateReply *reply) {
  throw std::runtime_error("Unimplemented");
}

void GcsAutoscalerStateManager::GetPendingResourceRequests(
    rpc::autoscaler::GetClusterResourceStateReply *reply) {
  // TODO(rickyx): We could actually get the load of each node from the cluster resource
  // manager. Need refactoring on the GcsResourceManager.
  // We could then do cluster_resource_manager_GetResourceLoad(), and decouple it
  // from gcs_resource_manager_.
  auto aggregate_load = gcs_resource_manager_.GetAggregatedResourceLoad();
  for (const auto &[shape, demand] : aggregate_load) {
    auto num_pending = demand.num_infeasible_requests_queued() + demand.backlog_size() +
                       demand.num_ready_requests_queued();
    if (num_pending > 0) {
      auto pending_req = reply->add_pending_resource_requests();
      pending_req->set_count(num_pending);
      auto req = pending_req->mutable_request();
      req->mutable_resources_bundle()->insert(shape.begin(), shape.end());
    }
  }
}

void GcsAutoscalerStateManager::GetNodeStates(
    rpc::autoscaler::GetClusterResourceStateReply *reply) {
  auto populate_node_state = [&](const rpc::GcsNodeInfo &gcs_node_info,
                                 rpc::autoscaler::NodeState::NodeStatus status) {
    auto node_state_proto = reply->add_node_states();
    node_state_proto->set_node_id(gcs_node_info.node_id());
    node_state_proto->set_instance_id(gcs_node_info.instance_id());
    node_state_proto->set_node_state_version(last_cluster_resource_state_version_);
    node_state_proto->set_status(status);

    if (status == rpc::autoscaler::NodeState::ALIVE) {
      auto const &node_resource_data = cluster_resource_manager_.GetNodeResources(
          scheduling::NodeID(node_state_proto->node_id()));

      // Copy resource available
      const auto &available = node_resource_data.available.ToResourceMap();
      node_state_proto->mutable_available_resources()->insert(available.begin(),
                                                              available.end());

      // Copy total resources
      const auto &total = node_resource_data.total.ToResourceMap();
      node_state_proto->mutable_total_resources()->insert(total.begin(), total.end());

      // TODO(rickyx): support dynamic labels
    }
  };

  const auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  std::for_each(alive_nodes.begin(), alive_nodes.end(), [&](const auto &gcs_node_info) {
    populate_node_state(*gcs_node_info.second, rpc::autoscaler::NodeState::ALIVE);
  });

  // This might be large if there are many nodes for a long-running cluster.
  // However, since we don't report resources for a dead node, the data size being
  // reported by dead node should be small.
  // TODO(rickyx): We will need to GC the head nodes in the future.
  // https://github.com/ray-project/ray/issues/35874
  const auto &dead_nodes = gcs_node_manager_.GetAllDeadNodes();
  std::for_each(dead_nodes.begin(), dead_nodes.end(), [&](const auto &gcs_node_info) {
    populate_node_state(*gcs_node_info.second, rpc::autoscaler::NodeState::DEAD);
  });
}

}  // namespace gcs
}  // namespace ray
