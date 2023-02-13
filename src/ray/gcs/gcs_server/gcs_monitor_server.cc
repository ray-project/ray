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

#include "ray/gcs/gcs_server/gcs_monitor_server.h"

#include "ray/common/constants.h"

namespace ray {
namespace gcs {

GcsMonitorServer::GcsMonitorServer(std::shared_ptr<GcsNodeManager> gcs_node_manager,
                                   ClusterResourceManager &cluster_resource_manager)
    : gcs_node_manager_(gcs_node_manager),
      cluster_resource_manager_(cluster_resource_manager) {}

void GcsMonitorServer::HandleGetRayVersion(rpc::GetRayVersionRequest request,
                                           rpc::GetRayVersionReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  reply->set_version(kRayVersion);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void GcsMonitorServer::HandleDrainAndKillNode(
    rpc::DrainAndKillNodeRequest request,
    rpc::DrainAndKillNodeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  for (const auto &node_id_bytes : request.node_ids()) {
    const auto node_id = NodeID::FromBinary(node_id_bytes);
    gcs_node_manager_->DrainNode(node_id);
    *reply->add_drained_nodes() = node_id_bytes;
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}


void GcsMonitorServer::PopulateNodeStatuses(rpc::GetSchedulingStatusReply *reply) const {
  const absl::flat_hash_map<scheduling::NodeID, Node> &scheduling_nodes =
      cluster_resource_manager_.GetResourceView();
  const absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>>
    &gcs_node_manager_nodes = gcs_node_manager_->GetAllAliveNodes();

  for (const auto &pair : gcs_node_manager_nodes) {
    const auto &node_id = pair.first;
    const auto &gcs_node_info = pair.second;

    const auto &it = scheduling_nodes.find(scheduling::NodeID(node_id.Binary()));
    if (it == scheduling_nodes.end()) {
      // A node may be in GcsNodeManager but not ClusterResourceManager due to
      // a couple of edge cases/race conditions.
      continue;
    }
    const auto &node_resources = it->second.GetLocalView();
    const auto &available = node_resources.available.ToResourceMap();
    const auto &total = node_resources.total.ToResourceMap();

    auto node_status = reply->add_node_statuses();
    node_status->set_node_id(node_id.Binary());
    node_status->set_address(gcs_node_info->node_manager_address());
    node_status->mutable_available_resources()->insert(available.begin(), available.end());
    node_status->mutable_total_resources()->insert(total.begin(), total.end());
  }
}

void GcsMonitorServer::HandleGetSchedulingStatus(
    rpc::GetSchedulingStatusRequest request,
    rpc::GetSchedulingStatusReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  PopulateNodeStatuses(reply);

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

}  // namespace gcs
}  // namespace ray
