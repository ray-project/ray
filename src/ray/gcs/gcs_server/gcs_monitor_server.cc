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

#include <boost/range/adaptor/transformed.hpp>

#include "ray/common/constants.h"

namespace ray {
namespace gcs {

static const rpc::ResourceRequest_ResourceRequestType
    PLACEMENT_STRATEGY_TO_REQUEST_TYPE[rpc::PlacementStrategy_ARRAYSIZE] = {
        rpc::ResourceRequest_ResourceRequestType::
            ResourceRequest_ResourceRequestType_PACK_RESERVATION,
        rpc::ResourceRequest_ResourceRequestType::
            ResourceRequest_ResourceRequestType_SPREAD_RESERVATION,
        rpc::ResourceRequest_ResourceRequestType::
            ResourceRequest_ResourceRequestType_STRICT_PACK_RESERVATION,
        rpc::ResourceRequest_ResourceRequestType::
            ResourceRequest_ResourceRequestType_STRICT_SPREAD_RESERVATION};

void GcsPlacementGroupToResourceRequest(const GcsPlacementGroup &gcs_placement_group,
                                        rpc::ResourceRequest &resource_request) {
  rpc::ResourceRequest_ResourceRequestType request_type =
      PLACEMENT_STRATEGY_TO_REQUEST_TYPE[gcs_placement_group.GetStrategy()];

  // A defensive check in case to ensure we handle new strategies. 0 is the
  // default value, and corresponds to TASK_RESERVATION so it should never be a
  // correct request type for placement groups.
  RAY_CHECK((int)request_type != 0)
      << "Unknown placement strategy encountered. " << gcs_placement_group.DebugString();

  resource_request.set_resource_request_type(request_type);
  resource_request.set_count(1);

  for (const auto &bundle : gcs_placement_group.GetUnplacedBundles()) {
    const auto &resource_map = bundle->GetRequiredResources().ToResourceMap();
    resource_request.add_bundles()->mutable_resources()->insert(resource_map.begin(),
                                                                resource_map.end());
  }
}

GcsMonitorServer::GcsMonitorServer(
    GcsNodeManager &gcs_node_manager,
    ClusterResourceManager &cluster_resource_manager,
    std::shared_ptr<GcsResourceManager> gcs_resource_manager,
    std::shared_ptr<GcsPlacementGroupManager> gcs_placement_group_manager)
    : gcs_node_manager_(gcs_node_manager),
      cluster_resource_manager_(cluster_resource_manager),
      gcs_resource_manager_(gcs_resource_manager),
      gcs_placement_group_manager_(gcs_placement_group_manager) {}

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
    gcs_node_manager_.DrainNode(node_id);
    *reply->add_drained_nodes() = node_id_bytes;
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void GcsMonitorServer::PopulateNodeStatuses(rpc::GetSchedulingStatusReply *reply) const {
  const auto &scheduling_nodes = cluster_resource_manager_.GetResourceView();
  const auto &gcs_node_manager_nodes = gcs_node_manager_.GetAllAliveNodes();

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
    const auto available = node_resources.available.GetResourceMap();
    const auto total = node_resources.total.GetResourceMap();

    auto node_status = reply->add_node_statuses();
    node_status->set_node_id(node_id.Binary());
    node_status->set_address(gcs_node_info->node_manager_address());
    node_status->mutable_available_resources()->insert(available.begin(),
                                                       available.end());
    node_status->mutable_total_resources()->insert(total.begin(), total.end());
  }
}

void GcsMonitorServer::PopulateResourceDemands(
    rpc::GetSchedulingStatusReply *reply) const {
  auto task_load_by_shape =
      absl::flat_hash_map<google::protobuf::Map<std::string, double>,
                          rpc::ResourceRequest *>();
  const auto &resources_report_by_node = gcs_resource_manager_->NodeResourceReportView();
  // NOTE: The nodes returned in this loop are eventually consistent with the
  // loop in `PopulateNodeStatuses`. This shouldn't be a problem since nodes
  // shouldn't have resource demands when nodes are downscaled and there will
  // always be race conditions if a node with demands dies unexpectedly.
  for (const auto &node_report_pair : resources_report_by_node) {
    for (const auto &resource_demand :
         node_report_pair.second.resource_load_by_shape().resource_demands()) {
      rpc::ResourceRequest *request;
      auto it = task_load_by_shape.find(resource_demand.shape());
      if (it == task_load_by_shape.end()) {
        request = reply->add_resource_requests();
        task_load_by_shape[resource_demand.shape()] = request;
        request->set_resource_request_type(
            rpc::ResourceRequest_ResourceRequestType::
                ResourceRequest_ResourceRequestType_TASK_RESERVATION);
        request->set_count(0);
        request->add_bundles()->mutable_resources()->insert(
            resource_demand.shape().begin(), resource_demand.shape().end());
      } else {
        request = it->second;
      }

      RAY_CHECK(request)
          << "Monitor server resource demand could not be properly allocated.";
      int count = request->count();
      count += resource_demand.num_ready_requests_queued();
      count += resource_demand.num_infeasible_requests_queued();
      count += resource_demand.backlog_size();
      request->set_count(count);
    }
  }
}

void GcsMonitorServer::PopulatePlacementGroupDemands(
    rpc::GetSchedulingStatusReply *reply) const {
  auto add_gcs_pgs_to_reply = [reply](const std::shared_ptr<GcsPlacementGroup> &gcs_pg) {
    auto new_resource_request_ptr = reply->add_resource_requests();

    RAY_CHECK(gcs_pg != nullptr) << "GcsPlacementGroup not found.";
    RAY_CHECK(new_resource_request_ptr != nullptr)
        << "Failed to allocate resource request.";
    GcsPlacementGroupToResourceRequest(*gcs_pg, *new_resource_request_ptr);
  };
  {
    auto extract_pg_from_pending_value =
        [](const absl::btree_multimap<
            int64_t,
            std::pair<ExponentialBackOff, std::shared_ptr<gcs::GcsPlacementGroup>>>::
               value_type &value) { return value.second.second; };
    auto pending_pg_range = gcs_placement_group_manager_->GetPendingPlacementGroups() |
                            boost::adaptors::transformed(extract_pg_from_pending_value);

    std::for_each(pending_pg_range.begin(), pending_pg_range.end(), add_gcs_pgs_to_reply);
  }
  {
    auto infeasible_pg_range =
        gcs_placement_group_manager_->GetInfeasiblePlacementGroups();
    std::for_each(
        infeasible_pg_range.begin(), infeasible_pg_range.end(), add_gcs_pgs_to_reply);
  }
}

void GcsMonitorServer::HandleGetSchedulingStatus(
    rpc::GetSchedulingStatusRequest request,
    rpc::GetSchedulingStatusReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  PopulateNodeStatuses(reply);
  PopulateResourceDemands(reply);
  PopulatePlacementGroupDemands(reply);

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

}  // namespace gcs
}  // namespace ray
