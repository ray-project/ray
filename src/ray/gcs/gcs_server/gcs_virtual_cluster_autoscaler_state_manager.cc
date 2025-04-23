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

#include "ray/gcs/gcs_server/gcs_virtual_cluster_autoscaler_state_manager.h"

#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/pb_util.h"

namespace ray {
namespace gcs {

GcsVirtualClusterAutoscalerStateManager::GcsVirtualClusterAutoscalerStateManager(
    std::string session_name,
    GcsNodeManager &gcs_node_manager,
    GcsActorManager &gcs_actor_manager,
    const GcsPlacementGroupManager &gcs_placement_group_manager,
    rpc::NodeManagerClientPool &raylet_client_pool,
    InternalKVInterface &kv,
    instrumented_io_context &io_context,
    std::shared_ptr<GcsVirtualClusterManager> gcs_virtual_cluster_manager)
    : GcsAutoscalerStateManager(session_name,
                                gcs_node_manager,
                                gcs_actor_manager,
                                gcs_placement_group_manager,
                                raylet_client_pool,
                                kv,
                                io_context),
      gcs_virtual_cluster_manager_(gcs_virtual_cluster_manager) {}

void GcsVirtualClusterAutoscalerStateManager::HandleGetClusterResourceState(
    rpc::autoscaler::GetClusterResourceStateRequest request,
    rpc::autoscaler::GetClusterResourceStateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  RAY_CHECK(request.last_seen_cluster_resource_state_version() <=
            last_cluster_resource_state_version_);

  auto state = reply->mutable_cluster_resource_state();
  MakeVirtualClusterResourceStatesInternal(state);

  // We are not using GCS_RPC_SEND_REPLY like other GCS managers to avoid the client
  // having to parse the gcs status code embedded.
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsVirtualClusterAutoscalerStateManager::MakeVirtualClusterResourceStatesInternal(
    rpc::autoscaler::ClusterResourceState *state) {
  RAY_LOG(INFO) << "Getting virtual cluster resource states";
  auto virtual_cluster_states = state->mutable_virtual_cluster_states();
  auto primary_cluster = gcs_virtual_cluster_manager_->GetPrimaryCluster();
  primary_cluster->ForeachVirtualCluster(
      [virtual_cluster_states,
       this](const std::shared_ptr<VirtualCluster> &virtual_cluster) {
        rpc::autoscaler::VirtualClusterState virtual_cluster_state;
        auto parent_id =
            (VirtualClusterID::FromBinary(virtual_cluster->GetID())).ParentID();
        if (!parent_id.IsNil()) {
          virtual_cluster_state.set_parent_virtual_cluster_id(parent_id.Binary());
        }
        virtual_cluster_state.set_divisible(virtual_cluster->Divisible());
        virtual_cluster_state.set_revision(virtual_cluster->GetRevision());
        // Collect all nodes belonging to this virtual cluster.
        const auto &visible_node_instances = virtual_cluster->GetVisibleNodeInstances();
        if (virtual_cluster->Divisible()) {
          for (const auto &[template_id, job_node_instances] : visible_node_instances) {
            const auto unassigned_instances_iter =
                job_node_instances.find(kUndividedClusterId);
            if (unassigned_instances_iter != job_node_instances.end()) {
              for (const auto &[id, node_instance] : unassigned_instances_iter->second) {
                virtual_cluster_state.add_nodes(id);
              }
            }
          }
        } else {
          for (const auto &[template_id, job_node_instances] : visible_node_instances) {
            for (const auto &[job_cluster_id, node_instances] : job_node_instances) {
              for (const auto &[id, node_instance] : node_instances) {
                virtual_cluster_state.add_nodes(id);
              }
            }
          }
          // Collect the pending resource requests in this virtual cluster.
          GetVirtualClusterPendingResourceRequests(&virtual_cluster_state);
        }

        (*virtual_cluster_states)[virtual_cluster->GetID()] = virtual_cluster_state;
      });

  // Collect the pending gang resource requests for each virtual cluster.
  GetVirtualClusterPendingGangResourceRequests(state);

  // Collect the info of the primary cluster.
  rpc::autoscaler::VirtualClusterState primary_cluster_state;
  primary_cluster_state.set_divisible(true);
  primary_cluster_state.set_revision(primary_cluster->GetRevision());
  // For the primary cluster, we only need the nodes unassinged to any virtual cluster.
  for (const auto &[template_id, job_node_instances] :
       primary_cluster->GetVisibleNodeInstances()) {
    const auto unassigned_instances_iter = job_node_instances.find(kUndividedClusterId);
    if (unassigned_instances_iter != job_node_instances.end()) {
      for (const auto &[id, node_instance] : unassigned_instances_iter->second) {
        primary_cluster_state.add_nodes(id);
      }
    }
  }
  (*virtual_cluster_states)[kPrimaryClusterID] = primary_cluster_state;

  // Collect the cluster-level info.
  state->set_last_seen_autoscaler_state_version(last_seen_autoscaler_state_version_);
  state->set_cluster_resource_state_version(
      IncrementAndGetNextClusterResourceStateVersion());
  state->set_cluster_session_name(session_name_);

  GetNodeStates(state);
  // For now, we only support resource contraints at the cluster-level.
  GetClusterResourceConstraints(state);
}

void GcsVirtualClusterAutoscalerStateManager::GetVirtualClusterPendingResourceRequests(
    rpc::autoscaler::VirtualClusterState *state) {
  absl::flat_hash_map<google::protobuf::Map<std::string, double>, rpc::ResourceDemand>
      aggregate_load;
  for (const auto &node : state->nodes()) {
    const auto node_id = NodeID::FromHex(node);
    auto const node_resource_iter = node_resource_info_.find(node_id);
    if (node_resource_iter != node_resource_info_.end()) {
      auto const &node_resource_data = node_resource_iter->second.second;
      gcs::FillAggregateLoad(node_resource_data, &aggregate_load);
    }
  }

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

void GcsVirtualClusterAutoscalerStateManager::
    GetVirtualClusterPendingGangResourceRequests(
        rpc::autoscaler::ClusterResourceState *state) {
  // Get the gang resource requests from the placement group load.
  auto placement_group_load = gcs_placement_group_manager_.GetPlacementGroupLoad();
  if (!placement_group_load || placement_group_load->placement_group_data_size() == 0) {
    return;
  }

  // Iterate through each placement group load.
  for (auto &&pg_data :
       std::move(*placement_group_load->mutable_placement_group_data())) {
    auto virtual_cluster_state_iter =
        state->mutable_virtual_cluster_states()->find(pg_data.virtual_cluster_id());
    if (virtual_cluster_state_iter == state->mutable_virtual_cluster_states()->end()) {
      continue;
    }
    auto *gang_resource_req =
        virtual_cluster_state_iter->second.add_pending_gang_resource_requests();
    auto pg_state = pg_data.state();
    auto pg_id = PlacementGroupID::FromBinary(pg_data.placement_group_id());
    // For each placement group, if it's not pending/rescheduling, skip it since.
    // it's not part of the load.
    if (pg_state != rpc::PlacementGroupTableData::PENDING &&
        pg_state != rpc::PlacementGroupTableData::RESCHEDULING) {
      continue;
    }

    const auto pg_constraint =
        GenPlacementConstraintForPlacementGroup(pg_id.Hex(), pg_data.strategy());

    // Add the strategy as detail info for the gang resource request.
    gang_resource_req->set_details(FormatPlacementGroupDetails(pg_data));

    // Copy the PG's bundles to the request.
    for (auto &&bundle : std::move(*pg_data.mutable_bundles())) {
      if (!NodeID::FromBinary(bundle.node_id()).IsNil()) {
        // We will be skipping **placed** bundle (which has node id associated with it).
        // This is to avoid double counting the bundles that are already placed when
        // reporting PG related load.
        RAY_CHECK(pg_state == rpc::PlacementGroupTableData::RESCHEDULING);
        // NOTE: This bundle is placed in a PG, this must be a bundle that was lost due
        // to node crashed.
        continue;
      }
      // Add the resources.
      auto resource_req = gang_resource_req->add_requests();
      *resource_req->mutable_resources_bundle() =
          std::move(*bundle.mutable_unit_resources());

      // Add the placement constraint.
      if (pg_constraint.has_value()) {
        resource_req->add_placement_constraints()->CopyFrom(pg_constraint.value());
      }
    }
  }
}

}  // namespace gcs
}  // namespace ray