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

#include "ray/gcs/gcs_autoscaler_state_manager.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/protobuf_utils.h"
#include "ray/util/string_utils.h"
#include "ray/util/time.h"

namespace ray {
namespace gcs {

GcsAutoscalerStateManager::GcsAutoscalerStateManager(
    std::string session_name,
    GcsNodeManager &gcs_node_manager,
    GcsActorManager &gcs_actor_manager,
    const GcsPlacementGroupManager &gcs_placement_group_manager,
    rpc::RayletClientPool &raylet_client_pool,
    InternalKVInterface &kv,
    instrumented_io_context &io_context,
    pubsub::GcsPublisher *gcs_publisher)
    : session_name_(std::move(session_name)),
      gcs_node_manager_(gcs_node_manager),
      gcs_actor_manager_(gcs_actor_manager),
      gcs_placement_group_manager_(gcs_placement_group_manager),
      raylet_client_pool_(raylet_client_pool),
      kv_(kv),
      io_context_(io_context),
      gcs_publisher_(gcs_publisher) {}

void GcsAutoscalerStateManager::HandleGetClusterResourceState(
    rpc::autoscaler::GetClusterResourceStateRequest request,
    rpc::autoscaler::GetClusterResourceStateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
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
  RAY_CHECK(thread_checker_.IsOnSameThread());

  // Create the callback to cancel the infeasible requests if the feature is enabled
  bool has_new_infeasible_requests = false;
  std::function<void()> callback = [this, &has_new_infeasible_requests]() {
    bool enable_infeasible_task_early_exit =
        RayConfig::instance().enable_infeasible_task_early_exit();

    if (enable_infeasible_task_early_exit) {
      this->CancelInfeasibleRequests();
    } else if (has_new_infeasible_requests) {
      // publish error message
      std::string error_message =
          "There are tasks with infeasible resource requests that cannot "
          "be scheduled. See "
          "https://docs.ray.io/en/latest/ray-core/scheduling/"
          "index.html#ray-scheduling-resources "
          "for more details. Possible solutions: "
          "1. Updating the ray cluster to include nodes with all required resources "
          "2. To cause the tasks with infeasible requests to raise an error instead "
          "of hanging, set the 'RAY_enable_infeasible_task_early_exit=true'. "
          "This feature will be turned on by default in a future release of Ray.";
      RAY_LOG(WARNING) << error_message;

      if (gcs_publisher_ != nullptr) {
        std::string error_type = "infeasible_resource_requests";
        auto error_data = CreateErrorTableData(
            error_type, error_message, absl::FromUnixMillis(current_time_ms()));
        gcs_publisher_->PublishError(session_name_, std::move(error_data));
      }
    }
  };

  // Never seen any autoscaling state before - so just takes this.
  if (!autoscaling_state_.has_value()) {
    autoscaling_state_ = *std::move(request.mutable_autoscaling_state());

    if (autoscaling_state_->infeasible_resource_requests_size() > 0) {
      has_new_infeasible_requests = true;
    }

    send_reply_callback(ray::Status::OK(), callback, nullptr);
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
    send_reply_callback(ray::Status::OK(), callback, nullptr);
    return;
  }

  // We should overwrite the cache version.
  if (autoscaling_state_->infeasible_resource_requests_size() <
      request.mutable_autoscaling_state()->infeasible_resource_requests_size()) {
    has_new_infeasible_requests = true;
  }
  autoscaling_state_ = std::move(*request.mutable_autoscaling_state());
  send_reply_callback(ray::Status::OK(), callback, nullptr);
}

void GcsAutoscalerStateManager::HandleRequestClusterResourceConstraint(
    rpc::autoscaler::RequestClusterResourceConstraintRequest request,
    rpc::autoscaler::RequestClusterResourceConstraintReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  cluster_resource_constraint_ =
      std::move(*request.mutable_cluster_resource_constraint());

  // We are not using GCS_RPC_SEND_REPLY like other GCS managers to avoid the client
  // having to parse the gcs status code embedded.
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsAutoscalerStateManager::HandleReportClusterConfig(
    rpc::autoscaler::ReportClusterConfigRequest request,
    rpc::autoscaler::ReportClusterConfigReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  // Save to kv for persistency in case of GCS FT.
  kv_.Put(kGcsAutoscalerStateNamespace,
          kGcsAutoscalerClusterConfigKey,
          request.cluster_config().SerializeAsString(),
          /*overwrite=*/true,
          {[send_reply_callback = std::move(send_reply_callback)](bool added) {
             send_reply_callback(ray::Status::OK(), nullptr, nullptr);
           },
           io_context_});
}

void GcsAutoscalerStateManager::HandleGetClusterStatus(
    rpc::autoscaler::GetClusterStatusRequest request,
    rpc::autoscaler::GetClusterStatusReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  auto ray_resource_state = reply->mutable_cluster_resource_state();
  MakeClusterResourceStateInternal(ray_resource_state);

  if (autoscaling_state_) {
    reply->mutable_autoscaling_state()->CopyFrom(*autoscaling_state_);
  }
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsAutoscalerStateManager::MakeClusterResourceStateInternal(
    rpc::autoscaler::ClusterResourceState *state) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
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
  RAY_CHECK(thread_checker_.IsOnSameThread());
  // Get the gang resource requests from the placement group load.
  auto placement_group_load = gcs_placement_group_manager_.GetPlacementGroupLoad();
  if (!placement_group_load || placement_group_load->placement_group_data_size() == 0) {
    return;
  }

  // Iterate through each placement group load.
  for (auto &&pg_data :
       std::move(*placement_group_load->mutable_placement_group_data())) {
    auto *gang_resource_req = state->add_pending_gang_resource_requests();
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

    // Create a BundleSelector. Only one BundleSelector will be created for now.
    // Multiple will be added when we implement the fallback mechanism.
    auto *bundle_selector = gang_resource_req->add_bundle_selectors();

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

      const auto &unit_resources = bundle.unit_resources();

      // Add the resources. This field will be removed after migrating to
      // use the BundleSelector for GangResourceRequests.
      auto legacy_resource_req = gang_resource_req->add_requests();
      *legacy_resource_req->mutable_resources_bundle() = unit_resources;

      // Add ResourceRequest for this bundle.
      auto *bundle_resource_req = bundle_selector->add_resource_requests();
      *bundle_resource_req->mutable_resources_bundle() = unit_resources;

      // Parse label selector map into LabelSelector proto in ResourceRequest
      if (!bundle.label_selector().empty()) {
        ray::LabelSelector selector(bundle.label_selector());
        selector.ToProto(bundle_resource_req->add_label_selectors());
      }

      // Add the placement constraint.
      if (pg_constraint.has_value()) {
        legacy_resource_req->add_placement_constraints()->CopyFrom(pg_constraint.value());
        bundle_resource_req->add_placement_constraints()->CopyFrom(pg_constraint.value());
      }
    }
  }
}

void GcsAutoscalerStateManager::GetClusterResourceConstraints(
    rpc::autoscaler::ClusterResourceState *state) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  if (cluster_resource_constraint_.has_value()) {
    state->add_cluster_resource_constraints()->CopyFrom(
        cluster_resource_constraint_.value());
  }
}

void GcsAutoscalerStateManager::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  NodeID node_id = NodeID::FromBinary(node.node_id());
  if (node_resource_info_.contains(node_id)) {
    // early termination as we already know about this node
    return;
  }
  auto node_info =
      node_resource_info_
          .emplace(node_id, std::make_pair(absl::Now(), rpc::ResourcesData()))
          .first;
  // Note: We populate total available resources but not load (which is only received from
  // autoscaler reports). Temporary underreporting when node is added is fine.
  (*node_info->second.second.mutable_resources_total()) = node.resources_total();
  (*node_info->second.second.mutable_resources_available()) = node.resources_total();
  // Populate node labels.
  (*node_info->second.second.mutable_labels()) = node.labels();
}

void GcsAutoscalerStateManager::UpdateResourceLoadAndUsage(rpc::ResourcesData data) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  NodeID node_id = NodeID::FromBinary(data.node_id());
  auto iter = node_resource_info_.find(node_id);
  if (iter == node_resource_info_.end()) {
    RAY_LOG(WARNING).WithField(node_id)
        << "Ignoring resource usage for node that is not alive.";
    return;
  }

  auto &new_data = iter->second.second;
  new_data = std::move(data);
  // Last update time
  iter->second.first = absl::Now();
}

absl::flat_hash_map<ResourceDemandKey, rpc::ResourceDemand>
GcsAutoscalerStateManager::GetAggregatedResourceLoad() const {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  absl::flat_hash_map<ResourceDemandKey, rpc::ResourceDemand> aggregate_load;
  for (const auto &info : node_resource_info_) {
    gcs::FillAggregateLoad(info.second.second, &aggregate_load);
  }
  return aggregate_load;
};

void GcsAutoscalerStateManager::Initialize(const GcsInitData &gcs_init_data) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  for (const auto &entry : gcs_init_data.Nodes()) {
    if (entry.second.state() == rpc::GcsNodeInfo::ALIVE) {
      OnNodeAdd(entry.second);
    }
  }
}

void GcsAutoscalerStateManager::GetPendingResourceRequests(
    rpc::autoscaler::ClusterResourceState *state) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  auto aggregate_load = GetAggregatedResourceLoad();
  for (auto &[key, demand] : aggregate_load) {
    const auto &shape = key.shape;

    auto num_pending = demand.num_infeasible_requests_queued() + demand.backlog_size() +
                       demand.num_ready_requests_queued();
    if (num_pending > 0) {
      auto pending_req = state->add_pending_resource_requests();
      pending_req->set_count(num_pending);
      auto req = pending_req->mutable_request();
      req->mutable_resources_bundle()->insert(shape.begin(), shape.end());

      // Add label selectors to ResourceRequest
      for (auto &selector : key.label_selectors) {
        *req->add_label_selectors() = std::move(selector);
      }
    }
  }
}

void GcsAutoscalerStateManager::GetNodeStates(
    rpc::autoscaler::ClusterResourceState *state) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  auto populate_node_state = [this, state](const rpc::GcsNodeInfo &gcs_node_info) {
    auto node_state_proto = state->add_node_states();
    node_state_proto->set_node_id(gcs_node_info.node_id());
    node_state_proto->set_instance_id(gcs_node_info.instance_id());
    node_state_proto->set_ray_node_type_name(gcs_node_info.node_type_name());
    node_state_proto->set_node_state_version(last_cluster_resource_state_version_);
    node_state_proto->set_node_ip_address(gcs_node_info.node_manager_address());
    node_state_proto->set_instance_type_name(gcs_node_info.instance_type_name());

    // The only node state we use from GcsNodeInfo is the dead state.
    // All others are populated with the locally kept ResourcesData,
    // which may be more stale than GcsNodeInfo but is more consistent between
    // usage and load. GcsNodeInfo state contains only usage and is updated with
    // Ray Syncer usage messages, which happen at a much higher cadence than
    // autoscaler status polls, and so could be out of sync with load data,
    // which is only sent in response to the poll.
    //
    // See (https://github.com/ray-project/ray/issues/36926) for examples.
    if (gcs_node_info.state() == rpc::GcsNodeInfo::DEAD) {
      node_state_proto->set_status(rpc::autoscaler::NodeStatus::DEAD);
      // We don't need populate other info for a dead node.
      return;
    }

    node_state_proto->mutable_node_activity()->CopyFrom(
        gcs_node_info.state_snapshot().node_activity());

    auto const node_id = NodeID::FromBinary(node_state_proto->node_id());
    // The node is alive. We need to check if the node is idle.
    auto node_resource_iter = node_resource_info_.find(node_id);

    if (node_resource_iter == node_resource_info_.end()) {
      // TODO:(zac)  There exists a possibility that the GcsNodeManager node list is more
      // up to date then the resource information within this class.  In the future all
      // state will get updated transactionally together, but for now, we'll utilize this
      // 'escape hatch' option and in place update the state. See
      // https://github.com/ray-project/ray/issues/57009 and once resolved we can delete
      // this logic
      OnNodeAdd(gcs_node_info);
      node_resource_iter = node_resource_info_.find(node_id);
    }
    RAY_CHECK(node_resource_iter != node_resource_info_.end());

    auto const &node_resource_item = node_resource_iter->second;
    auto const &node_resource_data = node_resource_item.second;
    if (node_resource_data.is_draining()) {
      node_state_proto->set_status(rpc::autoscaler::NodeStatus::DRAINING);
    } else if (node_resource_data.idle_duration_ms() > 0) {
      // The node was reported idle.
      node_state_proto->set_status(rpc::autoscaler::NodeStatus::IDLE);

      // We approximate the idle duration by the time since the last idle report
      // plus the idle duration reported by the node:
      //  idle_dur = <idle-dur-reported-by-raylet> +
      //             <time-since-autoscaler-state-manager-gets-last-report>
      //
      // This is because with lightweight resource update, we don't keep reporting
      // the idle time duration when there's no resource change. We also don't want to
      // use raylet reported idle timestamp since there might be clock skew.
      node_state_proto->set_idle_duration_ms(
          node_resource_data.idle_duration_ms() +
          absl::ToInt64Milliseconds(absl::Now() - node_resource_item.first));
    } else {
      node_state_proto->set_status(rpc::autoscaler::NodeStatus::RUNNING);
    }

    // Copy resource available
    const auto &available = node_resource_data.resources_available();
    node_state_proto->mutable_available_resources()->insert(available.begin(),
                                                            available.end());

    // Copy total resources
    const auto &total = node_resource_data.resources_total();
    node_state_proto->mutable_total_resources()->insert(total.begin(), total.end());

    // Add dynamic PG labels.
    // DEPRECATED: Dynamic labels feature is deprecated. Do not introduce new usages.
    // This assignment is kept only for backward compatibility in the autoscaler, where
    // the placement group ID is needed to enforce antiaffinity constraints for
    // strict-spread placement group scheduling.
    const auto &pgs_on_node = gcs_placement_group_manager_.GetBundlesOnNode(node_id);
    for (const auto &[pg_id, _bundle_indices] : pgs_on_node) {
      node_state_proto->mutable_dynamic_labels()->insert(
          {FormatPlacementGroupLabelName(pg_id.Hex()), ""});
    }
    // Add Ray node labels.
    const auto &node_labels = gcs_node_info.labels();
    node_state_proto->mutable_labels()->insert(node_labels.begin(), node_labels.end());
  };

  const auto alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  std::for_each(alive_nodes.begin(), alive_nodes.end(), [&](const auto &gcs_node_info) {
    populate_node_state(*gcs_node_info.second);
  });

  // This might be large if there are many nodes for a long-running cluster.
  // However, since we don't report resources for a dead node, the data size being
  // reported by dead node should be small.
  // TODO(rickyx): We will need to GC the head nodes in the future.
  // https://github.com/ray-project/ray/issues/35874
  const auto dead_nodes = gcs_node_manager_.GetAllDeadNodes();
  std::for_each(dead_nodes.begin(), dead_nodes.end(), [&](const auto &gcs_node_info) {
    populate_node_state(*gcs_node_info.second);
  });
}

void GcsAutoscalerStateManager::HandleDrainNode(
    rpc::autoscaler::DrainNodeRequest request,
    rpc::autoscaler::DrainNodeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  const NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(INFO).WithField(node_id)
      << "HandleDrainNode, reason: " << request.reason_message()
      << ", deadline: " << request.deadline_timestamp_ms();

  int64_t draining_deadline_timestamp_ms = request.deadline_timestamp_ms();
  if (draining_deadline_timestamp_ms < 0) {
    std::ostringstream stream;
    stream << "Draining deadline must be non-negative, received "
           << draining_deadline_timestamp_ms;
    auto msg = stream.str();
    RAY_LOG(WARNING) << msg;
    send_reply_callback(Status::Invalid(msg), nullptr, nullptr);
    return;
  }

  auto maybe_node = gcs_node_manager_.GetAliveNode(node_id);
  if (!maybe_node.has_value()) {
    if (gcs_node_manager_.IsNodeDead(node_id)) {
      // The node is dead so treat it as drained.
      reply->set_is_accepted(true);
    } else {
      // Since gcs only stores limit number of dead nodes
      // so we don't know whether the node is dead or doesn't exist.
      // Since it's not running so still treat it as drained.
      RAY_LOG(WARNING).WithField(node_id) << "Request to drain an unknown node";
      reply->set_is_accepted(true);
    }
    send_reply_callback(ray::Status::OK(), nullptr, nullptr);
    return;
  }

  gcs_actor_manager_.SetPreemptedAndPublish(node_id);

  auto node = std::move(maybe_node.value());
  auto raylet_address = rpc::RayletClientPool::GenerateRayletAddress(
      node_id, node->node_manager_address(), node->node_manager_port());
  const auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(raylet_address);
  raylet_client->DrainRaylet(
      request.reason(),
      request.reason_message(),
      draining_deadline_timestamp_ms,
      [this, request, reply, send_reply_callback, node_id](
          const Status &status, const rpc::DrainRayletReply &raylet_reply) {
        reply->set_is_accepted(raylet_reply.is_accepted());

        if (raylet_reply.is_accepted()) {
          gcs_node_manager_.SetNodeDraining(
              node_id, std::make_shared<rpc::autoscaler::DrainNodeRequest>(request));
        } else {
          reply->set_rejection_reason_message(raylet_reply.rejection_reason_message());
        }
        send_reply_callback(status, nullptr, nullptr);
      });
}

std::string GcsAutoscalerStateManager::DebugString() const {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  std::ostringstream stream;
  stream << "GcsAutoscalerStateManager: "
         << "\n- last_seen_autoscaler_state_version_: "
         << last_seen_autoscaler_state_version_
         << "\n- last_cluster_resource_state_version_: "
         << last_cluster_resource_state_version_ << "\n- pending demands:\n";

  auto aggregate_load = GetAggregatedResourceLoad();
  for (const auto &[key, demand] : aggregate_load) {
    auto num_pending = demand.num_infeasible_requests_queued() + demand.backlog_size() +
                       demand.num_ready_requests_queued();

    stream << "\t{";
    if (num_pending > 0) {
      for (const auto &entry : key.shape) {
        stream << entry.first << ": " << entry.second << ", ";
      }
      if (!key.label_selectors.empty()) {
        stream << "label_selectors: [";
        for (const auto &selector : key.label_selectors) {
          stream << "{";
          for (const auto &constraint : selector.label_constraints()) {
            stream << constraint.label_key() << " "
                   << (constraint.operator_() ==
                               rpc::LabelSelectorOperator::LABEL_OPERATOR_IN
                           ? "in"
                           : "!in")
                   << " [";
            for (const auto &val : constraint.label_values()) {
              stream << val << ",";
            }
            stream << "]"
                   << " ";
          }
          stream << "}, ";
        }
        stream << "]";
      }
    }
    stream << "} * " << num_pending << "\n";
  }
  return stream.str();
}

absl::flat_hash_map<ray::NodeID, std::vector<google::protobuf::Map<std::string, double>>>
GcsAutoscalerStateManager::GetPerNodeInfeasibleResourceRequests() const {
  RAY_CHECK(thread_checker_.IsOnSameThread());

  absl::flat_hash_map<ray::NodeID,
                      std::vector<google::protobuf::Map<std::string, double>>>
      per_node_infeasible_requests;
  if (!autoscaling_state_.has_value()) {
    return per_node_infeasible_requests;
  }

  // Early return if there is no infeasible resource requests
  auto infeasible_resource_shapes_size =
      autoscaling_state_.value().infeasible_resource_requests_size();
  if (infeasible_resource_shapes_size == 0) {
    return per_node_infeasible_requests;
  }

  // Obtain the infeasible requests from the autoscaler state
  std::vector<google::protobuf::Map<std::string, double>>
      autoscaler_infeasible_resource_shapes;
  autoscaler_infeasible_resource_shapes.reserve(infeasible_resource_shapes_size);
  for (int i = 0; i < infeasible_resource_shapes_size; i++) {
    autoscaler_infeasible_resource_shapes.emplace_back(
        autoscaling_state_.value().infeasible_resource_requests(i).resources_bundle());
  }

  // Collect the infeasible requests per node
  for (const auto &[node_id, time_resource_data_pair] : node_resource_info_) {
    // Iterate through the resource load on each nodes
    const auto &resource_load_by_shape =
        time_resource_data_pair.second.resource_load_by_shape();

    for (int i = 0; i < resource_load_by_shape.resource_demands_size(); i++) {
      // Check with each infeasible resource shapes from the autoscaler state
      for (const auto &shape : autoscaler_infeasible_resource_shapes) {
        const auto &resource_demand = resource_load_by_shape.resource_demands(i);
        if (resource_demand.num_infeasible_requests_queued() > 0 &&
            MapEqual(shape, resource_demand.shape())) {
          per_node_infeasible_requests[node_id].emplace_back(std::move(shape));
          break;
        }
      }
    }
  }
  return per_node_infeasible_requests;
}

void GcsAutoscalerStateManager::CancelInfeasibleRequests() const {
  RAY_CHECK(thread_checker_.IsOnSameThread());

  // Obtain the node & infeasible request mapping
  auto per_node_infeasible_requests = GetPerNodeInfeasibleResourceRequests();
  if (per_node_infeasible_requests.empty()) {
    return;
  }

  // Cancel the infeasible requests for each nodes
  for (const auto &node_infeasible_request_pair : per_node_infeasible_requests) {
    const auto &node_id = node_infeasible_request_pair.first;
    const auto &infeasible_shapes = node_infeasible_request_pair.second;
    auto node = gcs_node_manager_.GetAliveNode(node_id);
    if (!node.has_value()) {
      continue;
    }
    auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
        node_id, node.value()->node_manager_address(), node.value()->node_manager_port());
    const auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(remote_address);

    std::string resource_shapes_str =
        ray::VectorToString(infeasible_shapes, ray::DebugString<std::string, double>);

    RAY_LOG(WARNING) << "Canceling infeasible requests on node " << node_id
                     << " with infeasible_shapes=" << resource_shapes_str;

    raylet_client->CancelLeasesWithResourceShapes(
        infeasible_shapes,
        [node_id](const Status &status,
                  const rpc::CancelLeasesWithResourceShapesReply &) {
          if (status.ok()) {
            RAY_LOG(INFO) << "Infeasible tasks cancelled on node " << node_id;
          } else {
            // Autoscaler will eventually retry the infeasible task cancellation
            RAY_LOG(WARNING) << "Failed to cancel infeasible requests on node " << node_id
                             << ". RPC failed with status: " << status.ToString();
          }
        });
  }
}

}  // namespace gcs
}  // namespace ray
