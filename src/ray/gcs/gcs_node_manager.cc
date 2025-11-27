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

#include "ray/gcs/gcs_node_manager.h"

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "ray/common/protobuf_utils.h"
#include "ray/observability/ray_node_definition_event.h"
#include "ray/observability/ray_node_lifecycle_event.h"
#include "ray/util/logging.h"
#include "ray/util/time.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

GcsNodeManager::GcsNodeManager(
    pubsub::GcsPublisher *gcs_publisher,
    gcs::GcsTableStorage *gcs_table_storage,
    instrumented_io_context &io_context,
    rpc::RayletClientPool *raylet_client_pool,
    const ClusterID &cluster_id,
    observability::RayEventRecorderInterface &ray_event_recorder,
    const std::string &session_name)
    : gcs_publisher_(gcs_publisher),
      gcs_table_storage_(gcs_table_storage),
      io_context_(io_context),
      raylet_client_pool_(raylet_client_pool),
      cluster_id_(cluster_id),
      ray_event_recorder_(ray_event_recorder),
      session_name_(session_name),
      export_event_write_enabled_(IsExportAPIEnabledNode()) {}

void GcsNodeManager::WriteNodeExportEvent(const rpc::GcsNodeInfo &node_info,
                                          bool is_register_event) const {
  if (RayConfig::instance().enable_ray_event()) {
    std::vector<std::unique_ptr<observability::RayEventInterface>> events;
    if (is_register_event) {
      events.push_back(std::make_unique<observability::RayNodeDefinitionEvent>(
          node_info, session_name_));
    }
    events.push_back(
        std::make_unique<observability::RayNodeLifecycleEvent>(node_info, session_name_));
    ray_event_recorder_.AddEvents(std::move(events));
    return;
  }
  if (!export_event_write_enabled_) {
    return;
  }
  std::shared_ptr<rpc::ExportNodeData> export_node_data_ptr =
      std::make_shared<rpc::ExportNodeData>();
  export_node_data_ptr->set_node_id(node_info.node_id());
  export_node_data_ptr->set_node_manager_address(node_info.node_manager_address());
  export_node_data_ptr->mutable_resources_total()->insert(
      node_info.resources_total().begin(), node_info.resources_total().end());
  export_node_data_ptr->set_node_name(node_info.node_name());
  export_node_data_ptr->set_start_time_ms(node_info.start_time_ms());
  export_node_data_ptr->set_end_time_ms(node_info.end_time_ms());
  export_node_data_ptr->set_is_head_node(node_info.is_head_node());
  export_node_data_ptr->mutable_labels()->insert(node_info.labels().begin(),
                                                 node_info.labels().end());
  export_node_data_ptr->set_state(ConvertGCSNodeStateToExport(node_info.state()));
  if (!node_info.death_info().reason_message().empty() ||
      node_info.death_info().reason() !=
          rpc::NodeDeathInfo_Reason::NodeDeathInfo_Reason_UNSPECIFIED) {
    export_node_data_ptr->mutable_death_info()->set_reason_message(
        node_info.death_info().reason_message());
    export_node_data_ptr->mutable_death_info()->set_reason(
        ConvertNodeDeathReasonToExport(node_info.death_info().reason()));
  }
  RayExportEvent(export_node_data_ptr).SendEvent();
}

void GcsNodeManager::HandleGetClusterId(rpc::GetClusterIdRequest request,
                                        rpc::GetClusterIdReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  reply->set_cluster_id(cluster_id_.Binary());
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsNodeManager::HandleRegisterNode(rpc::RegisterNodeRequest request,
                                        rpc::RegisterNodeReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  // This function invokes a read lock
  // TODO(#56391): node creation time should be assigned here instead of in the raylet.
  const rpc::GcsNodeInfo &node_info = request.node_info();
  NodeID node_id = NodeID::FromBinary(node_info.node_id());
  RAY_LOG(INFO)
          .WithField(node_id)
          .WithField("node_name", node_info.node_name())
          .WithField("node_address", node_info.node_manager_address())
      << "Registering new node.";

  auto on_done = [this, node_id, node_info_copy = node_info, reply, send_reply_callback](
                     const Status &status) mutable {
    RAY_CHECK_OK(status) << "Failed to register node '" << node_id << "'.";
    absl::MutexLock lock_(&mutex_);
    RAY_LOG(DEBUG).WithField(node_id) << "Finished registering node.";
    AddNodeToCache(std::make_shared<rpc::GcsNodeInfo>(node_info_copy));
    WriteNodeExportEvent(node_info_copy, /*is_register_event*/ true);
    PublishNodeInfoToPubsub(node_id, node_info_copy);
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  if (node_info.is_head_node()) {
    // mark all old head nodes as dead if exists:
    // 1. should never happen when HA is not used
    // 2. happens when a new head node is started
    std::vector<NodeID> head_nodes;
    {
      absl::ReaderMutexLock lock(&mutex_);
      for (auto &node : alive_nodes_) {
        if (node.second->is_head_node()) {
          head_nodes.push_back(node.first);
        }
      }
    }

    RAY_CHECK_LE(head_nodes.size(), 1UL);
    if (head_nodes.size() == 1) {
      OnNodeFailure(head_nodes[0],
                    [this, node_id, node_info, on_done = std::move(on_done)]() {
                      gcs_table_storage_->NodeTable().Put(
                          node_id, node_info, {on_done, io_context_});
                    });
    } else {
      gcs_table_storage_->NodeTable().Put(
          node_id, node_info, {std::move(on_done), io_context_});
    }
  } else {
    gcs_table_storage_->NodeTable().Put(
        node_id, node_info, {std::move(on_done), io_context_});
  }
  ++counts_[CountType::REGISTER_NODE_REQUEST];
}

void GcsNodeManager::HandleCheckAlive(rpc::CheckAliveRequest request,
                                      rpc::CheckAliveReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  absl::ReaderMutexLock lock(&mutex_);
  reply->set_ray_version(kRayVersion);
  for (const auto &id : request.node_ids()) {
    const auto node_id = NodeID::FromBinary(id);
    const bool is_alive = alive_nodes_.contains(node_id);
    reply->mutable_raylet_alive()->Add(is_alive);
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsNodeManager::HandleUnregisterNode(rpc::UnregisterNodeRequest request,
                                          rpc::UnregisterNodeReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG).WithField(node_id) << "HandleUnregisterNode() for node";
  auto node = RemoveNodeFromCache(
      node_id, request.node_death_info(), rpc::GcsNodeInfo::DEAD, current_sys_time_ms());
  if (!node) {
    RAY_LOG(INFO).WithField(node_id) << "Node is already removed";
    return;
  }

  AddDeadNodeToCache(node);
  auto node_info_delta = std::make_shared<rpc::GcsNodeInfo>();
  node_info_delta->set_node_id(node->node_id());
  node_info_delta->mutable_death_info()->CopyFrom(request.node_death_info());
  node_info_delta->set_state(node->state());
  node_info_delta->set_end_time_ms(node->end_time_ms());

  auto on_put_done = [this, node_id, node_info_delta, node](const Status &status) {
    PublishNodeInfoToPubsub(node_id, *node_info_delta);
    WriteNodeExportEvent(*node, /*is_register_event*/ false);
  };
  gcs_table_storage_->NodeTable().Put(node_id, *node, {on_put_done, io_context_});
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsNodeManager::HandleDrainNode(rpc::DrainNodeRequest request,
                                     rpc::DrainNodeReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  for (const auto &node_drain_request : request.drain_node_data()) {
    const auto node_id = NodeID::FromBinary(node_drain_request.node_id());

    DrainNode(node_id);
    auto drain_node_status = reply->add_drain_node_status();
    drain_node_status->set_node_id(node_id.Binary());
  };
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::DRAIN_NODE_REQUEST];
}

void GcsNodeManager::DrainNode(const NodeID &node_id) {
  RAY_LOG(INFO).WithField(node_id) << "DrainNode() for node";
  auto maybe_node = GetAliveNode(node_id);
  if (!maybe_node.has_value()) {
    RAY_LOG(WARNING).WithField(node_id) << "Skip draining node which is already removed";
    return;
  }
  auto &node = maybe_node.value();

  // Set the address.
  auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
      node_id, node->node_manager_address(), node->node_manager_port());
  auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(remote_address);
  RAY_CHECK(raylet_client);
  // NOTE(sang): Drain API is not supposed to kill the raylet, but we are doing
  // this until the proper "drain" behavior is implemented.
  raylet_client->ShutdownRaylet(
      node_id,
      /*graceful*/ true,
      [node_id](const Status &status, const rpc::ShutdownRayletReply &reply) {
        RAY_LOG(INFO).WithField(node_id) << "Raylet is drained. Status " << status;
      });
}

void GcsNodeManager::HandleGetAllNodeInfo(rpc::GetAllNodeInfoRequest request,
                                          rpc::GetAllNodeInfoReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  absl::ReaderMutexLock lock(&mutex_);
  int64_t limit =
      (request.limit() > 0) ? request.limit() : std::numeric_limits<int64_t>::max();
  absl::flat_hash_set<NodeID> node_ids;
  absl::flat_hash_set<std::string> node_names;
  absl::flat_hash_set<std::string> node_ip_addresses;
  bool only_node_id_filters = true;
  for (auto &selector : *request.mutable_node_selectors()) {
    switch (selector.node_selector_case()) {
    case rpc::GetAllNodeInfoRequest_NodeSelector::kNodeId:
      node_ids.insert(NodeID::FromBinary(selector.node_id()));
      break;
    case rpc::GetAllNodeInfoRequest_NodeSelector::kNodeName:
      node_names.insert(std::move(*selector.mutable_node_name()));
      only_node_id_filters = false;
      break;
    case rpc::GetAllNodeInfoRequest_NodeSelector::kNodeIpAddress:
      node_ip_addresses.insert(std::move(*selector.mutable_node_ip_address()));
      only_node_id_filters = false;
      break;
    case rpc::GetAllNodeInfoRequest_NodeSelector::NODE_SELECTOR_NOT_SET:
      continue;
    }
  }
  const size_t total_num_nodes = alive_nodes_.size() + dead_nodes_.size();
  int64_t num_added = 0;

  if (request.node_selectors_size() > 0 && only_node_id_filters) {
    // optimized path if request only wants specific node ids
    for (const auto &node_id : node_ids) {
      if (!request.has_state_filter() ||
          request.state_filter() == rpc::GcsNodeInfo::ALIVE) {
        auto iter = alive_nodes_.find(node_id);
        if (iter != alive_nodes_.end()) {
          *reply->add_node_info_list() = *iter->second;
          ++num_added;
        }
      }
      if (!request.has_state_filter() ||
          request.state_filter() == rpc::GcsNodeInfo::DEAD) {
        auto iter = dead_nodes_.find(node_id);
        if (iter != dead_nodes_.end()) {
          *reply->add_node_info_list() = *iter->second;
          ++num_added;
        }
      }
    }
    reply->set_total(total_num_nodes);
    reply->set_num_filtered(total_num_nodes - num_added);
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    ++counts_[CountType::GET_ALL_NODE_INFO_REQUEST];
    return;
  }

  const bool has_node_selectors = request.node_selectors_size() > 0;
  auto add_to_response =
      [&](const absl::flat_hash_map<NodeID, std::shared_ptr<const rpc::GcsNodeInfo>>
              &nodes) {
        for (const auto &[node_id, node_info_ptr] : nodes) {
          if (num_added >= limit) {
            break;
          }
          if (!has_node_selectors || node_ids.contains(node_id) ||
              node_names.contains(node_info_ptr->node_name()) ||
              node_ip_addresses.contains(node_info_ptr->node_manager_address())) {
            *reply->add_node_info_list() = *node_info_ptr;
            num_added += 1;
          }
        }
      };

  if (request.has_state_filter()) {
    switch (request.state_filter()) {
    case rpc::GcsNodeInfo::ALIVE:
      if (!has_node_selectors) {
        reply->mutable_node_info_list()->Reserve(alive_nodes_.size());
      }
      add_to_response(alive_nodes_);
      break;
    case rpc::GcsNodeInfo::DEAD:
      if (!has_node_selectors) {
        reply->mutable_node_info_list()->Reserve(dead_nodes_.size());
      }
      add_to_response(dead_nodes_);
      break;
    default:
      RAY_LOG(ERROR) << "Unexpected state filter: " << request.state_filter();
      break;
    }
  } else {
    if (!has_node_selectors) {
      reply->mutable_node_info_list()->Reserve(alive_nodes_.size() + dead_nodes_.size());
    }
    add_to_response(alive_nodes_);
    add_to_response(dead_nodes_);
  }

  reply->set_total(total_num_nodes);
  reply->set_num_filtered(total_num_nodes - reply->node_info_list_size());
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_NODE_INFO_REQUEST];
}

namespace {
// Utility function to convert GcsNodeInfo to GcsNodeAddressAndLiveness
rpc::GcsNodeAddressAndLiveness ConvertToGcsNodeAddressAndLiveness(
    const rpc::GcsNodeInfo &source) {
  rpc::GcsNodeAddressAndLiveness destination;
  destination.set_node_id(source.node_id());
  destination.set_node_manager_address(source.node_manager_address());
  destination.set_node_manager_port(source.node_manager_port());
  destination.set_object_manager_port(source.object_manager_port());
  destination.set_state(source.state());
  destination.mutable_death_info()->CopyFrom(source.death_info());
  return destination;
}
}  // namespace

void GcsNodeManager::GetAllNodeAddressAndLiveness(
    const absl::flat_hash_set<NodeID> &node_ids,
    std::optional<rpc::GcsNodeInfo::GcsNodeState> state_filter,
    int64_t limit,
    const std::function<void(rpc::GcsNodeAddressAndLiveness &&)> &callback) const {
  absl::ReaderMutexLock lock(&mutex_);
  int64_t num_added = 0;

  if (!node_ids.empty()) {
    // optimized path if request only wants specific node ids
    for (const auto &node_id : node_ids) {
      if (num_added >= limit) {
        break;
      }
      if (!state_filter.has_value() || state_filter == rpc::GcsNodeInfo::ALIVE) {
        auto iter = alive_nodes_.find(node_id);
        if (iter != alive_nodes_.end()) {
          callback(ConvertToGcsNodeAddressAndLiveness(*iter->second));
          ++num_added;
        }
      }
      if (!state_filter.has_value() || state_filter == rpc::GcsNodeInfo::DEAD) {
        auto iter = dead_nodes_.find(node_id);
        if (iter != dead_nodes_.end()) {
          callback(ConvertToGcsNodeAddressAndLiveness(*iter->second));
          ++num_added;
        }
      }
    }
    return;
  }

  auto add_with_callback =
      [&](const absl::flat_hash_map<NodeID, std::shared_ptr<const rpc::GcsNodeInfo>>
              &nodes) {
        for (const auto &[node_id, node_info_ptr] : nodes) {
          if (num_added >= limit) {
            break;
          }
          callback(ConvertToGcsNodeAddressAndLiveness(*node_info_ptr));
          num_added += 1;
        }
      };

  if (state_filter.has_value()) {
    switch (state_filter.value()) {
    case rpc::GcsNodeInfo::ALIVE:
      add_with_callback(alive_nodes_);
      break;
    case rpc::GcsNodeInfo::DEAD:
      add_with_callback(dead_nodes_);
      break;
    default:
      RAY_LOG(ERROR) << "Unexpected state filter: " << state_filter.value();
      break;
    }
  } else {
    add_with_callback(alive_nodes_);
    add_with_callback(dead_nodes_);
  }
}

void GcsNodeManager::HandleGetAllNodeAddressAndLiveness(
    rpc::GetAllNodeAddressAndLivenessRequest request,
    rpc::GetAllNodeAddressAndLivenessReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // Extract node IDs from the request
  absl::flat_hash_set<NodeID> node_ids;
  for (auto &selector : *request.mutable_node_ids()) {
    node_ids.insert(NodeID::FromBinary(selector));
  }

  // Extract state filter from the request
  std::optional<rpc::GcsNodeInfo::GcsNodeState> state_filter;
  if (request.has_state_filter()) {
    state_filter = request.state_filter();
  }

  // Extract limit from the request
  int64_t limit =
      (request.limit() > 0) ? request.limit() : std::numeric_limits<int64_t>::max();

  // Call helper method which handles its own locking and directly populates reply
  GetAllNodeAddressAndLiveness(
      node_ids, state_filter, limit, [reply](rpc::GcsNodeAddressAndLiveness &&node) {
        *reply->add_node_info_list() = std::move(node);
      });

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_NODE_INFO_REQUEST];
}

std::shared_ptr<const rpc::GcsNodeInfo> GcsNodeManager::SelectRandomAliveNode() const {
  absl::ReaderMutexLock lock(&mutex_);
  if (alive_nodes_.empty()) {
    return nullptr;
  }

  static std::mt19937_64 gen_(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> distribution(0, alive_nodes_.size() - 1);
  int key_index = distribution(gen_);
  int index = 0;
  auto iter = alive_nodes_.begin();
  for (; index != key_index && iter != alive_nodes_.end(); ++index, ++iter) {
  }
  return iter->second;
}

std::optional<std::shared_ptr<const rpc::GcsNodeInfo>>
GcsNodeManager::GetAliveNodeFromCache(const ray::NodeID &node_id) const {
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    return {};
  }

  return iter->second;
}

std::optional<rpc::GcsNodeAddressAndLiveness> GcsNodeManager::GetAliveNodeAddress(
    const ray::NodeID &node_id) const {
  absl::ReaderMutexLock lock(&mutex_);
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    return {};
  }

  return ConvertToGcsNodeAddressAndLiveness(*iter->second.get());
}

std::optional<std::shared_ptr<const rpc::GcsNodeInfo>> GcsNodeManager::GetAliveNode(
    const ray::NodeID &node_id) const {
  absl::ReaderMutexLock lock(&mutex_);
  return GetAliveNodeFromCache(node_id);
}

bool GcsNodeManager::IsNodeDead(const ray::NodeID &node_id) const {
  absl::ReaderMutexLock lock(&mutex_);
  return dead_nodes_.contains(node_id);
}

bool GcsNodeManager::IsNodeAlive(const ray::NodeID &node_id) const {
  absl::ReaderMutexLock lock(&mutex_);
  return alive_nodes_.contains(node_id);
}

rpc::NodeDeathInfo GcsNodeManager::InferDeathInfo(const NodeID &node_id) {
  auto iter = draining_nodes_.find(node_id);
  rpc::NodeDeathInfo death_info;
  bool expect_force_termination;
  if (iter == draining_nodes_.end()) {
    expect_force_termination = false;
  } else if (iter->second->deadline_timestamp_ms() == 0) {
    // If there is no draining deadline, there should be no force termination
    expect_force_termination = false;
  } else {
    expect_force_termination =
        (current_sys_time_ms() > iter->second->deadline_timestamp_ms()) &&
        (iter->second->reason() ==
         rpc::autoscaler::DrainNodeReason::DRAIN_NODE_REASON_PREEMPTION);
  }

  if (expect_force_termination) {
    death_info.set_reason(rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED);
    death_info.set_reason_message(iter->second->reason_message());
    RAY_LOG(INFO).WithField(node_id) << "Node was forcibly preempted";
  } else {
    death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    death_info.set_reason_message(
        "health check failed due to missing too many heartbeats");
  }
  return death_info;
}

void GcsNodeManager::AddNode(std::shared_ptr<const rpc::GcsNodeInfo> node) {
  absl::MutexLock lock(&mutex_);
  AddNodeToCache(node);
}

void GcsNodeManager::AddNodeToCache(std::shared_ptr<const rpc::GcsNodeInfo> node) {
  auto node_id = NodeID::FromBinary(node->node_id());
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    alive_nodes_.emplace(node_id, node);
    // Notify all listeners by posting back on their io_context
    for (auto &listener : node_added_listeners_) {
      listener.Post("NodeManager.AddNodeCallback", node);
    }
  }
}

void GcsNodeManager::SetNodeDraining(
    const NodeID &node_id,
    std::shared_ptr<rpc::autoscaler::DrainNodeRequest> drain_request) {
  absl::MutexLock lock(&mutex_);
  auto maybe_node = GetAliveNodeFromCache(node_id);
  if (!maybe_node.has_value()) {
    RAY_LOG(INFO).WithField(node_id)
        << "Skip setting node to be draining, which is already removed";
    return;
  }

  auto iter = draining_nodes_.find(node_id);
  if (iter == draining_nodes_.end()) {
    draining_nodes_.emplace(node_id, drain_request);
    RAY_LOG(INFO).WithField(node_id)
        << "Set node to be draining, request = " << drain_request->DebugString();
  } else {
    RAY_LOG(INFO).WithField(node_id)
        << "Drain request for node already exists. Overwriting the existing request "
        << iter->second->DebugString() << " with the new request "
        << drain_request->DebugString();
    iter->second = drain_request;
  }
}

std::shared_ptr<const rpc::GcsNodeInfo> GcsNodeManager::RemoveNode(
    const NodeID &node_id,
    const rpc::NodeDeathInfo &node_death_info,
    const rpc::GcsNodeInfo::GcsNodeState node_state,
    const int64_t update_time) {
  absl::MutexLock lock(&mutex_);
  return RemoveNodeFromCache(node_id, node_death_info, node_state, update_time);
}

std::shared_ptr<const rpc::GcsNodeInfo> GcsNodeManager::RemoveNodeFromCache(
    const NodeID &node_id,
    const rpc::NodeDeathInfo &node_death_info,
    const rpc::GcsNodeInfo::GcsNodeState node_state,
    const int64_t update_time) {
  std::shared_ptr<const rpc::GcsNodeInfo> removed_node;
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    // Set node death info. For thread safety, we don't update the node info in place (as
    // it's a const) so instead we create a node to return based on the information on
    // hand before removing it from the cache.
    const auto updated = std::make_shared<rpc::GcsNodeInfo>(*iter->second);
    *updated->mutable_death_info() = node_death_info;
    updated->set_state(node_state);
    updated->set_end_time_ms(update_time);
    removed_node = std::shared_ptr<const rpc::GcsNodeInfo>(updated);

    RAY_LOG(INFO).WithField(node_id).WithField("node_name", removed_node->node_name())
        << ", death reason = " << rpc::NodeDeathInfo_Reason_Name(node_death_info.reason())
        << ", death message = " << node_death_info.reason_message();
    // Record stats that there's a new removed node.
    ray_metric_node_failures_total_.Record(1);
    // Remove from alive nodes.
    alive_nodes_.erase(iter);
    // Remove from draining nodes if present.
    draining_nodes_.erase(node_id);
    if (node_death_info.reason() == rpc::NodeDeathInfo::UNEXPECTED_TERMINATION) {
      // Broadcast a warning to all of the drivers indicating that the node
      // has been marked as dead.
      // TODO(rkn): Define this constant somewhere else.
      std::string type = "node_removed";
      std::ostringstream error_message;
      error_message << "The node with node id: " << node_id
                    << " and address: " << removed_node->node_manager_address()
                    << " and node name: " << removed_node->node_name()
                    << " has been marked dead because the detector"
                    << " has missed too many heartbeats from it. This can happen when a "
                       "\t(1) raylet crashes unexpectedly (OOM, etc.) \n"
                    << "\t(2) raylet has lagging heartbeats due to slow network or busy "
                       "workload.";
      RAY_EVENT(ERROR, "RAY_NODE_REMOVED")
              .WithField("node_id", node_id.Hex())
              .WithField("ip", removed_node->node_manager_address())
          << error_message.str();
      RAY_LOG(WARNING) << error_message.str();
      auto error_data = CreateErrorTableData(
          type, error_message.str(), absl::FromUnixMillis(current_time_ms()));
      gcs_publisher_->PublishError(node_id.Hex(), std::move(error_data));
    }

    // Notify all listeners.
    for (auto &listener : node_removed_listeners_) {
      listener.Post("NodeManager.RemoveNodeCallback", removed_node);
    }
  }
  return removed_node;
}

void GcsNodeManager::OnNodeFailure(
    const NodeID &node_id, const std::function<void()> &node_table_updated_callback) {
  absl::MutexLock lock(&mutex_);
  InternalOnNodeFailure(node_id, node_table_updated_callback);
}

void GcsNodeManager::InternalOnNodeFailure(
    const NodeID &node_id, const std::function<void()> &node_table_updated_callback) {
  auto maybe_node = GetAliveNodeFromCache(node_id);
  if (maybe_node.has_value()) {
    rpc::NodeDeathInfo death_info = InferDeathInfo(node_id);
    auto node = RemoveNodeFromCache(
        node_id, death_info, rpc::GcsNodeInfo::DEAD, current_sys_time_ms());

    AddDeadNodeToCache(node);
    rpc::GcsNodeInfo node_info_delta;
    node_info_delta.set_node_id(node->node_id());
    node_info_delta.set_state(node->state());
    node_info_delta.set_end_time_ms(node->end_time_ms());
    node_info_delta.mutable_death_info()->CopyFrom(node->death_info());

    auto on_done = [this,
                    node_id,
                    node_table_updated_callback,
                    node_info_delta = std::move(node_info_delta),
                    node](const Status &status) mutable {
      WriteNodeExportEvent(*node, /*is_register_event*/ false);
      if (node_table_updated_callback != nullptr) {
        node_table_updated_callback();
      }
      PublishNodeInfoToPubsub(node_id, node_info_delta);
    };
    gcs_table_storage_->NodeTable().Put(
        node_id, *node, {std::move(on_done), io_context_});
  } else if (node_table_updated_callback != nullptr) {
    node_table_updated_callback();
  }
}

void GcsNodeManager::Initialize(const GcsInitData &gcs_init_data) {
  absl::MutexLock lock(&mutex_);
  for (const auto &[node_id, node_info] : gcs_init_data.Nodes()) {
    if (node_info.state() == rpc::GcsNodeInfo::ALIVE) {
      AddNodeToCache(std::make_shared<rpc::GcsNodeInfo>(node_info));

      // Ask the raylet to do initialization in case of GCS restart.
      // The protocol is correct because when a new node joined, Raylet will do:
      //    - RegisterNode (write node to the node table)
      //    - Setup subscription
      // With this, it means we only need to ask the node registered to do resubscription.
      // And for the node failed to register, they will crash on the client side due to
      // registration failure.
      auto remote_address = rpc::RayletClientPool::GenerateRayletAddress(
          node_id, node_info.node_manager_address(), node_info.node_manager_port());
      auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(remote_address);
      raylet_client->NotifyGCSRestart(
          [](const Status &status, const rpc::NotifyGCSRestartReply &reply) {
            if (!status.ok()) {
              RAY_LOG(WARNING) << "NotifyGCSRestart failed. This is expected if the "
                                  "target node has died. Status: "
                               << status;
            }
          });
    } else if (node_info.state() == rpc::GcsNodeInfo::DEAD) {
      dead_nodes_.emplace(node_id, std::make_shared<rpc::GcsNodeInfo>(node_info));
      sorted_dead_node_list_.emplace_back(node_id, node_info.end_time_ms());
    }
  }
  std::sort(
      sorted_dead_node_list_.begin(),
      sorted_dead_node_list_.end(),
      [](const auto &left, const auto &right) { return left.second < right.second; });
}

void GcsNodeManager::AddDeadNodeToCache(std::shared_ptr<const rpc::GcsNodeInfo> node) {
  if (dead_nodes_.size() >= RayConfig::instance().maximum_gcs_dead_node_cached_count()) {
    const auto &node_id = sorted_dead_node_list_.front().first;
    gcs_table_storage_->NodeTable().Delete(node_id, {[](const auto &) {}, io_context_});
    dead_nodes_.erase(sorted_dead_node_list_.front().first);
    sorted_dead_node_list_.pop_front();
  }
  auto node_id = NodeID::FromBinary(node->node_id());
  dead_nodes_.emplace(node_id, node);
  sorted_dead_node_list_.emplace_back(node_id, node->end_time_ms());
}

void GcsNodeManager::PublishNodeInfoToPubsub(const NodeID &node_id,
                                             const rpc::GcsNodeInfo &node_info) const {
  gcs_publisher_->PublishNodeInfo(node_id, node_info);

  // Convert once and move to avoid copying
  auto address_and_liveness = ConvertToGcsNodeAddressAndLiveness(node_info);
  gcs_publisher_->PublishNodeAddressAndLiveness(node_id, std::move(address_and_liveness));
}

std::string GcsNodeManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsNodeManager: "
         << "\n- RegisterNode request count: "
         << counts_[CountType::REGISTER_NODE_REQUEST]
         << "\n- DrainNode request count: " << counts_[CountType::DRAIN_NODE_REQUEST]
         << "\n- GetAllNodeInfo request count: "
         << counts_[CountType::GET_ALL_NODE_INFO_REQUEST];
  return stream.str();
}

void GcsNodeManager::UpdateAliveNode(
    const NodeID &node_id,
    const rpc::syncer::ResourceViewSyncMessage &resource_view_sync_message) {
  absl::MutexLock lock(&mutex_);
  auto maybe_node_info = GetAliveNodeFromCache(node_id);
  if (maybe_node_info == absl::nullopt) {
    return;
  }

  auto new_node_info = *maybe_node_info.value();
  auto current_snapshot_state = new_node_info.state_snapshot().state();
  auto *snapshot = new_node_info.mutable_state_snapshot();

  if (resource_view_sync_message.idle_duration_ms() > 0) {
    snapshot->set_state(rpc::NodeSnapshot::IDLE);
    snapshot->set_idle_duration_ms(resource_view_sync_message.idle_duration_ms());
  } else {
    snapshot->set_state(rpc::NodeSnapshot::ACTIVE);
    snapshot->mutable_node_activity()->CopyFrom(
        resource_view_sync_message.node_activity());
  }
  if (resource_view_sync_message.is_draining()) {
    bool first_time_draining = current_snapshot_state != rpc::NodeSnapshot::DRAINING;
    snapshot->set_state(rpc::NodeSnapshot::DRAINING);
    // Write the export event for the draining state once. Note that we explicitly do
    // not write IDLE and ACTIVE events as they have very high cardinality.
    if (first_time_draining) {
      WriteNodeExportEvent(new_node_info, /*is_register_event*/ false);
    }
  }

  // N.B. For thread safety, all updates to alive_nodes_ need to follow a
  // read/modify/write sort of pattern.  This is because the underlying map contains const
  // variables
  alive_nodes_[node_id] =
      std::make_shared<const rpc::GcsNodeInfo>(std::move(new_node_info));
}

}  // namespace gcs
}  // namespace ray
