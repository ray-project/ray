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

#include <optional>
#include <utility>

#include "ray/common/ray_config.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

//////////////////////////////////////////////////////////////////////////////////////////
GcsNodeManager::GcsNodeManager(
    std::shared_ptr<GcsPublisher> gcs_publisher,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    const ClusterID &cluster_id)
    : gcs_publisher_(std::move(gcs_publisher)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      raylet_client_pool_(std::move(raylet_client_pool)),
      cluster_id_(cluster_id) {}

// Note: ServerCall will populate the cluster_id.
void GcsNodeManager::HandleGetClusterId(rpc::GetClusterIdRequest request,
                                        rpc::GetClusterIdReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Registering GCS client!";
  reply->set_cluster_id(cluster_id_.Binary());
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsNodeManager::HandleRegisterNode(rpc::RegisterNodeRequest request,
                                        rpc::RegisterNodeReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_info().node_id());
  RAY_LOG(INFO) << "Registering node info, node id = " << node_id
                << ", address = " << request.node_info().node_manager_address()
                << ", node name = " << request.node_info().node_name();
  auto on_done = [this, node_id, request, reply, send_reply_callback](
                     const Status &status) {
    RAY_CHECK_OK(status);
    RAY_LOG(INFO) << "Finished registering node info, node id = " << node_id
                  << ", address = " << request.node_info().node_manager_address()
                  << ", node name = " << request.node_info().node_name();
    RAY_CHECK_OK(gcs_publisher_->PublishNodeInfo(node_id, request.node_info(), nullptr));
    AddNode(std::make_shared<rpc::GcsNodeInfo>(request.node_info()));
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  if (request.node_info().is_head_node()) {
    // mark all old head nodes as dead if exists:
    // 1. should never happen when HA is not used
    // 2. happens when a new head node is started

    std::vector<NodeID> head_nodes;
    for (auto &node : alive_nodes_) {
      if (node.second->is_head_node()) {
        head_nodes.push_back(node.first);
      }
    }

    assert(head_nodes.size() <= 1);
    if (head_nodes.size() == 1) {
      OnNodeFailure(head_nodes[0],
                    [this, request, on_done, node_id](const Status &status) {
                      RAY_CHECK_OK(status);
                      RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(
                          node_id, request.node_info(), on_done));
                    });
    } else {
      RAY_CHECK_OK(
          gcs_table_storage_->NodeTable().Put(node_id, request.node_info(), on_done));
    }
  } else {
    RAY_CHECK_OK(
        gcs_table_storage_->NodeTable().Put(node_id, request.node_info(), on_done));
  }
  ++counts_[CountType::REGISTER_NODE_REQUEST];
}

void GcsNodeManager::HandleCheckAlive(rpc::CheckAliveRequest request,
                                      rpc::CheckAliveReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  reply->set_ray_version(kRayVersion);
  for (const auto &addr : request.raylet_address()) {
    bool is_alive = node_map_.right.count(addr) != 0;
    reply->mutable_raylet_alive()->Add(is_alive);
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsNodeManager::HandleUnregisterNode(rpc::UnregisterNodeRequest request,
                                          rpc::UnregisterNodeReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "HandleUnregisterNode() for node id = " << node_id;
  auto node = RemoveNode(node_id, request.node_death_info());
  if (!node) {
    RAY_LOG(INFO) << "Node " << node_id << " is already removed";
    return;
  }

  node->set_state(rpc::GcsNodeInfo::DEAD);
  node->set_end_time_ms(current_sys_time_ms());

  AddDeadNodeToCache(node);

  auto node_info_delta = std::make_shared<rpc::GcsNodeInfo>();
  node_info_delta->set_node_id(node->node_id());
  node_info_delta->mutable_death_info()->CopyFrom(request.node_death_info());
  node_info_delta->set_state(node->state());
  node_info_delta->set_end_time_ms(node->end_time_ms());

  auto on_put_done = [=](const Status &status) {
    RAY_CHECK_OK(gcs_publisher_->PublishNodeInfo(node_id, *node_info_delta, nullptr));
  };
  RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(node_id, *node, on_put_done));
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsNodeManager::HandleDrainNode(rpc::DrainNodeRequest request,
                                     rpc::DrainNodeReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  auto num_drain_request = request.drain_node_data_size();
  for (auto i = 0; i < num_drain_request; i++) {
    const auto &node_drain_request = request.drain_node_data(i);
    const auto node_id = NodeID::FromBinary(node_drain_request.node_id());

    DrainNode(node_id);
    auto drain_node_status = reply->add_drain_node_status();
    drain_node_status->set_node_id(node_id.Binary());
  };
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::DRAIN_NODE_REQUEST];
}

void GcsNodeManager::DrainNode(const NodeID &node_id) {
  RAY_LOG(INFO) << "DrainNode() for node id = " << node_id;
  auto maybe_node = GetAliveNode(node_id);
  if (!maybe_node.has_value()) {
    RAY_LOG(WARNING) << "Skip draining node " << node_id << " which is already removed";
    return;
  }
  auto node = maybe_node.value();

  // Set the address.
  rpc::Address remote_address;
  remote_address.set_raylet_id(node->node_id());
  remote_address.set_ip_address(node->node_manager_address());
  remote_address.set_port(node->node_manager_port());

  auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(remote_address);
  RAY_CHECK(raylet_client);
  // NOTE(sang): Drain API is not supposed to kill the raylet, but we are doing
  // this until the proper "drain" behavior is implemented.
  raylet_client->ShutdownRaylet(
      node_id,
      /*graceful*/ true,
      [node_id](const Status &status, const rpc::ShutdownRayletReply &reply) {
        RAY_LOG(INFO) << "Raylet " << node_id << " is drained. Status " << status;
      });
}

void GcsNodeManager::HandleGetAllNodeInfo(rpc::GetAllNodeInfoRequest request,
                                          rpc::GetAllNodeInfoReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  // Here the unsafe allocate is safe here, because entry.second's life cycle is longer
  // then reply.
  // The request will be sent when call send_reply_callback and after that, reply will
  // not be used any more. But entry is still valid.
  for (const auto &entry : alive_nodes_) {
    *reply->add_node_info_list() = *entry.second;
  }
  for (const auto &entry : dead_nodes_) {
    *reply->add_node_info_list() = *entry.second;
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_NODE_INFO_REQUEST];
}

void GcsNodeManager::HandleGetInternalConfig(rpc::GetInternalConfigRequest request,
                                             rpc::GetInternalConfigReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  auto get_system_config = [reply, send_reply_callback](
                               const ray::Status &status,
                               const boost::optional<rpc::StoredConfig> &config) {
    if (config.has_value()) {
      reply->set_config(config.get().config());
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(
      gcs_table_storage_->InternalConfigTable().Get(UniqueID::Nil(), get_system_config));
  ++counts_[CountType::GET_INTERNAL_CONFIG_REQUEST];
}

absl::optional<std::shared_ptr<rpc::GcsNodeInfo>> GcsNodeManager::GetAliveNode(
    const ray::NodeID &node_id) const {
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    return {};
  }

  return iter->second;
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
    RAY_LOG(INFO) << "Node " << node_id << " was forcibly preempted";
  } else {
    death_info.set_reason(rpc::NodeDeathInfo::UNEXPECTED_TERMINATION);
    death_info.set_reason_message(
        "health check failed due to missing too many heartbeats");
  }
  return death_info;
}

void GcsNodeManager::AddNode(std::shared_ptr<rpc::GcsNodeInfo> node) {
  auto node_id = NodeID::FromBinary(node->node_id());
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    auto node_addr =
        node->node_manager_address() + ":" + std::to_string(node->node_manager_port());
    node_map_.insert(NodeIDAddrBiMap::value_type(node_id, node_addr));
    alive_nodes_.emplace(node_id, node);
    // Notify all listeners.
    for (auto &listener : node_added_listeners_) {
      listener(node);
    }
  }
}

void GcsNodeManager::SetNodeDraining(
    const NodeID &node_id,
    std::shared_ptr<rpc::autoscaler::DrainNodeRequest> drain_request) {
  auto maybe_node = GetAliveNode(node_id);
  if (!maybe_node.has_value()) {
    RAY_LOG(INFO) << "Skip setting node " << node_id << " to be draining, "
                  << "which is already removed";
    return;
  }
  auto iter = draining_nodes_.find(node_id);
  if (iter == draining_nodes_.end()) {
    draining_nodes_.emplace(node_id, drain_request);
    RAY_LOG(INFO) << "Set node " << node_id
                  << " to be draining, request = " << drain_request->DebugString();
  } else {
    RAY_LOG(INFO) << "Drain request for node " << node_id << " already exists. "
                  << "Overwriting the existing request " << iter->second->DebugString()
                  << " with the new request " << drain_request->DebugString();
    iter->second = drain_request;
  }
}

std::shared_ptr<rpc::GcsNodeInfo> GcsNodeManager::RemoveNode(
    const ray::NodeID &node_id, const rpc::NodeDeathInfo &node_death_info) {
  std::shared_ptr<rpc::GcsNodeInfo> removed_node;
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    removed_node = std::move(iter->second);

    // Set node death info.
    auto death_info = removed_node->mutable_death_info();
    death_info->CopyFrom(node_death_info);

    RAY_LOG(INFO) << "Removing node, node id = " << node_id
                  << ", node name = " << removed_node->node_name() << ", death reason = "
                  << rpc::NodeDeathInfo_Reason_Name(death_info->reason())
                  << ", death message = " << death_info->reason_message();
    // Record stats that there's a new removed node.
    stats::NodeFailureTotal.Record(1);
    // Remove from alive nodes.
    alive_nodes_.erase(iter);
    node_map_.left.erase(node_id);
    // Remove from draining nodes if present.
    draining_nodes_.erase(node_id);
    if (death_info->reason() == rpc::NodeDeathInfo::UNEXPECTED_TERMINATION) {
      // Broadcast a warning to all of the drivers indicating that the node
      // has been marked as dead.
      // TODO(rkn): Define this constant somewhere else.
      std::string type = "node_removed";
      std::ostringstream error_message;
      error_message
          << "The node with node id: " << node_id
          << " and address: " << removed_node->node_manager_address()
          << " and node name: " << removed_node->node_name()
          << " has been marked dead because the detector"
          << " has missed too many heartbeats from it. This can happen when a "
             "\t(1) raylet crashes unexpectedly (OOM, etc.) \n"
          << "\t(2) raylet has lagging heartbeats due to slow network or busy workload.";
      RAY_EVENT(ERROR, EL_RAY_NODE_REMOVED)
              .WithField("node_id", node_id.Hex())
              .WithField("ip", removed_node->node_manager_address())
          << error_message.str();
      RAY_LOG(WARNING) << error_message.str();
      auto error_data_ptr =
          gcs::CreateErrorTableData(type, error_message.str(), current_time_ms());
      RAY_CHECK_OK(gcs_publisher_->PublishError(node_id.Hex(), *error_data_ptr, nullptr));
    }

    // Notify all listeners.
    for (auto &listener : node_removed_listeners_) {
      listener(removed_node);
    }
  }
  return removed_node;
}

void GcsNodeManager::OnNodeFailure(const NodeID &node_id,
                                   const StatusCallback &node_table_updated_callback) {
  auto maybe_node = GetAliveNode(node_id);
  if (maybe_node.has_value()) {
    rpc::NodeDeathInfo death_info = InferDeathInfo(node_id);
    auto node = RemoveNode(node_id, death_info);
    node->set_state(rpc::GcsNodeInfo::DEAD);
    node->set_end_time_ms(current_sys_time_ms());

    AddDeadNodeToCache(node);
    auto node_info_delta = std::make_shared<rpc::GcsNodeInfo>();
    node_info_delta->set_node_id(node->node_id());
    node_info_delta->set_state(node->state());
    node_info_delta->set_end_time_ms(node->end_time_ms());
    node_info_delta->mutable_death_info()->CopyFrom(node->death_info());

    auto on_done = [this, node_id, node_table_updated_callback, node_info_delta](
                       const Status &status) {
      if (node_table_updated_callback != nullptr) {
        node_table_updated_callback(Status::OK());
      }
      RAY_CHECK_OK(gcs_publisher_->PublishNodeInfo(node_id, *node_info_delta, nullptr));
    };
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(node_id, *node, on_done));
  } else if (node_table_updated_callback != nullptr) {
    node_table_updated_callback(Status::OK());
  }
}

void GcsNodeManager::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &[node_id, node_info] : gcs_init_data.Nodes()) {
    if (node_info.state() == rpc::GcsNodeInfo::ALIVE) {
      AddNode(std::make_shared<rpc::GcsNodeInfo>(node_info));

      // Ask the raylet to do initialization in case of GCS restart.
      // The protocol is correct because when a new node joined, Raylet will do:
      //    - RegisterNode (write node to the node table)
      //    - Setup subscription
      // With this, it means we only need to ask the node registered to do resubscription.
      // And for the node failed to register, they will crash on the client side due to
      // registeration failure.
      rpc::Address remote_address;
      remote_address.set_raylet_id(node_info.node_id());
      remote_address.set_ip_address(node_info.node_manager_address());
      remote_address.set_port(node_info.node_manager_port());
      auto raylet_client = raylet_client_pool_->GetOrConnectByAddress(remote_address);
      raylet_client->NotifyGCSRestart(nullptr);
    } else if (node_info.state() == rpc::GcsNodeInfo::DEAD) {
      dead_nodes_.emplace(node_id, std::make_shared<rpc::GcsNodeInfo>(node_info));
      sorted_dead_node_list_.emplace_back(node_id, node_info.end_time_ms());
    }
  }
  sorted_dead_node_list_.sort(
      [](const std::pair<NodeID, int64_t> &left,
         const std::pair<NodeID, int64_t> &right) { return left.second < right.second; });
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
  sorted_dead_node_list_.emplace_back(node_id, node->end_time_ms());
}

std::string GcsNodeManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsNodeManager: "
         << "\n- RegisterNode request count: "
         << counts_[CountType::REGISTER_NODE_REQUEST]
         << "\n- DrainNode request count: " << counts_[CountType::DRAIN_NODE_REQUEST]
         << "\n- GetAllNodeInfo request count: "
         << counts_[CountType::GET_ALL_NODE_INFO_REQUEST]
         << "\n- GetInternalConfig request count: "
         << counts_[CountType::GET_INTERNAL_CONFIG_REQUEST];
  return stream.str();
}

}  // namespace gcs
}  // namespace ray
