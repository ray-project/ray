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

#include "ray/common/ray_config.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "ray/util/event.h"
#include "ray/util/event_label.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

//////////////////////////////////////////////////////////////////////////////////////////
GcsNodeManager::GcsNodeManager(std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
                               std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
    : gcs_pub_sub_(gcs_pub_sub), gcs_table_storage_(gcs_table_storage) {}

void GcsNodeManager::HandleRegisterNode(const rpc::RegisterNodeRequest &request,
                                        rpc::RegisterNodeReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_info().node_id());
  RAY_LOG(INFO) << "Registering node info, node id = " << node_id
                << ", address = " << request.node_info().node_manager_address();
  auto on_done = [this, node_id, request, reply,
                  send_reply_callback](const Status &status) {
    RAY_CHECK_OK(status);
    RAY_LOG(INFO) << "Finished registering node info, node id = " << node_id
                  << ", address = " << request.node_info().node_manager_address();
    RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_CHANNEL, node_id.Hex(),
                                       request.node_info().SerializeAsString(), nullptr));
    AddNode(std::make_shared<rpc::GcsNodeInfo>(request.node_info()));
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(
      gcs_table_storage_->NodeTable().Put(node_id, request.node_info(), on_done));
  ++counts_[CountType::REGISTER_NODE_REQUEST];
}

void GcsNodeManager::HandleUnregisterNode(const rpc::UnregisterNodeRequest &request,
                                          rpc::UnregisterNodeReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(INFO) << "Unregistering node info, node id = " << node_id;
  if (auto node = RemoveNode(node_id, /* is_intended = */ true)) {
    node->set_state(rpc::GcsNodeInfo::DEAD);
    node->set_timestamp(current_sys_time_ms());
    AddDeadNodeToCache(node);
    auto node_info_delta = std::make_shared<rpc::GcsNodeInfo>();
    node_info_delta->set_node_id(node->node_id());
    node_info_delta->set_state(node->state());
    node_info_delta->set_timestamp(node->timestamp());

    auto on_done = [this, node_id, node_info_delta, reply,
                    send_reply_callback](const Status &status) {
      auto on_done = [this, node_id, node_info_delta, reply,
                      send_reply_callback](const Status &status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(
            NODE_CHANNEL, node_id.Hex(), node_info_delta->SerializeAsString(), nullptr));
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        RAY_LOG(INFO) << "Finished unregistering node info, node id = " << node_id;
      };
      RAY_CHECK_OK(gcs_table_storage_->NodeResourceTable().Delete(node_id, on_done));
    };
    // Update node state to DEAD instead of deleting it.
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(node_id, *node, on_done));
  }
  ++counts_[CountType::UNREGISTER_NODE_REQUEST];
}

void GcsNodeManager::HandleGetAllNodeInfo(const rpc::GetAllNodeInfoRequest &request,
                                          rpc::GetAllNodeInfoReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  for (const auto &entry : alive_nodes_) {
    reply->add_node_info_list()->CopyFrom(*entry.second);
  }
  for (const auto &entry : dead_nodes_) {
    reply->add_node_info_list()->CopyFrom(*entry.second);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_NODE_INFO_REQUEST];
}

void GcsNodeManager::HandleGetInternalConfig(const rpc::GetInternalConfigRequest &request,
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

void GcsNodeManager::AddNode(std::shared_ptr<rpc::GcsNodeInfo> node) {
  auto node_id = NodeID::FromBinary(node->node_id());
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    alive_nodes_.emplace(node_id, node);

    // Notify all listeners.
    for (auto &listener : node_added_listeners_) {
      listener(node);
    }
  }
}

std::shared_ptr<rpc::GcsNodeInfo> GcsNodeManager::RemoveNode(
    const ray::NodeID &node_id, bool is_intended /*= false*/) {
  RAY_LOG(INFO) << "Removing node, node id = " << node_id;
  std::shared_ptr<rpc::GcsNodeInfo> removed_node;
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    removed_node = std::move(iter->second);
    // Record stats that there's a new removed node.
    stats::NodeFailureTotal.Record(1);
    // Remove from alive nodes.
    alive_nodes_.erase(iter);
    if (!is_intended) {
      // Broadcast a warning to all of the drivers indicating that the node
      // has been marked as dead.
      // TODO(rkn): Define this constant somewhere else.
      std::string type = "node_removed";
      std::ostringstream error_message;
      error_message << "The node with node id: " << node_id
                    << " and ip: " << removed_node->node_manager_address()
                    << " has been marked dead because the detector"
                    << " has missed too many heartbeats from it. This can happen when a "
                       "raylet crashes unexpectedly or has lagging heartbeats.";
      RAY_LOG(INFO) << "Publish RemoveNode, msg=" << error_message.str();
      auto error_data_ptr =
          gcs::CreateErrorTableData(type, error_message.str(), current_time_ms());
      RAY_EVENT(ERROR, EL_RAY_NODE_REMOVED)
              .WithField("node_id", node_id.Hex())
              .WithField("ip", removed_node->node_manager_address())
          << error_message.str();
      RAY_CHECK_OK(gcs_pub_sub_->Publish(ERROR_INFO_CHANNEL, node_id.Hex(),
                                         error_data_ptr->SerializeAsString(), nullptr));
    }

    // Notify all listeners.
    for (auto &listener : node_removed_listeners_) {
      listener(removed_node);
    }
  }
  return removed_node;
}

void GcsNodeManager::OnNodeFailure(const NodeID &node_id) {
  if (auto node = RemoveNode(node_id, /* is_intended = */ false)) {
    node->set_state(rpc::GcsNodeInfo::DEAD);
    node->set_timestamp(current_sys_time_ms());
    AddDeadNodeToCache(node);
    auto node_info_delta = std::make_shared<rpc::GcsNodeInfo>();
    node_info_delta->set_node_id(node->node_id());
    node_info_delta->set_state(node->state());
    node_info_delta->set_timestamp(node->timestamp());

    auto on_done = [this, node_id, node_info_delta](const Status &status) {
      auto on_done = [this, node_id, node_info_delta](const Status &status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(
            NODE_CHANNEL, node_id.Hex(), node_info_delta->SerializeAsString(), nullptr));
      };
      RAY_CHECK_OK(gcs_table_storage_->NodeResourceTable().Delete(node_id, on_done));
    };
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(node_id, *node, on_done));
  }
}

void GcsNodeManager::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &item : gcs_init_data.Nodes()) {
    if (item.second.state() == rpc::GcsNodeInfo::ALIVE) {
      AddNode(std::make_shared<rpc::GcsNodeInfo>(item.second));
    } else if (item.second.state() == rpc::GcsNodeInfo::DEAD) {
      dead_nodes_.emplace(item.first, std::make_shared<rpc::GcsNodeInfo>(item.second));
      sorted_dead_node_list_.emplace_back(item.first, item.second.timestamp());
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
  sorted_dead_node_list_.emplace_back(node_id, node->timestamp());
}

std::string GcsNodeManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsNodeManager: {RegisterNode request count: "
         << counts_[CountType::REGISTER_NODE_REQUEST]
         << ", UnregisterNode request count: "
         << counts_[CountType::UNREGISTER_NODE_REQUEST]
         << ", GetAllNodeInfo request count: "
         << counts_[CountType::GET_ALL_NODE_INFO_REQUEST]
         << ", GetInternalConfig request count: "
         << counts_[CountType::GET_INTERNAL_CONFIG_REQUEST] << "}";
  return stream.str();
}

}  // namespace gcs
}  // namespace ray
