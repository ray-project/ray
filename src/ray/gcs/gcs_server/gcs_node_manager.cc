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

#include "gcs_node_manager.h"
#include <ray/common/ray_config.h>
#include <ray/gcs/pb_util.h>
#include <ray/protobuf/gcs.pb.h>

namespace ray {
namespace gcs {

GcsNodeManager::NodeFailureDetector::NodeFailureDetector(
    boost::asio::io_service &io_service, gcs::NodeInfoAccessor &node_info_accessor,
    std::function<void(const ClientID &)> on_node_death_callback)
    : node_info_accessor_(node_info_accessor),
      on_node_death_callback_(std::move(on_node_death_callback)),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      detect_timer_(io_service) {
  Tick();
}

void GcsNodeManager::NodeFailureDetector::AddNode(const ray::ClientID &node_id) {
  heartbeats_.emplace(node_id, num_heartbeats_timeout_);
}

void GcsNodeManager::NodeFailureDetector::HandleHeartbeat(
    const ClientID &node_id, const rpc::HeartbeatTableData &heartbeat_data) {
  auto iter = heartbeats_.find(node_id);
  if (iter == heartbeats_.end()) {
    // Ignore this heartbeat as the node is not registered.
    // TODO(Shanly): Maybe we should reply the raylet with an error. So the raylet can
    // crash itself as soon as possible.
    return;
  }

  iter->second = num_heartbeats_timeout_;
  heartbeat_buffer_[node_id] = heartbeat_data;
}

/// A periodic timer that checks for timed out clients.
void GcsNodeManager::NodeFailureDetector::Tick() {
  DetectDeadNodes();
  SendBatchedHeartbeat();
  ScheduleTick();
}

void GcsNodeManager::NodeFailureDetector::DetectDeadNodes() {
  for (auto it = heartbeats_.begin(); it != heartbeats_.end();) {
    auto current = it++;
    current->second = current->second - 1;
    if (current->second == 0) {
      auto node_id = current->first;
      RAY_LOG(WARNING) << "Node timed out: " << node_id;
      heartbeats_.erase(current);
      heartbeat_buffer_.erase(node_id);
      if (on_node_death_callback_) {
        on_node_death_callback_(node_id);
      }
    }
  }
}

void GcsNodeManager::NodeFailureDetector::SendBatchedHeartbeat() {
  if (!heartbeat_buffer_.empty()) {
    auto batch = std::make_shared<rpc::HeartbeatBatchTableData>();
    for (const auto &heartbeat : heartbeat_buffer_) {
      batch->add_batch()->CopyFrom(heartbeat.second);
    }
    RAY_CHECK_OK(node_info_accessor_.AsyncReportBatchHeartbeat(batch, nullptr));
    heartbeat_buffer_.clear();
  }
}

void GcsNodeManager::NodeFailureDetector::ScheduleTick() {
  auto heartbeat_period = boost::posix_time::milliseconds(
      RayConfig::instance().raylet_heartbeat_timeout_milliseconds());
  detect_timer_.expires_from_now(heartbeat_period);
  detect_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      // `operation_canceled` is set when `detect_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Checking heartbeat failed with error: " << error.message();
    Tick();
  });
}

//////////////////////////////////////////////////////////////////////////////////////////
GcsNodeManager::GcsNodeManager(boost::asio::io_service &io_service,
                               gcs::NodeInfoAccessor &node_info_accessor,
                               gcs::ErrorInfoAccessor &error_info_accessor)
    : node_info_accessor_(node_info_accessor),
      error_info_accessor_(error_info_accessor),
      node_failure_detector_(new NodeFailureDetector(
          io_service, node_info_accessor, [this](const ClientID &node_id) {
            if (auto node = RemoveNode(node_id, /* is_intended = */ false)) {
              node->set_state(rpc::GcsNodeInfo::DEAD);
              RAY_CHECK(dead_nodes_.emplace(node_id, node).second);
              RAY_CHECK_OK(node_info_accessor_.AsyncUnregister(node_id, nullptr));
              // TODO(Shanly): Remove node resources from resource table.
            }
          })) {
  // TODO(Shanly): Load node info list from storage synchronously.
  // TODO(Shanly): Load cluster resources from storage synchronously.
}

void GcsNodeManager::HandleRegisterNode(const rpc::RegisterNodeRequest &request,
                                        rpc::RegisterNodeReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_info().node_id());
  RAY_LOG(INFO) << "Registering node info, node id = " << node_id;
  AddNode(std::make_shared<rpc::GcsNodeInfo>(request.node_info()));
  auto on_done = [node_id, reply, send_reply_callback](Status status) {
    RAY_CHECK_OK(status);
    RAY_LOG(INFO) << "Finished registering node info, node id = " << node_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(node_info_accessor_.AsyncRegister(request.node_info(), on_done));
}

void GcsNodeManager::HandleUnregisterNode(const rpc::UnregisterNodeRequest &request,
                                          rpc::UnregisterNodeReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(INFO) << "Unregistering node info, node id = " << node_id;
  auto on_done = [node_id, request, reply, send_reply_callback](Status status) {
    RAY_CHECK_OK(status);
    RAY_LOG(INFO) << "Finished unregistering node info, node id = " << node_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  if (auto node = RemoveNode(node_id, /* is_intended = */ true)) {
    node->set_state(rpc::GcsNodeInfo::DEAD);
    RAY_CHECK(dead_nodes_.emplace(node_id, node).second);
    RAY_CHECK_OK(node_info_accessor_.AsyncUnregister(node_id, on_done));
    // TODO(Shanly): Remove node resources from resource table.
  }
}

void GcsNodeManager::HandleGetAllNodeInfo(const rpc::GetAllNodeInfoRequest &request,
                                          rpc::GetAllNodeInfoReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all nodes info.";
  for (const auto &entry : alive_nodes_) {
    reply->add_node_info_list()->CopyFrom(*entry.second);
  }
  for (const auto &entry : dead_nodes_) {
    reply->add_node_info_list()->CopyFrom(*entry.second);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished getting all node info.";
}

void GcsNodeManager::HandleReportHeartbeat(const rpc::ReportHeartbeatRequest &request,
                                           rpc::ReportHeartbeatReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.heartbeat().client_id());
  RAY_LOG(DEBUG) << "Reporting heartbeat, node id = " << node_id;
  auto heartbeat_data = std::make_shared<rpc::HeartbeatTableData>();
  heartbeat_data->CopyFrom(request.heartbeat());
  node_failure_detector_->HandleHeartbeat(node_id, *heartbeat_data);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  // TODO(Shanly): Remove it later.
  // The heartbeat data is reported here because some python unit tests rely on the
  // heartbeat data in redis.
  RAY_CHECK_OK(node_info_accessor_.AsyncReportHeartbeat(heartbeat_data, nullptr));
  RAY_LOG(DEBUG) << "Finished reporting heartbeat, node id = " << node_id;
}

void GcsNodeManager::HandleGetResources(const rpc::GetResourcesRequest &request,
                                        rpc::GetResourcesReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Getting node resources, node id = " << node_id;
  auto iter = cluster_resources_.find(node_id);
  if (iter != cluster_resources_.end()) {
    for (auto &resource : iter->second) {
      (*reply->mutable_resources())[resource.first] = *resource.second;
    }
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished getting node resources, node id = " << node_id;
}

void GcsNodeManager::HandleUpdateResources(const rpc::UpdateResourcesRequest &request,
                                           rpc::UpdateResourcesReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Updating resources, node id = " << node_id;
  auto iter = cluster_resources_.find(node_id);
  if (iter != cluster_resources_.end()) {
    auto to_be_updated_resources = std::make_shared<gcs::NodeInfoAccessor::ResourceMap>();
    for (auto resource : request.resources()) {
      (*to_be_updated_resources)[resource.first] =
          std::make_shared<rpc::ResourceTableData>(resource.second);
    }
    for (auto &entry : *to_be_updated_resources) {
      iter->second[entry.first] = entry.second;
    }
    auto on_done = [node_id, to_be_updated_resources, reply,
                    send_reply_callback](Status status) {
      RAY_CHECK_OK(status);
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      RAY_LOG(DEBUG) << "Finished updating resources, node id = " << node_id;
    };

    RAY_CHECK_OK(node_info_accessor_.AsyncUpdateResources(
        node_id, *to_be_updated_resources, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid("Node is not exist."));
    RAY_LOG(ERROR) << "Failed to update resources as node " << node_id
                   << " is not registered.";
  }
}

void GcsNodeManager::HandleDeleteResources(const rpc::DeleteResourcesRequest &request,
                                           rpc::DeleteResourcesReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Deleting node resources, node id = " << node_id;
  auto resource_names = VectorFromProtobuf(request.resource_name_list());
  auto iter = cluster_resources_.find(node_id);
  if (iter != cluster_resources_.end()) {
    for (auto &resource_name : resource_names) {
      iter->second.erase(resource_name);
    }
    auto on_done = [reply, send_reply_callback](Status status) {
      RAY_CHECK_OK(status);
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    };
    RAY_CHECK_OK(
        node_info_accessor_.AsyncDeleteResources(node_id, resource_names, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    RAY_LOG(DEBUG) << "Finished deleting node resources, node id = " << node_id;
  }
}

std::shared_ptr<rpc::GcsNodeInfo> GcsNodeManager::GetNode(
    const ray::ClientID &node_id) const {
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    return nullptr;
  }

  return iter->second;
}

void GcsNodeManager::AddNode(std::shared_ptr<rpc::GcsNodeInfo> node) {
  auto node_id = ClientID::FromBinary(node->node_id());
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    alive_nodes_.emplace(node_id, node);
    // Add an empty resources for this node.
    RAY_CHECK(
        cluster_resources_.emplace(node_id, gcs::NodeInfoAccessor::ResourceMap()).second);
    // Register this node to the `node_failure_detector_` which will start monitoring it.
    node_failure_detector_->AddNode(node_id);
    // Notify all listeners.
    for (auto &listener : node_added_listeners_) {
      listener(node);
    }
  }
}

std::shared_ptr<rpc::GcsNodeInfo> GcsNodeManager::RemoveNode(
    const ray::ClientID &node_id, bool is_intended /*= false*/) {
  std::shared_ptr<rpc::GcsNodeInfo> removed_node;
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    removed_node = std::move(iter->second);
    // Remove from alive nodes.
    alive_nodes_.erase(iter);
    // Remove from cluster resources.
    RAY_CHECK(cluster_resources_.erase(node_id) != 0);
    if (!is_intended) {
      // Broadcast a warning to all of the drivers indicating that the node
      // has been marked as dead.
      // TODO(rkn): Define this constant somewhere else.
      std::string type = "node_removed";
      std::ostringstream error_message;
      error_message << "The node with node id " << node_id
                    << " has been marked dead because the detector"
                    << " has missed too many heartbeats from it.";
      auto error_data_ptr =
          gcs::CreateErrorTableData(type, error_message.str(), current_time_ms());
      RAY_CHECK_OK(error_info_accessor_.AsyncReportJobError(error_data_ptr, nullptr));
    }

    // Notify all listeners.
    for (auto &listener : node_removed_listeners_) {
      listener(removed_node);
    }
  }
  return removed_node;
}

}  // namespace gcs
}  // namespace ray
