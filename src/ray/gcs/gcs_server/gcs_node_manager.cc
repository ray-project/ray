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
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

GcsNodeManager::NodeFailureDetector::NodeFailureDetector(
    boost::asio::io_service &io_service,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::function<void(const ClientID &)> on_node_death_callback)
    : gcs_table_storage_(std::move(gcs_table_storage)),
      on_node_death_callback_(std::move(on_node_death_callback)),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      light_heartbeat_enabled_(RayConfig::instance().light_heartbeat_enabled()),
      detect_timer_(io_service),
      gcs_pub_sub_(std::move(gcs_pub_sub)) {}

void GcsNodeManager::NodeFailureDetector::Start() {
  if (!is_started_) {
    Tick();
    is_started_ = true;
  }
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
  if (!light_heartbeat_enabled_ || heartbeat_data.should_global_gc() ||
      heartbeat_data.resources_available_size() > 0 ||
      heartbeat_data.resources_total_size() > 0 ||
      heartbeat_data.resource_load_size() > 0) {
    heartbeat_buffer_[node_id] = heartbeat_data;
  }
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

    RAY_CHECK_OK(gcs_pub_sub_->Publish(HEARTBEAT_BATCH_CHANNEL, "",
                                       batch->SerializeAsString(), nullptr));
    heartbeat_buffer_.clear();
  }
}

void GcsNodeManager::NodeFailureDetector::ScheduleTick() {
  auto heartbeat_period = boost::posix_time::milliseconds(
      RayConfig::instance().raylet_heartbeat_timeout_milliseconds());
  detect_timer_.expires_from_now(heartbeat_period);
  detect_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      // `operation_aborted` is set when `detect_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Checking heartbeat failed with error: " << error.message();
    Tick();
  });
}

//////////////////////////////////////////////////////////////////////////////////////////
GcsNodeManager::GcsNodeManager(boost::asio::io_service &main_io_service,
                               boost::asio::io_service &node_failure_detector_io_service,
                               gcs::ErrorInfoAccessor &error_info_accessor,
                               std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
                               std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
    : error_info_accessor_(error_info_accessor),
      main_io_service_(main_io_service),
      node_failure_detector_(new NodeFailureDetector(
          node_failure_detector_io_service, gcs_table_storage, gcs_pub_sub,
          [this](const ClientID &node_id) {
            // Post this to main event loop to avoid potential concurrency issues.
            main_io_service_.post([this, node_id] {
              if (auto node = RemoveNode(node_id, /* is_intended = */ false)) {
                node->set_state(rpc::GcsNodeInfo::DEAD);
                RAY_CHECK(dead_nodes_.emplace(node_id, node).second);
                auto on_done = [this, node_id, node](const Status &status) {
                  auto on_done = [this, node_id, node](const Status &status) {
                    RAY_CHECK_OK(gcs_pub_sub_->Publish(
                        NODE_CHANNEL, node_id.Hex(), node->SerializeAsString(), nullptr));
                  };
                  RAY_CHECK_OK(
                      gcs_table_storage_->NodeResourceTable().Delete(node_id, on_done));
                };
                RAY_CHECK_OK(
                    gcs_table_storage_->NodeTable().Put(node_id, *node, on_done));
              }
            });
          })),
      node_failure_detector_service_(node_failure_detector_io_service),
      gcs_pub_sub_(gcs_pub_sub),
      gcs_table_storage_(gcs_table_storage) {}

void GcsNodeManager::HandleRegisterNode(const rpc::RegisterNodeRequest &request,
                                        rpc::RegisterNodeReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_info().node_id());
  RAY_LOG(INFO) << "Registering node info, node id = " << node_id;
  AddNode(std::make_shared<rpc::GcsNodeInfo>(request.node_info()));
  auto on_done = [this, node_id, request, reply,
                  send_reply_callback](const Status &status) {
    RAY_CHECK_OK(status);
    RAY_LOG(INFO) << "Finished registering node info, node id = " << node_id;
    RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_CHANNEL, node_id.Hex(),
                                       request.node_info().SerializeAsString(), nullptr));
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(
      gcs_table_storage_->NodeTable().Put(node_id, request.node_info(), on_done));
}

void GcsNodeManager::HandleUnregisterNode(const rpc::UnregisterNodeRequest &request,
                                          rpc::UnregisterNodeReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(INFO) << "Unregistering node info, node id = " << node_id;
  if (auto node = RemoveNode(node_id, /* is_intended = */ true)) {
    node->set_state(rpc::GcsNodeInfo::DEAD);
    RAY_CHECK(dead_nodes_.emplace(node_id, node).second);

    auto on_done = [this, node_id, node, reply,
                    send_reply_callback](const Status &status) {
      auto on_done = [this, node_id, node, reply,
                      send_reply_callback](const Status &status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_CHANNEL, node_id.Hex(),
                                           node->SerializeAsString(), nullptr));
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        RAY_LOG(INFO) << "Finished unregistering node info, node id = " << node_id;
      };
      RAY_CHECK_OK(gcs_table_storage_->NodeResourceTable().Delete(node_id, on_done));
    };
    // Update node state to DEAD instead of deleting it.
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(node_id, *node, on_done));
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
  auto heartbeat_data = std::make_shared<rpc::HeartbeatTableData>();
  heartbeat_data->CopyFrom(request.heartbeat());
  // Note: To avoid heartbeats being delayed by main thread, make sure heartbeat is always
  // handled by its own IO service.
  node_failure_detector_service_.post([this, node_id, heartbeat_data] {
    node_failure_detector_->HandleHeartbeat(node_id, *heartbeat_data);
  });
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_CHECK_OK(gcs_pub_sub_->Publish(HEARTBEAT_CHANNEL, node_id.Hex(),
                                     heartbeat_data->SerializeAsString(), nullptr));
}

void GcsNodeManager::HandleGetResources(const rpc::GetResourcesRequest &request,
                                        rpc::GetResourcesReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Getting node resources, node id = " << node_id;
  auto iter = cluster_resources_.find(node_id);
  if (iter != cluster_resources_.end()) {
    for (auto &resource : iter->second.items()) {
      (*reply->mutable_resources())[resource.first] = resource.second;
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
  auto to_be_updated_resources = request.resources();
  if (iter != cluster_resources_.end()) {
    for (auto &entry : to_be_updated_resources) {
      (*iter->second.mutable_items())[entry.first] = entry.second;
    }
    auto on_done = [this, node_id, to_be_updated_resources, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      for (auto &it : to_be_updated_resources) {
        (*node_resource_change.mutable_updated_resources())[it.first] =
            it.second.resource_capacity();
      }
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Hex(),
                                         node_resource_change.SerializeAsString(),
                                         nullptr));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      RAY_LOG(DEBUG) << "Finished updating resources, node id = " << node_id;
    };

    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, iter->second, on_done));
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
      RAY_IGNORE_EXPR(iter->second.mutable_items()->erase(resource_name));
    }
    auto on_done = [this, node_id, resource_names, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      for (const auto &resource_name : resource_names) {
        node_resource_change.add_deleted_resources(resource_name);
      }
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Hex(),
                                         node_resource_change.SerializeAsString(),
                                         nullptr));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    };
    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, iter->second, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    RAY_LOG(DEBUG) << "Finished deleting node resources, node id = " << node_id;
  }
}

void GcsNodeManager::HandleSetInternalConfig(const rpc::SetInternalConfigRequest &request,
                                             rpc::SetInternalConfigReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  auto on_done = [reply, send_reply_callback, request](const Status status) {
    RAY_LOG(DEBUG) << "Set internal config: " << request.config().DebugString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(gcs_table_storage_->InternalConfigTable().Put(UniqueID::Nil(),
                                                             request.config(), on_done));
}

void GcsNodeManager::HandleGetInternalConfig(const rpc::GetInternalConfigRequest &request,
                                             rpc::GetInternalConfigReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  auto get_internal_config = [reply, send_reply_callback](
                                 ray::Status status,
                                 const boost::optional<rpc::StoredConfig> &config) {
    if (config.has_value()) {
      reply->mutable_config()->CopyFrom(config.get());
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(gcs_table_storage_->InternalConfigTable().Get(UniqueID::Nil(),
                                                             get_internal_config));
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
    RAY_CHECK(cluster_resources_.emplace(node_id, rpc::ResourceMap()).second);
    // Register this node to the `node_failure_detector_` which will start monitoring it.
    // Note: To avoid heartbeats being delayed by main thread, make sure node addition is
    // always handled by its own IO service.
    node_failure_detector_service_.post(
        [this, node_id] { node_failure_detector_->AddNode(node_id); });

    // Notify all listeners.
    for (auto &listener : node_added_listeners_) {
      listener(node);
    }
  }
}

std::shared_ptr<rpc::GcsNodeInfo> GcsNodeManager::RemoveNode(
    const ray::ClientID &node_id, bool is_intended /*= false*/) {
  RAY_LOG(INFO) << "Removing node, node id = " << node_id;
  std::shared_ptr<rpc::GcsNodeInfo> removed_node;
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    removed_node = std::move(iter->second);
    // Remove from alive nodes.
    alive_nodes_.erase(iter);
    // Remove from cluster resources.
    cluster_resources_.erase(node_id);
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

void GcsNodeManager::LoadInitialData(const EmptyCallback &done) {
  RAY_LOG(INFO) << "Loading initial data.";

  auto get_node_callback = [this, done](
                               const std::unordered_map<ClientID, GcsNodeInfo> &result) {
    for (auto &item : result) {
      if (item.second.state() == rpc::GcsNodeInfo::ALIVE) {
        // Call `AddNode` for this node to make sure it is tracked by the failure
        // detector.
        AddNode(std::make_shared<rpc::GcsNodeInfo>(item.second));
      } else if (item.second.state() == rpc::GcsNodeInfo::DEAD) {
        dead_nodes_.emplace(item.first, std::make_shared<rpc::GcsNodeInfo>(item.second));
      }
    }

    auto get_node_resource_callback =
        [this, done](const std::unordered_map<ClientID, ResourceMap> &result) {
          for (auto &item : result) {
            if (alive_nodes_.count(item.first)) {
              cluster_resources_[item.first] = item.second;
            }
          }
          RAY_LOG(INFO) << "Finished loading initial data.";
          done();
        };
    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().GetAll(get_node_resource_callback));
  };
  RAY_CHECK_OK(gcs_table_storage_->NodeTable().GetAll(get_node_callback));
}

void GcsNodeManager::StartNodeFailureDetector() {
  // Note: To avoid heartbeats being delayed by main thread, make sure detector start is
  // always handled by its own IO service.
  node_failure_detector_service_.post([this] { node_failure_detector_->Start(); });
}

}  // namespace gcs
}  // namespace ray
