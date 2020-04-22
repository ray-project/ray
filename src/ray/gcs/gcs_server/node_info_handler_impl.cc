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

#include "node_info_handler_impl.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultNodeInfoHandler::HandleRegisterNode(
    const rpc::RegisterNodeRequest &request, rpc::RegisterNodeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_info().node_id());
  RAY_LOG(DEBUG) << "Registering node info, node id = " << node_id;
  gcs_node_manager_.AddNode(std::make_shared<rpc::GcsNodeInfo>(request.node_info()));
  auto on_done = [this, node_id, request, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register node info: " << status.ToString()
                     << ", node id = " << node_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_CHANNEL, node_id.Binary(),
                                         request.node_info().SerializeAsString(),
                                         nullptr));
      RAY_LOG(DEBUG) << "Finished registering node info, node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Nodes().AsyncRegister(request.node_info(), on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultNodeInfoHandler::HandleUnregisterNode(
    const rpc::UnregisterNodeRequest &request, rpc::UnregisterNodeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Unregistering node info, node id = " << node_id;
  auto node_info = gcs_node_manager_.GetNode(node_id);

  if (node_info) {
    UnregisterNode(node_id, *node_info, reply, send_reply_callback);
    gcs_node_manager_.RemoveNode(node_id);
  } else {
    auto on_done = [this, node_id, reply, send_reply_callback](
                       const Status &status,
                       const std::vector<rpc::GcsNodeInfo> &result) {
      if (status.ok()) {
        auto it = std::find_if(result.begin(), result.end(),
                               [node_id](const rpc::GcsNodeInfo &node_info) {
                                 return node_info.node_id() == node_id.Binary();
                               });
        RAY_CHECK(it != result.end());

        rpc::GcsNodeInfo node_info(*it);
        UnregisterNode(node_id, node_info, reply, send_reply_callback);
      } else {
        RAY_LOG(ERROR)
            << "Failed to unregister node info because node info could not be obtained: "
            << status.ToString() << ", node id = " << node_id;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      }
    };
    RAY_CHECK_OK(gcs_client_.Nodes().AsyncGetAll(on_done));
  }
}

void DefaultNodeInfoHandler::HandleGetAllNodeInfo(
    const rpc::GetAllNodeInfoRequest &request, rpc::GetAllNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all nodes info.";
  auto on_done = [reply, send_reply_callback](
                     const Status &status, const std::vector<rpc::GcsNodeInfo> &result) {
    if (status.ok()) {
      for (const rpc::GcsNodeInfo &node_info : result) {
        reply->add_node_info_list()->CopyFrom(node_info);
      }
      RAY_LOG(DEBUG) << "Finished getting all node info.";
    } else {
      RAY_LOG(ERROR) << "Failed to get all nodes info: " << status.ToString();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Nodes().AsyncGetAll(on_done);
  if (!status.ok()) {
    on_done(status, std::vector<rpc::GcsNodeInfo>());
  }
}

void DefaultNodeInfoHandler::HandleReportHeartbeat(
    const ReportHeartbeatRequest &request, ReportHeartbeatReply *reply,
    SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.heartbeat().client_id());
  RAY_LOG(DEBUG) << "Reporting heartbeat, node id = " << node_id;
  auto heartbeat_data = std::make_shared<rpc::HeartbeatTableData>(request.heartbeat());
  auto on_done = [node_id, heartbeat_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report heartbeat: " << status.ToString()
                     << ", node id = " << node_id;
    } else {
      RAY_LOG(DEBUG) << "Finished reporting heartbeat, node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  gcs_node_manager_.HandleHeartbeat(node_id, *heartbeat_data);

  Status status = gcs_client_.Nodes().AsyncReportHeartbeat(heartbeat_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultNodeInfoHandler::HandleGetResources(const GetResourcesRequest &request,
                                                GetResourcesReply *reply,
                                                SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Getting node resources, node id = " << node_id;

  auto on_done = [node_id, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<gcs::NodeInfoAccessor::ResourceMap> &result) {
    if (status.ok()) {
      if (result) {
        for (auto &resource : *result) {
          (*reply->mutable_resources())[resource.first] = *resource.second;
        }
      }
      RAY_LOG(DEBUG) << "Finished getting node resources, node id = " << node_id;
    } else {
      RAY_LOG(ERROR) << "Failed to get node resources: " << status.ToString()
                     << ", node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Nodes().AsyncGetResources(node_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultNodeInfoHandler::HandleGetAllResources(
    const GetAllResourcesRequest &request, GetAllResourcesReply *reply,
    SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all resources.";
  // TODO(ffbin): get all resources.
  Status status = Status::OK();
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  RAY_LOG(DEBUG) << "Finished getting all resources.";
}

void DefaultNodeInfoHandler::HandleUpdateResources(
    const UpdateResourcesRequest &request, UpdateResourcesReply *reply,
    SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(INFO) << "Updating node resources, node id = " << node_id;

  gcs::NodeInfoAccessor::ResourceMap resources;
  for (auto resource : request.resources()) {
    resources[resource.first] = std::make_shared<rpc::ResourceTableData>(resource.second);
  }

  auto on_done = [this, request, resources, node_id, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to update node resources: " << status.ToString()
                     << ", node id = " << node_id;
    } else {
      rpc::ResourceMap resource_data;
      resource_data.mutable_items()->insert(request.resources().begin(),
                                            request.resources().end());
      ResourceChange resource_change;
      resource_change.set_is_add(true);
      resource_change.mutable_data()->CopyFrom(resource_data);
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Binary(),
                                         resource_change.SerializeAsString(), nullptr));

      gcs_node_resource_manager_.UpdateNodeResources(node_id, resources);
      RAY_LOG(INFO) << "Finished updating node resources, node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Nodes().AsyncUpdateResources(node_id, resources, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultNodeInfoHandler::HandleDeleteResources(
    const DeleteResourcesRequest &request, DeleteResourcesReply *reply,
    SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  auto resource_names = VectorFromProtobuf(request.resource_name_list());
  RAY_LOG(DEBUG) << "Deleting node resources, node id = " << node_id;

  auto resources = gcs_node_resource_manager_.GetNodeResources(node_id, resource_names);
  gcs_node_resource_manager_.DeleteNodeResources(node_id, resource_names);

  if (resources) {
    DeleteResources(node_id, resource_names, *resources, reply, send_reply_callback);
  } else {
    auto on_done =
        [this, node_id, resource_names, reply, send_reply_callback](
            const Status &status,
            const boost::optional<gcs::NodeInfoAccessor::ResourceMap> &result) {
          RAY_DCHECK(result);
          gcs::NodeInfoAccessor::ResourceMap resources;
          for (auto &resource_name : resource_names) {
            auto it = result->find(resource_name);
            RAY_CHECK(it != result->end());
            resources[resource_name] = it->second;
          }
          DeleteResources(node_id, resource_names, resources, reply, send_reply_callback);
        };
    RAY_CHECK_OK(gcs_client_.Nodes().AsyncGetResources(node_id, on_done));
  }
}

void DefaultNodeInfoHandler::UnregisterNode(
    const ClientID &node_id, rpc::GcsNodeInfo &node_info, rpc::UnregisterNodeReply *reply,
    const SendReplyCallback &send_reply_callback) {
  node_info.set_state(rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  auto on_done = [this, node_id, node_info, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to unregister node info: " << status.ToString()
                     << ", node id = " << node_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_CHANNEL, node_id.Binary(),
                                         node_info.SerializeAsString(), nullptr));
      RAY_LOG(DEBUG) << "Finished unregistering node info, node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_client_.Nodes().AsyncUnregister(node_id, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultNodeInfoHandler::DeleteResources(
    const ClientID &node_id, const std::vector<std::string> &resource_names,
    const gcs::NodeInfoAccessor::ResourceMap &delete_resources,
    DeleteResourcesReply *reply, const SendReplyCallback &send_reply_callback) {
  ResourceChange resource_change;
  resource_change.set_is_add(false);
  ResourceMap resource_map;
  for (auto &delete_resource : delete_resources) {
    (*resource_map.mutable_items())[delete_resource.first] = *delete_resource.second;
  }
  resource_change.mutable_data()->CopyFrom(resource_map);

  auto on_done = [this, node_id, resource_change, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to delete node resources: " << status.ToString()
                     << ", node id = " << node_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Binary(),
                                         resource_change.SerializeAsString(), nullptr));
      RAY_LOG(DEBUG) << "Finished deleting node resources, node id = " << node_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status delete_status =
      gcs_client_.Nodes().AsyncDeleteResources(node_id, resource_names, on_done);
  if (!delete_status.ok()) {
    on_done(delete_status);
  }
}

}  // namespace rpc
}  // namespace ray
