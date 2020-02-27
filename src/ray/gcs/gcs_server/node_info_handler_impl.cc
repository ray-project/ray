#include "node_info_handler_impl.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultNodeInfoHandler::HandleRegisterNode(
    const rpc::RegisterNodeRequest &request, rpc::RegisterNodeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_info().node_id());
  RAY_LOG(DEBUG) << "Registering node info, node id = " << node_id;

  auto on_done = [node_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register node info: " << status.ToString()
                     << ", node id = " << node_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Nodes().AsyncRegister(request.node_info(), on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished registering node info, node id = " << node_id;
}

void DefaultNodeInfoHandler::HandleUnregisterNode(
    const rpc::UnregisterNodeRequest &request, rpc::UnregisterNodeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Unregistering node info, node id = " << node_id;

  auto on_done = [node_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to unregister node info: " << status.ToString()
                     << ", node id = " << node_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Nodes().AsyncUnregister(node_id, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished unregistering node info, node id = " << node_id;
}

void DefaultNodeInfoHandler::HandleGetAllNodeInfo(
    const rpc::GetAllNodeInfoRequest &request, rpc::GetAllNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all nodes info.";
  auto on_done = [reply, send_reply_callback](
                     Status status, const std::vector<rpc::GcsNodeInfo> &result) {
    if (status.ok()) {
      for (const rpc::GcsNodeInfo &node_info : result) {
        reply->add_node_info_list()->CopyFrom(node_info);
      }
    } else {
      RAY_LOG(ERROR) << "Failed to get all nodes info: " << status.ToString();
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Nodes().AsyncGetAll(on_done);
  if (!status.ok()) {
    on_done(status, std::vector<rpc::GcsNodeInfo>());
  }
  RAY_LOG(DEBUG) << "Finished getting all node info.";
}

void DefaultNodeInfoHandler::HandleReportHeartbeat(
    const ReportHeartbeatRequest &request, ReportHeartbeatReply *reply,
    SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.heartbeat().client_id());
  RAY_LOG(DEBUG) << "Reporting heartbeat, node id = " << node_id;

  auto on_done = [node_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report heartbeat: " << status.ToString()
                     << ", node id = " << node_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  auto heartbeat_data = std::make_shared<rpc::HeartbeatTableData>();
  heartbeat_data->CopyFrom(request.heartbeat());
  Status status = gcs_client_.Nodes().AsyncReportHeartbeat(heartbeat_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished reporting heartbeat, node id = " << node_id;
}

void DefaultNodeInfoHandler::HandleReportBatchHeartbeat(
    const ReportBatchHeartbeatRequest &request, ReportBatchHeartbeatReply *reply,
    SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Reporting batch heartbeat, batch size = "
                 << request.heartbeat_batch().batch_size();

  auto on_done = [&request, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report batch heartbeat: " << status.ToString()
                     << ", batch size = " << request.heartbeat_batch().batch_size();
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  auto heartbeat_batch_data = std::make_shared<rpc::HeartbeatBatchTableData>();
  heartbeat_batch_data->CopyFrom(request.heartbeat_batch());
  Status status =
      gcs_client_.Nodes().AsyncReportBatchHeartbeat(heartbeat_batch_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }

  RAY_LOG(DEBUG) << "Finished reporting batch heartbeat, batch size = "
                 << request.heartbeat_batch().batch_size();
}

void DefaultNodeInfoHandler::HandleGetResources(const GetResourcesRequest &request,
                                                GetResourcesReply *reply,
                                                SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Getting node resources, node id = " << node_id;

  auto on_done = [node_id, reply, send_reply_callback](
                     Status status,
                     const boost::optional<gcs::NodeInfoAccessor::ResourceMap> &result) {
    if (status.ok()) {
      if (result) {
        for (auto &resource : *result) {
          (*reply->mutable_resources())[resource.first] = *resource.second;
        }
      }
    } else {
      RAY_LOG(ERROR) << "Failed to get node resources: " << status.ToString()
                     << ", node id = " << node_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Nodes().AsyncGetResources(node_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }

  RAY_LOG(DEBUG) << "Finished getting node resources, node id = " << node_id;
}

void DefaultNodeInfoHandler::HandleUpdateResources(
    const UpdateResourcesRequest &request, UpdateResourcesReply *reply,
    SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Updating node resources, node id = " << node_id;

  gcs::NodeInfoAccessor::ResourceMap resources;
  for (auto resource : request.resources()) {
    resources[resource.first] = std::make_shared<rpc::ResourceTableData>(resource.second);
  }

  auto on_done = [node_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to update node resources: " << status.ToString()
                     << ", node id = " << node_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Nodes().AsyncUpdateResources(node_id, resources, on_done);
  if (!status.ok()) {
    on_done(status);
  }

  RAY_LOG(DEBUG) << "Finished updating node resources, node id = " << node_id;
}

void DefaultNodeInfoHandler::HandleDeleteResources(
    const DeleteResourcesRequest &request, DeleteResourcesReply *reply,
    SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  auto resource_names = VectorFromProtobuf(request.resource_name_list());
  RAY_LOG(DEBUG) << "Deleting node resources, node id = " << node_id;

  auto on_done = [node_id, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to delete node resources: " << status.ToString()
                     << ", node id = " << node_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status =
      gcs_client_.Nodes().AsyncDeleteResources(node_id, resource_names, on_done);
  if (!status.ok()) {
    on_done(status);
  }

  RAY_LOG(DEBUG) << "Finished deleting node resources, node id = " << node_id;
}

}  // namespace rpc
}  // namespace ray
