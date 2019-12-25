#include "node_info_handler_impl.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultNodeInfoHandler::HandleRegisterNodeInfo(
    const rpc::RegisterNodeInfoRequest &request, rpc::RegisterNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_info().node_id());
  RAY_LOG(DEBUG) << "Registering node info, node id = " << node_id;
  Status status = gcs_client_.Nodes().Register(request.node_info());
  if (!status.ok()) {
    RAY_LOG(DEBUG) << "Failed to register node info, node id = " << node_id;
  }
  send_reply_callback(status, nullptr, nullptr);
  RAY_LOG(DEBUG) << "Finished registering node info, node id = " << node_id;
}

void DefaultNodeInfoHandler::HandleUnregisterNodeInfo(
    const rpc::UnregisterNodeInfoRequest &request, rpc::UnregisterNodeInfoReply *reply,
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

void DefaultNodeInfoHandler::HandleGetAllNodesInfo(
    const rpc::GetAllNodesInfoRequest &request, rpc::GetAllNodesInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all nodes info.";
  auto on_done = [reply, send_reply_callback](
                     Status status, const std::vector<rpc::GcsNodeInfo> &result) {
    if (status.ok()) {
      for (const rpc::GcsNodeInfo &node_info : result) {
        reply->add_node_infos()->CopyFrom(node_info);
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

}  // namespace rpc
}  // namespace ray
