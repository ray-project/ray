#include "node_info_handler_impl.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultNodeInfoHandler::HandleRegisterNodeInfo(
    const rpc::RegisterNodeInfoRequest &request, rpc::RegisterNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  (void) gcs_client_;
  GcsNodeInfo node_info = request.node_info();
  RAY_LOG(DEBUG) << "Registering node info, node id = " << node_info.node_id();
  send_reply_callback(Status::OK(), nullptr, nullptr);
  RAY_LOG(DEBUG) << "Finished registering node info, node id = " << node_info.node_id();
}

void DefaultNodeInfoHandler::HandleUnregisterNodeInfo(
    const rpc::UnregisterNodeInfoRequest &request, rpc::UnregisterNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Unregistering node info, node id = " << request.node_id();
  send_reply_callback(Status::OK(), nullptr, nullptr);
  RAY_LOG(DEBUG) << "Finished unregistering node info, node id = " << request.node_id();
}

void DefaultNodeInfoHandler::HandleGetAllNodesInfo(
    const rpc::GetAllNodesInfoRequest &request, rpc::GetAllNodesInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all nodes info.";
  send_reply_callback(Status::OK(), nullptr, nullptr);
  RAY_LOG(DEBUG) << "Finished getting all node info.";
}

}  // namespace rpc
}  // namespace ray
