#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

namespace ray {
namespace gcs {

GcsVirtualClusterManager::GcsVirtualClusterManager(instrumented_io_context &io_context)
    : io_context_(io_context) {}

void GcsVirtualClusterManager::HandleCreateVirtualCluster(
    rpc::CreateVirtualClusterRequest request,
    rpc::CreateVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  io_context_.post([] {}, "");
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsVirtualClusterManager::HandleRemoveVirtualCluster(
    rpc::RemoveVirtualClusterRequest request,
    rpc::RemoveVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

}  // namespace gcs
}  // namespace ray
