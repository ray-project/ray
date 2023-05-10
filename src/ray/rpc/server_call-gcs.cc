#include "ray/rpc/server_call-gcs.h"

namespace ray {
namespace rpc {
// Server call template overrides for services in gcs_service.proto

// In this case, the client is requesting the token,
// so we accept anything that does not have an incorrect cluster token.
template <>
void ServerCallImpl<NodeInfoGcsService, RegisterClientRequest, RegisterClientReply>::
    HandleRequest() {
  if (cluster_id_ != nullptr) {
    auto &metadata = context_.client_metadata();
    if (auto it = metadata.find(kClusterIdKey);
        it != metadata.end() && it->second != cluster_id_->Binary()) {
      RAY_LOG(DEBUG) << "Wrong cluster ID token in request!";
      SendReply(Status::AuthError("WrongClusterToken"));
    }
  }
  // reply_->set_cluster_id(cluster_id_);
  HandleRequestNoAuth();
}

}  // namespace rpc
}  // namespace ray