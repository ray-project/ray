#ifndef RAY_GCS_NODE_INFO_HANDLER_IMPL_H
#define RAY_GCS_NODE_INFO_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `NodeInfoHandler`.
class DefaultNodeInfoHandler : public rpc::NodeInfoHandler {
 public:
  explicit DefaultNodeInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleRegisterNodeInfo(const RegisterNodeInfoRequest &request,
                              RegisterNodeInfoReply *reply,
                              SendReplyCallback send_reply_callback) override;

  void HandleUnregisterNodeInfo(const UnregisterNodeInfoRequest &request,
                                UnregisterNodeInfoReply *reply,
                                SendReplyCallback send_reply_callback) override;

  void HandleGetAllNodesInfo(const GetAllNodesInfoRequest &request,
                             GetAllNodesInfoReply *reply,
                             SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_NODE_INFO_HANDLER_IMPL_H
