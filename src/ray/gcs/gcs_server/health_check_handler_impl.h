#ifndef RAY_GCS_HEALTH_CHECK_HANDLER_IMPL_H
#define RAY_GCS_HEALTH_CHECK_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `HealthCheckHandler`.
class DefaultHealthCheckHandler : public rpc::HealthCheckHandler {
 public:
  void HandlePing(const PingRequest &request,
                  PingReply *reply,
                  SendReplyCallback send_reply_callback) override;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_HEALTH_CHECK_HANDLER_IMPL_H
