#ifndef RAY_GCS_ERROR_INFO_HANDLER_IMPL_H
#define RAY_GCS_ERROR_INFO_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `ErrorInfoHandler`.
class DefaultErrorInfoHandler : public rpc::ErrorInfoHandler {
 public:
  explicit DefaultErrorInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleReportJobError(const ReportJobErrorRequest &request,
                            ReportJobErrorReply *reply,
                            SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_ERROR_INFO_HANDLER_IMPL_H
