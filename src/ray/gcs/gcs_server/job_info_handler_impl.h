#ifndef RAY_GCS_JOB_INFO_HANDLER_IMPL_H
#define RAY_GCS_JOB_INFO_HANDLER_IMPL_H

#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `JobInfoHandler`.
class DefaultJobInfoHandler : public rpc::JobInfoHandler {
 public:
  explicit DefaultJobInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                    SendReplyCallback send_reply_callback) override;

  void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                             MarkJobFinishedReply *reply,
                             SendReplyCallback send_reply_callback) override;

 private:
  gcs::RedisGcsClient &gcs_client_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_JOB_INFO_HANDLER_IMPL_H
