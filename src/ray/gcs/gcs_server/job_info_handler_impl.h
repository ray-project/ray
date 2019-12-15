#pragma once
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This class is used to implement `JobInfoHandler`, but only two logs have been printed.
/// The detailed implementation is reflected in the following PR.
class DefaultJobInfoHandler : public rpc::JobInfoHandler {
 public:
  explicit DefaultJobInfoHandler(gcs::RedisGcsClient &gcs_client)
      : gcs_client_(gcs_client) {}

  void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                    SendReplyCallback send_reply_callback) override;

  void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                             MarkJobFinishedReply *reply,
                             SendReplyCallback send_reply_callback) override;

  uint64_t GetAddJobCount() { return metrics_[ADD_JOB]; }

  uint64_t GetMarkJobFinishedCount() { return metrics_[MARK_JOB_FINISHED]; }

 private:
  gcs::RedisGcsClient &gcs_client_;

  enum { ADD_JOB = 0, MARK_JOB_FINISHED = 1, COMMAND_TYPE_END = 2 };
  /// metrics
  uint64_t metrics_[COMMAND_TYPE_END] = {0};
};

}  // namespace rpc
}  // namespace ray
