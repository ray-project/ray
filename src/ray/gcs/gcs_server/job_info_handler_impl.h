#ifndef RAY_GCS_JOB_INFO_HANDLER_IMPL_H
#define RAY_GCS_JOB_INFO_HANDLER_IMPL_H

#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This class is used to implement `JobInfoHandler`, but only two logs have been printed.
/// The detailed implementation is reflected in the following PR.
class DefaultJobInfoHandler : public rpc::JobInfoHandler {
 public:
  void HandleAddJob(const AddJobRequest &request, AddJobReply *reply,
                    SendReplyCallback send_reply_callback) override;

  void HandleMarkJobFinished(const MarkJobFinishedRequest &request,
                             MarkJobFinishedReply *reply,
                             SendReplyCallback send_reply_callback) override;
};

}  // namespace rpc
}  // namespace ray

#endif
