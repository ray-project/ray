#include "job_info_handler_impl.h"

namespace ray {
namespace rpc {
void DefaultJobInfoHandler::HandleAddJob(const rpc::AddJobRequest &request,
                                         rpc::AddJobReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Received new job ...";
  // TODO(zsl): The detailed implementation will be committed in next PR.
}

void DefaultJobInfoHandler::HandleMarkJobFinished(
    const rpc::MarkJobFinishedRequest &request, rpc::MarkJobFinishedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Mark job as finished ...";
  // TODO(zsl): The detailed implementation will be committed in next PR.
}
}  // namespace rpc
}  // namespace ray
