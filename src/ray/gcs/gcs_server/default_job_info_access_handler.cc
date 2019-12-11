#include "default_job_info_access_handler.h"

namespace ray {
namespace rpc {
void DefaultJobInfoAccessHandler::HandleAddJob(
    const rpc::AddJobRequest &request, rpc::AddJobReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Received new job ...";
}

void DefaultJobInfoAccessHandler::HandleMarkJobFinished(
    const rpc::MarkJobFinishedRequest &request, rpc::MarkJobFinishedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Mark job as finished ...";
}
}  // namespace rpc
}  // namespace ray
