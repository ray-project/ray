#include "job_info_handler_impl.h"

namespace ray {
namespace rpc {
void DefaultJobInfoHandler::HandleAddJob(const rpc::AddJobRequest &request,
                                         rpc::AddJobReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Begin handle add job, job id is:" << request.data().job_id()
                 << " ,driver id is: " << request.data().driver_pid();
  Status status = gcs_client_.job_table().AppendJobData(
      JobID::FromBinary(request.data().job_id()),
      /*is_dead=*/false, std::time(nullptr), request.data().node_manager_address(),
      request.data().driver_pid());
  reply->set_success(status.ok());
  send_reply_callback(status, nullptr, nullptr);
  ++metrics_[ADD_JOB];
  RAY_LOG(DEBUG) << "Finish handle add job, job id is:" << request.data().job_id()
                 << " ,driver id is: " << request.data().driver_pid();
}

void DefaultJobInfoHandler::HandleMarkJobFinished(
    const rpc::MarkJobFinishedRequest &request, rpc::MarkJobFinishedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Begin handle mark job finished, job id is:"
                 << request.data().job_id()
                 << " ,driver id is: " << request.data().driver_pid();
  Status status = gcs_client_.job_table().AppendJobData(
      JobID::FromBinary(request.data().job_id()),
      /*is_dead=*/true, std::time(nullptr), request.data().node_manager_address(),
      request.data().driver_pid());
  reply->set_success(status.ok());
  send_reply_callback(status, nullptr, nullptr);
  ++metrics_[MARK_JOB_FINISHED];
  RAY_LOG(DEBUG) << "Finish handle mark job finished, job id is:"
                 << request.data().job_id()
                 << " ,driver id is: " << request.data().driver_pid();
}
}  // namespace rpc
}  // namespace ray
