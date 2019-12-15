#include "job_info_handler_impl.h"

namespace ray {
namespace rpc {
void DefaultJobInfoHandler::HandleAddJob(const rpc::AddJobRequest &request,
                                         rpc::AddJobReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Begin handle add job, job id is:" << request.data().job_id()
                 << " ,driver id is: " << request.data().driver_pid();
  auto job_table_data = std::make_shared<JobTableData>();
  job_table_data->CopyFrom(request.data());
  Status status = gcs_client_.Jobs().AsyncAdd(
      job_table_data, [this, reply, send_reply_callback](Status status) {
        reply->set_success(status.ok());
        send_reply_callback(status, nullptr, nullptr);
        ++metrics_[ADD_JOB];
      });
  RAY_LOG(DEBUG) << "Finish handle add job, job id is:" << request.data().job_id()
                 << " ,driver id is: " << request.data().driver_pid();
}

void DefaultJobInfoHandler::HandleMarkJobFinished(
    const rpc::MarkJobFinishedRequest &request, rpc::MarkJobFinishedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Begin handle mark job finished, job id is:" << request.job_id();
  JobID job_id = JobID::FromBinary(request.job_id());
  Status status = gcs_client_.Jobs().AsyncMarkFinished(
      job_id, [this, reply, send_reply_callback](Status status) {
        reply->set_success(status.ok());
        send_reply_callback(status, nullptr, nullptr);
        ++metrics_[MARK_JOB_FINISHED];
      });
  RAY_LOG(DEBUG) << "Finish handle mark job finished, job id is:" << request.job_id();
}
}  // namespace rpc
}  // namespace ray
