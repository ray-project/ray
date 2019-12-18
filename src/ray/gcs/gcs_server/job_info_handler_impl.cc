#include "job_info_handler_impl.h"

namespace ray {
namespace rpc {
void DefaultJobInfoHandler::HandleAddJob(const rpc::AddJobRequest &request,
                                         rpc::AddJobReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Begin add job, job id is:" << request.data().job_id()
                 << ",driver id is:" << request.data().driver_pid();
  auto job_table_data = std::make_shared<JobTableData>();
  job_table_data->CopyFrom(request.data());
  Status status = gcs_client_.Jobs().AsyncAdd(
      job_table_data, [reply, send_reply_callback](Status status) {
        reply->set_success(status.ok());
        send_reply_callback(status, nullptr, nullptr);
      });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to add job, job id is:" << request.data().job_id()
                   << ",driver id is:" << request.data().driver_pid();
    send_reply_callback(status, nullptr, nullptr);
  }
  RAY_LOG(DEBUG) << "Finish add job, job id is:" << request.data().job_id()
                 << ",driver id is:" << request.data().driver_pid();
}

void DefaultJobInfoHandler::HandleMarkJobFinished(
    const rpc::MarkJobFinishedRequest &request, rpc::MarkJobFinishedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Begin mark job finished, job id is:" << request.job_id();
  JobID job_id = JobID::FromBinary(request.job_id());
  Status status = gcs_client_.Jobs().AsyncMarkFinished(
      job_id, [reply, send_reply_callback](Status status) {
        reply->set_success(status.ok());
        send_reply_callback(status, nullptr, nullptr);
      });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to mark job finished, job id is:" << request.job_id();
    send_reply_callback(status, nullptr, nullptr);
  }
  RAY_LOG(DEBUG) << "Finish mark job finished, job id is:" << request.job_id();
}
}  // namespace rpc
}  // namespace ray
