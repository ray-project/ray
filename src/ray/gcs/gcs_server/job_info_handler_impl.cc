#include "job_info_handler_impl.h"

namespace ray {
namespace rpc {
void DefaultJobInfoHandler::HandleAddJob(const rpc::AddJobRequest &request,
                                         rpc::AddJobReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.data().job_id());
  RAY_LOG(INFO) << "Adding job, job id = " << job_id
                 << ", driver pid = " << request.data().driver_pid();
  auto job_table_data = std::make_shared<JobTableData>();
  job_table_data->CopyFrom(request.data());
  auto on_done = [job_id, request, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add job, job id = " << job_id
                     << ", driver pid = " << request.data().driver_pid();
    }
    reply->set_success(status.ok());
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Jobs().AsyncAdd(job_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(INFO) << "Finished adding job, job id = " << job_id
                 << ", driver pid = " << request.data().driver_pid();
}

void DefaultJobInfoHandler::HandleMarkJobFinished(
    const rpc::MarkJobFinishedRequest &request, rpc::MarkJobFinishedReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(DEBUG) << "Marking job state, job id = " << job_id;
  auto on_done = [job_id, reply, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to mark job state, job id = " << job_id;
    }
    reply->set_success(status.ok());
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_client_.Jobs().AsyncMarkFinished(job_id, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished marking job state, job id = " << job_id;
}
}  // namespace rpc
}  // namespace ray
