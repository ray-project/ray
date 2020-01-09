#include "error_info_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultErrorInfoHandler::HandleReportError(const ReportErrorRequest &request,
                                                ReportErrorReply *reply,
                                                SendReplyCallback send_reply_callback) {
  (void)gcs_client_;
  JobID job_id = JobID::FromBinary(request.error_data().job_id());
  RAY_LOG(DEBUG) << "Reporting error, job id = " << job_id
                 << ", type = " << request.error_data().type();
  auto error_table_data = std::make_shared<ErrorTableData>();
  error_table_data->CopyFrom(request.error_data());
  auto on_done = [job_id, request, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report error, job id = " << job_id
                     << ", type = " << request.error_data().type();
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  //  Status status = gcs_client_.Errors().AsyncReportError(error_table_data, on_done);
  //  if (!status.ok()) {
  //    on_done(status);
  //  }
  RAY_LOG(DEBUG) << "Finished reporting error, job id = " << job_id
                 << ", type = " << request.error_data().type();
}

}  // namespace rpc
}  // namespace ray
