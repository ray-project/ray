#include "worker_info_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultWorkerInfoHandler::HandleReportWorkerFailure(
    const ReportWorkerFailureRequest &request, ReportWorkerFailureReply *reply,
    SendReplyCallback send_reply_callback) {
  Address worker_address = request.worker_failure().worker_address();
  RAY_LOG(DEBUG) << "Reporting worker failure, " << worker_address.DebugString();
  auto worker_failure_data = std::make_shared<WorkerFailureData>();
  worker_failure_data->CopyFrom(request.worker_failure());
  auto on_done = [worker_address, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report worker failure, "
                     << worker_address.DebugString();
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status =
      gcs_client_.Workers().AsyncReportWorkerFailure(worker_failure_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished reporting worker failure, " << worker_address.DebugString();
}

}  // namespace rpc
}  // namespace ray
