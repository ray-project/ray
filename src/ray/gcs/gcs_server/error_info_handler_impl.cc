// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "error_info_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultErrorInfoHandler::HandleReportJobError(
    const ReportJobErrorRequest &request, ReportJobErrorReply *reply,
    SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.error_data().job_id());
  std::string type = request.error_data().type();
  RAY_LOG(DEBUG) << "Reporting job error, job id = " << job_id << ", type = " << type;
  auto on_done = [this, job_id, type, request, reply,
                  send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report job error, job id = " << job_id
                     << ", type = " << type;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(ERROR_INFO_CHANNEL, job_id.Hex(),
                                         request.error_data().SerializeAsString(),
                                         nullptr));
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status =
      gcs_table_storage_->ErrorInfoTable().Put(job_id, request.error_data(), on_done);
  if (!status.ok()) {
    on_done(status);
  }
  RAY_LOG(DEBUG) << "Finished reporting job error, job id = " << job_id
                 << ", type = " << type;
}

}  // namespace rpc
}  // namespace ray
