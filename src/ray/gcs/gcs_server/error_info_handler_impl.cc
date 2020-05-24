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

void DefaultErrorInfoHandler::HandleGetAllJobErrorInfo(
    const GetAllJobErrorInfoRequest &request, GetAllJobErrorInfoReply *reply,
    SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all job error info.";
  auto on_done = [reply, send_reply_callback](
                     const std::unordered_map<JobID, ErrorTableData> &result) {
    for (auto &data : result) {
      reply->add_error_info_list()->CopyFrom(data.second);
    }
    RAY_LOG(DEBUG) << "Finished getting all job error info.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  Status status = gcs_table_storage_->ErrorInfoTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(std::unordered_map<JobID, ErrorTableData>());
  }
}

void DefaultErrorInfoHandler::HandleGetJobErrorInfo(
    const GetJobErrorInfoRequest &request, GetJobErrorInfoReply *reply,
    SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(DEBUG) << "Getting job error info, job id = " << job_id;
  auto on_done = [job_id, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<ErrorTableData> &result) {
    if (result) {
      reply->mutable_error_info()->CopyFrom(*result);
    }
    RAY_LOG(DEBUG) << "Finished getting job error info, job id = " << job_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  Status status = gcs_table_storage_->ErrorInfoTable().Get(job_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

}  // namespace rpc
}  // namespace ray
