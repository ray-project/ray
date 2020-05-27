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
#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {

void DefaultErrorInfoHandler::HandleReportJobError(
    const ReportJobErrorRequest &request, ReportJobErrorReply *reply,
    SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.error_data().job_id());
  std::string type = request.error_data().type();
  RAY_LOG(DEBUG) << "Reporting job error, job id = " << job_id << ", type = " << type;
  auto &list = job_to_errors_[job_id];
  if (list.size() >= RayConfig::instance().error_info_entries_size_per_job()) {
    list.pop_front();
  }
  list.push_back(request.error_data());

  RAY_CHECK_OK(gcs_pub_sub_->Publish(ERROR_INFO_CHANNEL, job_id.Hex(),
                                     request.error_data().SerializeAsString(), nullptr));
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished reporting job error, job id = " << job_id
                 << ", type = " << type;
}

void DefaultErrorInfoHandler::HandleGetAllJobErrorInfo(
    const GetAllJobErrorInfoRequest &request, GetAllJobErrorInfoReply *reply,
    SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all job error info.";

  for (auto &item : job_to_errors_) {
    JobErrorInfo job_error_info;
    job_error_info.set_job_id(item.first.Binary());
    for (auto &error_item : item.second) {
      job_error_info.add_error_info_list()->CopyFrom(error_item);
    }
    reply->add_job_error_info_list()->CopyFrom(job_error_info);
  }

  RAY_LOG(DEBUG) << "Finished getting all job error info.";
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void DefaultErrorInfoHandler::HandleGetJobErrorInfo(
    const GetJobErrorInfoRequest &request, GetJobErrorInfoReply *reply,
    SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(DEBUG) << "Getting job error info, job id = " << job_id;
  auto list = job_to_errors_[job_id];
  if (!list.empty()) {
    rpc::JobErrorInfo job_error_info;
    job_error_info.set_job_id(job_id.Binary());
    for (auto &item : list) {
      job_error_info.add_error_info_list()->CopyFrom(item);
    }
    reply->mutable_job_error_info()->CopyFrom(job_error_info);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished getting job error info, job id = " << job_id;
}

}  // namespace rpc
}  // namespace ray
