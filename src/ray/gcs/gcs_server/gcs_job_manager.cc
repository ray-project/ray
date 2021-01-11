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

#include "ray/gcs/gcs_server/gcs_job_manager.h"

#include "ray/gcs/pb_util.h"

namespace ray {
namespace gcs {

void GcsJobManager::HandleAddJob(const rpc::AddJobRequest &request,
                                 rpc::AddJobReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.data().job_id());
  RAY_LOG(INFO) << "Adding job, job id = " << job_id
                << ", driver pid = " << request.data().driver_pid();
  auto on_done = [this, job_id, request, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add job, job id = " << job_id
                     << ", driver pid = " << request.data().driver_pid();
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                         request.data().SerializeAsString(), nullptr));
      RAY_LOG(INFO) << "Finished adding job, job id = " << job_id
                    << ", driver pid = " << request.data().driver_pid();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->JobTable().Put(job_id, request.data(), on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsJobManager::HandleMarkJobFinished(const rpc::MarkJobFinishedRequest &request,
                                          rpc::MarkJobFinishedReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(INFO) << "Marking job state, job id = " << job_id;
  auto job_table_data =
      gcs::CreateJobTableData(job_id, /*is_dead*/ true, std::time(nullptr), "", -1);
  auto on_done = [this, job_id, job_table_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to mark job state, job id = " << job_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                         job_table_data->SerializeAsString(), nullptr));
      ClearJobInfos(job_id);
      RAY_LOG(INFO) << "Finished marking job state, job id = " << job_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsJobManager::ClearJobInfos(const JobID &job_id) {
  // Notify all listeners.
  for (auto &listener : job_finished_listeners_) {
    listener(std::make_shared<JobID>(job_id));
  }
}

/// Add listener to monitor the add action of nodes.
///
/// \param listener The handler which process the add of nodes.
void GcsJobManager::AddJobFinishedListener(
    std::function<void(std::shared_ptr<JobID>)> listener) {
  RAY_CHECK(listener);
  job_finished_listeners_.emplace_back(std::move(listener));
}

void GcsJobManager::HandleGetAllJobInfo(const rpc::GetAllJobInfoRequest &request,
                                        rpc::GetAllJobInfoReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO) << "Getting all job info.";
  auto on_done = [reply, send_reply_callback](
                     const std::unordered_map<JobID, JobTableData> &result) {
    for (auto &data : result) {
      reply->add_job_info_list()->CopyFrom(data.second);
    }
    RAY_LOG(INFO) << "Finished getting all job info.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  Status status = gcs_table_storage_->JobTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(std::unordered_map<JobID, JobTableData>());
  }
}

}  // namespace gcs
}  // namespace ray
