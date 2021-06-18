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

void GcsJobManager::Initialize(const GcsInitData &gcs_init_data) {
  for (auto &pair : gcs_init_data.Jobs()) {
    const auto &job_id = pair.first;
    const auto &job_table_data = pair.second;
    const auto &ray_namespace = job_table_data.config().ray_namespace();
    ray_namespaces_[job_id] = ray_namespace;
  }
}

void GcsJobManager::HandleAddJob(const rpc::AddJobRequest &request,
                                 rpc::AddJobReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  rpc::JobTableData mutable_job_table_data;
  mutable_job_table_data.CopyFrom(request.data());
  mutable_job_table_data.set_start_time(current_sys_time_ms());
  JobID job_id = JobID::FromBinary(mutable_job_table_data.job_id());
  RAY_LOG(INFO) << "Adding job, job id = " << job_id
                << ", driver pid = " << mutable_job_table_data.driver_pid();

  auto on_done = [this, job_id, mutable_job_table_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add job, job id = " << job_id
                     << ", driver pid = " << mutable_job_table_data.driver_pid();
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                         mutable_job_table_data.SerializeAsString(),
                                         nullptr));
      if (mutable_job_table_data.config().has_runtime_env()) {
        runtime_env_manager_.AddURIReference(
            job_id.Hex(), mutable_job_table_data.config().runtime_env());
      }
      RAY_LOG(INFO) << "Finished adding job, job id = " << job_id
                    << ", driver pid = " << mutable_job_table_data.driver_pid();
      ray_namespaces_[job_id] = mutable_job_table_data.config().ray_namespace();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status =
      gcs_table_storage_->JobTable().Put(job_id, mutable_job_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsJobManager::MarkJobAsFinished(rpc::JobTableData job_table_data,
                                      std::function<void(Status)> done_callback) {
  const JobID job_id = JobID::FromBinary(job_table_data.job_id());

  auto time = current_sys_time_ms();
  job_table_data.set_timestamp(time);
  job_table_data.set_end_time(time);
  job_table_data.set_is_dead(true);
  job_table_data.set_driver_ip_address("");
  job_table_data.set_driver_pid(-1);
  auto on_done = [this, job_id, job_table_data, done_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to mark job state, job id = " << job_id;
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                         job_table_data.SerializeAsString(), nullptr));
      runtime_env_manager_.RemoveURIReference(job_id.Hex());
      ClearJobInfos(job_id);
      RAY_LOG(INFO) << "Finished marking job state, job id = " << job_id;
    }
    done_callback(status);
  };

  Status status = gcs_table_storage_->JobTable().Put(job_id, job_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsJobManager::HandleMarkJobFinished(const rpc::MarkJobFinishedRequest &request,
                                          rpc::MarkJobFinishedReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  const JobID job_id = JobID::FromBinary(request.job_id());

  auto send_reply = [send_reply_callback, reply](Status status) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->JobTable().Get(
      job_id, [this, job_id, send_reply](
                  Status status, const boost::optional<rpc::JobTableData> &result) {
        if (status.ok() && result) {
          MarkJobAsFinished(*result, send_reply);
        } else {
          RAY_LOG(ERROR) << "Tried to mark job " << job_id
                         << " as finished, but there was no record of it starting!";
          send_reply(status);
        }
      });
  if (!status.ok()) {
    send_reply(status);
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

void GcsJobManager::HandleReportJobError(const rpc::ReportJobErrorRequest &request,
                                         rpc::ReportJobErrorReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  auto job_id = JobID::FromBinary(request.job_error().job_id());
  RAY_CHECK_OK(gcs_pub_sub_->Publish(ERROR_INFO_CHANNEL, job_id.Hex(),
                                     request.job_error().SerializeAsString(), nullptr));
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsJobManager::HandleGetNextJobID(const rpc::GetNextJobIDRequest &request,
                                       rpc::GetNextJobIDReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  reply->set_job_id(gcs_table_storage_->GetNextJobID());
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

std::string GcsJobManager::GetRayNamespace(const JobID &job_id) const {
  auto it = ray_namespaces_.find(job_id);
  RAY_CHECK(it != ray_namespaces_.end()) << "Couldn't find job with id: " << job_id;
  return it->second;
}

}  // namespace gcs
}  // namespace ray
