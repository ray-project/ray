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

#include "ray/gcs/gcs_client/accessor.h"
#include "ray/gcs/pb_util.h"

namespace ray {
namespace gcs {

void GcsJobManager::Initialize(const GcsInitData &gcs_init_data) {
  for (auto &pair : gcs_init_data.Jobs()) {
    const auto &job_id = pair.first;
    const auto &job_table_data = pair.second;
    cached_job_configs_[job_id] =
        std::make_shared<rpc::JobConfig>(job_table_data.config());
    function_manager_.AddJobReference(job_id);
  }
}

void GcsJobManager::HandleAddJob(rpc::AddJobRequest request,
                                 rpc::AddJobReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  rpc::JobTableData mutable_job_table_data;
  mutable_job_table_data.CopyFrom(request.data());
  auto time = current_sys_time_ms();
  mutable_job_table_data.set_start_time(time);
  mutable_job_table_data.set_timestamp(time);
  JobID job_id = JobID::FromBinary(mutable_job_table_data.job_id());
  RAY_LOG(INFO) << "Adding job, job id = " << job_id
                << ", driver pid = " << mutable_job_table_data.driver_pid();

  auto on_done = [this, job_id, mutable_job_table_data, reply, send_reply_callback](
                     const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add job, job id = " << job_id
                     << ", driver pid = " << mutable_job_table_data.driver_pid();
    } else {
      RAY_CHECK_OK(gcs_publisher_->PublishJob(job_id, mutable_job_table_data, nullptr));
      if (mutable_job_table_data.config().has_runtime_env_info()) {
        runtime_env_manager_.AddURIReference(
            job_id.Hex(), mutable_job_table_data.config().runtime_env_info());
      }
      function_manager_.AddJobReference(job_id);
      RAY_LOG(INFO) << "Finished adding job, job id = " << job_id
                    << ", driver pid = " << mutable_job_table_data.driver_pid();
      cached_job_configs_[job_id] =
          std::make_shared<rpc::JobConfig>(mutable_job_table_data.config());
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
  auto on_done = [this, job_id, job_table_data, done_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to mark job state, job id = " << job_id;
    } else {
      RAY_CHECK_OK(gcs_publisher_->PublishJob(job_id, job_table_data, nullptr));
      runtime_env_manager_.RemoveURIReference(job_id.Hex());
      ClearJobInfos(job_table_data);
      RAY_LOG(INFO) << "Finished marking job state, job id = " << job_id;
    }
    function_manager_.RemoveJobReference(job_id);
    done_callback(status);
  };

  Status status = gcs_table_storage_->JobTable().Put(job_id, job_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsJobManager::HandleMarkJobFinished(rpc::MarkJobFinishedRequest request,
                                          rpc::MarkJobFinishedReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  const JobID job_id = JobID::FromBinary(request.job_id());

  auto send_reply = [send_reply_callback, reply](Status status) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->JobTable().Get(
      job_id,
      [this, job_id, send_reply](Status status,
                                 const boost::optional<rpc::JobTableData> &result) {
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

void GcsJobManager::ClearJobInfos(const rpc::JobTableData &job_data) {
  // Notify all listeners.
  for (auto &listener : job_finished_listeners_) {
    listener(job_data);
  }
  // Clear cache.
  // TODO(qwang): This line will cause `test_actor_advanced.py::test_detached_actor`
  // case fail under GCS HA mode. Because detached actor is still alive after
  // job is finished. After `DRIVER_EXITED` state being introduced in issue
  // https://github.com/ray-project/ray/issues/21128, this line should work.
  // RAY_UNUSED(cached_job_configs_.erase(job_id));
}

/// Add listener to monitor the add action of nodes.
///
/// \param listener The handler which process the add of nodes.
void GcsJobManager::AddJobFinishedListener(JobFinishListenerCallback listener) {
  RAY_CHECK(listener);
  job_finished_listeners_.emplace_back(std::move(listener));
}

void GcsJobManager::HandleGetAllJobInfo(rpc::GetAllJobInfoRequest request,
                                        rpc::GetAllJobInfoReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO) << "Getting all job info.";

  int limit = std::numeric_limits<int>::max();
  if (request.has_limit()) {
    limit = request.limit();
    if (limit < 0) {
      RAY_LOG(ERROR) << "Invalid limit " << limit
                     << " specified in GetAllJobInfoRequest, "
                     << "must be nonnegative.";
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid("Invalid limit"));
      return;
    }
    RAY_LOG(INFO) << "Getting job info with limit " << limit << ".";
  }

  auto on_done = [this, reply, send_reply_callback, limit](
                     const absl::flat_hash_map<JobID, JobTableData> &result) {
    // Internal KV keys for jobs that were submitted via the Ray Job API.
    std::vector<std::string> job_api_data_keys;

    // Maps job data keys to the index of the job in the table.
    std::unordered_map<std::string, int> job_data_key_to_index;

    // Load the job table data into the reply.
    int i = 0;
    for (auto &data : result) {
      if (i >= limit) {
        break;
      }
      reply->add_job_info_list()->CopyFrom(data.second);
      auto &metadata = data.second.config().metadata();
      auto iter = metadata.find("job_submission_id");
      if (iter != metadata.end()) {
        // This job was submitted via the Ray Job API, so it has JobInfo in the kv.
        std::string job_submission_id = iter->second;
        std::string job_data_key = JobDataKey(job_submission_id);
        job_api_data_keys.push_back(job_data_key);
        job_data_key_to_index[job_data_key] = i;
      }
      i++;
    }

    RAY_CHECK(job_api_data_keys.size() == job_data_key_to_index.size());

    auto kv_multi_get_callback =
        [reply, send_reply_callback, job_data_key_to_index](
            std::unordered_map<std::string, std::string> result) {
          for (auto &data : result) {
            std::string job_data_key = data.first;
            // The JobInfo stored by the Ray Job API.
            std::string job_info_json = data.second;
            if (!job_info_json.empty()) {
              // Parse the JSON into a JobsAPIInfo proto.
              rpc::JobsAPIInfo jobs_api_info;
              auto status = google::protobuf::util::JsonStringToMessage(job_info_json,
                                                                        &jobs_api_info);
              if (!status.ok()) {
                RAY_LOG(ERROR)
                    << "Failed to parse JobInfo JSON into JobsAPIInfo protobuf. JSON: "
                    << job_info_json << " Error: " << status.message();
              }
              // Add the JobInfo to the correct index in the reply.
              reply->mutable_job_info_list(job_data_key_to_index.at(job_data_key))
                  ->mutable_job_info()
                  ->CopyFrom(std::move(jobs_api_info));
            }
          }
          RAY_LOG(INFO) << "Finished getting all job info.";
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
        };
    internal_kv_.MultiGet("job", job_api_data_keys, kv_multi_get_callback);
  };
  Status status = gcs_table_storage_->JobTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(absl::flat_hash_map<JobID, JobTableData>());
  }
}

void GcsJobManager::HandleReportJobError(rpc::ReportJobErrorRequest request,
                                         rpc::ReportJobErrorReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  auto job_id = JobID::FromBinary(request.job_error().job_id());
  RAY_CHECK_OK(gcs_publisher_->PublishError(job_id.Hex(), request.job_error(), nullptr));
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsJobManager::HandleGetNextJobID(rpc::GetNextJobIDRequest request,
                                       rpc::GetNextJobIDReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  reply->set_job_id(gcs_table_storage_->GetNextJobID());
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

std::shared_ptr<rpc::JobConfig> GcsJobManager::GetJobConfig(const JobID &job_id) const {
  auto it = cached_job_configs_.find(job_id);
  RAY_CHECK(it != cached_job_configs_.end()) << "Couldn't find job with id: " << job_id;
  return it->second;
}

}  // namespace gcs
}  // namespace ray
