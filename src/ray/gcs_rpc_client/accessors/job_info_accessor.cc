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

#include "ray/gcs_rpc_client/accessors/job_info_accessor.h"

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/pubsub/gcs_subscriber.h"
#include "ray/util/container_util.h"

namespace ray {
namespace gcs {

JobInfoAccessor::JobInfoAccessor(GcsClientContext *context) : context_(context) {}

void JobInfoAccessor::AsyncAdd(const std::shared_ptr<rpc::JobTableData> &data_ptr,
                               const StatusCallback &callback) {
  JobID job_id = JobID::FromBinary(data_ptr->job_id());
  RAY_LOG(DEBUG).WithField(job_id)
      << "Adding job, driver pid = " << data_ptr->driver_pid();
  rpc::AddJobRequest request;
  request.mutable_data()->CopyFrom(*data_ptr);
  context_->GetGcsRpcClient().AddJob(
      std::move(request),
      [job_id, data_ptr, callback](const Status &status, rpc::AddJobReply &&) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG).WithField(job_id) << "Finished adding job, status = " << status
                                         << ", driver pid = " << data_ptr->driver_pid();
      });
}

void JobInfoAccessor::AsyncMarkFinished(const JobID &job_id,
                                        const StatusCallback &callback) {
  RAY_LOG(DEBUG).WithField(job_id) << "Marking job state";
  rpc::MarkJobFinishedRequest request;
  request.set_job_id(job_id.Binary());
  context_->GetGcsRpcClient().MarkJobFinished(
      std::move(request),
      [job_id, callback](const Status &status, rpc::MarkJobFinishedReply &&) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG).WithField(job_id)
            << "Finished marking job state, status = " << status;
      });
}

Status JobInfoAccessor::AsyncSubscribeAll(
    const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  fetch_all_data_operation_ = [this, subscribe](const StatusCallback &done_callback) {
    auto callback = [subscribe, done_callback](
                        const Status &status,
                        std::vector<rpc::JobTableData> &&job_info_list) {
      for (auto &job_info : job_info_list) {
        subscribe(JobID::FromBinary(job_info.job_id()), std::move(job_info));
      }
      if (done_callback) {
        done_callback(status);
      }
    };
    AsyncGetAll(/*job_or_submission_id=*/std::nullopt,
                /*skip_submission_job_info_field=*/true,
                /*skip_is_running_tasks_field=*/true,
                callback,
                /*timeout_ms=*/-1);
  };
  subscribe_operation_ = [this, subscribe](const StatusCallback &done_callback) {
    return context_->GetGcsSubscriber().SubscribeAllJobs(subscribe, done_callback);
  };
  return subscribe_operation_(
      [this, done](const Status &status) { fetch_all_data_operation_(done); });
}

void JobInfoAccessor::AsyncResubscribe() {
  RAY_LOG(DEBUG) << "Reestablishing subscription for job info.";
  auto fetch_all_done = [](const Status &status) {
    RAY_LOG(INFO) << "Finished fetching all job information from gcs server after gcs "
                     "server or pub-sub server is restarted.";
  };

  if (subscribe_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_operation_([this, fetch_all_done](const Status &) {
      fetch_all_data_operation_(fetch_all_done);
    }));
  }
}

void JobInfoAccessor::AsyncGetAll(const std::optional<std::string> &job_or_submission_id,
                                  bool skip_submission_job_info_field,
                                  bool skip_is_running_tasks_field,
                                  const MultiItemCallback<rpc::JobTableData> &callback,
                                  int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "Getting all job info.";
  RAY_CHECK(callback);
  rpc::GetAllJobInfoRequest request;
  request.set_skip_submission_job_info_field(skip_submission_job_info_field);
  request.set_skip_is_running_tasks_field(skip_is_running_tasks_field);
  if (job_or_submission_id.has_value()) {
    request.set_job_or_submission_id(job_or_submission_id.value());
  }
  context_->GetGcsRpcClient().GetAllJobInfo(
      std::move(request),
      [callback](const Status &status, rpc::GetAllJobInfoReply &&reply) {
        callback(status, VectorFromProtobuf(std::move(*reply.mutable_job_info_list())));
        RAY_LOG(DEBUG) << "Finished getting all job info.";
      },
      timeout_ms);
}

Status JobInfoAccessor::GetAll(const std::optional<std::string> &job_or_submission_id,
                               bool skip_submission_job_info_field,
                               bool skip_is_running_tasks_field,
                               std::vector<rpc::JobTableData> &job_data_list,
                               int64_t timeout_ms) {
  rpc::GetAllJobInfoRequest request;
  request.set_skip_submission_job_info_field(skip_submission_job_info_field);
  request.set_skip_is_running_tasks_field(skip_is_running_tasks_field);
  if (job_or_submission_id.has_value()) {
    request.set_job_or_submission_id(job_or_submission_id.value());
  }
  rpc::GetAllJobInfoReply reply;
  RAY_RETURN_NOT_OK(context_->GetGcsRpcClient().SyncGetAllJobInfo(
      std::move(request), &reply, timeout_ms));
  job_data_list = VectorFromProtobuf(std::move(*reply.mutable_job_info_list()));
  return Status::OK();
}

void JobInfoAccessor::AsyncGetNextJobID(const ItemCallback<JobID> &callback) {
  RAY_LOG(DEBUG) << "Getting next job id";
  rpc::GetNextJobIDRequest request;
  context_->GetGcsRpcClient().GetNextJobID(
      std::move(request),
      [callback](const Status &status, rpc::GetNextJobIDReply &&reply) {
        RAY_CHECK_OK(status);
        auto job_id = JobID::FromInt(reply.job_id());
        RAY_LOG(DEBUG) << "Finished getting next job id = " << job_id;
        callback(std::move(job_id));
      });
}

}  // namespace gcs
}  // namespace ray
