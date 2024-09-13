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
    cached_job_configs_[job_id] =
        std::make_shared<rpc::JobConfig>(job_table_data.config());
    function_manager_.AddJobReference(job_id);
  }
}

void GcsJobManager::WriteDriverJobExportEvent(rpc::JobTableData job_data) const {
  /// Write job_data as a export driver job event if
  /// enable_export_api_write() is enabled and if this job is
  /// not in the _ray_internal_ namespace.
  if (!RayConfig::instance().enable_export_api_write()) {
    return;
  }
  if (job_data.config().ray_namespace().find(kRayInternalNamespacePrefix) == 0) {
    // Namespace of this job starts with _ray_internal_ so
    // don't write export event.
    return;
  }
  std::shared_ptr<rpc::ExportDriverJobEventData> export_driver_job_data_ptr =
      std::make_shared<rpc::ExportDriverJobEventData>();
  export_driver_job_data_ptr->set_job_id(job_data.job_id());
  export_driver_job_data_ptr->set_is_dead(job_data.is_dead());
  export_driver_job_data_ptr->set_driver_pid(job_data.driver_pid());
  export_driver_job_data_ptr->set_start_time(job_data.start_time());
  export_driver_job_data_ptr->set_end_time(job_data.end_time());
  export_driver_job_data_ptr->set_entrypoint(job_data.entrypoint());
  export_driver_job_data_ptr->set_driver_ip_address(
      job_data.driver_address().ip_address());
  export_driver_job_data_ptr->mutable_config()->mutable_metadata()->insert(
      job_data.config().metadata().begin(), job_data.config().metadata().end());

  auto export_runtime_env_info =
      export_driver_job_data_ptr->mutable_config()->mutable_runtime_env_info();
  export_runtime_env_info->set_serialized_runtime_env(
      job_data.config().runtime_env_info().serialized_runtime_env());
  auto export_runtime_env_uris = export_runtime_env_info->mutable_uris();
  export_runtime_env_uris->set_working_dir_uri(
      job_data.config().runtime_env_info().uris().working_dir_uri());
  export_runtime_env_uris->mutable_py_modules_uris()->CopyFrom(
      job_data.config().runtime_env_info().uris().py_modules_uris());
  auto export_runtime_env_config = export_runtime_env_info->mutable_runtime_env_config();
  export_runtime_env_config->set_setup_timeout_seconds(
      job_data.config().runtime_env_info().runtime_env_config().setup_timeout_seconds());
  export_runtime_env_config->set_eager_install(
      job_data.config().runtime_env_info().runtime_env_config().eager_install());
  export_runtime_env_config->mutable_log_files()->CopyFrom(
      job_data.config().runtime_env_info().runtime_env_config().log_files());

  RayExportEvent(export_driver_job_data_ptr).SendEvent();
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
    WriteDriverJobExportEvent(mutable_job_table_data);
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
    WriteDriverJobExportEvent(job_table_data);
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
                                 const std::optional<rpc::JobTableData> &result) {
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
  // Get all job info. This is a complex operation:
  // 1. One GetAll from the job table.
  // 2. For each job, Send RPC to core worker for is_running_tasks value.
  // 3. One MultiKVGet for jobs submitted via the Ray Job API.
  // Step 2 and 3 are asynchronous and concurrent among jobs. After all jobs are
  // processed, send the reply.
  //
  // We support filtering by job_id or job_submission_id. job_id is easy to handle: just
  // only get job info by the id. job_submission_id however is not indexed. So we have
  // to get all job info and filter by job_submission_id.
  RAY_LOG(DEBUG) << "Getting all job info.";

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
    RAY_LOG(DEBUG) << "Getting job info with limit " << limit << ".";
  }

  std::optional<std::string> job_or_submission_id;
  if (request.has_job_or_submission_id()) {
    job_or_submission_id = request.job_or_submission_id();
  }

  auto filter_ok = [job_or_submission_id](
                       const ray::JobID &job_id,
                       std::optional<std::string_view> job_submission_id) {
    if (!job_or_submission_id.has_value()) {
      return true;
    }
    if (job_id.Hex() == *job_or_submission_id) {
      return true;
    }
    if (job_submission_id.has_value() &&
        (job_submission_id.value() == *job_or_submission_id)) {
      return true;
    }
    return false;
  };
  auto on_done = [this, filter_ok, request, reply, send_reply_callback, limit](
                     const absl::flat_hash_map<JobID, JobTableData> &&result) {
    // Internal KV keys for jobs that were submitted via the Ray Job API.
    std::vector<std::string> job_api_data_keys;

    // Maps a Job API data key to the indices of the corresponding jobs in the table. Note
    // that multiple jobs can come from the same Ray Job API submission (e.g. if the
    // entrypoint script calls ray.init() multiple times).
    std::unordered_map<std::string, std::vector<int>> job_data_key_to_indices;

    // Create a shared counter for the number of jobs processed
    std::shared_ptr<int> num_processed_jobs = std::make_shared<int>(0);

    // Create a shared boolean flag for the internal KV callback completion
    std::shared_ptr<bool> kv_callback_done = std::make_shared<bool>(false);

    // Function to send the reply once all jobs have been processed and KV callback
    // completed
    auto try_send_reply =
        [num_processed_jobs, kv_callback_done, reply, send_reply_callback]() {
          if (*num_processed_jobs == reply->job_info_list_size() && *kv_callback_done) {
            RAY_LOG(DEBUG) << "Finished getting all job info.";
            GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
          }
        };

    // Load the job table data into the reply.
    int i = 0;
    for (auto &data : result) {
      if (i >= limit) {
        break;
      }

      auto &metadata = data.second.config().metadata();
      auto iter = metadata.find("job_submission_id");
      if (!filter_ok(data.first,
                     iter == metadata.end()
                         ? std::nullopt
                         : std::optional<std::string_view>(iter->second))) {
        continue;
      }
      reply->add_job_info_list()->CopyFrom(data.second);
      if (iter != metadata.end()) {
        // This job was submitted via the Ray Job API, so it has JobInfo in the kv.
        std::string job_submission_id = iter->second;
        std::string job_data_key = JobDataKey(job_submission_id);
        job_api_data_keys.push_back(job_data_key);
        job_data_key_to_indices[job_data_key].push_back(i);
      }

      if (!request.skip_is_running_tasks_field()) {
        JobID job_id = data.first;
        WorkerID worker_id =
            WorkerID::FromBinary(data.second.driver_address().worker_id());

        // If job is not dead, get is_running_tasks from the core worker for the driver.
        if (data.second.is_dead()) {
          reply->mutable_job_info_list(i)->set_is_running_tasks(false);
          core_worker_clients_.Disconnect(worker_id);
          (*num_processed_jobs)++;
          try_send_reply();
        } else {
          // Get is_running_tasks from the core worker for the driver.
          auto client = core_worker_clients_.GetOrConnect(data.second.driver_address());
          auto request = std::make_unique<rpc::NumPendingTasksRequest>();
          constexpr int64_t kNumPendingTasksRequestTimeoutMs = 1000;
          RAY_LOG(DEBUG) << "Send NumPendingTasksRequest to worker " << worker_id
                         << ", timeout " << kNumPendingTasksRequestTimeoutMs << " ms.";
          client->NumPendingTasks(
              std::move(request),
              [job_id, worker_id, reply, i, num_processed_jobs, try_send_reply](
                  const Status &status,
                  const rpc::NumPendingTasksReply &num_pending_tasks_reply) {
                RAY_LOG(DEBUG).WithField(worker_id)
                    << "Received NumPendingTasksReply from worker.";
                if (!status.ok()) {
                  RAY_LOG(WARNING).WithField(job_id).WithField(worker_id)
                      << "Failed to get num_pending_tasks from core worker: " << status
                      << ", is_running_tasks is unset.";
                  reply->mutable_job_info_list(i)->clear_is_running_tasks();
                } else {
                  bool is_running_tasks = num_pending_tasks_reply.num_pending_tasks() > 0;
                  reply->mutable_job_info_list(i)->set_is_running_tasks(is_running_tasks);
                }
                (*num_processed_jobs)++;
                try_send_reply();
              },
              kNumPendingTasksRequestTimeoutMs);
        }
      } else {
        (*num_processed_jobs)++;
        try_send_reply();
      }
      i++;
    }

    if (!request.skip_submission_job_info_field()) {
      // Load the JobInfo for jobs submitted via the Ray Job API.
      auto kv_multi_get_callback =
          [reply,
           send_reply_callback,
           job_data_key_to_indices,
           kv_callback_done,
           try_send_reply](std::unordered_map<std::string, std::string> &&result) {
            for (const auto &data : result) {
              const std::string &job_data_key = data.first;
              // The JobInfo stored by the Ray Job API.
              const std::string &job_info_json = data.second;
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
                // Add the JobInfo to the correct indices in the reply.
                for (int i : job_data_key_to_indices.at(job_data_key)) {
                  reply->mutable_job_info_list(i)->mutable_job_info()->CopyFrom(
                      jobs_api_info);
                }
              }
            }
            *kv_callback_done = true;
            try_send_reply();
          };
      internal_kv_.MultiGet("job", job_api_data_keys, kv_multi_get_callback);
    } else {
      *kv_callback_done = true;
      try_send_reply();
    }
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

void GcsJobManager::OnNodeDead(const NodeID &node_id) {
  RAY_LOG(INFO) << "Node " << node_id
                << " failed, mark all jobs from this node as finished";

  auto on_done = [this, node_id](const absl::flat_hash_map<JobID, JobTableData> &result) {
    // If job is not dead and from driver in current node, then mark it as finished
    for (auto &data : result) {
      if (!data.second.is_dead() &&
          NodeID::FromBinary(data.second.driver_address().raylet_id()) == node_id) {
        RAY_LOG(DEBUG) << "Marking job: " << data.first << " as finished";
        MarkJobAsFinished(data.second, [data](Status status) {
          if (!status.ok()) {
            RAY_LOG(WARNING) << "Failed to mark job as finished. Status: " << status;
          }
        });
      }
    }
  };

  // make all jobs in current node to finished
  RAY_CHECK_OK(gcs_table_storage_->JobTable().GetAll(on_done));
}

}  // namespace gcs
}  // namespace ray
