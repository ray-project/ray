// Copyright 2022 The Ray Authors.
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

#include "ray/core_worker/task_event_buffer.h"

#include "ray/gcs/pb_util.h"

namespace ray {
namespace core {

namespace worker {

TaskEventBufferImpl::TaskEventBufferImpl(std::unique_ptr<gcs::GcsClient> gcs_client)
    : work_guard_(boost::asio::make_work_guard(io_service_)),
      periodical_runner_(io_service_),
      gcs_client_(std::move(gcs_client)),
      buffer_() {}

Status TaskEventBufferImpl::Start(bool auto_flush) {
  absl::MutexLock lock(&mutex_);
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  RAY_CHECK(report_interval_ms > 0)
      << "RAY_task_events_report_interval_ms should be > 0 to use TaskEventBuffer.";

  buffer_.set_capacity(
      {RayConfig::instance().task_events_max_num_task_events_in_buffer()});
  // Reporting to GCS, set up gcs client and and events flushing.
  auto status = gcs_client_->Connect(io_service_);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to connect to GCS, TaskEventBuffer will stop now. [status="
                   << status.ToString() << "].";

    enabled_ = false;
    return status;
  }

  enabled_ = true;

  io_thread_ = std::thread([this]() {
#ifndef _WIN32
    // Block SIGINT and SIGTERM so they will be handled by the main thread.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);
#endif
    SetThreadName("task_event_buffer.io");
    io_service_.run();
    RAY_LOG(INFO) << "Task event buffer io service stopped.";
  });

  if (!auto_flush) {
    return Status::OK();
  }

  RAY_LOG(INFO) << "Reporting task events to GCS every " << report_interval_ms << "ms.";
  periodical_runner_.RunFnPeriodically([this] { FlushEvents(/* forced */ false); },
                                       report_interval_ms,
                                       "CoreWorker.deadline_timer.flush_task_events");
  return Status::OK();
}

void TaskEventBufferImpl::Stop() {
  if (!enabled_) {
    return;
  }
  RAY_LOG(INFO) << "Shutting down TaskEventBuffer.";

  // Shutting down the io service to exit the io_thread. This should prevent
  // any other callbacks to be run on the io thread.
  io_service_.stop();
  if (io_thread_.joinable()) {
    RAY_LOG(DEBUG) << "Joining io thread from TaskEventBuffer";
    io_thread_.join();
  }

  {
    absl::MutexLock lock(&mutex_);
    // It's now safe to disconnect the GCS client since it will not be used by any
    // callbacks.
    gcs_client_->Disconnect();
  }
}

bool TaskEventBufferImpl::Enabled() const { return enabled_; }

void TaskEventBufferImpl::AddTaskEvent(TaskEvent task_event) {
  if (!enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);

  auto limit = RayConfig::instance().task_events_max_num_task_events_in_buffer();
  if (limit > 0 && buffer_.full()) {
    const auto &to_evict = buffer_.front();
    if (to_evict.profile_events.has_value()) {
      num_profile_task_events_dropped_++;
    } else {
      num_status_task_events_dropped_++;
    }
  }
  buffer_.push_back(std::move(task_event));
}

void TaskEventBufferImpl::MakeTaskInfo(rpc::TaskInfoEntry *task_info,
                                       const TaskSpecification &task_spec) {
  rpc::TaskType type;
  if (task_spec.IsNormalTask()) {
    type = rpc::TaskType::NORMAL_TASK;
  } else if (task_spec.IsDriverTask()) {
    type = rpc::TaskType::DRIVER_TASK;
  } else if (task_spec.IsActorCreationTask()) {
    type = rpc::TaskType::ACTOR_CREATION_TASK;
    task_info->set_actor_id(task_spec.ActorCreationId().Binary());
  } else {
    RAY_CHECK(task_spec.IsActorTask());
    type = rpc::TaskType::ACTOR_TASK;
    task_info->set_actor_id(task_spec.ActorId().Binary());
  }
  task_info->set_type(type);
  task_info->set_name(task_spec.GetName());
  task_info->set_language(task_spec.GetLanguage());
  task_info->set_func_or_class_name(task_spec.FunctionDescriptor()->CallString());
  // NOTE(rickyx): we will have scheduling states recorded in the events list.
  task_info->set_scheduling_state(rpc::TaskStatus::NIL);
  task_info->set_job_id(task_spec.JobId().Binary());

  task_info->set_task_id(task_spec.TaskId().Binary());
  // NOTE: we set the parent task id of a task to be submitter's task id, where
  // the submitter depends on the owner coreworker's:
  // - if the owner coreworker runs a normal task, the submitter's task id is the task id.
  // - if the owner coreworker runs an actor, the submitter's task id will be the actor's
  // creation task id.
  task_info->set_parent_task_id(task_spec.SubmitterTaskId().Binary());
  const auto &resources_map = task_spec.GetRequiredResources().GetResourceMap();
  task_info->mutable_required_resources()->insert(resources_map.begin(),
                                                  resources_map.end());
  task_info->mutable_runtime_env_info()->CopyFrom(task_spec.RuntimeEnvInfo());
  const auto &pg_id = task_spec.PlacementGroupBundleId().first;
  if (!pg_id.IsNil()) {
    task_info->set_placement_group_id(pg_id.Binary());
  }
}

void TaskEventBufferImpl::MakeRpcTaskEvents(rpc::TaskEvents *rpc_task_events,
                                            const TaskEvent &task_event) {
  const auto &status = task_event.task_status;
  // Common fields
  rpc_task_events->set_task_id(task_event.task_id.Binary());
  rpc_task_events->set_job_id(task_event.job_id.Binary());
  rpc_task_events->set_attempt_number(task_event.attempt_number);

  // Task info
  if (task_event.include_task_info || status == rpc::TaskStatus::PENDING_ARGS_AVAIL) {
    RAY_CHECK(task_event.task_spec != nullptr)
        << "include?" << task_event.include_task_info << ", status: " << status
        << ", task: " << task_event.task_id << " attempt: " << task_event.attempt_number;
    MakeTaskInfo(rpc_task_events->mutable_task_info(), *task_event.task_spec);
  }

  if (task_event.profile_events.has_value()) {
    // Profile events
    rpc_task_events->mutable_profile_events()->CopyFrom(
        task_event.profile_events.value());
  } else {
    // Task update
    auto state_updates = rpc_task_events->mutable_state_updates();
    if (task_event.node_id.has_value()) {
      RAY_CHECK(status == rpc::TaskStatus::SUBMITTED_TO_WORKER)
          << "Node ID should be included when task status changes to "
             "SUBMITTED_TO_WORKER.";
      state_updates->set_node_id(task_event.node_id->Binary());
    }
    if (task_event.worker_id.has_value()) {
      RAY_CHECK(status == rpc::TaskStatus::SUBMITTED_TO_WORKER)
          << "Worker ID should be included when task status changes to "
             "SUBMITTED_TO_WORKER.";
      state_updates->set_worker_id(task_event.worker_id->Binary());
    }
    gcs::FillTaskStatusUpdateTime(status, task_event.timestamp, state_updates);
  }
}

void TaskEventBufferImpl::FlushEvents(bool forced) {
  if (!enabled_) {
    return;
  }
  size_t num_status_task_events_dropped = 0;
  size_t num_profile_task_events_dropped = 0;
  std::vector<TaskEvent> to_send;

  {
    absl::MutexLock lock(&mutex_);

    // Skip if GCS hasn't finished processing the previous message.
    if (grpc_in_progress_ && !forced) {
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
          << "GCS hasn't replied to the previous flush events call (likely "
             "overloaded). "
             "Skipping reporting task state events and retry later."
          << "[cur_buffer_size=" << buffer_.size() << "].";
      return;
    }

    // No data to send.
    if (buffer_.empty()) {
      return;
    }

    size_t num_to_send =
        std::min(static_cast<size_t>(RayConfig::instance().task_events_send_batch_size()),
                 static_cast<size_t>(buffer_.size()));
    to_send.insert(to_send.end(), buffer_.begin(), buffer_.begin() + num_to_send);
    buffer_.erase(buffer_.begin(), buffer_.begin() + num_to_send);

    // Send and reset the counters
    num_profile_task_events_dropped = num_profile_task_events_dropped_;
    num_profile_task_events_dropped_ = 0;

    num_status_task_events_dropped = num_status_task_events_dropped_;
    num_status_task_events_dropped_ = 0;
  }

  // Convert to rpc::TaskEventsData
  auto data = std::make_unique<rpc::TaskEventData>();
  data->set_num_profile_task_events_dropped(num_profile_task_events_dropped);
  data->set_num_status_task_events_dropped(num_status_task_events_dropped);

  size_t num_task_events = to_send.size();
  size_t num_profile_event_to_send = 0;
  size_t num_status_event_to_send = 0;
  for (const auto &task_event : to_send) {
    auto events_by_task = data->add_events_by_task();
    if (task_event.profile_events.has_value()) {
      num_profile_event_to_send++;
    } else {
      num_status_event_to_send++;
    }
    MakeRpcTaskEvents(events_by_task, task_event);
  }

  gcs::TaskInfoAccessor *task_accessor;
  {
    // Sending the protobuf to GCS.
    absl::MutexLock lock(&mutex_);
    // Some debug tracking.
    total_num_events_ += num_task_events;
    total_events_bytes_ += data->ByteSizeLong();
    // The flag should be unset when on_complete is invoked.
    grpc_in_progress_ = true;
    task_accessor = &gcs_client_->Tasks();
  }

  auto on_complete = [this, num_task_events](const Status &status) {
    absl::MutexLock lock(&mutex_);
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to push " << num_task_events
                       << " task state events to GCS. Data will be lost. [status="
                       << status.ToString() << "]";
    }
    grpc_in_progress_ = false;
  };

  auto status = task_accessor->AsyncAddTaskEventData(std::move(data), on_complete);
  {
    absl::MutexLock lock(&mutex_);
    if (!status.ok()) {
      // If we couldn't even send the data by invoking client side callbacks, there's
      // something seriously wrong, and losing data in this case should not be too
      // worse. So we will silently drop these task events.
      RAY_LOG(WARNING)
          << "Failed to push task state events to GCS. Data will be lost. [status="
          << status.ToString() << "]";
      grpc_in_progress_ = false;

      // Fail to send, currently dropping events.
      num_status_task_events_dropped_ += num_status_event_to_send;
      num_profile_task_events_dropped_ += num_profile_event_to_send;
    }
  }
}

const std::string TaskEventBufferImpl::DebugString() {
  std::stringstream ss;

  if (!Enabled()) {
    ss << "Task Event Buffer is disabled.";
    return ss.str();
  }

  bool grpc_in_progress;
  size_t num_status_task_events_dropped, num_profile_task_events_dropped,
      data_buffer_size;
  uint64_t total_events_bytes, total_num_events;

  {
    absl::MutexLock lock(&mutex_);
    grpc_in_progress = grpc_in_progress_;
    num_status_task_events_dropped = num_status_task_events_dropped_;
    num_profile_task_events_dropped = num_profile_task_events_dropped_;
    total_events_bytes = total_events_bytes_;
    total_num_events = total_num_events_;
    data_buffer_size = buffer_.size();
  }

  ss << "\nIO Service Stats:\n";
  ss << io_service_.stats().StatsString();
  ss << "\nOther Stats:"
     << "\n\tgrpc_in_progress:" << grpc_in_progress
     << "\n\tcurrent number of task events in buffer: " << data_buffer_size
     << "\n\ttotal task events sent: " << 1.0 * total_events_bytes / 1024 / 1024 << " MiB"
     << "\n\ttotal number of task events sent: " << total_num_events
     << "\n\tnum status task events dropped: " << num_status_task_events_dropped
     << "\n\tnum profile task events dropped: " << num_profile_task_events_dropped
     << "\n";

  return ss.str();
}

}  // namespace worker

}  // namespace core
}  // namespace ray
