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

TaskEvent::TaskEvent(const TaskID &task_id, const JobID &job_id, int32_t attempt_number)
    : task_id_(task_id), job_id_(job_id), attempt_number_(attempt_number) {}

TaskStatusEvent::TaskStatusEvent(
    const TaskID &task_id,
    const JobID &job_id,
    int32_t attempt_number,
    const rpc::TaskStatus &task_status,
    int64_t timestamp,
    const std::shared_ptr<const TaskSpecification> &task_spec,
    const absl::optional<NodeID> &node_id,
    const absl::optional<WorkerID> &worker_id)
    : TaskEvent(task_id, job_id, attempt_number),
      task_status_(task_status),
      timestamp_(timestamp),
      task_spec_(task_spec),
      node_id_(node_id),
      worker_id_(worker_id) {}

TaskProfileEvent::TaskProfileEvent(const TaskID &task_id,
                                   const JobID &job_id,
                                   int32_t attempt_number,
                                   const std::string &component_type,
                                   const std::string &component_id,
                                   const std::string &node_ip_address,
                                   const std::string &event_name,
                                   int64_t start_time)
    : TaskEvent(task_id, job_id, attempt_number),
      component_type_(component_type),
      component_id_(component_id),
      node_ip_address_(node_ip_address),
      event_name_(event_name),
      start_time_(start_time) {}

void TaskStatusEvent::ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) {
  // Base fields
  rpc_task_events->set_task_id(task_id_.Binary());
  rpc_task_events->set_job_id(job_id_.Binary());
  rpc_task_events->set_attempt_number(attempt_number_);

  // Task info.
  if (task_spec_) {
    gcs::FillTaskInfo(rpc_task_events->mutable_task_info(), *task_spec_);
  }

  // Task status update.
  auto state_updates = rpc_task_events->mutable_state_updates();

  if (node_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "Node ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    state_updates->set_node_id(node_id_->Binary());
  }

  if (worker_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "Worker ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    state_updates->set_worker_id(worker_id_->Binary());
  }
  gcs::FillTaskStatusUpdateTime(task_status_, timestamp_, state_updates);
}

void TaskProfileEvent::ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) {
  // Base fields
  rpc_task_events->set_task_id(task_id_.Binary());
  rpc_task_events->set_job_id(job_id_.Binary());
  rpc_task_events->set_attempt_number(attempt_number_);

  // Profile data
  auto profile_events = rpc_task_events->mutable_profile_events();
  profile_events->set_component_type(std::move(component_type_));
  profile_events->set_component_id(std::move(component_id_));
  profile_events->set_node_ip_address(std::move(node_ip_address_));
  auto event_entry = profile_events->add_events();
  event_entry->set_event_name(std::move(event_name_));
  event_entry->set_start_time(start_time_);
  event_entry->set_end_time(end_time_);
  event_entry->set_extra_data(std::move(extra_data_));
}

TaskEventBufferImpl::TaskEventBufferImpl(std::unique_ptr<gcs::GcsClient> gcs_client)
    : work_guard_(boost::asio::make_work_guard(io_service_)),
      periodical_runner_(io_service_),
      gcs_client_(std::move(gcs_client)),
      buffer_(RayConfig::instance().task_events_max_buffer_size()) {}

Status TaskEventBufferImpl::Start(bool auto_flush) {
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  RAY_CHECK(report_interval_ms > 0)
      << "RAY_task_events_report_interval_ms should be > 0 to use TaskEventBuffer.";

  // Reporting to GCS, set up gcs client and and events flushing.
  {
    absl::MutexLock lock(&gcs_client_mutex_);
    auto status = gcs_client_->Connect(io_service_);
    if (!status.ok()) {
      RAY_LOG(ERROR)
          << "Failed to connect to GCS, TaskEventBuffer will stop now. [status="
          << status.ToString() << "].";

      enabled_ = false;
      return status;
    }
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
    absl::MutexLock lock(&gcs_client_mutex_);
    // It's now safe to disconnect the GCS client since it will not be used by any
    // callbacks.
    gcs_client_->Disconnect();
  }
}

bool TaskEventBufferImpl::Enabled() const { return enabled_; }

void TaskEventBufferImpl::AddTaskEvent(std::unique_ptr<TaskEvent> task_event) {
  if (!enabled_) {
    return;
  }

  TaskEvent *ptr = task_event.release();
  RAY_CHECK(ptr != nullptr);
  while (!buffer_.push(ptr)) {
    TaskEvent *to_evict = nullptr;
    if (buffer_.pop(to_evict)) {
      RAY_CHECK(to_evict != nullptr);
      num_task_events_stored_--;
      // Buffer full - try evicting.
      if (to_evict->IsProfileEvent()) {
        num_profile_task_events_dropped_++;
      } else {
        num_status_task_events_dropped_++;
      }

      delete to_evict;
    }
    // Try adding again.
  }
  num_task_events_stored_++;
}

void TaskEventBufferImpl::FlushEvents(bool forced) {
  if (!enabled_) {
    return;
  }
  size_t num_batch = RayConfig::instance().task_events_send_batch_size();

  // Skip if GCS hasn't finished processing the previous message.
  if (grpc_in_progress_ && !forced) {
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
        << "GCS hasn't replied to the previous flush events call (likely "
           "overloaded). "
           "Skipping reporting task state events and retry later.";
    return;
  }

  auto data = std::make_unique<rpc::TaskEventData>();
  // Add data to send.
  size_t num_profile_event_to_send = 0;
  size_t num_status_event_to_send = 0;
  size_t num_status_task_events_dropped = 0;
  size_t num_profile_task_events_dropped = 0;
  while (!buffer_.empty() &&
         static_cast<size_t>(data->events_by_task_size()) < num_batch) {
    TaskEvent *e = nullptr;
    if (buffer_.pop(e)) {
      RAY_CHECK(e != nullptr);
      num_task_events_stored_--;
      auto events_by_task = data->add_events_by_task();
      if (e->IsProfileEvent()) {
        num_profile_event_to_send++;
      } else {
        num_status_event_to_send++;
      }
      e->ToRpcTaskEvents(events_by_task);

      delete e;
    }
  }

  // Set counters.
  num_profile_task_events_dropped = num_profile_task_events_dropped_;
  num_profile_task_events_dropped_ = 0;
  num_status_task_events_dropped = num_status_task_events_dropped_;
  num_status_task_events_dropped_ = 0;

  data->set_num_profile_task_events_dropped(num_profile_task_events_dropped);
  data->set_num_status_task_events_dropped(num_status_task_events_dropped);

  size_t data_size = data->ByteSizeLong();
  stats_counters_.Increment(TaskEventBufferCounter::kTotalTaskEventsReported,
                            data->events_by_task_size());
  stats_counters_.Increment(TaskEventBufferCounter::kTotalTaskEventsBytesReported,
                            data_size);

  gcs::TaskInfoAccessor *task_accessor;
  {
    // Sending the protobuf to GCS.
    absl::MutexLock lock(&gcs_client_mutex_);
    // Some debug tracking.
    task_accessor = &gcs_client_->Tasks();
  }

  auto on_complete = [this](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to push task events to GCS. Data will be lost. [status="
                       << status.ToString() << "]";
    }
    grpc_in_progress_ = false;
  };

  // The flag should be unset when on_complete is invoked.
  grpc_in_progress_ = true;
  auto status = task_accessor->AsyncAddTaskEventData(std::move(data), on_complete);
  {
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

  grpc_in_progress = grpc_in_progress_;
  num_status_task_events_dropped = num_status_task_events_dropped_;
  num_profile_task_events_dropped = num_profile_task_events_dropped_;
  total_events_bytes =
      stats_counters_.Get(TaskEventBufferCounter::kTotalTaskEventsBytesReported);
  total_num_events =
      stats_counters_.Get(TaskEventBufferCounter::kTotalTaskEventsReported);
  data_buffer_size = num_task_events_stored_;

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
