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

namespace ray {
namespace core {

namespace worker {

TaskEventBufferImpl::TaskEventBufferImpl(std::unique_ptr<gcs::GcsClient> gcs_client,
                                         bool manual_flush)
    : work_guard_(boost::asio::make_work_guard(io_service_)),
      periodical_runner_(io_service_),
      gcs_client_(std::move(gcs_client)) {
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  if (report_interval_ms <= 0) {
    gcs_client_.reset();
    return;
  }
  recording_on_ = true;

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

  // Reporting to GCS, set up gcs client and and events flushing.
  auto status = gcs_client_->Connect(io_service_);
  if (!status.ok()) {
    RAY_LOG(ERROR)
        << "Failed to connect to GCS, TaskEventBuffer will not report to GCS. [status="
        << status.ToString() << "].";

    recording_on_ = false;

    // Early clean up resources.
    gcs_client_.reset();
    Stop();
    return;
  }

  if (manual_flush) {
    return;
  }

  RAY_LOG(INFO) << "Reporting task events to GCS every " << report_interval_ms << "ms.";
  periodical_runner_.RunFnPeriodically([this] { FlushEvents(/* forced */ false); },
                                       report_interval_ms,
                                       "CoreWorker.deadline_timer.flush_task_events");
}

void TaskEventBufferImpl::Stop() {
  RAY_LOG(INFO) << "Shutting down TaskEventBuffer.";
  {
    absl::MutexLock lock(&mutex_);
    if (gcs_client_) {
      // Stop GCS client
      gcs_client_->Disconnect();
    }

    io_service_.stop();
  }

  if (io_thread_.joinable()) {
    RAY_LOG(DEBUG) << "Joining io thread from TaskEventBuffer";
    io_thread_.join();
  }

  if (gcs_client_) {
    gcs_client_.reset();
  }
}

void TaskEventBufferImpl::AddTaskStatusEvent(
    TaskID task_id,
    rpc::TaskStatus task_status,
    std::unique_ptr<rpc::TaskInfoEntry> task_info,
    std::unique_ptr<rpc::TaskStateEntry> task_state_update) {
  if (!recording_on_) {
    return;
  }
  absl::MutexLock lock(&mutex_);

  // TODO(rickyx): What if too many in local buffer? Trigger force flush or drop?
  auto events_task = task_events_data_.add_events_by_task();
  events_task->set_task_id(task_id.Binary());

  auto status_events = events_task->mutable_status_events();

  if (task_state_update) {
    status_events->mutable_task_state()->Swap(task_state_update.get());
  }

  if (task_info) {
    status_events->mutable_task_info()->Swap(task_info.get());
  }

  // Add the event.
  auto event = status_events->add_events();
  event->set_task_status(task_status);
  event->set_start_time(absl::GetCurrentTimeNanos());
}

void TaskEventBufferImpl::AddProfileEvent(TaskID task_id,
                                          rpc::ProfileEventEntry event,
                                          const std::string &component_type,
                                          const std::string &component_id,
                                          const std::string &node_ip_address) {
  if (!recording_on_) {
    return;
  }
  absl::MutexLock lock(&mutex_);

  auto events_task = task_events_data_.add_events_by_task();

  events_task->set_task_id(task_id.Binary());
  auto profile_events = events_task->mutable_profile_events();
  profile_events->set_component_type(component_type);
  profile_events->set_component_id(component_id);
  profile_events->set_node_ip_address(node_ip_address);
  profile_events->add_events()->Swap(&event);
}

void TaskEventBufferImpl::FlushEvents(bool forced) {
  RAY_CHECK(recording_on_) << "Task state events reporting should have be on. Set "
                              "RAY_task_events_report_interval_ms > 0 to turn on";
  std::unique_ptr<rpc::TaskEventData> cur_task_events_data =
      std::make_unique<rpc::TaskEventData>();
  {
    absl::MutexLock lock(&mutex_);
    RAY_LOG_EVERY_MS(INFO, 30000)
        << "Pushed task state events to GCS. [total_bytes="
        << (1.0 * total_events_bytes_) / 1024 / 1024
        << "MiB][total_count=" << total_num_events_ << "]."
        << "Task event buffer currently has " << task_events_data_.events_by_task_size()
        << " tasks' events (" << 1.0 * task_events_data_.ByteSizeLong() / 1024 / 1024
        << "MiB).";

    // Skip if GCS hasn't finished processing the previous message.
    if (grpc_in_progress_ && !forced) {
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
          << "GCS hasn't replied to the previous flush events call (likely overloaded). "
             "Skipping reporting task state events and retry later. Pending "
          << task_events_data_.events_by_task_size() << "tasks' events ("
          << 1.0 * task_events_data_.ByteSizeLong() / 1024 / 1024
          << "MiB)stored in buffer. ";
      return;
    }

    if (task_events_data_.events_by_task_size() == 0) {
      return;
    }

    task_events_data_.Swap(cur_task_events_data.get());
  }
  // mutex released. Below operations should be lock-free.

  // Some debug tracking.
  auto num_events = cur_task_events_data->events_by_task_size();
  RAY_CHECK(num_events > 0) << "There should be some task events to be pushed.";

  total_num_events_ += num_events;
  total_events_bytes_ += cur_task_events_data->ByteSizeLong();

  auto on_complete = [this, num_events](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to push " << num_events
                       << " task state events to GCS. Data will be lost. [status="
                       << status.ToString() << "]";
    } else {
      RAY_LOG(DEBUG) << "Push " << num_events << " task state events to GCS.";
    }
    grpc_in_progress_ = false;
  };

  // The flag should be unset when on_complete is invoked.
  grpc_in_progress_ = true;
  auto status = gcs_client_->Tasks().AsyncAddTaskEventData(
      std::move(cur_task_events_data), on_complete);
  if (!status.ok()) {
    // If we couldn't even send the data by invoking client side callbacks, there's
    // something seriously wrong, and losing data in this case should not be too
    // worse. So we will silently drop these task events.
    RAY_LOG(WARNING)
        << "Failed to push task state events to GCS. Data will be lost. [status="
        << status.ToString() << "]";
    grpc_in_progress_ = false;
  }
}

}  // namespace worker

}  // namespace core
}  // namespace ray