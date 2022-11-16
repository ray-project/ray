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

TaskEventBuffer::TaskEventBuffer(instrumented_io_context &io_service,
                                 const std::shared_ptr<gcs::GcsClient> &gcs_client)
    : io_service_(io_service), periodical_runner_(io_service_), gcs_client_(gcs_client) {
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  if (report_interval_ms >= 0) {
    recording_on_ = true;
    RAY_LOG(INFO) << "Reporting task state events to GCS every: " << report_interval_ms
                  << "ms";
    periodical_runner_.RunFnPeriodically([this] { FlushEvents(/* forced */ false); },
                                         report_interval_ms,
                                         "CoreWorker.deadline_timer.flush_task_events");
  } else {
    recording_on_ = false;
  }
}

void TaskEventBuffer::AddTaskStatusEvent(
    TaskID task_id,
    rpc::TaskStatus task_status,
    std::unique_ptr<rpc::TaskInfoEntry> task_info,
    std::unique_ptr<rpc::TaskStateEntry> task_state_update) {
  if (!recording_on_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  RAY_LOG_EVERY_MS(INFO, 30000)
      << "Task event buffer currently has " << task_events_data_.events_by_task_size()
      << " tasks' events (" << 1.0 * task_events_data_.ByteSizeLong() / 1024 / 1024
      << "MiB).";

  // TODO(rickyx): What if too many in local buffer? Trigger force flush or drop?
  auto events_task = task_events_data_.add_events_by_task();
  events_task->set_task_id(task_id.Binary());
  if (task_state_update) {
    events_task->mutable_task_state()->Swap(task_state_update.get());
  }

  if (task_info) {
    events_task->mutable_task_info()->Swap(task_info.get());
  }

  // Add the event.
  auto event = events_task->add_task_events();
  event->set_event_type(rpc::TaskEventType::STATUS_EVENT);
  event->set_task_status(task_status);
  event->set_start_time(absl::GetCurrentTimeNanos());
}

void TaskEventBuffer::AddTaskEvent(TaskID task_id, rpc::TaskEventEntry event) {
  if (!recording_on_) {
    return;
  }
  absl::MutexLock lock(&mutex_);

  auto events_task = task_events_data_.add_events_by_task();

  events_task->set_task_id(task_id.Binary());
  events_task->add_task_events()->Swap(&event);
}

void TaskEventBuffer::FlushEvents(bool forced) {
  RAY_CHECK(recording_on_) << "Task state events recording should have be on. Set "
                              "RAY_task_events_report_interval_ms > 0 to turn on";
  std::unique_ptr<rpc::TaskEventData> cur_task_events_data =
      std::make_unique<rpc::TaskEventData>();
  RAY_LOG_EVERY_MS(INFO, 30000) << "Pushed task state events to GCS. [total_bytes="
                                << (1.0 * total_events_bytes_) / 1024 / 1024
                                << "MiB][total_count=" << total_num_events_ << "]";
  {
    absl::MutexLock lock(&mutex_);

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