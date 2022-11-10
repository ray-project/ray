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

#include "ray/core_worker/task_state_buffer.h"

namespace ray {
namespace core {

namespace worker {

TaskStateBuffer::TaskStateBuffer(instrumented_io_context &io_service,
                                 const std::shared_ptr<gcs::GcsClient> &gcs_client)
    : io_service_(io_service), periodical_runner_(io_service_), gcs_client_(gcs_client) {
  periodical_runner_.RunFnPeriodically([this] { FlushEvents(); },
                                       1000,  // TODO(rickyx) use a flag
                                       "CoreWorker.deadline_timer.flush_task_events");
}

void TaskStateBuffer::AddTaskEvent(TaskID task_id,
                                   rpc::TaskInfoEntry &&task_info_update,
                                   rpc::TaskStatus task_status) {
  absl::MutexLock lock(&mutex_);

  RAY_LOG(INFO) << "Adding task event. [task_id=" << task_id.Hex()
                << "][task_status=" << task_status << "]";
  auto task_events_itr = GetOrInitTaskEvents(task_id);

  // Apply any change in the task info
  task_events_itr->second.mutable_task_info()->MergeFrom(task_info_update);

  // Add the event.
  auto event = task_events_itr->second.add_task_events();
  event->set_task_status(task_status);
  event->set_event_time(absl::GetCurrentTimeNanos());
}

TaskIdEventMap::iterator TaskStateBuffer::GetOrInitTaskEvents(TaskID task_id) {
  // TODO: handle cases for too many events
  auto task_events_itr = task_events_map_.find(task_id);

  // Existing task state events entry in the buffer, return it.
  if (task_events_itr != task_events_map_.end()) {
    return task_events_itr;
  }

  // No existing entry, init a new TaskStateEvents.
  rpc::TaskStateEvents task_state_events;

  // Set id for this task
  task_state_events.set_task_id(task_id.Binary());

  // Add the newly task events to the buffer
  task_events_map_.emplace(task_id, std::move(task_state_events));
  return task_events_map_.find(task_id);
}

std::unique_ptr<rpc::TaskStateEventData> TaskStateBuffer::GetAndResetBuffer() {
  auto task_state_data = std::make_unique<rpc::TaskStateEventData>();

  // Fill up metadata
  task_state_data->set_component_type(component_type_);
  task_state_data->set_component_id(component_id_);
  task_state_data->set_node_id(node_id_.Binary());

  // Fill up the task events
  for (auto &[task_id, task_events] : task_events_map_) {
    auto events_of_task = task_state_data->add_events_by_task();
    *events_of_task = std::move(task_events);
  }

  // Clear the buffer
  task_events_map_.clear();

  return task_state_data;
}

void TaskStateBuffer::FlushEvents() {
  absl::MutexLock lock(&mutex_);
  // TODO: Respond to back pressure
  // TODO: handle RPC failure
  auto on_complete = [this](const Status &status) {
    RAY_LOG(INFO) << "Pushed ok";
    return;
  };

  if (!task_events_map_.empty()) {
    auto cur_task_state_data = GetAndResetBuffer();
    auto status = gcs_client_->Tasks().AsyncAddTaskStateEventData(
        std::move(cur_task_state_data), on_complete);
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to push task state events to GCS.";
    }
  }
}

}  // namespace worker

}  // namespace core
}  // namespace ray