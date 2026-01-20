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

#include "ray/core_worker/profile_event.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/time/clock.h"
#include "ray/observability/ray_task_profile_event.h"

namespace ray {
namespace core {

namespace worker {

ProfileEvent::ProfileEvent(TaskEventBuffer *task_event_buffer,
                           observability::RayEventRecorderInterface *ray_event_recorder,
                           WorkerContext &worker_context,
                           const std::string &node_ip_address,
                           const std::string &event_name,
                           const std::string &session_name,
                           const NodeID &node_id)
    : task_event_buffer_(task_event_buffer),
      ray_event_recorder_(ray_event_recorder),
      session_name_(session_name),
      node_id_(node_id) {
  const auto &task_spec = worker_context.GetCurrentTask();
  if (task_spec && !task_spec->EnableTaskEvents()) {
    event_ = nullptr;
    return;
  }

  if (worker_context.GetWorkerType() == rpc::WorkerType::DRIVER &&
      RayConfig::instance().task_events_skip_driver_for_test()) {
    return;
  }

  // Determine which path to use.
  use_ray_event_recorder_ = (ray_event_recorder_ != nullptr);

  if (use_ray_event_recorder_) {
    // Store data for RayEventRecorder path - will create RayTaskProfileEvent in
    // destructor.
    task_id_ = worker_context.GetCurrentTaskID();
    job_id_ = worker_context.GetCurrentJobID();
    attempt_number_ = task_spec == nullptr ? 0 : task_spec->AttemptNumber();
    component_type_ = WorkerTypeString(worker_context.GetWorkerType());
    component_id_ = worker_context.GetWorkerID().Binary();
    node_ip_address_ = node_ip_address;
    event_name_ = event_name;
    start_time_ = absl::GetCurrentTimeNanos();
  } else if (task_event_buffer_ != nullptr) {
    // Use existing TaskEventBuffer path.
    event_ = std::make_unique<TaskProfileEvent>(
        worker_context.GetCurrentTaskID(),
        worker_context.GetCurrentJobID(),
        task_spec == nullptr ? 0 : task_spec->AttemptNumber(),
        WorkerTypeString(worker_context.GetWorkerType()),
        worker_context.GetWorkerID().Binary(),
        node_ip_address,
        event_name,
        absl::GetCurrentTimeNanos(),
        task_event_buffer_->GetSessionName(),
        task_event_buffer_->GetNodeID());
  }
}

ProfileEvent::~ProfileEvent() {
  if (use_ray_event_recorder_ && ray_event_recorder_ != nullptr) {
    // Create RayTaskProfileEvent and send through recorder.
    int64_t end_time = absl::GetCurrentTimeNanos();
    auto profile_event =
        std::make_unique<observability::RayTaskProfileEvent>(task_id_,
                                                             attempt_number_,
                                                             job_id_,
                                                             node_id_,
                                                             component_type_,
                                                             component_id_,
                                                             node_ip_address_,
                                                             event_name_,
                                                             start_time_,
                                                             end_time,
                                                             session_name_);
    if (!extra_data_.empty()) {
      profile_event->SetExtraData(extra_data_);
    }
    std::vector<std::unique_ptr<observability::RayEventInterface>> events;
    events.push_back(std::move(profile_event));
    ray_event_recorder_->AddEvents(std::move(events));
  } else if (event_ != nullptr && task_event_buffer_ != nullptr) {
    // Use existing TaskEventBuffer path.
    event_->SetEndTime(absl::GetCurrentTimeNanos());
    task_event_buffer_->AddTaskEvent(std::move(event_));
  }
}

void ProfileEvent::SetExtraData(const std::string &extra_data) {
  if (use_ray_event_recorder_) {
    extra_data_ = extra_data;
  } else if (event_ != nullptr) {
    event_->SetExtraData(extra_data);
  }
}

}  // namespace worker

}  // namespace core
}  // namespace ray
