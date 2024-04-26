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

#include "absl/time/clock.h"

namespace ray {
namespace core {

namespace worker {

ProfileEvent::ProfileEvent(TaskEventBuffer &task_event_buffer,
                           WorkerContext &worker_context,
                           const std::string &node_ip_address,
                           const std::string &event_name)
    : task_event_buffer_(task_event_buffer) {
  const auto &task_spec = worker_context.GetCurrentTask();
  if (task_spec && !task_spec->EnableTaskEvents()) {
    event_ = nullptr;
    return;
  }

  if (worker_context.GetWorkerType() == rpc::WorkerType::DRIVER &&
      RayConfig::instance().task_events_skip_driver_for_test()) {
    return;
  }
  event_.reset(new TaskProfileEvent(worker_context.GetCurrentTaskID(),
                                    worker_context.GetCurrentJobID(),
                                    task_spec == nullptr ? 0 : task_spec->AttemptNumber(),
                                    WorkerTypeString(worker_context.GetWorkerType()),
                                    worker_context.GetWorkerID().Binary(),
                                    node_ip_address,
                                    event_name,
                                    absl::GetCurrentTimeNanos()));
}

ProfileEvent::~ProfileEvent() {
  if (event_ == nullptr) {
    return;
  }
  event_->SetEndTime(absl::GetCurrentTimeNanos());
  // Add task event to the task event buffer
  task_event_buffer_.AddTaskEvent(std::move(event_));
}

void ProfileEvent::SetExtraData(const std::string &extra_data) {
  if (event_ == nullptr) {
    return;
  }
  event_->SetExtraData(extra_data);
}

}  // namespace worker

}  // namespace core
}  // namespace ray
