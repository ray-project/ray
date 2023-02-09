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
  rpc_profile_event_.set_job_id(worker_context.GetCurrentJobID().Binary());
  rpc_profile_event_.set_task_id(worker_context.GetCurrentTaskID().Binary());
  auto task_spec = worker_context.GetCurrentTask();
  rpc_profile_event_.set_attempt_number(
      task_spec == nullptr ? 0 : task_spec->AttemptNumber());

  auto profile_events = rpc_profile_event_.mutable_profile_events();
  profile_events->set_component_type(WorkerTypeString(worker_context.GetWorkerType()));
  profile_events->set_component_id(worker_context.GetWorkerID().Binary());
  profile_events->set_node_ip_address(node_ip_address);
  auto event_entry = profile_events->add_events();
  event_entry->set_event_name(event_name);

  event_entry->set_start_time(absl::GetCurrentTimeNanos());
}

ProfileEvent::~ProfileEvent() {
  auto &event = rpc_profile_event_.mutable_profile_events()->mutable_events()->at(0);
  event.set_end_time(absl::GetCurrentTimeNanos());
  // Add task event to the task event buffer
  task_event_buffer_.AddTaskEvent(std::move(rpc_profile_event_));
}

void ProfileEvent::SetExtraData(const std::string &extra_data) {
  auto &event = rpc_profile_event_.mutable_profile_events()->mutable_events()->at(0);
  event.set_extra_data(extra_data);
}

}  // namespace worker

}  // namespace core
}  // namespace ray
