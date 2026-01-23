// Copyright 2026 The Ray Authors.
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

#include "ray/observability/ray_task_profile_event.h"

namespace ray {
namespace observability {

RayTaskProfileEvent::RayTaskProfileEvent(const TaskID &task_id,
                                         int32_t attempt_number,
                                         const JobID &job_id,
                                         const NodeID &node_id,
                                         const std::string &component_type,
                                         const std::string &component_id,
                                         const std::string &node_ip_address,
                                         const std::string &event_name,
                                         int64_t start_time_ns,
                                         int64_t end_time_ns,
                                         const std::string &session_name)
    : RayEvent<rpc::events::TaskProfileEvents>(rpc::events::RayEvent::CORE_WORKER,
                                               rpc::events::RayEvent::TASK_PROFILE_EVENT,
                                               rpc::events::RayEvent::INFO,
                                               "",
                                               session_name) {
  // Task identifier fields
  data_.set_task_id(task_id.Binary());
  data_.set_job_id(job_id.Binary());
  data_.set_attempt_number(attempt_number);

  // Profile events container
  auto profile_events = data_.mutable_profile_events();
  profile_events->set_component_type(component_type);
  profile_events->set_component_id(component_id);
  profile_events->set_node_ip_address(node_ip_address);

  // Individual profile event entry
  auto event_entry = profile_events->add_events();
  event_entry->set_event_name(event_name);
  event_entry->set_start_time(start_time_ns);
  event_entry->set_end_time(end_time_ns);
}

std::string RayTaskProfileEvent::GetEntityId() const {
  return data_.task_id() + "_" + std::to_string(data_.attempt_number());
}

void RayTaskProfileEvent::MergeData(RayEvent<rpc::events::TaskProfileEvents> &&other) {
  // Profile events are not merged - keep only the first one
}

ray::rpc::events::RayEvent RayTaskProfileEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_task_profile_events()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
