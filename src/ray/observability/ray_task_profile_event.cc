// Copyright 2025 The Ray Authors.
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

#include <string>
#include <utility>

namespace ray {
namespace observability {

RayTaskProfileEvent::RayTaskProfileEvent(rpc::events::TaskProfileEvents data,
                                         const std::string &session_name)
    : RayEvent<rpc::events::TaskProfileEvents>(rpc::events::RayEvent::CORE_WORKER,
                                               rpc::events::RayEvent::TASK_PROFILE_EVENT,
                                               rpc::events::RayEvent::INFO,
                                               "",
                                               session_name) {
  data_ = std::move(data);
}

std::string RayTaskProfileEvent::GetEntityId() const {
  return data_.task_id() + std::to_string(data_.attempt_number());
}

TaskAttemptId RayTaskProfileEvent::GetTaskAttempt() const {
  return {data_.task_id(), data_.attempt_number()};
}

void RayTaskProfileEvent::MergeData(RayEvent<rpc::events::TaskProfileEvents> &&other) {
  auto &&other_event = static_cast<RayTaskProfileEvent &&>(other);
  // Concatenate the profiling span entries; component/task identifiers are identical for
  // the same attempt so MergeFrom's scalar overlay is a no-op for them.
  data_.MergeFrom(other_event.data_);
}

ray::rpc::events::RayEvent RayTaskProfileEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_task_profile_events()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
