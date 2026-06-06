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

#include "ray/observability/ray_task_lifecycle_event.h"

#include <string>
#include <utility>

namespace ray {
namespace observability {

RayTaskLifecycleEvent::RayTaskLifecycleEvent(rpc::events::TaskLifecycleEvent data,
                                             const std::string &session_name)
    : RayEvent<rpc::events::TaskLifecycleEvent>(
          rpc::events::RayEvent::CORE_WORKER,
          rpc::events::RayEvent::TASK_LIFECYCLE_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  data_ = std::move(data);
}

std::string RayTaskLifecycleEvent::GetEntityId() const {
  return data_.task_id() + std::to_string(data_.task_attempt());
}

void RayTaskLifecycleEvent::MergeData(RayEvent<rpc::events::TaskLifecycleEvent> &&other) {
  auto &&other_event = static_cast<RayTaskLifecycleEvent &&>(other);
  // MergeFrom concatenates the repeated state_transitions (preserving chronological
  // order, since the recorder merges later events into the earlier accumulator) and
  // overlays the dynamic scalar/message fields (node_id, worker_id, error_info, etc.)
  // that individual lifecycle events set. This reproduces the per-attempt coalescing that
  // the legacy TaskEventBuffer flush did by repeatedly populating one proto.
  data_.MergeFrom(other_event.data_);
}

ray::rpc::events::RayEvent RayTaskLifecycleEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_task_lifecycle_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
