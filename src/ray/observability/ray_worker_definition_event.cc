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

#include "ray/observability/ray_worker_definition_event.h"

namespace ray {
namespace observability {

RayWorkerDefinitionEvent::RayWorkerDefinitionEvent(const rpc::WorkerTableData &data,
                                                   const std::string &session_name)
    : RayEvent<rpc::events::WorkerDefinitionEvent>(
          rpc::events::RayEvent::GCS,
          rpc::events::RayEvent::WORKER_DEFINITION_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  data_.set_worker_id(data.worker_address().worker_id());
  data_.set_node_id(data.worker_address().node_id());
  data_.set_worker_type(data.worker_type());
  data_.set_pid(data.pid());
}

std::string RayWorkerDefinitionEvent::GetEntityId() const { return data_.worker_id(); }

void RayWorkerDefinitionEvent::MergeData(
    RayEvent<rpc::events::WorkerDefinitionEvent> &&other) {
  // Definition events are static. Merging does not change the event.
  return;
}

ray::rpc::events::RayEvent RayWorkerDefinitionEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_worker_definition_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
