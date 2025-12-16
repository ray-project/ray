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

#include "ray/observability/ray_node_definition_event.h"

namespace ray {
namespace observability {

RayNodeDefinitionEvent::RayNodeDefinitionEvent(const rpc::GcsNodeInfo &data,
                                               const std::string &session_name)
    : RayEvent<rpc::events::NodeDefinitionEvent>(
          rpc::events::RayEvent::GCS,
          rpc::events::RayEvent::NODE_DEFINITION_EVENT,
          rpc::events::RayEvent::INFO,
          "",
          session_name) {
  data_.set_node_id(data.node_id());
  data_.set_node_ip_address(data.node_manager_address());
  data_.mutable_start_timestamp()->CopyFrom(
      AbslTimeNanosToProtoTimestamp(absl::ToInt64Nanoseconds(
          absl::FromUnixMillis(data.start_time_ms()) - absl::UnixEpoch())));
  data_.mutable_labels()->insert(data.labels().begin(), data.labels().end());
}

std::string RayNodeDefinitionEvent::GetEntityId() const { return data_.node_id(); }

void RayNodeDefinitionEvent::MergeData(
    RayEvent<rpc::events::NodeDefinitionEvent> &&other) {
  // Definition events are static. Merging do not change the event.
  return;
}

ray::rpc::events::RayEvent RayNodeDefinitionEvent::SerializeData() && {
  ray::rpc::events::RayEvent event;
  event.mutable_node_definition_event()->Swap(&data_);
  return event;
}

}  // namespace observability
}  // namespace ray
