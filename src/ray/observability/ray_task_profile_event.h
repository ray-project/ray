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

#pragma once

#include "ray/common/grpc_util.h"
#include "ray/observability/ray_event.h"
#include "src/ray/protobuf/events_task_profile_events.pb.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"

namespace ray {
namespace observability {

template class RayEvent<rpc::events::TaskProfileEvents>;

class RayTaskProfileEvent : public RayEvent<rpc::events::TaskProfileEvents> {
 public:
  RayTaskProfileEvent(const TaskID &task_id,
                      int32_t attempt_number,
                      const JobID &job_id,
                      const NodeID &node_id,
                      const std::string &component_type,
                      const std::string &component_id,
                      const std::string &node_ip_address,
                      const std::string &event_name,
                      int64_t start_time_ns,
                      int64_t end_time_ns,
                      const std::string &session_name);

  std::string GetEntityId() const override;
  void MergeData(RayEvent<rpc::events::TaskProfileEvents> &&other) override;
  ray::rpc::events::RayEvent SerializeData() && override;
};

}  // namespace observability
}  // namespace ray
