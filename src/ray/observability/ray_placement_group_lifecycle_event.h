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

#pragma once

#include "ray/observability/ray_event.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"

namespace ray {
namespace observability {

template class RayEvent<rpc::events::PlacementGroupLifecycleEvent>;

class RayPlacementGroupLifecycleEvent
    : public RayEvent<rpc::events::PlacementGroupLifecycleEvent> {
 public:
  RayPlacementGroupLifecycleEvent(const rpc::PlacementGroupTableData &data,
                                  rpc::events::PlacementGroupLifecycleEvent::State state,
                                  const std::string &session_name);

  std::string GetEntityId() const override;
  void MergeData(RayEvent<rpc::events::PlacementGroupLifecycleEvent> &&other) override;
  ray::rpc::events::RayEvent SerializeData() && override;

  // Convert from PlacementGroupTableData state to lifecycle event state.
  static rpc::events::PlacementGroupLifecycleEvent::State ConvertState(
      rpc::PlacementGroupTableData::PlacementGroupState state);

  // Convert from PlacementGroupStats scheduling state to lifecycle event scheduling
  // state.
  static rpc::events::PlacementGroupLifecycleEvent::SchedulingState
  ConvertSchedulingState(rpc::PlacementGroupStats::SchedulingState state);
};

}  // namespace observability
}  // namespace ray
