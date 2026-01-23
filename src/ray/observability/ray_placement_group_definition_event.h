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

template class RayEvent<rpc::events::PlacementGroupDefinitionEvent>;

class RayPlacementGroupDefinitionEvent
    : public RayEvent<rpc::events::PlacementGroupDefinitionEvent> {
 public:
  RayPlacementGroupDefinitionEvent(const rpc::PlacementGroupTableData &data,
                                   const std::string &session_name);

  std::string GetEntityId() const override;
  void MergeData(RayEvent<rpc::events::PlacementGroupDefinitionEvent> &&other) override;
  ray::rpc::events::RayEvent SerializeData() && override;
};

}  // namespace observability
}  // namespace ray
