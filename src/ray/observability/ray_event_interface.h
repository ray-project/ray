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

#include <memory>

#include "src/ray/protobuf/public/events_base_event.pb.h"

namespace ray {
namespace observability {

class RayEventInterface {
 public:
  virtual ~RayEventInterface() = default;

  // Entity ID is a concept in Ray Event framework that captures the unique identifier
  // of the entity that the event is associated with. For example, the entity ID of
  // a task is the pair of task ID and task attempt ID, for a driver job, it is the
  // driver job ID.
  //
  // Entity ID is used for two purposes:
  // 1. To associate the execution event with the definition event.
  // 2. To merge the individual execution events into a single execution event (single
  // data point to a time series).
  virtual std::string GetEntityId() const = 0;

  // Merge with another data point to form a time series. Merge is meant as an
  // optimization for the data size.
  //
  // For example, given three events:
  //
  // 1. event 1: {entity_id: "1", type: "task", state_transitions: [("started", 1000)]}
  // 2. event 2: {entity_id: "1", type: "task", state_transitions: [("running", 1001)]}
  // 3. event 3: {entity_id: "1", type: "task", state_transitions: [("completed", 1002)]}
  //
  // The merged event will be:
  //
  // {entity_id: "1", type: "task", state_transitions: [("started", 1000), ("running",
  // 1001),
  // ("completed", 1002)]}
  //
  // This function assumes that the two events have the same type and entity ID.
  virtual void Merge(RayEventInterface &&other) = 0;

  // Serialize the event data to a RayEvent proto.
  virtual ray::rpc::events::RayEvent Serialize() && = 0;

  virtual ray::rpc::events::RayEvent::EventType GetEventType() const = 0;
};

}  // namespace observability
}  // namespace ray
