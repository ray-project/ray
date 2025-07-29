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

#include "absl/time/time.h"
#include "ray/common/grpc_util.h"
#include "ray/observability/ray_event_interface.h"
#include "src/ray/protobuf/events_base_event.pb.h"

namespace ray {
namespace observability {

// RayEvent is a base class for all Ray events. It is used to serialize the event data
// to a RayEvent proto before sending it to the aggregator.
template <typename T>
class RayEvent : public RayEventInterface {
 public:
  void Merge(RayEventInterface &other) override {
    RAY_CHECK(GetResourceId() == other.GetResourceId());
    RAY_CHECK(GetEventType() == other.GetEventType());
    Merge(static_cast<RayEvent<T> &>(other));
  }

  T &GetData() { return data_; }

  ray::rpc::events::RayEvent::EventType GetEventType() const override {
    return event_type_;
  }

 protected:
  T data_;  // The nested event message within the RayEvent proto.
  ray::rpc::events::RayEvent::EventType event_type_;
  virtual void Merge(RayEvent<T> &other) = 0;
};

}  // namespace observability
}  // namespace ray
