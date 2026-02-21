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
#include "ray/common/id.h"
#include "ray/observability/ray_event_interface.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"

namespace ray {
namespace observability {

// RayEvent is a base class for all Ray events. It is used to serialize the event data
// to a RayEvent proto before sending it to the aggregator.
template <typename T>
class RayEvent : public RayEventInterface {
 public:
  void Merge(RayEventInterface &&other) override {
    RAY_CHECK_EQ(GetEntityId(), other.GetEntityId());
    RAY_CHECK_EQ(GetEventType(), other.GetEventType());
    MergeData(static_cast<RayEvent<T> &&>(other));
  }

  ray::rpc::events::RayEvent Serialize() && override {
    ray::rpc::events::RayEvent event = std::move(*this).SerializeData();
    event.set_event_id(UniqueID::FromRandom().Binary());
    event.set_source_type(source_type_);
    event.set_event_type(event_type_);
    event.set_severity(severity_);
    event.set_message(message_);
    event.set_session_name(session_name_);
    event.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
        absl::ToInt64Nanoseconds(event_timestamp_ - absl::UnixEpoch())));

    return event;
  }

  ray::rpc::events::RayEvent::EventType GetEventType() const override {
    return event_type_;
  }

 protected:
  RayEvent(ray::rpc::events::RayEvent::SourceType source_type,
           ray::rpc::events::RayEvent::EventType event_type,
           ray::rpc::events::RayEvent::Severity severity,
           const std::string &message,
           const std::string &session_name)
      : source_type_(source_type),
        event_type_(event_type),
        severity_(severity),
        message_(message),
        session_name_(session_name) {
    event_timestamp_ = absl::Now();
  }

  T data_;  // The nested event message within the RayEvent proto.
  absl::Time event_timestamp_;
  ray::rpc::events::RayEvent::SourceType source_type_;
  ray::rpc::events::RayEvent::EventType event_type_;
  ray::rpc::events::RayEvent::Severity severity_;
  std::string message_;
  std::string session_name_;
  virtual void MergeData(RayEvent<T> &&other) = 0;
  virtual ray::rpc::events::RayEvent SerializeData() && = 0;
};

}  // namespace observability
}  // namespace ray
