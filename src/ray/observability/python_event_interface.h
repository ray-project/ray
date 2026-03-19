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

#include <memory>
#include <string>
#include <vector>

#include "absl/time/time.h"
#include "ray/observability/ray_event_interface.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"

namespace ray {
namespace observability {

/// PythonRayEvent is an implementation of RayEventInterface that can be created
/// from Python with pre-serialized protobuf event data.
///
/// This class is used by Cython bindings to create events from Python that can
/// be submitted to the RayEventRecorder.
class PythonRayEvent : public RayEventInterface {
 public:
  /// Create a PythonRayEvent from serialized event data.
  ///
  /// \param source_type The source type of the event.
  /// \param event_type The event type.
  /// \param severity The severity of the event.
  /// \param entity_id The entity ID for this event.
  /// \param message An optional message associated with the event.
  /// \param session_name The Ray session name.
  /// \param serialized_event_data The serialized protobuf data for the nested event
  ///        message (e.g., SubmissionJobDefinitionEvent or SubmissionJobLifecycleEvent).
  /// \param nested_event_field_number The field number in RayEvent proto for the nested
  ///        event message (e.g., 19 for submission_job_definition_event). This is used
  ///        with protobuf reflection to set the correct field without type-specific code.
  PythonRayEvent(rpc::events::RayEvent::SourceType source_type,
                 rpc::events::RayEvent::EventType event_type,
                 rpc::events::RayEvent::Severity severity,
                 std::string entity_id,
                 std::string message,
                 std::string session_name,
                 std::string serialized_event_data,
                 int nested_event_field_number);

  std::string GetEntityId() const override;
  void Merge(RayEventInterface &&other) override;
  rpc::events::RayEvent Serialize() && override;
  rpc::events::RayEvent::EventType GetEventType() const override;

 private:
  rpc::events::RayEvent::SourceType source_type_;
  rpc::events::RayEvent::EventType event_type_;
  rpc::events::RayEvent::Severity severity_;
  std::string entity_id_;
  std::string message_;
  std::string session_name_;
  std::string serialized_event_data_;
  int nested_event_field_number_;
  absl::Time event_timestamp_;
};

/// Factory function to create a PythonRayEvent. This is the function that
/// Cython will call.
///
/// \param source_type Integer value of SourceType enum.
/// \param event_type Integer value of EventType enum.
/// \param severity Integer value of Severity enum.
/// \param entity_id The entity ID for this event.
/// \param message An optional message.
/// \param session_name The Ray session name.
/// \param serialized_event_data The serialized protobuf event data.
/// \param nested_event_field_number The field number in RayEvent proto for the nested
///        event message. Python callers use RayEventProto.<FIELD>_FIELD_NUMBER constants.
/// \return A unique_ptr to the created event.
std::unique_ptr<RayEventInterface> CreatePythonRayEvent(
    int source_type,
    int event_type,
    int severity,
    const std::string &entity_id,
    const std::string &message,
    const std::string &session_name,
    const std::string &serialized_event_data,
    int nested_event_field_number);

/// Serialize Python-emitted events directly to a JSON array string.
///
/// Each event is serialized through RayEventInterface::Serialize, then converted
/// to JSON via protobuf's MessageToJsonString. Returns a JSON array string
/// (e.g., "[{...}, {...}]")
std::string SerializeEventsToRayEventsDataJson(
    std::vector<std::unique_ptr<RayEventInterface>> &&events);

}  // namespace observability
}  // namespace ray
