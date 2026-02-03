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

#include "ray/observability/python_event_interface.h"

#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/public/events_submission_job_definition_event.pb.h"
#include "src/ray/protobuf/public/events_submission_job_lifecycle_event.pb.h"

namespace ray {
namespace observability {

PythonRayEvent::PythonRayEvent(rpc::events::RayEvent::SourceType source_type,
                               rpc::events::RayEvent::EventType event_type,
                               rpc::events::RayEvent::Severity severity,
                               std::string entity_id,
                               std::string message,
                               std::string session_name,
                               std::string serialized_event_data)
    : source_type_(source_type),
      event_type_(event_type),
      severity_(severity),
      entity_id_(std::move(entity_id)),
      message_(std::move(message)),
      session_name_(std::move(session_name)),
      serialized_event_data_(std::move(serialized_event_data)),
      event_timestamp_(absl::Now()) {}

std::string PythonRayEvent::GetEntityId() const { return entity_id_; }

void PythonRayEvent::Merge(RayEventInterface &&other) {
  // For Python events, we don't support merging. Each event is independent.
  // This is because the serialized_event_data_ is opaque to us and we cannot
  // merge two serialized protobufs without knowing their structure.
  RAY_LOG(WARNING) << "Merge is not supported for Python-created events.";
}

rpc::events::RayEvent PythonRayEvent::Serialize() && {
  rpc::events::RayEvent event;

  // Set common fields
  event.set_event_id(UniqueID::FromRandom().Binary());
  event.set_source_type(source_type_);
  event.set_event_type(event_type_);
  event.set_severity(severity_);
  event.set_message(message_);
  event.set_session_name(session_name_);
  event.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(event_timestamp_ - absl::UnixEpoch())));

  // Parse and set the nested event message based on event type
  switch (event_type_) {
    case rpc::events::RayEvent::SUBMISSION_JOB_DEFINITION_EVENT: {
      auto *nested_event = event.mutable_submission_job_definition_event();
      if (!nested_event->ParseFromString(serialized_event_data_)) {
        RAY_LOG(ERROR) << "Failed to parse SubmissionJobDefinitionEvent from serialized "
                          "data";
      }
      break;
    }
    case rpc::events::RayEvent::SUBMISSION_JOB_LIFECYCLE_EVENT: {
      auto *nested_event = event.mutable_submission_job_lifecycle_event();
      if (!nested_event->ParseFromString(serialized_event_data_)) {
        RAY_LOG(ERROR)
            << "Failed to parse SubmissionJobLifecycleEvent from serialized data";
      }
      break;
    }
    default:
      RAY_LOG(ERROR) << "Unsupported event type for Python event: " << event_type_;
      break;
  }

  return event;
}

rpc::events::RayEvent::EventType PythonRayEvent::GetEventType() const {
  return event_type_;
}

std::unique_ptr<RayEventInterface> CreatePythonRayEvent(
    int source_type,
    int event_type,
    int severity,
    const std::string &entity_id,
    const std::string &message,
    const std::string &session_name,
    const std::string &serialized_event_data) {
  return std::make_unique<PythonRayEvent>(
      static_cast<rpc::events::RayEvent::SourceType>(source_type),
      static_cast<rpc::events::RayEvent::EventType>(event_type),
      static_cast<rpc::events::RayEvent::Severity>(severity),
      entity_id,
      message,
      session_name,
      serialized_event_data);
}

}  // namespace observability
}  // namespace ray
