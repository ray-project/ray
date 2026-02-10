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
#include <thread>
#include <vector>

#include "absl/time/time.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/observability/ray_event_interface.h"
#include "ray/observability/ray_event_recorder.h"
#include "ray/rpc/event_aggregator_client.h"
#include "ray/stats/metric.h"
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
  bool SupportsMerge() const override;

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

/// Owns all infrastructure for the Python-side event recorder: io_context,
/// background thread, gRPC client, metrics counter, and the recorder itself.
///
/// Call Shutdown() for orderly cleanup (flush + stop), or let the destructor
/// handle it. Both are safe to call multiple times.
class PythonEventRecorder {
 public:
  /// \param aggregator_address Address of the event aggregator server.
  /// \param aggregator_port Port of the event aggregator server.
  /// \param node_ip The IP address of the current node.
  /// \param node_id_hex Hex-encoded node ID.
  /// \param max_buffer_size Maximum number of events to buffer before dropping.
  PythonEventRecorder(const std::string &aggregator_address,
                      int aggregator_port,
                      const std::string &node_ip,
                      const std::string &node_id_hex,
                      size_t max_buffer_size);
  ~PythonEventRecorder();

  PythonEventRecorder(const PythonEventRecorder &) = delete;
  PythonEventRecorder &operator=(const PythonEventRecorder &) = delete;

  /// Add events to the recorder buffer for periodic export.
  void AddEvents(std::vector<std::unique_ptr<RayEventInterface>> &&data_list);

  /// Orderly shutdown: flush buffered events, stop io_context, join thread.
  /// Safe to call multiple times.
  void Shutdown();

 private:
  std::unique_ptr<instrumented_io_context> io_context_;
  std::unique_ptr<
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
      work_guard_;
  std::unique_ptr<std::thread> io_thread_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::unique_ptr<rpc::EventAggregatorClientImpl> event_aggregator_client_;
  ray::stats::Count dropped_events_counter_;
  std::unique_ptr<RayEventRecorder> recorder_;
};

}  // namespace observability
}  // namespace ray
