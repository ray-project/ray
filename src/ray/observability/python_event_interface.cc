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

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/observability/metric_constants.h"
#include "ray/observability/metrics.h"
#include "ray/util/logging.h"

namespace ray {
namespace observability {

PythonRayEvent::PythonRayEvent(rpc::events::RayEvent::SourceType source_type,
                               rpc::events::RayEvent::EventType event_type,
                               rpc::events::RayEvent::Severity severity,
                               std::string entity_id,
                               std::string message,
                               std::string session_name,
                               std::string serialized_event_data,
                               int nested_event_field_number)
    : source_type_(source_type),
      event_type_(event_type),
      severity_(severity),
      entity_id_(std::move(entity_id)),
      message_(std::move(message)),
      session_name_(std::move(session_name)),
      serialized_event_data_(std::move(serialized_event_data)),
      nested_event_field_number_(nested_event_field_number),
      event_timestamp_(absl::Now()) {}

std::string PythonRayEvent::GetEntityId() const { return entity_id_; }

bool PythonRayEvent::SupportsMerge() const { return false; }

void PythonRayEvent::Merge(RayEventInterface &&other) {
  RAY_LOG(FATAL) << "Merge should never be called on PythonRayEvent. "
                 << "The recorder should skip grouping for non-mergeable events.";
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

  // Use protobuf reflection to set the nested event message by field number.
  // Adding new Python event types requires no C++ changes.
  const auto *descriptor = event.GetDescriptor();
  const auto *field = descriptor->FindFieldByNumber(nested_event_field_number_);
  if (field != nullptr &&
      field->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
    auto *nested = event.GetReflection()->MutableMessage(&event, field);
    if (!nested->ParseFromString(serialized_event_data_)) {
      RAY_LOG(ERROR) << "Failed to parse nested event data for field " << field->name();
      event.GetReflection()->ClearField(&event, field);
    }
  } else {
    RAY_LOG(ERROR) << "Invalid nested event field number: " << nested_event_field_number_;
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
    const std::string &serialized_event_data,
    int nested_event_field_number) {
  return std::make_unique<PythonRayEvent>(
      static_cast<rpc::events::RayEvent::SourceType>(source_type),
      static_cast<rpc::events::RayEvent::EventType>(event_type),
      static_cast<rpc::events::RayEvent::Severity>(severity),
      entity_id,
      message,
      session_name,
      serialized_event_data,
      nested_event_field_number);
}

PythonEventRecorder::PythonEventRecorder(const std::string &aggregator_address,
                                         int aggregator_port,
                                         const std::string &node_ip,
                                         const std::string &node_id_hex,
                                         size_t max_buffer_size)
    : dropped_events_counter_(GetRayEventRecorderDroppedEventsCounterMetric()) {
  io_context_ = std::make_unique<instrumented_io_context>(
      /*emit_metrics=*/false, /*running_on_single_thread=*/true);

  work_guard_ = std::make_unique<
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
      io_context_->get_executor());

  io_thread_ = std::make_unique<std::thread>([this]() { io_context_->run(); });

  client_call_manager_ = std::make_unique<rpc::ClientCallManager>(
      *io_context_, /*record_stats=*/false, node_ip);

  event_aggregator_client_ =
      std::make_unique<rpc::EventAggregatorClientImpl>(*client_call_manager_);
  event_aggregator_client_->Connect(aggregator_port);

  recorder_ = std::make_unique<RayEventRecorder>(*event_aggregator_client_,
                                                 *io_context_,
                                                 max_buffer_size,
                                                 kMetricSourcePython,
                                                 dropped_events_counter_,
                                                 NodeID::FromHex(node_id_hex));
  recorder_->StartExportingEvents();
}

PythonEventRecorder::~PythonEventRecorder() { Shutdown(); }

void PythonEventRecorder::Shutdown() {
  if (recorder_) {
    recorder_->StopExportingEvents();
    recorder_.reset();
  }
  event_aggregator_client_.reset();
  client_call_manager_.reset();
  work_guard_.reset();
  if (io_context_) {
    io_context_->stop();
  }
  if (io_thread_ && io_thread_->joinable()) {
    io_thread_->join();
  }
  io_thread_.reset();
  io_context_.reset();
}

void PythonEventRecorder::AddEvents(
    std::vector<std::unique_ptr<RayEventInterface>> &&data_list) {
  RAY_CHECK(recorder_) << "PythonEventRecorder has been shut down.";
  recorder_->AddEvents(std::move(data_list));
}

}  // namespace observability
}  // namespace ray
