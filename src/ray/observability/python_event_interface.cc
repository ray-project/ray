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

#include "absl/strings/str_cat.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/observability/metrics.h"
#include "ray/util/logging.h"

namespace ray {
namespace observability {

PythonRayEvent::PythonRayEvent(rpc::events::RayEvent::SourceType source_type,
                               rpc::events::RayEvent::EventType event_type,
                               rpc::events::RayEvent::Severity severity,
                               const std::string &entity_id,
                               const std::string &message,
                               const std::string &session_name,
                               const std::string &serialized_event_data,
                               int nested_event_field_number,
                               const std::string &event_id,
                               int64_t timestamp_ns)
    : source_type_(source_type),
      event_type_(event_type),
      severity_(severity),
      entity_id_(entity_id),
      message_(message),
      session_name_(session_name),
      serialized_event_data_(serialized_event_data),
      nested_event_field_number_(nested_event_field_number),
      event_id_(event_id),
      event_timestamp_(timestamp_ns == 0 ? absl::Now()
                                         : absl::FromUnixNanos(timestamp_ns)) {}

std::string PythonRayEvent::GetEntityId() const { return entity_id_; }

bool PythonRayEvent::SupportsMerge() const { return false; }

void PythonRayEvent::Merge(RayEventInterface &&other) {
  RAY_CHECK(false) << "Merge should never be called on PythonRayEvent. "
                   << "The recorder should skip grouping for non-mergeable events.";
}

StatusOr<rpc::events::RayEvent> PythonRayEvent::Serialize() && {
  rpc::events::RayEvent event;

  event.set_event_id(event_id_.empty() ? UniqueID::FromRandom().Binary() : event_id_);
  event.set_source_type(source_type_);
  event.set_event_type(event_type_);
  event.set_severity(severity_);
  event.set_message(message_);
  event.set_session_name(session_name_);
  event.mutable_timestamp()->CopyFrom(AbslTimeNanosToProtoTimestamp(
      absl::ToInt64Nanoseconds(event_timestamp_ - absl::UnixEpoch())));

  // Use protobuf reflection to set the nested event message by field number.
  // this way, adding new Python event types will not require C++ changes.
  // The field number is validated in CreatePythonRayEvent.
  const google::protobuf::FieldDescriptor *field =
      event.GetDescriptor()->FindFieldByNumber(nested_event_field_number_);
  google::protobuf::Message *nested =
      event.GetReflection()->MutableMessage(&event, field);
  if (!nested->ParseFromString(serialized_event_data_)) {
    return Status::Invalid(
        absl::StrCat("Failed to parse nested event data for field ", field->name()));
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
    int nested_event_field_number,
    const std::string &event_id,
    int64_t timestamp_ns) {
  RAY_CHECK(rpc::events::RayEvent::SourceType_IsValid(source_type))
      << "Invalid SourceType enum value: " << source_type;
  RAY_CHECK(rpc::events::RayEvent::EventType_IsValid(event_type))
      << "Invalid EventType enum value: " << event_type;
  RAY_CHECK(rpc::events::RayEvent::Severity_IsValid(severity))
      << "Invalid Severity enum value: " << severity;
  const google::protobuf::FieldDescriptor *field =
      rpc::events::RayEvent::descriptor()->FindFieldByNumber(nested_event_field_number);
  RAY_CHECK(field != nullptr &&
            field->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE)
      << "Invalid nested event field number: " << nested_event_field_number;

  return std::make_unique<PythonRayEvent>(
      static_cast<rpc::events::RayEvent::SourceType>(source_type),
      static_cast<rpc::events::RayEvent::EventType>(event_type),
      static_cast<rpc::events::RayEvent::Severity>(severity),
      entity_id,
      message,
      session_name,
      serialized_event_data,
      nested_event_field_number,
      event_id,
      timestamp_ns);
}

PythonEventRecorder::PythonEventRecorder(int aggregator_port,
                                         const std::string &node_ip,
                                         const std::string &node_id_hex,
                                         size_t max_buffer_size,
                                         const std::string &metric_source)
    : dropped_events_counter_(GetRayEventRecorderDroppedEventsCounterMetric()),
      metric_source_str_(metric_source) {
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

  NodeID node_id = NodeID::FromHex(node_id_hex);
  RAY_CHECK(!node_id.IsNil()) << "Invalid node ID: " << node_id_hex;

  recorder_ = std::make_unique<RayEventRecorder>(*event_aggregator_client_,
                                                 *io_context_,
                                                 max_buffer_size,
                                                 metric_source_str_,
                                                 dropped_events_counter_,
                                                 node_id);
  recorder_->StartExportingEvents();
}

PythonEventRecorder::~PythonEventRecorder() { Shutdown(); }

void PythonEventRecorder::Shutdown() {
  if (recorder_) {
    recorder_->StopExportingEvents();
  }
  // Stop and join io_thread before destroying recorder_, event_aggregator_client_,
  // and client_call_manager_ so that no periodical-runner timer or in-flight gRPC
  // callback on io_thread can touch their members during destruction.
  work_guard_.reset();
  if (io_context_) {
    io_context_->stop();
  }
  if (io_thread_ && io_thread_->joinable()) {
    io_thread_->join();
  }
  recorder_.reset();
  event_aggregator_client_.reset();
  client_call_manager_.reset();
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
