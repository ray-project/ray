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

#include "ray/observability/ray_event_recorder.h"

#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace observability {

using std::literals::operator""sv;

RayEventRecorder::RayEventRecorder(rpc::EventAggregatorClient &event_aggregator_client,
                                   instrumented_io_context &io_service,
                                   ray::observability::MetricInterface &dropped_events_counter)
    : event_aggregator_client_(event_aggregator_client),
      periodical_runner_(PeriodicalRunner::Create(io_service)),
      max_buffer_size_(RayConfig::instance().ray_event_recorder_max_buffer_size()),
      dropped_events_counter_(dropped_events_counter) {}

void RayEventRecorder::StartExportingEvents() {
  absl::MutexLock lock(&mutex_);
  RAY_CHECK(!exporting_started_)
      << "RayEventRecorder::StartExportingEvents() should be called only once.";
  exporting_started_ = true;
  periodical_runner_->RunFnPeriodically(
      [this]() { ExportEvents(); },
      RayConfig::instance().ray_events_report_interval_ms(),
      "RayEventRecorder.ExportEvents");
}

void RayEventRecorder::ExportEvents() {
  absl::MutexLock lock(&mutex_);
  if (buffer_.empty()) {
    return;
  }
  rpc::events::AddEventsRequest request;
  rpc::events::RayEventsData ray_event_data;
  // TODO(#56391): To further optimize the performance, we can merge multiple
  // events with the same resource ID into a single event.
  for (auto &event : buffer_) {
    rpc::events::RayEvent ray_event = std::move(*event).Serialize();
    *ray_event_data.mutable_events()->Add() = std::move(ray_event);
  }
  *request.mutable_events_data() = std::move(ray_event_data);
  buffer_.clear();

  event_aggregator_client_.AddEvents(
      request, [](Status status, rpc::events::AddEventsReply reply) {
        if (!status.ok()) {
          // TODO(#56391): Add a metric to track the number of failed events. Also
          // add logic for error recovery.
          RAY_LOG(ERROR) << "Failed to record ray event: " << status.ToString();
        }
      });
}

void RayEventRecorder::AddEvents(
    std::vector<std::unique_ptr<RayEventInterface>> &&data_list) {
  absl::MutexLock lock(&mutex_);
  
  // If adding all events would exceed capacity, remove enough old events
  size_t new_size = buffer_.size() + data_list.size();
  if (new_size > max_buffer_size_) {
    size_t events_to_remove = new_size - max_buffer_size_;
    // Remove oldest events from the front
    buffer_.erase(buffer_.begin(), buffer_.begin() + events_to_remove);
    dropped_events_counter_.Record(events_to_remove, {{"Source"sv, "gcs"}});
  }
  
  // Add all new events at once
  for (auto &data : data_list) {
    buffer_.emplace_back(std::move(data));
  }
}

}  // namespace observability
}  // namespace ray
