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

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"

namespace ray {
namespace observability {

RayEventRecorder::RayEventRecorder(
    rpc::EventAggregatorClient &event_aggregator_client,
    instrumented_io_context &io_service,
    size_t max_buffer_size,
    std::string_view metric_source,
    ray::observability::MetricInterface &dropped_events_counter)
    : event_aggregator_client_(event_aggregator_client),
      periodical_runner_(PeriodicalRunner::Create(io_service)),
      max_buffer_size_(max_buffer_size),
      metric_source_(metric_source),
      buffer_(max_buffer_size),
      dropped_events_counter_(dropped_events_counter) {}

void RayEventRecorder::StartExportingEvents() {
  absl::MutexLock lock(&mutex_);
  if (!RayConfig::instance().enable_ray_event()) {
    RAY_LOG(INFO) << "Ray event recording is disabled. Skipping start exporting events.";
    return;
  }
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
  // group the event in the buffer_ by their entity id and type; then for each group,
  // merge the events into a single event. maintain the order of the events in the buffer.
  std::list<std::unique_ptr<RayEventInterface>> grouped_events;
  absl::flat_hash_map<RayEventKey,
                      std::list<std::unique_ptr<RayEventInterface>>::iterator>
      event_key_to_iterator;
  for (auto &event : buffer_) {
    auto key = std::make_pair(event->GetEntityId(), event->GetEventType());
    auto [it, inserted] = event_key_to_iterator.try_emplace(key);
    if (inserted) {
      grouped_events.push_back(std::move(event));
      event_key_to_iterator[key] = std::prev(grouped_events.end());
    } else {
      (*it->second)->Merge(std::move(*event));
    }
  }
  for (auto &event : grouped_events) {
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
  if (!RayConfig::instance().enable_ray_event()) {
    return;
  }
  if (data_list.size() + buffer_.size() > max_buffer_size_) {
    size_t events_to_remove = data_list.size() + buffer_.size() - max_buffer_size_;
    // Record dropped events from the buffer
    RAY_LOG(ERROR) << "Dropping " << events_to_remove << " events from the buffer.";
    dropped_events_counter_.Record(events_to_remove,
                                   {{"Source", std::string(metric_source_)}});
  }
  for (auto &event : data_list) {
    buffer_.push_back(std::move(event));
  }
}

}  // namespace observability
}  // namespace ray
