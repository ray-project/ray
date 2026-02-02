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
    ray::observability::MetricInterface &dropped_events_counter,
    ray::observability::MetricInterface &events_sent_counter,
    ray::observability::MetricInterface &events_failed_to_send_counter,
    const NodeID &node_id)
    : event_aggregator_client_(event_aggregator_client),
      periodical_runner_(PeriodicalRunner::Create(io_service)),
      max_buffer_size_(max_buffer_size),
      metric_source_(metric_source),
      buffer_(max_buffer_size),
      dropped_events_counter_(dropped_events_counter),
      events_sent_counter_(events_sent_counter),
      events_failed_to_send_counter_(events_failed_to_send_counter),
      node_id_(node_id) {}

void RayEventRecorder::StartExportingEvents() {
  absl::MutexLock lock(&mutex_);
  if (!RayConfig::instance().enable_ray_event()) {
    RAY_LOG(INFO) << "Ray event recording is disabled. Skipping start exporting events.";
    return;
  }
  if (exporting_started_) {
    return;
  }
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

  const size_t max_batch_size_bytes =
      RayConfig::instance().ray_event_recorder_send_batch_size_bytes();

  // Group events by entity_id + type (events with same key are merged).
  std::list<std::unique_ptr<RayEventInterface>> grouped_events;
  absl::flat_hash_map<RayEventKey,
                      std::list<std::unique_ptr<RayEventInterface>>::iterator>
      event_key_to_iterator;

  size_t processed = 0;
  for (auto it = buffer_.begin(); it != buffer_.end(); ++it, ++processed) {
    auto &event = *it;
    auto key = std::make_pair(event->GetEntityId(), event->GetEventType());
    auto [map_it, inserted] = event_key_to_iterator.try_emplace(key);
    if (inserted) {
      grouped_events.push_back(std::move(event));
      event_key_to_iterator[key] = std::prev(grouped_events.end());
    } else {
      (*map_it->second)->Merge(std::move(*event));
    }
  }

  // Remove processed events from buffer
  buffer_.erase(buffer_.begin(), buffer_.begin() + processed);

  // Serialize events and add to request, checking byte size
  rpc::events::AddEventsRequest request;
  size_t num_events = 0;
  auto it = grouped_events.begin();
  while (it != grouped_events.end()) {
    rpc::events::RayEvent ray_event = std::move(**it).Serialize();
    ray_event.set_node_id(node_id_.Binary());

    // Check if adding this event would exceed the byte limit
    // Always send at least one event even if it exceeds the limit
    if (num_events > 0 &&
        request.ByteSizeLong() + ray_event.ByteSizeLong() >= max_batch_size_bytes) {
      // Put remaining unsent events back at the front of the buffer
      while (it != grouped_events.end()) {
        buffer_.push_front(std::move(*it));
        ++it;
      }
      break;
    }

    *request.mutable_events_data()->mutable_events()->Add() = std::move(ray_event);
    ++num_events;
    ++it;
  }

  // Send with callback to record metrics
  // Capture metric_source_ by value since it's a string_view
  std::string metric_source_str(metric_source_);
  event_aggregator_client_.AddEvents(
      std::move(request),
      [this, num_events, metric_source_str](Status status,
                                            rpc::events::AddEventsReply reply) {
        if (status.ok()) {
          events_sent_counter_.Record(num_events, {{"Source", metric_source_str}});
        } else {
          events_failed_to_send_counter_.Record(num_events,
                                                {{"Source", metric_source_str}});
          RAY_LOG(ERROR) << "Failed to send " << num_events
                         << " ray events: " << status.ToString();
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
