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
#include "ray/util/graceful_shutdown.h"
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

void RayEventRecorder::StopExportingEvents() {
  {
    absl::MutexLock lock(&mutex_);
    if (!enabled_) {
      return;
    }
    // Set enabled_ to false early to prevent new events from being added during shutdown.
    // This prevents event loss from events added after ExportEvents() clears the buffer.
    enabled_ = false;
  }
  RAY_LOG(INFO) << "Stopping RayEventRecorder and flushing remaining events.";

  auto flush_timeout_ms = RayConfig::instance().task_events_shutdown_flush_timeout_ms();

  // Local handler implementing GracefulShutdownHandler interface.
  class ShutdownHandler : public GracefulShutdownHandler {
   public:
    explicit ShutdownHandler(RayEventRecorder *recorder) : recorder_(recorder) {}

    bool WaitUntilIdle(absl::Duration timeout) override {
      absl::MutexLock lock(&recorder_->grpc_completion_mutex_);
      auto deadline = absl::Now() + timeout;
      while (recorder_->grpc_in_progress_) {
        if (recorder_->grpc_completion_cv_.WaitWithDeadline(
                &recorder_->grpc_completion_mutex_, deadline)) {
          return false;  // Timeout
        }
      }
      return true;
    }

    void Flush() override {
      // ExportEvents() sends one batch at a time. Loop until the buffer is
      // fully drained, waiting for each in-flight gRPC to complete before
      // sending the next batch. This is safe because enabled_ is false so
      // no new events can be added, and the buffer is bounded.
      while (true) {
        recorder_->ExportEvents();
        // Wait for the gRPC call (if any) to complete before checking
        // whether more events remain.
        {
          absl::MutexLock lock(&recorder_->grpc_completion_mutex_);
          while (recorder_->grpc_in_progress_) {
            recorder_->grpc_completion_cv_.Wait(&recorder_->grpc_completion_mutex_);
          }
        }
        absl::MutexLock lock(&recorder_->mutex_);
        if (recorder_->buffer_.empty()) {
          break;
        }
      }
    }

   private:
    RayEventRecorder *recorder_;
  };

  ShutdownHandler handler(this);
  GracefulShutdownWithFlush(
      handler, absl::Milliseconds(flush_timeout_ms), "RayEventRecorder");
}

void RayEventRecorder::ExportEvents() {
  absl::MutexLock lock(&mutex_);
  if (buffer_.empty()) {
    return;
  }

  // Skip if there's already an in-flight gRPC call to avoid overlapping requests.
  // This prevents the race where StopExportingEvents() could return while a
  // periodic export is still in flight.
  if (grpc_in_progress_) {
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
        << "Previous RayEventRecorder export in progress: new events will be exported "
           "once previous export completes.";
    return;
  }

  const size_t max_batch_size_bytes =
      RayConfig::instance().ray_event_recorder_send_batch_size_bytes();

  // Group events by entity_id + type (events with same key are merged).
  // Stop grouping when batch size limit is reached.
  std::list<std::unique_ptr<RayEventInterface>> grouped_events;
  absl::flat_hash_map<RayEventKey,
                      std::list<std::unique_ptr<RayEventInterface>>::iterator>
      event_key_to_iterator;

  size_t processed = 0;
  size_t estimated_batch_size = 0;
  for (auto it = buffer_.begin(); it != buffer_.end(); ++it, ++processed) {
    auto &event = *it;
    auto key = std::make_pair(event->GetEntityId(), event->GetEventType());
    auto [map_it, inserted] = event_key_to_iterator.try_emplace(key);
    if (inserted) {
      // New event - check if adding it would exceed batch size
      size_t estimated_size = event->GetSerializedSizeEstimate();
      if (!grouped_events.empty() &&
          estimated_batch_size + estimated_size >= max_batch_size_bytes) {
        // leave remaining events in the buffer
        break;
      }
      grouped_events.push_back(std::move(event));
      event_key_to_iterator[key] = std::prev(grouped_events.end());
      estimated_batch_size += estimated_size;
    } else {
      // Merge into existing event (doesn't significantly change size)
      (*map_it->second)->Merge(std::move(*event));
    }
  }

  // Remove processed events from buffer
  buffer_.erase(buffer_.begin(), buffer_.begin() + processed);

  // Serialize events and add to request
  rpc::events::AddEventsRequest request;
  size_t num_events = 0;
  for (auto &event : grouped_events) {
    rpc::events::RayEvent ray_event = std::move(*event).Serialize();
    ray_event.set_node_id(node_id_.Binary());
    *request.mutable_events_data()->mutable_events()->Add() = std::move(ray_event);
    ++num_events;
  }

  // Send with callback to record metrics
  // Capture metric_source_ by value since it's a string_view
  std::string metric_source_str(metric_source_);
  grpc_in_progress_ = true;
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
        // Signal under mutex to avoid lost wakeup race condition
        {
          absl::MutexLock grpc_lock(&grpc_completion_mutex_);
          grpc_in_progress_ = false;
          grpc_completion_cv_.Signal();
        }
      });
}

void RayEventRecorder::AddEvents(
    std::vector<std::unique_ptr<RayEventInterface>> &&data_list) {
  absl::MutexLock lock(&mutex_);
  if (!enabled_) {
    return;
  }
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
