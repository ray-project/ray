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
    const NodeID &node_id)
    : event_aggregator_client_(event_aggregator_client),
      periodical_runner_(PeriodicalRunner::Create(io_service)),
      max_buffer_size_(max_buffer_size),
      metric_source_(metric_source),
      buffer_(max_buffer_size),
      dropped_events_counter_(dropped_events_counter),
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

    void Flush() override { recorder_->ExportEvents(); }

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
    // Set node_id centrally for all events
    ray_event.set_node_id(node_id_.Binary());
    *ray_event_data.mutable_events()->Add() = std::move(ray_event);
  }
  *request.mutable_events_data() = std::move(ray_event_data);
  buffer_.clear();

  grpc_in_progress_ = true;
  event_aggregator_client_.AddEvents(
      request, [this](Status status, rpc::events::AddEventsReply reply) {
        if (!status.ok()) {
          // TODO(#56391): Add a metric to track the number of failed events. Also
          // add logic for error recovery.
          RAY_LOG(ERROR) << "Failed to record ray event: " << status.ToString();
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
