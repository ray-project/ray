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

#include "ray/observability/ray_event_recorder_base.h"

#include <iterator>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "ray/common/ray_config.h"
#include "ray/util/graceful_shutdown.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"

namespace ray {
namespace observability {

RayEventRecorderBase::RayEventRecorderBase(
    rpc::EventAggregatorClient &event_aggregator_client,
    std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
    size_t max_buffer_size,
    std::string_view metric_source,
    ray::observability::MetricInterface &dropped_events_counter,
    const NodeID &node_id)
    : event_aggregator_client_(event_aggregator_client),
      periodical_runner_(std::move(periodical_runner)),
      max_buffer_size_(max_buffer_size),
      metric_source_(metric_source),
      dropped_events_counter_(dropped_events_counter),
      node_id_(node_id) {}

void RayEventRecorderBase::StartExportingEvents() {
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

void RayEventRecorderBase::StopExportingEvents() {
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
    explicit ShutdownHandler(RayEventRecorderBase *recorder) : recorder_(recorder) {}

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
    RayEventRecorderBase *recorder_;
  };

  ShutdownHandler handler(this);
  GracefulShutdownWithFlush(
      handler, absl::Milliseconds(flush_timeout_ms), "RayEventRecorder");
}

void RayEventRecorderBase::GroupAndSerializeEvents(
    std::list<std::unique_ptr<RayEventInterface>> &&events,
    rpc::events::RayEventsData *data) const {
  using RayEventKey = std::pair<std::string, rpc::events::RayEvent::EventType>;
  // group the events by their entity id and type; then for each group, merge the events
  // into a single event. maintain the order of the events.
  std::list<std::unique_ptr<RayEventInterface>> grouped_events;
  absl::flat_hash_map<RayEventKey,
                      std::list<std::unique_ptr<RayEventInterface>>::iterator>
      event_key_to_iterator;
  for (auto &event : events) {
    if (!event->SupportsMerge()) {
      // Non-mergeable events (e.g. those sent from Python) are sent individually.
      grouped_events.push_back(std::move(event));
      continue;
    }
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
    auto ray_event_or = std::move(*event).Serialize();
    if (ray_event_or.has_error()) {
      // TODO: Add a metric to track the number of events skipped due to
      // serialization failure.
      RAY_LOG(ERROR) << "Skipping event that failed to serialize: "
                     << ray_event_or.message();
      continue;
    }
    rpc::events::RayEvent ray_event = std::move(ray_event_or.value());
    // Set node_id centrally for all events
    ray_event.set_node_id(node_id_.Binary());
    *data->mutable_events()->Add() = std::move(ray_event);
  }
}

void RayEventRecorderBase::SendRequest(rpc::events::AddEventsRequest &&request) {
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

}  // namespace observability
}  // namespace ray
