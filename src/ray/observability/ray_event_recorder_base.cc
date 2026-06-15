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

#include "ray/observability/ray_event_recorder_base.h"

#include "ray/common/ray_config.h"
#include "ray/util/graceful_shutdown.h"
#include "ray/util/logging.h"

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

}  // namespace observability
}  // namespace ray
