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

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <string_view>

#include "absl/synchronization/mutex.h"
#include "ray/asio/periodical_runner_interface.h"
#include "ray/observability/metric_interface.h"
#include "ray/observability/ray_event_interface.h"
#include "ray/observability/ray_event_recorder_interface.h"
#include "ray/rpc/event_aggregator_client.h"

namespace ray {
namespace observability {

// Base for RayEventRecorderInterface implementations: owns the export-loop and
// gRPC-drain machinery shared by all recorders. Subclasses own the buffer
// and implement AddEvents and ExportEvents.
// This class is thread safe.
class RayEventRecorderBase : public RayEventRecorderInterface {
 public:
  // Start exporting events to the event aggregator by periodically sending events to
  // the event aggregator. This should be called only once. Subsequent calls will be
  // ignored.
  void StartExportingEvents() override;

  // Stop exporting events and perform a final flush to ensure all buffered events
  // are sent before shutdown. This should be called during graceful shutdown.
  void StopExportingEvents() override;

 protected:
  RayEventRecorderBase(rpc::EventAggregatorClient &event_aggregator_client,
                       std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
                       size_t max_buffer_size,
                       std::string_view metric_source,
                       ray::observability::MetricInterface &dropped_events_counter,
                       const NodeID &node_id);

  // Export events to the event aggregator. This is called periodically by the
  // PeriodicalRunner.
  virtual void ExportEvents() = 0;

  rpc::EventAggregatorClient &event_aggregator_client_;
  std::shared_ptr<PeriodicalRunnerInterface> periodical_runner_;
  // Lock for thread safety when modifying the buffer.
  absl::Mutex mutex_;

  // Maximum number of events to store in the buffer (configurable at runtime)
  size_t max_buffer_size_;
  std::string_view metric_source_;
  ray::observability::MetricInterface &dropped_events_counter_;
  // Flag to track if exporting has been started
  bool exporting_started_ ABSL_GUARDED_BY(mutex_) = false;
  // Flag to track if the recorder is enabled and accepting new events.
  // Set to false during shutdown to prevent event loss.
  bool enabled_ ABSL_GUARDED_BY(mutex_) = true;
  // Node ID to be set on all events
  const NodeID node_id_;

  // Flag to track if there's an in-flight gRPC call
  std::atomic<bool> grpc_in_progress_ = false;
  // Mutex and condition variable for waiting on gRPC completion during shutdown
  absl::Mutex grpc_completion_mutex_;
  absl::CondVar grpc_completion_cv_;
};

}  // namespace observability
}  // namespace ray
