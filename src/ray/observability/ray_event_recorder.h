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

#include <boost/circular_buffer.hpp>

#include "absl/synchronization/mutex.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/ray_config.h"
#include "ray/observability/metric_interface.h"
#include "ray/observability/ray_event_interface.h"
#include "ray/observability/ray_event_recorder_interface.h"
#include "ray/rpc/event_aggregator_client.h"

namespace ray {
namespace observability {

// RayEventRecorder is a class for recording different types of Ray
// events (e.g. task events, job events, etc.).  Internal buffer is used to store events
// before sending to the event aggregator.  Events are converted to RayEvent proto and
// added to the internal buffer.  PeriodicalRunner is used to send events to the event
// aggregator periodically.
//
// This class is thread safe.
class RayEventRecorder : public RayEventRecorderInterface {
 public:
  RayEventRecorder(rpc::EventAggregatorClient &event_aggregator_client,
                   instrumented_io_context &io_service,
                   size_t max_buffer_size,
                   std::string_view metric_source,
                   ray::observability::MetricInterface &dropped_events_counter);
  virtual ~RayEventRecorder() = default;

  // Start exporting events to the event aggregator by periodically sending events to
  // the event aggregator. This should be called only once. Subsequent calls will be
  // ignored.
  void StartExportingEvents();

  // Add a vector of data to the internal buffer. Data in the buffer will be sent to
  // the event aggregator periodically.
  void AddEvents(std::vector<std::unique_ptr<RayEventInterface>> &&data_list);

 private:
  using RayEventKey = std::pair<std::string, rpc::events::RayEvent::EventType>;

  rpc::EventAggregatorClient &event_aggregator_client_;
  std::shared_ptr<PeriodicalRunner> periodical_runner_;
  // Lock for thread safety when modifying the buffer.
  absl::Mutex mutex_;

  // Maximum number of events to store in the buffer (configurable at runtime)
  size_t max_buffer_size_;
  std::string_view metric_source_;
  // Bounded queue to store events before sending to the event aggregator.
  // When the queue is full, old events are dropped to make room for new ones.
  boost::circular_buffer<std::unique_ptr<RayEventInterface>> buffer_
      ABSL_GUARDED_BY(mutex_);
  ray::observability::MetricInterface &dropped_events_counter_;
  // Flag to track if exporting has been started
  bool exporting_started_ ABSL_GUARDED_BY(mutex_) = false;
  // Export events to the event aggregator. This is called periodically by the
  // PeriodicalRunner.
  void ExportEvents();
};

}  // namespace observability
}  // namespace ray
