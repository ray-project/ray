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
#include <memory>
#include <string_view>
#include <vector>

#include "ray/observability/ray_event_recorder_base.h"

namespace ray {
namespace observability {

// RayEventRecorder is a class for recording different types of Ray
// events (e.g. task events, job events, etc.).  Internal buffer is used to store events
// before sending to the event aggregator.  Events are converted to RayEvent proto and
// added to the internal buffer.  PeriodicalRunner is used to send events to the event
// aggregator periodically.
//
// This class is thread safe.
class RayEventRecorder : public RayEventRecorderBase {
 public:
  RayEventRecorder(rpc::EventAggregatorClient &event_aggregator_client,
                   std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
                   size_t max_buffer_size,
                   std::string_view metric_source,
                   ray::observability::MetricInterface &dropped_events_counter,
                   const NodeID &node_id);

  // Add a vector of data to the internal buffer. Data in the buffer will be sent to
  // the event aggregator periodically.
  void AddEvents(std::vector<std::unique_ptr<RayEventInterface>> &&data_list) override;

 private:
  void ExportEvents() override;

  // Bounded queue to store events before sending to the event aggregator.
  // When the queue is full, old events are dropped to make room for new ones.
  boost::circular_buffer<std::unique_ptr<RayEventInterface>> buffer_
      ABSL_GUARDED_BY(mutex_);
};

}  // namespace observability
}  // namespace ray
