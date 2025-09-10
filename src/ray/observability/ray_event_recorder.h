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

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "google/protobuf/timestamp.pb.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/ray_config.h"
#include "ray/observability/ray_event_interface.h"
#include "ray/observability/ray_event_recorder_interface.h"
#include "ray/rpc/event_aggregator_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"

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
                   instrumented_io_context &io_service);
  virtual ~RayEventRecorder() = default;

  // Start exporting events to the event aggregator by periodically sending events to
  // the event aggregator. This should be called only once. Subsequent calls will be
  // ignored.
  void StartExportingEvents();

  // Add a vector of data to the internal buffer. Data in the buffer will be sent to
  // the event aggregator periodically.
  void AddEvents(std::vector<std::unique_ptr<RayEventInterface>> &&data_list);

 private:
  rpc::EventAggregatorClient &event_aggregator_client_;
  std::shared_ptr<PeriodicalRunner> periodical_runner_;
  // Lock for thread safety when modifying the buffer.
  absl::Mutex mutex_;
  // Buffer to store events before sending to the event aggregator.
  // TODO(#56391): Add a max size for the buffer and overflow recovery logic.
  std::vector<std::unique_ptr<RayEventInterface>> buffer_ ABSL_GUARDED_BY(mutex_);
  // Flag to track if exporting has been started
  bool exporting_started_ ABSL_GUARDED_BY(mutex_) = false;
  // Export events to the event aggregator. This is called periodically by the
  // PeriodicalRunner.
  void ExportEvents();
};

}  // namespace observability
}  // namespace ray
