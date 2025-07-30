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

#include "absl/time/time.h"
#include "google/protobuf/timestamp.pb.h"
#include "ray/rpc/event_aggregator_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/events_base_event.pb.h"

namespace ray {
namespace telemetry {

// RayEventRecorderBase is a unified interface for recording different types of Ray
// events (e.g. task events, job events, etc.). The events are converted to RayEvent
// proto and sent to the event aggregator.
template <typename TEventData>
class RayEventRecorderBase {
 public:
  RayEventRecorderBase(
      std::unique_ptr<rpc::EventAggregatorClient> event_aggregator_client);
  virtual ~RayEventRecorderBase() = default;

  // Record a vector of data to the event aggregator.
  void RecordEvents(const std::vector<TEventData> &data_list);

 protected:
  // Helper method to set current timestamp in a protobuf Timestamp
  void SetCurrentTimestamp(google::protobuf::Timestamp *timestamp);

 private:
  std::unique_ptr<rpc::EventAggregatorClient> event_aggregator_client_;
  virtual void ConvertToRayEvent(const TEventData &data,
                                 rpc::events::RayEvent &ray_event) = 0;
};

}  // namespace telemetry
}  // namespace ray
