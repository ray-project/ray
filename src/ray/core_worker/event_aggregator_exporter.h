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

#include <memory>

#include "ray/rpc/event_aggregator_client.h"

namespace ray {

/// The class that sends ray events to the event aggregator.
class EventAggregatorExporter {
 public:
  EventAggregatorExporter() = default;
  explicit EventAggregatorExporter(std::shared_ptr<rpc::EventAggregatorClient> client)
      : client_impl_(client) {}
  virtual ~EventAggregatorExporter() = default;

  /// Send ray events to the event aggregator.
  ///
  /// \param data_ptr The ray event data to be added.
  /// \param callback The callback to be called when the event is added.
  /// \return The status of the operation.
  virtual Status AsyncAddRayEventData(std::unique_ptr<rpc::events::RayEventData> data_ptr,
                                      std::function<void(Status status)> callback);

 private:
  std::shared_ptr<rpc::EventAggregatorClient> client_impl_;
};

}  // namespace ray
