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
#include <vector>

#include "ray/observability/ray_event_interface.h"

namespace ray {
namespace observability {

class RayEventRecorderInterface {
 public:
  virtual ~RayEventRecorderInterface() = default;

  // Start exporting events to the event aggregator by periodically sending events to
  // the event aggregator. This should be called only once. Subsequent calls will be
  // ignored.
  virtual void StartExportingEvents() = 0;

  // Add a vector of data to the internal buffer. Data in the buffer will be sent to
  // the event aggregator periodically.
  virtual void AddEvents(std::vector<std::unique_ptr<RayEventInterface>> &&data_list) = 0;
};

}  // namespace observability
}  // namespace ray
