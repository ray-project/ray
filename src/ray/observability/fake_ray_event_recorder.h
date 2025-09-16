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

#include <mutex>

#include "ray/observability/ray_event_interface.h"
#include "ray/observability/ray_event_recorder_interface.h"

namespace ray {
namespace observability {

class FakeRayEventRecorder : public RayEventRecorderInterface {
 public:
  void StartExportingEvents() override {}
  void AddEvents(std::vector<std::unique_ptr<RayEventInterface>> &&data_list) override {
    absl::MutexLock lock(&mutex_);
    buffer_.insert(buffer_.end(),
                   std::make_move_iterator(data_list.begin()),
                   std::make_move_iterator(data_list.end()));
  }

  const std::vector<std::unique_ptr<RayEventInterface>> FlushBuffer() {
    absl::MutexLock lock(&mutex_);
    auto buffer = std::move(buffer_);
    buffer_.clear();
    return buffer;
  }

 private:
  std::vector<std::unique_ptr<RayEventInterface>> buffer_ ABSL_GUARDED_BY(mutex_);
  absl::Mutex mutex_;
};

}  // namespace observability
}  // namespace ray
