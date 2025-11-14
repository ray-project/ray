// Copyright 2024 The Ray Authors.
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

#include <array>
#include <string_view>
#include <type_traits>

#include "ray/gcs/gcs_task_manager.h"
#include "ray/observability/ray_event_recorder.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/ray_syncer/ray_syncer.h"
#include "ray/util/array.h"
#include "ray/util/type_traits.h"

namespace ray {
namespace gcs {

struct GcsServerIOContextPolicy {
  GcsServerIOContextPolicy() = delete;

  // IOContext name for each handler.
  // If a class needs a dedicated io context, it should be specialized here.
  // If a class does NOT have a dedicated io context, returns -1;
  template <typename T>
  static constexpr int GetDedicatedIOContextIndex() {
    if constexpr (std::is_same_v<T, GcsTaskManager>) {
      return IndexOf("task_io_context");
    } else if constexpr (std::is_same_v<T, pubsub::GcsPublisher>) {
      return IndexOf("pubsub_io_context");
    } else if constexpr (std::is_same_v<T, syncer::RaySyncer>) {
      return IndexOf("ray_syncer_io_context");
    } else if constexpr (std::is_same_v<T, observability::RayEventRecorder>) {
      return IndexOf("ray_event_io_context");
    } else {
      // default io context
      return -1;
    }
  }

  // This list must be unique and complete set of names returned from
  // GetDedicatedIOContextIndex. Or you can get runtime crashes when accessing a missing
  // name, or get leaks by creating unused threads.
  constexpr static std::array<std::string_view, 4> kAllDedicatedIOContextNames{
      "task_io_context",
      "pubsub_io_context",
      "ray_syncer_io_context",
      "ray_event_io_context"};
  constexpr static std::array<bool, 4> kAllDedicatedIOContextEnableLagProbe{
      true, true, true, true};

  constexpr static size_t IndexOf(std::string_view name) {
    return ray::IndexOf(kAllDedicatedIOContextNames, name);
  }
};

}  // namespace gcs
}  // namespace ray
