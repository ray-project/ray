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

#include "ray/gcs/gcs_kv_manager.h"
#include "ray/gcs/gcs_node_manager.h"
#include "ray/gcs/gcs_task_manager.h"
#include "ray/observability/ray_event_recorder.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/ray_syncer/ray_syncer.h"
#include "ray/util/type_traits.h"

namespace ray {
namespace gcs {

/// Static metadata describing a single dedicated io_context.
struct IOContextMetadata {
  /// Name of the io_context (and its thread). Must be unique and non-empty.
  std::string_view name;
  /// Whether to enable the asio lag probe on this io_context.
  bool enable_lag_probe;
};

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
    } else if constexpr (std::is_same_v<T, pubsub::ObservabilityPublisher>) {
      return IndexOf("observability_pubsub_io_context");
    } else if constexpr (std::is_same_v<T, syncer::RaySyncer>) {
      return IndexOf("ray_syncer_io_context");
    } else if constexpr (std::is_same_v<T, observability::RayEventRecorder>) {
      return IndexOf("ray_event_io_context");
    } else if constexpr (std::is_same_v<T, GcsInternalKVManager>) {
      return IndexOf("internal_kv_io_context");
    } else if constexpr (std::is_same_v<T, GcsNodeManager>) {
      return IndexOf("node_manager_io_context");
    } else {
      // default io context
      return -1;
    }
  }

  // The complete set of dedicated io_contexts. Names must be unique, non-empty,
  // and a complete set of those returned from GetDedicatedIOContextIndex. Or you
  // can get runtime crashes when accessing a missing name, or get leaks by
  // creating unused threads.
  constexpr static std::array<IOContextMetadata, 7> kAllDedicatedIOContexts{{
      {"task_io_context", /*enable_lag_probe=*/true},
      {"pubsub_io_context", /*enable_lag_probe=*/true},
      {"observability_pubsub_io_context", /*enable_lag_probe=*/true},
      {"ray_syncer_io_context", /*enable_lag_probe=*/true},
      {"ray_event_io_context", /*enable_lag_probe=*/true},
      {"internal_kv_io_context", /*enable_lag_probe=*/true},
      {"node_manager_io_context", /*enable_lag_probe=*/true},
  }};

  constexpr static size_t IndexOf(std::string_view name) {
    for (size_t i = 0; i < kAllDedicatedIOContexts.size(); ++i) {
      if (kAllDedicatedIOContexts[i].name == name) {
        return i;
      }
    }
    // Throwing in constexpr context leads to a compile error.
    throw "Value not found in kAllDedicatedIOContexts";
  }
};

}  // namespace gcs
}  // namespace ray
