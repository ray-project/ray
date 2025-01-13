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

// NOTE: This file is proprietary and confidential. It is NOT licensed under
// Apache License. However in order to pass the linter at
// `ci/lint/lint.sh copyright_format`, a license header is added.

#pragma once

#include <array>
#include <string_view>
#include <type_traits>

#include "ray/common/ray_syncer/ray_syncer.h"
#include "ray/gcs/gcs_server/gcs_task_manager.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/util/array.h"
#include "ray/util/type_traits.h"

namespace ray {
namespace gcs {

// Anyscale-specific GCS server IO context policy.
// This policy should provide IO Contexts for types exactly as GcsServerIOContextPolicy,
// but with a different (more optimized) assignment. This is enforced by static_asserts.
struct AnyscaleGcsServerIOContextPolicy {
  AnyscaleGcsServerIOContextPolicy() = delete;

  // IOContext name for each handler.
  // If a class needs a dedicated io context, it should be specialized here.
  // If a class does NOT have a dedicated io context, returns -1;
  template <typename T>
  static constexpr int GetDedicatedIOContextIndex() {
    if constexpr (std::is_same_v<T, GcsTaskManager>) {
      return IndexOf("task_io_context");
    } else if constexpr (std::is_same_v<T, GcsPublisher>) {
      return IndexOf("pubsub_io_context");
    } else if constexpr (std::is_same_v<T, syncer::RaySyncer>) {
      return IndexOf("ray_syncer_io_context");
    } else if constexpr (std::is_same_v<T, GcsInternalKVManager>) {
      return IndexOf("internal_kv_io_context");
    } else {
      // Due to if-constexpr limitations, this have to be in an else block.
      // Using this template to put T into compile error message.
      static_assert(AlwaysFalse<T>, "unknown type");
    }
  }

  // This list must be unique and complete set of names returned from
  // GetDedicatedIOContextIndex. Or you can get runtime crashes when accessing a missing
  // name, or get leaks by creating unused threads.
  constexpr static std::array<std::string_view, 4> kAllDedicatedIOContextNames{
      "task_io_context",
      "pubsub_io_context",
      "ray_syncer_io_context",
      "internal_kv_io_context"};
  constexpr static std::array<bool, 4> kAllDedicatedIOContextEnableLagProbe{
      true, true, true, true};

  constexpr static size_t IndexOf(std::string_view name) {
    return ray::IndexOf(kAllDedicatedIOContextNames, name);
  }
};

}  // namespace gcs
}  // namespace ray
