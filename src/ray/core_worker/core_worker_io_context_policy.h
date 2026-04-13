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

#include <array>
#include <string_view>
#include <type_traits>

#include "ray/util/array.h"

namespace ray {
namespace core {

// Tag type for the dedicated pubsub io context in CoreWorker.
struct CoreWorkerPubsubIOContext {};

struct CoreWorkerIOContextPolicy {
  CoreWorkerIOContextPolicy() = delete;

  template <typename T>
  static constexpr int GetDedicatedIOContextIndex() {
    if constexpr (std::is_same_v<T, CoreWorkerPubsubIOContext>) {
      return IndexOf("core_worker.pubsub");
    } else {
      return -1;
    }
  }

  constexpr static std::array<std::string_view, 1> kAllDedicatedIOContextNames{
      "core_worker.pubsub"};
  constexpr static std::array<bool, 1> kAllDedicatedIOContextEnableLagProbe{true};

  constexpr static size_t IndexOf(std::string_view name) {
    return ray::IndexOf(kAllDedicatedIOContextNames, name);
  }
};

}  // namespace core
}  // namespace ray
