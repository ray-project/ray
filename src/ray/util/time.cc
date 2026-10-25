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

#include "ray/util/time.h"

namespace ray {

std::optional<std::chrono::steady_clock::time_point> ToTimeoutPoint(int64_t timeout_ms) {
  std::optional<std::chrono::steady_clock::time_point> timeout_point;
  if (timeout_ms == -1) {
    return timeout_point;
  }
  auto now = std::chrono::steady_clock::now();
  auto timeout_duration = std::chrono::milliseconds(timeout_ms);
  timeout_point.emplace(now + timeout_duration);
  return timeout_point;
}

}  // namespace ray
