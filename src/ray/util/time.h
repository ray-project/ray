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

#include <chrono>
#include <optional>

namespace ray {

/// Return the number of milliseconds since the steady clock epoch. NOTE: The
/// returned timestamp may be used for accurately measuring intervals but has
/// no relation to wall clock time. It must not be used for synchronization
/// across multiple nodes.
inline int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

inline int64_t current_time_ns() {
  std::chrono::nanoseconds ns_since_epoch =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ns_since_epoch.count();
}

inline int64_t current_sys_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

inline int64_t current_sys_time_s() {
  std::chrono::seconds s_since_epoch = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch());
  return s_since_epoch.count();
}

/// Converts a timeout in milliseconds to a timeout point.
/// \param timeout_ms The timeout in milliseconds.
/// \return The timeout point, or std::nullopt if timeout_ms is -1.
std::optional<std::chrono::steady_clock::time_point> ToTimeoutPoint(int64_t timeout_ms);

}  // namespace ray
