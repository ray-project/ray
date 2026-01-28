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

#include <string_view>

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "ray/util/logging.h"

namespace ray {

/// Performs a graceful shutdown with the "wait, flush, wait" pattern.
///
/// This utility encapsulates the common shutdown pattern used by event buffers:
/// 1. Wait for any in-flight operations to complete
/// 2. Perform a final flush
/// 3. Wait for the flush operation to complete
///
/// @param mutex The mutex protecting the condition variable
/// @param cv The condition variable to wait on
/// @param is_idle_fn A callable that returns true when there are no in-flight operations
/// @param flush_fn A callable that performs the final flush
/// @param timeout The maximum time to wait for shutdown
/// @param component_name Name of the component for logging (e.g., "TaskEventBuffer")
/// @return true if shutdown completed successfully, false if timed out
template <typename IsIdleFn, typename FlushFn>
bool GracefulShutdownWithFlush(absl::Mutex &mutex,
                               absl::CondVar &cv,
                               IsIdleFn &&is_idle_fn,
                               FlushFn &&flush_fn,
                               absl::Duration timeout,
                               std::string_view component_name) {
  const auto shutdown_deadline = absl::Now() + timeout;

  // Helper to wait for in-flight operations to complete until the shared deadline.
  // Returns true if completed, false if timed out.
  auto wait_until_idle = [&](absl::Time deadline) {
    absl::MutexLock lock(&mutex);
    while (!is_idle_fn()) {
      if (cv.WaitWithDeadline(&mutex, deadline)) {
        return false;  // Timeout
      }
    }
    return true;  // Completed
  };

  // First wait for any in-flight operations to complete, then flush, then wait again.
  // This ensures no data is lost during shutdown.
  if (wait_until_idle(shutdown_deadline)) {
    flush_fn();
    wait_until_idle(shutdown_deadline);
    return true;
  } else {
    RAY_LOG(WARNING) << component_name << " shutdown timed out waiting for gRPC. "
                     << "Some events may be lost.";
    return false;
  }
}

}  // namespace ray
