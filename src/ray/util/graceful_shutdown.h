// Copyright 2026 The Ray Authors.
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

#include "absl/time/time.h"
#include "ray/util/logging.h"

namespace ray {

/// Interface for components that support graceful shutdown with the
/// "wait, flush, wait" pattern.
class GracefulShutdownHandler {
 public:
  virtual ~GracefulShutdownHandler() = default;

  /// Wait for in-flight operations to complete.
  /// @param timeout Maximum time to wait
  /// @return true if idle, false if timed out
  virtual bool WaitUntilIdle(absl::Duration timeout) = 0;

  /// Perform a final flush of buffered data.
  virtual void Flush() = 0;
};

/// Performs a graceful shutdown with the "wait, flush, wait" pattern.
///
/// This utility encapsulates the common shutdown pattern used by event buffers:
/// 1. Wait for any in-flight operations to complete
/// 2. Perform a final flush
/// 3. Wait for the flush operation to complete
///
/// Warnings are logged if any wait times out.
///
/// @param handler The component implementing GracefulShutdownHandler
/// @param timeout The maximum time to wait for shutdown
/// @param component_name Name of the component for logging (e.g., "TaskEventBuffer")
inline void GracefulShutdownWithFlush(GracefulShutdownHandler &handler,
                                      absl::Duration timeout,
                                      std::string_view component_name) {
  // First wait for any in-flight operations to complete, then flush, then wait again.
  // This ensures no data is lost during shutdown.
  if (!handler.WaitUntilIdle(timeout)) {
    RAY_LOG(WARNING) << component_name
                     << " shutdown timed out waiting for in-flight operations. "
                     << "Some events may be lost.";
    return;
  }

  handler.Flush();

  if (!handler.WaitUntilIdle(timeout)) {
    RAY_LOG(WARNING) << component_name
                     << " shutdown timed out waiting for flush to complete. "
                     << "Some events may be lost.";
  }
}

}  // namespace ray
