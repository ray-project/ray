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

#include "ray/common/memory_monitor.h"

namespace ray {

/// A no-op memory monitor that does not perform any monitoring.
/// Used on platforms where memory monitoring is not supported (e.g., non-Linux).
class NoopMemoryMonitor : public MemoryMonitor {
 public:
  /// Constructor.
  ///
  /// \param io_service the event loop (unused in noop implementation).
  /// \param monitor_callback function callback (unused in noop implementation).
  NoopMemoryMonitor(instrumented_io_context &io_service,
                    KillWorkersCallback kill_workers_callback)
      : MemoryMonitor(io_service, kill_workers_callback) {
    // No-op: does not start any periodic monitoring
  }

  // Uncopyable type.
  NoopMemoryMonitor(const NoopMemoryMonitor &) = delete;
  NoopMemoryMonitor &operator=(const NoopMemoryMonitor &) = delete;
  NoopMemoryMonitor(NoopMemoryMonitor &&) = default;
  NoopMemoryMonitor &operator=(NoopMemoryMonitor &&) = default;
  ~NoopMemoryMonitor() = default;
};

}  // namespace ray
