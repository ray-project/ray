// Copyright 2022 The Ray Authors.
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

#include <gtest/gtest_prod.h>

#include <cstdint>
#include <memory>

#include "ray/common/asio/periodical_runner.h"
#include "ray/common/memory_monitor.h"

namespace ray {

/// File-system based memory monitor with threshold support.
/// Monitors the memory usage of the node using /proc filesystem and cgroups.
/// It checks the memory usage periodically and invokes the callback.
/// This class is thread safe.
class ThresholdMemoryMonitor : public MemoryMonitor {
 public:
  /// Constructor.
  ///
  /// \param io_service the event loop.
  /// \param usage_threshold a value in [0-1] to indicate the max usage.
  /// \param min_memory_free_bytes to indicate the minimum amount of free space before it
  /// becomes over the threshold.
  /// \param monitor_interval_ms the frequency to update the usage. 0 disables the the
  /// monitor and callbacks won't fire.
  /// \param monitor_callback function to execute on a dedicated thread owned by this
  /// monitor when the usage is refreshed.
  ThresholdMemoryMonitor(instrumented_io_context &io_service,
                         KillWorkersCallback kill_workers_callback,
                         float usage_threshold,
                         int64_t min_memory_free_bytes,
                         uint64_t monitor_interval_ms);

 private:
  /// The computed threshold in bytes based on usage_threshold_ and
  /// min_memory_free_bytes_.
  int64_t computed_threshold_bytes_;

  /// The computed threshold fraction on usage_threshold_ and min_memory_free_bytes_.
  float computed_threshold_fraction_;

  /// Periodical runner for memory monitoring.
  std::shared_ptr<PeriodicalRunner> runner_;
};

}  // namespace ray
