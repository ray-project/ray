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

#include <optional>

namespace ray {
namespace raylet {

// Per-raylet IPPR memory-pressure monitor interface.
//
// Semantically distinct from ray::MemoryMonitorInterface
// (src/ray/common/memory_monitor_interface.h): the latter produces a KillWorkersCallback
// (a local kill-worker side effect); this interface produces a pollable
// memory-pressure ratio (an upward, autoscaler-driven scale-up). The two are not merged.
//
// Implementations carry their own sampling loop (self-driven). All methods MUST be
// thread-safe: an implementation's sampling runs on its own thread, while
// GetCurrentSignal / OnResize are called from the NodeManager main thread.
class MemoryPressureSignalMonitor {
 public:
  virtual ~MemoryPressureSignalMonitor() = default;

  // Pull the latest pressure ratio (cgroup current/limit); empty when there is no
  // pressure. Used by NodeManager to fill ResourcesData and DebugString.
  virtual std::optional<double> GetCurrentSignal() const = 0;

  // Notify that a resize has landed. Performs atomically: (1) read the "was there ever
  // pressure since last time" latch as the return value; (2) clear the current signal,
  // reset the debounce counter, and reset the latch. The return value indicates whether
  // this resize was driven by memory pressure, for use by NodeManager's isolation
  // decision.
  //
  // Why a return value rather than a two-step read + reset: by the time the resize RPC
  // reaches the raylet, kubelet has already enlarged the cgroup limit, so GetCurrentSignal
  // has most likely decayed because the ratio fell below the release threshold; hence we
  // must read a latch that spans the decay. Combining the read and reset into a single
  // atomic call removes any need for NodeManager to reason about the timing contract.
  virtual bool OnResize() = 0;
};

}  // namespace raylet
}  // namespace ray
