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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/observability/metric_interface.h"

namespace ray {

using SteadyClock = std::function<std::chrono::steady_clock::time_point()>;

/// The probe state machine. Tracks registered io_contexts, posts probes, and
/// evaluates health. Does NOT own a thread — call Tick() to advance one cycle.
///
/// This separation from IOContextMonitorThread allows deterministic unit testing
/// by calling Tick() directly with a fake clock.
class IOContextMonitor {
 public:
  /// @param component_name Human-readable name for logging (e.g. "gcs", "raylet").
  /// @param io_contexts Named io_contexts to monitor.
  /// @param lag_gauge Gauge metric for recording probe lag (ms). Tagged by "Name".
  /// @param deadline_exceeded_counter Counter incremented each time a probe exceeds
  ///   the deadline. Tagged by "Name".
  /// @param healthy_deadline_ms If a probe has been outstanding longer than this, the
  ///   io_context is considered unhealthy.
  /// @param clock Injectable clock for testing. Defaults to steady_clock::now.
  IOContextMonitor(
      std::string component_name,
      std::vector<std::pair<std::string, instrumented_io_context *>> io_contexts,
      observability::MetricInterface &lag_gauge,
      observability::MetricInterface &deadline_exceeded_counter,
      std::chrono::milliseconds healthy_deadline_ms = std::chrono::milliseconds{5000},
      SteadyClock clock = std::chrono::steady_clock::now);

  /// Run one probe cycle: check previous probes, emit metrics/logs, post new probes.
  /// Returns true if all registered io_contexts are healthy.
  bool Tick();

 private:
  struct ProbeState {
    std::string name;
    instrumented_io_context *io_context;
    std::atomic<bool> last_probe_completed{true};
    std::atomic<int64_t> probe_complete_time_ns{0};
    std::chrono::steady_clock::time_point probe_post_time{};
    bool healthy{true};
  };

  bool ProcessProbe(ProbeState &probe);

  const std::string component_name_;
  const std::chrono::milliseconds healthy_deadline_ms_;
  const SteadyClock clock_;

  observability::MetricInterface &lag_gauge_;
  observability::MetricInterface &deadline_exceeded_counter_;

  std::vector<std::unique_ptr<ProbeState>> probe_states_;
};

/// Runs an IOContextMonitor on a dedicated thread, calling Tick() at a
/// configurable interval.
class IOContextMonitorThread {
 public:
  /// @param monitor The monitor to drive. Takes ownership.
  /// @param probe_interval_ms How often to call monitor->Tick().
  /// @param health_callback Called from the monitor thread after each tick with the
  ///   health status. Useful for updating gRPC SetServingStatus.
  IOContextMonitorThread(std::unique_ptr<IOContextMonitor> monitor,
                         std::chrono::milliseconds probe_interval_ms,
                         std::function<void(bool healthy)> health_callback = nullptr);
  ~IOContextMonitorThread();

  IOContextMonitorThread(const IOContextMonitorThread &) = delete;
  IOContextMonitorThread &operator=(const IOContextMonitorThread &) = delete;

  void Start();
  void Stop();

 private:
  void Run();

  std::unique_ptr<IOContextMonitor> monitor_;
  std::chrono::milliseconds probe_interval_ms_;
  std::function<void(bool)> health_callback_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> running_{false};
  std::thread thread_;
};

}  // namespace ray
