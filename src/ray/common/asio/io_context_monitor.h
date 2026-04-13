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

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/observability/metric_interface.h"
#include "ray/util/clock.h"

namespace ray {

/// The probe state machine. Tracks registered io_contexts, posts probes, and
/// evaluates health. Does NOT own a thread — call Tick() to advance one cycle.
///
/// This separation from IOContextMonitorThread allows deterministic unit testing
/// by calling Tick() directly with a FakeClock.
class IOContextMonitor {
 public:
  /// @param component_name Human-readable name for logging (e.g. "gcs", "raylet").
  /// @param io_contexts Named io_contexts to monitor.
  /// @param lag_gauge Gauge metric for recording probe lag (ms). Tagged by "Name".
  /// @param deadline_exceeded_counter Counter incremented each time a probe exceeds
  ///   the deadline. Tagged by "Name".
  /// @param healthy_deadline If a probe has been outstanding longer than this, the
  ///   io_context is considered unhealthy.
  /// @param clock Clock to use for time. Must outlive the monitor.
  IOContextMonitor(
      std::string component_name,
      std::vector<std::pair<std::string, instrumented_io_context *>> io_contexts,
      observability::MetricInterface &lag_gauge,
      observability::MetricInterface &deadline_exceeded_counter,
      absl::Duration healthy_deadline,
      ClockInterface &clock);

  /// Run one probe cycle: check previous probes, emit metrics/logs, post new probes.
  /// Returns true if all registered io_contexts are healthy.
  bool Tick();

 private:
  struct ProbeState : std::enable_shared_from_this<ProbeState> {
    std::string name;
    instrumented_io_context *io_context;
    std::atomic<bool> last_probe_completed{true};
    std::atomic<int64_t> probe_complete_time_ns{0};
    absl::Time probe_post_time = absl::InfinitePast();
    bool healthy{true};
  };

  bool ProcessProbe(ProbeState &probe);

  const std::string component_name_;
  const absl::Duration healthy_deadline_;
  ClockInterface &clock_;

  observability::MetricInterface &lag_gauge_;
  observability::MetricInterface &deadline_exceeded_counter_;

  std::vector<std::shared_ptr<ProbeState>> probe_states_;
};

/// Runs an IOContextMonitor on a dedicated thread, calling Tick() at a
/// configurable interval.
class IOContextMonitorThread {
 public:
  /// @param monitor The monitor to drive. Takes ownership.
  /// @param probe_interval How often to call monitor->Tick().
  /// @param health_status_callback Called from the monitor thread after each tick with
  /// the
  ///   health status.
  IOContextMonitorThread(std::unique_ptr<IOContextMonitor> monitor,
                         absl::Duration probe_interval,
                         std::function<void(bool healthy)> health_status_callback);
  ~IOContextMonitorThread();

  IOContextMonitorThread(const IOContextMonitorThread &) = delete;
  IOContextMonitorThread &operator=(const IOContextMonitorThread &) = delete;

  void Start();
  void Stop();

 private:
  void Run();

  std::unique_ptr<IOContextMonitor> monitor_;
  absl::Duration probe_interval_;
  std::function<void(bool)> health_status_callback_;
  absl::Mutex mutex_;
  std::atomic<bool> running_{false};
  std::thread thread_;
};

}  // namespace ray
