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
/// evaluates health based on the latency to execute the probes.
///
/// This separation from IOContextMonitorThread allows deterministic unit testing
/// by calling Tick() directly with a FakeClock.
class IOContextMonitor {
 public:
  /// @param component_name Human-readable name for logging (e.g. "gcs", "raylet").
  /// @param io_contexts Named io_contexts to monitor. Must outlive the monitor.
  /// @param lag_gauge Gauge metric for recording probe lag (ms). Tagged by "Name".
  /// @param deadline_exceeded_counter Counter incremented each time a probe exceeds
  ///   the deadline. Tagged by "Name".
  /// @param healthy_deadline If a probe has been outstanding longer than this, the
  ///   io_context is considered unhealthy.
  /// @param clock Clock to use for time. Defaults to a real clock. Inject a
  ///   FakeClock in tests for deterministic behavior.
  IOContextMonitor(
      std::string component_name,
      std::vector<std::pair<std::string, instrumented_io_context *>> io_contexts,
      observability::MetricInterface &lag_gauge,
      observability::MetricInterface &deadline_exceeded_counter,
      absl::Duration healthy_deadline,
      std::shared_ptr<ClockInterface> clock = std::make_shared<Clock>());

  /// Run one probe cycle: check previous probes, emit metrics/logs, post new probes.
  /// Returns true iff all registered io_contexts are healthy.
  bool Tick();

 private:
  struct ProbeState {
    ProbeState(std::string name,
               instrumented_io_context &io_context,
               std::shared_ptr<ClockInterface> clock)
        : name(std::move(name)), io_context(io_context), clock(std::move(clock)) {}

    const std::string name;
    instrumented_io_context &io_context;
    const std::shared_ptr<ClockInterface> clock;

    // Mutex protecting fields written by the probe callback (on the io_context
    // thread) and read by the monitor in CheckProbeStatus.
    absl::Mutex mu;
    // Defaults to true to kick off the probe in the first iteration.
    bool last_probe_completed ABSL_GUARDED_BY(mu) = true;
    absl::Time probe_complete_time ABSL_GUARDED_BY(mu) = absl::InfinitePast();

    // Only accessed from the monitor (via Tick/CheckProbeStatus).
    absl::Time probe_post_time = absl::InfinitePast();
    bool healthy = true;
    bool deadline_exceeded_recorded = false;
  };

  bool CheckProbeStatus(const std::shared_ptr<ProbeState> &probe);
  static void ExecuteProbeOnIOContext(const std::shared_ptr<ProbeState> &probe)
      ABSL_LOCKS_EXCLUDED(probe->mu);

  const std::string component_name_;
  const absl::Duration healthy_deadline_;
  const std::shared_ptr<ClockInterface> clock_;
  observability::MetricInterface &lag_gauge_;
  observability::MetricInterface &deadline_exceeded_counter_;
  std::vector<std::shared_ptr<ProbeState>> probe_states_;
};

/// Runs an IOContextMonitor on a dedicated thread, calling Tick() at a
/// configurable interval.
class IOContextMonitorThread {
 public:
  /// @param monitor The monitor to call into.
  /// @param probe_interval How often to call monitor->Tick().
  /// @param health_callback Called from the monitor thread after each tick with the
  ///   health status (true means healthy).
  IOContextMonitorThread(std::unique_ptr<IOContextMonitor> monitor,
                         absl::Duration probe_interval,
                         std::function<void(bool healthy)> health_callback);
  ~IOContextMonitorThread();

  IOContextMonitorThread(const IOContextMonitorThread &) = delete;
  IOContextMonitorThread &operator=(const IOContextMonitorThread &) = delete;

  void Start();
  void Stop();

 private:
  void Run() ABSL_LOCKS_EXCLUDED(mutex_);

  std::unique_ptr<IOContextMonitor> monitor_;
  const absl::Duration probe_interval_;
  const std::function<void(bool)> health_callback_;
  absl::Mutex mutex_;
  bool running_ ABSL_GUARDED_BY(mutex_) = false;
  std::thread thread_;
};

}  // namespace ray
