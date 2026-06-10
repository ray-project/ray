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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "absl/synchronization/mutex.h"
#include "boost/asio/executor_work_guard.hpp"
#include "ray/asio/instrumented_io_context.h"
#include "ray/asio/periodical_runner.h"
#include "ray/common/cgroup/memory_pressure_reader.h"
#include "ray/raylet/memory_pressure_signal_monitor.h"

namespace ray {
namespace raylet {

// Default implementation of ratio + hysteresis + consecutive-hit decision.
//
// Self-driven: on construction it starts an independent io_service + thread +
// PeriodicalRunner, calling Poll() periodically per poll_interval_ms; on
// destruction it stops the io_service + joins the thread (aligned with
// ThresholdMemoryMonitor).
//
// Thread safety: Poll runs on its own thread, while GetCurrentSignal/OnResize are
// called by the NodeManager main thread; all shared state is guarded by mu_.
class RatioHysteresisPressureMonitor : public MemoryPressureSignalMonitor {
 public:
  struct Config {
    std::string pod_name;        // read from POD_NAME env in CreatePressureMonitor
    double pressure_ratio = 0.85;
    double release_hysteresis = 0.10;  // release threshold = ratio - hysteresis
    int consecutive_hits = 2;
    int release_consecutive_hits = 3;
    int64_t persistent_warning_interval_ms = 60000;  // rate-limit
    // Sampling period. Passing 0 does not start the internal self-driven sampling
    // thread (PeriodicalRunner returns immediately for period<=0); in that case
    // Poll() can only be driven manually from outside -- which is how unit tests
    // get a deterministic, race-free driver. The production path is always
    // positive (default 2000).
    uint64_t poll_interval_ms = 2000;
  };

  RatioHysteresisPressureMonitor(std::unique_ptr<MemoryPressureReader> reader,
                                 Config config);
  ~RatioHysteresisPressureMonitor() override;

  std::optional<double> GetCurrentSignal() const override;
  // Returns whether this resize was pressure-driven; for full semantics see
  // MemoryPressureSignalMonitor::OnResize.
  bool OnResize() override;

  // A single sample. In production this is driven by the internal
  // PeriodicalRunner; unit tests call it directly to drive it manually. Unit
  // tests must set poll_interval_ms to 0 to disable the internal self-driven
  // thread (see Config::poll_interval_ms), then call Poll() manually -- otherwise
  // the internal thread and the test thread calling Poll() concurrently would
  // race on the injected reader_ (the state machine is guarded by mu_, but
  // reader_'s thread safety depends on the reader implementation).
  void Poll();

  // Lets unit tests override the wall-clock source.
  using ClockFn = std::function<int64_t()>;
  void SetClockForTest(ClockFn fn);

  // Lets unit tests observe the Config wired in at construction time.
  // CreatePressureMonitor() returns through the interface, which does not expose
  // Config; factory-function unit tests dynamic_cast to this and then assert
  // field by field that the env->Config mapping is correct. config_ is immutable
  // after construction, so no locking is needed.
  const Config &GetConfigForTest() const { return config_; }

 private:
  static int64_t DefaultClockMs();
  // The caller must already hold mu_.
  void MaybeEmitPersistentWarningLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // reader_ and config_ are immutable after construction (neither the pointer nor
  // the value is rewritten), so they need no mu_ guarding. reader_'s Read() is
  // only called by Poll(); see Poll()'s concurrency constraints.
  std::unique_ptr<MemoryPressureReader> reader_;
  Config config_;

  mutable absl::Mutex mu_;
  ClockFn clock_ ABSL_GUARDED_BY(mu_);
  int consecutive_hits_ ABSL_GUARDED_BY(mu_) = 0;
  int release_hits_ ABSL_GUARDED_BY(mu_) = 0;
  int64_t last_persistent_warning_ms_ ABSL_GUARDED_BY(mu_) = 0;
  int64_t last_read_failure_warning_ms_ ABSL_GUARDED_BY(mu_) = 0;
  // The latest pressure ratio (cgroup current/limit); empty when no pressure.
  std::optional<double> current_signal_ ABSL_GUARDED_BY(mu_);
  // Whether the most recent Poll was "at the ceiling" (ratio >= 0.99 and the
  // signal active). Recomputed at the end of each Poll, reset to false by
  // OnResize(). Used only for the "sustained at-ceiling" warning tiering.
  bool at_max_capacity_ ABSL_GUARDED_BY(mu_) = false;
  // Latch: set when the signal first activates, reset only by OnResize().
  // Distinct from current_signal_ (which the Poll release path clears on its own).
  bool had_active_signal_since_reset_ ABSL_GUARDED_BY(mu_) = false;

  // Self-driving facilities (declaration order is initialization order:
  // io_service -> work_guard -> thread -> runner).
  instrumented_io_context io_service_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
  std::thread thread_;
  std::shared_ptr<PeriodicalRunner> runner_;
};

}  // namespace raylet
}  // namespace ray
