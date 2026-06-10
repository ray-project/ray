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
#include "ray/raylet/ratio_hysteresis_pressure_monitor.h"

#include <utility>

#include "ray/util/logging.h"
#include "ray/util/thread_utils.h"
#include "ray/util/time.h"

namespace ray {
namespace raylet {

RatioHysteresisPressureMonitor::RatioHysteresisPressureMonitor(
    std::unique_ptr<MemoryPressureReader> reader, Config config)
    : reader_(std::move(reader)),
      config_(std::move(config)),
      clock_(&RatioHysteresisPressureMonitor::DefaultClockMs),
      io_service_(/*enable_metrics=*/false,
                  /*running_on_single_thread=*/true,
                  "MemoryPressureMonitor.IOContext"),
      work_guard_(boost::asio::make_work_guard(io_service_.get_executor())),
      thread_([this] {
        SetThreadName("MemoryPressureMonitor.IOContextThread");
        io_service_.run();
      }),
      runner_(PeriodicalRunner::Create(io_service_)) {
  // Clamp release_hysteresis to prevent a misconfiguration (e.g. 0.10 written as 1.0)
  // from driving release_threshold negative, which would make the signal impossible to
  // clear. Keep a minimum effective release window of 0.01 to ensure the
  // `ratio < release_threshold` semantics in Poll() remain reachable.
  if (config_.release_hysteresis < 0.0) {
    config_.release_hysteresis = 0.0;
  }
  const double max_hysteresis = config_.pressure_ratio - 0.01;
  if (config_.release_hysteresis > max_hysteresis) {
    RAY_LOG(WARNING) << "RatioHysteresisPressureMonitor release_hysteresis="
                     << config_.release_hysteresis
                     << " >= pressure_ratio - 0.01; clamping to " << max_hysteresis
                     << " so the release threshold stays above zero.";
    config_.release_hysteresis = max_hysteresis;
  }
  runner_->RunFnPeriodically([this] { Poll(); },
                             config_.poll_interval_ms,
                             "MemoryPressureMonitor.Poll");
}

RatioHysteresisPressureMonitor::~RatioHysteresisPressureMonitor() {
  runner_.reset();
  io_service_.stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}

int64_t RatioHysteresisPressureMonitor::DefaultClockMs() { return current_sys_time_ms(); }

void RatioHysteresisPressureMonitor::SetClockForTest(ClockFn fn) {
  absl::MutexLock lock(&mu_);
  clock_ = std::move(fn);
}

void RatioHysteresisPressureMonitor::Poll() {
  if (reader_ == nullptr) {
    return;
  }
  int64_t current = 0;
  int64_t limit = 0;
  Status s = reader_->Read(&current, &limit);

  absl::MutexLock lock(&mu_);
  if (!s.ok()) {
    int64_t now = clock_();
    if (now - last_read_failure_warning_ms_ >= 60000) {
      RAY_LOG(WARNING) << "RatioHysteresisPressureMonitor read failed: " << s.ToString()
                       << " (subsequent failures throttled 60s)";
      last_read_failure_warning_ms_ = now;
    }
    return;
  }
  if (limit <= 0) {
    // Uncapped cgroup — ratio is meaningless, skip.
    return;
  }
  const double ratio = static_cast<double>(current) / static_cast<double>(limit);
  const double release_threshold = config_.pressure_ratio - config_.release_hysteresis;

  if (ratio >= config_.pressure_ratio) {
    ++consecutive_hits_;
    release_hits_ = 0;
    if (consecutive_hits_ >= config_.consecutive_hits) {
      const bool was_active = current_signal_.has_value();
      current_signal_ = ratio;
      // Latch stays set across the cgroup-enlargement-driven decay of
      // current_signal_; only OnResize() clears it. See header.
      had_active_signal_since_reset_ = true;
      if (!was_active) {
        RAY_LOG(INFO) << "RatioHysteresisPressureMonitor signal activated pod="
                      << config_.pod_name << " ratio=" << ratio;
      }
    }
  } else if (ratio < release_threshold) {
    ++release_hits_;
    consecutive_hits_ = 0;
    if (current_signal_.has_value() &&
        release_hits_ >= config_.release_consecutive_hits) {
      RAY_LOG(INFO) << "RatioHysteresisPressureMonitor signal cleared pod="
                    << config_.pod_name << " ratio=" << ratio;
      current_signal_.reset();
      // Reset to zero after clearing to avoid pointlessly accumulating the count while
      // continuously below release_threshold.
      release_hits_ = 0;
    }
  } else {
    // Between release_threshold and pressure_ratio — neither accrue nor
    // release. Breaks both counters so a spike-then-hover doesn't linger.
    consecutive_hits_ = 0;
    release_hits_ = 0;
  }

  // Persistent pressure at max capacity — warn at most once per
  // persistent_warning_interval_ms. The cgroup instantaneous count rarely equals limit
  // exactly (kernel counting granularity / reserved pages / OOM kill firing before the
  // ceiling), so the "at ceiling" determination instead uses ratio >= kAtMaxRatio, from
  // the same source as the accumulation path above.
  constexpr double kAtMaxRatio = 0.99;
  at_max_capacity_ = current_signal_.has_value() && ratio >= kAtMaxRatio;
  MaybeEmitPersistentWarningLocked();
}

bool RatioHysteresisPressureMonitor::OnResize() {
  absl::MutexLock lock(&mu_);
  // Read the "was there ever pressure since last time" latch as the return value
  // (distinguishing pressure-driven vs. task-driven resize), completing the reset within
  // the same critical section. By the time the resize RPC arrives, kubelet has already
  // enlarged the cgroup limit, so the live signal may have decayed; hence we must use a
  // latch that spans the decay rather than current_signal_.
  const bool was_pressure_driven = had_active_signal_since_reset_;
  consecutive_hits_ = 0;
  release_hits_ = 0;
  current_signal_.reset();
  at_max_capacity_ = false;
  last_persistent_warning_ms_ = 0;
  had_active_signal_since_reset_ = false;
  RAY_LOG(INFO) << "RatioHysteresisPressureMonitor OnResize pod=" << config_.pod_name
                << " was_pressure_driven=" << was_pressure_driven;
  return was_pressure_driven;
}

void RatioHysteresisPressureMonitor::MaybeEmitPersistentWarningLocked() {
  if (!current_signal_.has_value()) {
    return;
  }
  const int64_t now = clock_();
  if (now - last_persistent_warning_ms_ < config_.persistent_warning_interval_ms) {
    return;
  }
  if (at_max_capacity_) {
    RAY_LOG(WARNING) << "memory pressure persists at max capacity for pod="
                     << config_.pod_name << " ratio=" << *current_signal_;
  } else {
    RAY_LOG(INFO) << "memory pressure persists for pod=" << config_.pod_name
                  << " ratio=" << *current_signal_;
  }
  last_persistent_warning_ms_ = now;
}

std::optional<double> RatioHysteresisPressureMonitor::GetCurrentSignal() const {
  absl::MutexLock lock(&mu_);
  return current_signal_;
}

}  // namespace raylet
}  // namespace ray
