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

#include "ray/asio/io_context_monitor.h"

#include "absl/time/clock.h"
#include "ray/util/logging.h"
#include "ray/util/thread_utils.h"

namespace ray {

// ---------------------------------------------------------------------------
// IOContextMonitor
// ---------------------------------------------------------------------------

IOContextMonitor::IOContextMonitor(
    std::string component_name,
    std::vector<std::pair<std::string, instrumented_io_context *>> io_contexts,
    observability::MetricInterface &latency_gauge,
    observability::MetricInterface &unhealthy_counter,
    absl::Duration healthy_deadline,
    absl::Duration latency_window,
    std::shared_ptr<ClockInterface> clock)
    : component_name_(std::move(component_name)),
      healthy_deadline_(healthy_deadline),
      clock_(std::move(clock)),
      latency_gauge_(latency_gauge),
      unhealthy_counter_(unhealthy_counter) {
  for (auto &[name, io_context] : io_contexts) {
    probe_states_.push_back(std::make_shared<ProbeState>(
        std::move(name), *io_context, clock_, latency_window));
  }
}

bool IOContextMonitor::Tick() {
  bool all_healthy = true;
  for (auto &probe : probe_states_) {
    if (!ProcessProbe(probe)) {
      all_healthy = false;
    }
  }
  return all_healthy;
}

bool IOContextMonitor::ProcessProbe(const std::shared_ptr<ProbeState> &probe) {
  absl::MutexLock lock(&probe->mu);
  absl::Time now = clock_->Now();

  // Time elapsed for the probe: the actual lag if it has completed, or the
  // wall-clock time since posting if it's still outstanding.
  absl::Duration elapsed =
      (probe->last_probe_completed ? probe->probe_complete_time : now) -
      probe->probe_post_time;
  bool has_active_probe = probe->probe_post_time != absl::InfinitePast();

  // Check if the probe has exceeded the deadline, whether or not it has finished.
  // The deadline_warning_logged guard ensures each probe is counted at most once.
  if (has_active_probe && elapsed >= healthy_deadline_ &&
      !probe->deadline_warning_logged) {
    RAY_LOG(WARNING) << "[" << component_name_ << "] io_context '" << probe->name
                     << "' exceeded probe deadline ("
                     << absl::ToInt64Milliseconds(elapsed) << "ms)";
    probe->healthy = false;
    probe->deadline_warning_logged = true;
    unhealthy_counter_.Record(1, {{"Name", probe->name}});
  }

  // A new probe will only be started once the existing one completes.
  if (probe->last_probe_completed) {
    // Record latency and health status from the completed probe, then post a new one.
    if (has_active_probe) {
      // Feed the completed probe's latency into the sliding window. Only export the
      // metric when the windowed max changes to avoid redundant updates.
      if (auto windowed_max_ms =
              probe->latency_window.Add(now, absl::ToDoubleMilliseconds(elapsed))) {
        latency_gauge_.Record(*windowed_max_ms, {{"Name", probe->name}});
      }

      // Only mark healthy if the probe's actual lag was within the deadline.
      if (elapsed < healthy_deadline_) {
        probe->healthy = true;
      }
    }

    // Post a new probe. The callback captures the shared_ptr to keep the
    // ProbeState alive even if the monitor is destroyed while a probe is
    // outstanding.
    probe->probe_post_time = now;
    probe->last_probe_completed = false;
    probe->deadline_warning_logged = false;
    probe->io_context.post([probe]() { ExecuteProbeOnIOContext(probe); },
                           "io_context_monitor_probe");
  }

  return probe->healthy;
}

void IOContextMonitor::ExecuteProbeOnIOContext(const std::shared_ptr<ProbeState> &probe) {
  absl::MutexLock lock(&probe->mu);
  probe->probe_complete_time = probe->clock->Now();
  probe->last_probe_completed = true;
}

// ---------------------------------------------------------------------------
// IOContextMonitorThread
// ---------------------------------------------------------------------------

IOContextMonitorThread::IOContextMonitorThread(
    std::unique_ptr<IOContextMonitor> monitor,
    absl::Duration probe_interval,
    std::function<void(bool healthy)> health_callback)
    : monitor_(std::move(monitor)),
      probe_interval_(probe_interval),
      health_callback_(std::move(health_callback)) {}

IOContextMonitorThread::~IOContextMonitorThread() { Stop(); }

void IOContextMonitorThread::Start() {
  absl::MutexLock lock(&mutex_);
  if (running_) {
    return;
  }
  running_ = true;
  thread_ = std::thread([this] { Run(); });
}

void IOContextMonitorThread::Stop() {
  {
    absl::MutexLock lock(&mutex_);
    if (!running_) {
      return;
    }
    running_ = false;
  }  // Release lock before join so the thread can observe the change.
  if (thread_.joinable()) {
    thread_.join();
  }
}

void IOContextMonitorThread::Run() {
  SetThreadName("io_context_monitor");

  while (true) {
    bool healthy = monitor_->Tick();
    health_callback_(healthy);

    absl::MutexLock lock(&mutex_);
    mutex_.AwaitWithTimeout(absl::Condition(
                                +[](bool *running) { return !*running; }, &running_),
                            probe_interval_);

    if (!running_) {
      break;
    }
  }
}

}  // namespace ray
