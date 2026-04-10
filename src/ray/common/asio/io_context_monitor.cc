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

#include "ray/common/asio/io_context_monitor.h"

#include "ray/util/logging.h"
#include "ray/util/clock.h"
#include "ray/util/thread_utils.h"

namespace ray {

IOContextMonitor::IOContextMonitor(
    std::string component_name,
    std::vector<std::pair<std::string, instrumented_io_context *>> io_contexts,
    observability::MetricInterface &lag_gauge,
    observability::MetricInterface &deadline_exceeded_counter,
    absl::Duration healthy_deadline,
    ClockInterface &clock)
    : component_name_(std::move(component_name)),
      healthy_deadline_(healthy_deadline),
      clock_(clock),
      lag_gauge_(lag_gauge),
      deadline_exceeded_counter_(deadline_exceeded_counter) {
  for (auto &[name, io_context] : io_contexts) {
    auto state = std::make_unique<ProbeState>();
    state->name = std::move(name);
    state->io_context = io_context;
    probe_states_.push_back(std::move(state));
  }
}

bool IOContextMonitor::Tick() {
  bool all_healthy = true;
  for (auto &probe : probe_states_) {
    if (!ProcessProbe(*probe)) {
      all_healthy = false;
    }
  }
  return all_healthy;
}

bool IOContextMonitor::ProcessProbe(ProbeState &probe) {
  absl::Time now = clock_.Now();

  if (!probe.last_probe_completed.load(std::memory_order_acquire)) {
    // Previous probe still outstanding.
    absl::Duration elapsed = now - probe.probe_post_time;
    int64_t elapsed_ms = absl::ToInt64Milliseconds(elapsed);

    RAY_LOG(WARNING) << "[" << component_name_ << "] io_context '" << probe.name
                     << "' has not responded to probe (" << elapsed_ms << "ms elapsed)";

    if (elapsed >= healthy_deadline_) {
      if (probe.healthy) {
        deadline_exceeded_counter_.Record(1, {{"Name", probe.name}});
      }
      probe.healthy = false;
    }
    // Do NOT post another probe — avoid accumulation on stuck threads.
    return probe.healthy;
  }

  // Previous probe completed. Record actual lag.
  if (probe.probe_post_time != absl::InfinitePast()) {
    int64_t complete_ns = probe.probe_complete_time_ns.load(std::memory_order_acquire);
    absl::Time complete_time = absl::FromUnixNanos(complete_ns);
    double lag_ms = absl::ToDoubleMilliseconds(complete_time - probe.probe_post_time);
    lag_gauge_.Record(lag_ms, {{"Name", probe.name}});

    // Only mark healthy if we're still within the deadline window. If we're
    // past the deadline, the probe completed late — wait for a fresh one.
    if (now - probe.probe_post_time < healthy_deadline_) {
      probe.healthy = true;
    }
  }

  // Post a new probe.
  probe.last_probe_completed.store(false, std::memory_order_release);
  probe.probe_post_time = now;

  auto *probe_ptr = &probe;
  probe.io_context->post(
      [probe_ptr]() {
        int64_t complete_ns = absl::GetCurrentTimeNanos();
        probe_ptr->probe_complete_time_ns.store(complete_ns, std::memory_order_release);
        probe_ptr->last_probe_completed.store(true, std::memory_order_release);
      },
      "io_context_monitor_probe");

  return probe.healthy;
}

IOContextMonitorThread::IOContextMonitorThread(
    std::unique_ptr<IOContextMonitor> monitor,
    absl::Duration probe_interval,
    std::function<void(bool healthy)> health_status_callback)
    : monitor_(std::move(monitor)),
      probe_interval_(probe_interval),
      health_status_callback_(std::move(health_status_callback)) {}

IOContextMonitorThread::~IOContextMonitorThread() { Stop(); }

void IOContextMonitorThread::Start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return;
  }
  thread_ = std::thread([this] { Run(); });
}

void IOContextMonitorThread::Stop() {
  {
    absl::MutexLock lock(&mutex_);
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
      return;
    }
  }
  if (thread_.joinable()) {
    thread_.join();
  }
}

void IOContextMonitorThread::Run() {
  SetThreadName("io_context_monitor");

  while (running_.load(std::memory_order_relaxed)) {
    bool healthy = monitor_->Tick();
    if (health_status_callback_) {
      health_status_callback_(healthy);
    }

    absl::MutexLock lock(&mutex_);
    mutex_.AwaitWithTimeout(
        absl::Condition(
            +[](std::atomic<bool> *running) { return !running->load(); }, &running_),
        probe_interval_);
  }
}

}  // namespace ray
