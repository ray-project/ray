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

#include "ray/common/threshold_memory_monitor.h"

#include "absl/strings/str_format.h"
#include "ray/common/memory_monitor_utils.h"
#include "ray/util/logging.h"
#include "ray/util/thread_utils.h"

namespace ray {

ThresholdMemoryMonitor::ThresholdMemoryMonitor(KillWorkersCallback kill_workers_callback,
                                               float usage_threshold,
                                               int64_t min_memory_free_bytes,
                                               uint64_t monitor_interval_ms,
                                               const std::string root_cgroup_path)
    : kill_workers_callback_(std::move(kill_workers_callback)),
      worker_killing_in_progress_(false),
      root_cgroup_path_(root_cgroup_path),
      io_service_(/*enable_metrics=*/false,
                  /*running_on_single_thread=*/true,
                  "MemoryMonitor.IOContext"),
      work_guard_(boost::asio::make_work_guard(io_service_.get_executor())),
      thread_([this] {
        SetThreadName("MemoryMonitor.IOContextThread");
        io_service_.run();
      }),
      runner_(PeriodicalRunner::Create(io_service_)) {
  RAY_CHECK_GE(usage_threshold, 0)
      << "Invalid configuration: usage_threshold must be >= 0";
  RAY_CHECK_LE(usage_threshold, 1)
      << "Invalid configuration: usage_threshold must be <= 1";

  int64_t total_memory_bytes =
      MemoryMonitorUtils::TakeSystemMemorySnapshot(root_cgroup_path_).total_bytes;
  computed_threshold_bytes_ = MemoryMonitorUtils::GetMemoryThreshold(
      total_memory_bytes, usage_threshold, min_memory_free_bytes);
  float computed_threshold_fraction = static_cast<float>(computed_threshold_bytes_) /
                                      static_cast<float>(total_memory_bytes);
  RAY_LOG(INFO) << absl::StrFormat(
      "MemoryMonitor initialized with usage threshold at %d bytes "
      "(%f%% of system memory), total system memory bytes: %d, monitor interval: %dms",
      computed_threshold_bytes_,
      computed_threshold_fraction * 100,
      total_memory_bytes,
      monitor_interval_ms);

  runner_->RunFnPeriodically(
      [this] {
        SystemMemorySnapshot cur_memory_snapshot =
            MemoryMonitorUtils::TakeSystemMemorySnapshot(root_cgroup_path_);

        bool is_usage_above_threshold =
            IsUsageAboveThreshold(cur_memory_snapshot, computed_threshold_bytes_);

        if (is_usage_above_threshold && IsEnabled()) {
          Disable();
          kill_workers_callback_(cur_memory_snapshot);
        }
      },
      monitor_interval_ms,
      "MemoryMonitor.CheckIsMemoryUsageAboveThreshold");
}

ThresholdMemoryMonitor::~ThresholdMemoryMonitor() {
  runner_.reset();
  io_service_.stop();
  if (thread_.joinable()) {
    thread_.join();
  }
}

void ThresholdMemoryMonitor::Enable() { worker_killing_in_progress_.store(false); }

void ThresholdMemoryMonitor::Disable() { worker_killing_in_progress_.store(true); }

bool ThresholdMemoryMonitor::IsEnabled() { return !worker_killing_in_progress_.load(); }

bool ThresholdMemoryMonitor::IsUsageAboveThreshold(
    const SystemMemorySnapshot &system_memory, int64_t threshold_bytes) {
  int64_t used_memory_bytes = system_memory.used_bytes;
  int64_t total_memory_bytes = system_memory.total_bytes;
  if (total_memory_bytes == MemoryMonitorInterface::kNull ||
      used_memory_bytes == MemoryMonitorInterface::kNull) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "Unable to capture node memory. Monitor will not be able "
        << "to detect memory usage above threshold.";
    return false;
  }
  bool is_usage_above_threshold = used_memory_bytes > threshold_bytes;
  if (is_usage_above_threshold) {
    RAY_LOG_EVERY_MS(INFO, MemoryMonitorInterface::kLogIntervalMs) << absl::StrFormat(
        "Node memory usage above threshold, used: %d, threshold_bytes: %d, "
        "total bytes: %d, threshold fraction: %f",
        used_memory_bytes,
        threshold_bytes,
        total_memory_bytes,
        static_cast<float>(threshold_bytes) / static_cast<float>(total_memory_bytes));
  }
  return is_usage_above_threshold;
}

}  // namespace ray
