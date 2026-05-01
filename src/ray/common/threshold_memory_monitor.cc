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
                                               int64_t memory_usage_threshold_bytes,
                                               uint64_t monitor_interval_ms,
                                               bool resource_isolation_enabled,
                                               const std::string &root_cgroup_path,
                                               const std::string &user_cgroup_path,
                                               const std::string &system_cgroup_path)
    : kill_workers_callback_(std::move(kill_workers_callback)),
      worker_killing_in_progress_(false),
      memory_usage_threshold_bytes_(memory_usage_threshold_bytes),
      resource_isolation_enabled_(resource_isolation_enabled),
      root_cgroup_path_(root_cgroup_path),
      user_cgroup_path_(user_cgroup_path),
      system_cgroup_path_(system_cgroup_path),
      io_service_(/*enable_metrics=*/false,
                  /*running_on_single_thread=*/true,
                  "MemoryMonitor.IOContext"),
      work_guard_(boost::asio::make_work_guard(io_service_.get_executor())),
      thread_([this] {
        SetThreadName("MemoryMonitor.IOContextThread");
        io_service_.run();
      }),
      runner_(PeriodicalRunner::Create(io_service_)) {
  int64_t total_memory_bytes =
      MemoryMonitorUtils::TakeHostSystemMemorySnapshot(root_cgroup_path_).total_bytes;
  float computed_threshold_fraction = static_cast<float>(memory_usage_threshold_bytes_) /
                                      static_cast<float>(total_memory_bytes);
  RAY_LOG(INFO) << absl::StrFormat(
      "ThresholdMemoryMonitor initialized with usage threshold at %d bytes "
      "(%f%% of system memory), total system memory bytes: %d, monitor interval: %dms"
      "%s",
      memory_usage_threshold_bytes_,
      computed_threshold_fraction * 100,
      total_memory_bytes,
      monitor_interval_ms,
      resource_isolation_enabled_
          ? ", resource isolation enabled, monitoring only user slice memory usage"
          : "");

  runner_->RunFnPeriodically(
      [this] {
        bool is_usage_above_threshold = false;
        if (resource_isolation_enabled_) {
          is_usage_above_threshold = IsResourceIsolationThresholdExceeded();
        } else {
          is_usage_above_threshold = IsHostMemoryThresholdExceeded();
        }

        if (is_usage_above_threshold && IsEnabled()) {
          Disable();
          kill_workers_callback_();
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

bool ThresholdMemoryMonitor::IsEnabled() const {
  return !worker_killing_in_progress_.load();
}

bool ThresholdMemoryMonitor::IsHostMemoryThresholdExceeded() {
  SystemMemorySnapshot cur_memory_snapshot =
      MemoryMonitorUtils::TakeHostSystemMemorySnapshot(root_cgroup_path_);
  int64_t used_memory_bytes = cur_memory_snapshot.used_bytes;
  int64_t total_memory_bytes = cur_memory_snapshot.total_bytes;
  if (total_memory_bytes == MemoryMonitorInterface::kNull ||
      used_memory_bytes == MemoryMonitorInterface::kNull) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "Unable to capture node memory. Monitor will not be able "
        << "to detect memory usage above threshold.";
    return false;
  }
  bool is_usage_above_threshold = used_memory_bytes > memory_usage_threshold_bytes_;
  if (is_usage_above_threshold) {
    RAY_LOG_EVERY_MS(INFO, MemoryMonitorInterface::kLogIntervalMs) << absl::StrFormat(
        "Node memory usage above threshold, used: %d, threshold_bytes: %d, "
        "total bytes: %d, threshold fraction: %f",
        used_memory_bytes,
        memory_usage_threshold_bytes_,
        total_memory_bytes,
        static_cast<float>(memory_usage_threshold_bytes_) /
            static_cast<float>(total_memory_bytes));
  }
  return is_usage_above_threshold;
}

bool ThresholdMemoryMonitor::IsResourceIsolationThresholdExceeded() {
  StatusSetOr<SystemMemorySnapshot, StatusT::NotFound> user_slice_memory_snapshot_or =
      MemoryMonitorUtils::TakeUserSliceSystemMemorySnapshot(user_cgroup_path_,
                                                            system_cgroup_path_);
  if (!user_slice_memory_snapshot_or.has_value()) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs) << absl::StrFormat(
        "Failed to take user slice memory snapshot due to: %s. "
        "The threshold monitor will not be able to provide resource isolation "
        "protection.",
        user_slice_memory_snapshot_or.message());
    return false;
  }
  SystemMemorySnapshot user_slice_memory_snapshot = user_slice_memory_snapshot_or.value();
  bool is_usage_above_threshold =
      user_slice_memory_snapshot.used_bytes > memory_usage_threshold_bytes_;
  if (is_usage_above_threshold) {
    RAY_LOG_EVERY_MS(INFO, MemoryMonitorInterface::kLogIntervalMs) << absl::StrFormat(
        "User slice memory usage above threshold, used: %d, threshold_bytes: %d, "
        "total bytes: %d, threshold fraction: %f",
        user_slice_memory_snapshot.used_bytes,
        memory_usage_threshold_bytes_,
        user_slice_memory_snapshot.total_bytes,
        static_cast<float>(memory_usage_threshold_bytes_) /
            static_cast<float>(user_slice_memory_snapshot.total_bytes));
  }
  return is_usage_above_threshold;
}

}  // namespace ray
