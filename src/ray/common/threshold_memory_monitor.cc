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

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <string>

#include "absl/strings/str_format.h"
#include "ray/common/memory_monitor_utils.h"
#include "ray/common/status_or.h"
#include "ray/util/logging.h"
#include "ray/util/thread_utils.h"

namespace ray {

ThresholdMemoryMonitor::ThresholdMemoryMonitor(
    KillWorkersCallback kill_workers_callback,
    float usage_threshold,
    int64_t min_memory_free_bytes,
    uint64_t monitor_interval_ms,
    bool resource_isolation_enabled,
    const CgroupManagerInterface &cgroup_manager,
    const std::string &root_cgroup_path,
    const std::string &user_cgroup_path,
    const std::string &system_cgroup_path)
    : kill_workers_callback_(std::move(kill_workers_callback)),
      worker_killing_in_progress_(false),
      usage_threshold_(usage_threshold),
      min_memory_free_bytes_(min_memory_free_bytes),
      resource_isolation_enabled_(resource_isolation_enabled),
      cgroup_manager_(cgroup_manager),
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
  // The effective threshold is recomputed against the live cgroup limit on
  // every poll (see ComputeMemoryThresholdBytes), so we deliberately do not
  // cache a startup value here -- doing so would drift out of sync the moment
  // the cgroup limit changes (e.g. Kubernetes in-place pod resize).
  RAY_LOG(INFO) << absl::StrFormat(
      "ThresholdMemoryMonitor initialized with usage_threshold=%.3f, "
      "min_memory_free_bytes=%d, monitor interval: %dms. The effective "
      "threshold tracks the live cgroup memory limit on every poll.%s",
      usage_threshold_,
      min_memory_free_bytes_,
      monitor_interval_ms,
      resource_isolation_enabled_
          ? " Resource isolation enabled, monitoring only user slice memory usage."
          : "");

  runner_->RunFnPeriodically(
      [this] {
        std::optional<MemoryUsageSnapshot> exceeded_snapshot;
        if (resource_isolation_enabled_) {
          exceeded_snapshot = IsResourceIsolationThresholdExceeded();
        } else {
          exceeded_snapshot = IsHostMemoryThresholdExceeded();
        }

        if (exceeded_snapshot.has_value() && IsEnabled()) {
          const MemoryUsageSnapshot &cur_memory_snapshot = exceeded_snapshot.value();
          Disable();
          int64_t threshold_bytes =
              ComputeMemoryThresholdBytes(cur_memory_snapshot.total_bytes);
          std::string trigger_reason = absl::StrFormat(
              "Memory usage %dB exceeded threshold of %dB (%.1f%% of %dB total)",
              cur_memory_snapshot.used_bytes,
              threshold_bytes,
              (cur_memory_snapshot.total_bytes > 0 &&
                       threshold_bytes != MemoryMonitorInterface::kNull
                   ? static_cast<float>(threshold_bytes) /
                         static_cast<float>(cur_memory_snapshot.total_bytes) * 100
                   : 0.0f),
              cur_memory_snapshot.total_bytes);
          kill_workers_callback_(trigger_reason);
        }
      },
      monitor_interval_ms,
      "MemoryMonitor.CheckIsMemoryUsageAboveThreshold");
}

int64_t ThresholdMemoryMonitor::ComputeMemoryThresholdBytes(
    int64_t total_memory_bytes) const {
  // This runs on every poll. Unlike MemoryMonitorUtils::GetMemoryThreshold
  // (which is fine to RAY_CHECK at startup), any failure here must NOT abort
  // the raylet -- a transient cgroup read failure or a runtime resize that
  // pushes the cgroup total below min_memory_free_bytes would otherwise crash
  // the process every memory_monitor_refresh_ms tick. In resource isolation
  // mode, memory.high is the authoritative user-slice threshold; if it cannot
  // be read or parsed, skip this poll instead of falling back to the host total
  // from /proc/meminfo, which can be much larger than the user-slice cap.
  if (total_memory_bytes == MemoryMonitorInterface::kNull) {
    return MemoryMonitorInterface::kNull;
  }

  int64_t threshold_fraction =
      static_cast<int64_t>(total_memory_bytes * usage_threshold_);
  int64_t resolved_threshold_bytes = threshold_fraction;
  if (min_memory_free_bytes_ > MemoryMonitorInterface::kNull) {
    int64_t threshold_absolute = total_memory_bytes - min_memory_free_bytes_;
    if (threshold_absolute < 0) {
      // Cgroup downsized below the configured min-free reservation. Don't
      // crash; just ignore the absolute floor for this poll.
      RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
          << "Cgroup memory total " << total_memory_bytes
          << " is smaller than min_memory_free_bytes " << min_memory_free_bytes_
          << "; ignoring the absolute threshold for this poll and using the "
             "fractional threshold "
          << threshold_fraction << ".";
    } else {
      resolved_threshold_bytes = std::max(threshold_fraction, threshold_absolute);
    }
  }

  if (!resource_isolation_enabled_) {
    return resolved_threshold_bytes;
  }

  StatusOr<std::string> user_slice_upper_bound_or =
      cgroup_manager_.GetUserCgroupConstraintValue(
          MemoryMonitorUtils::kCgroupsV2MemoryHighPath);
  if (!user_slice_upper_bound_or.ok()) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "Failed to read user cgroup memory limit ("
        << MemoryMonitorUtils::kCgroupsV2MemoryHighPath << ") from "
        << cgroup_manager_.GetUserCgroupPath() << ": "
        << user_slice_upper_bound_or.ToString()
        << ". Skipping this resource-isolation memory monitor poll instead of "
           "falling back to host-based threshold "
        << resolved_threshold_bytes << ".";
    return MemoryMonitorInterface::kNull;
  }
  const std::string &user_slice_upper_bound_str = user_slice_upper_bound_or.value();
  if (user_slice_upper_bound_str.empty() ||
      !std::all_of(user_slice_upper_bound_str.begin(),
                   user_slice_upper_bound_str.end(),
                   [](unsigned char c) { return std::isdigit(c); })) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "User cgroup memory limit is not a valid non-negative integer: \""
        << user_slice_upper_bound_str << "\" from " << cgroup_manager_.GetUserCgroupPath()
        << ". Skipping this resource-isolation memory monitor poll instead of "
           "falling back to host-based threshold "
        << resolved_threshold_bytes << ".";
    return MemoryMonitorInterface::kNull;
  }

  errno = 0;
  char *parse_end = nullptr;
  const long long user_slice_upper_bound =
      std::strtoll(user_slice_upper_bound_str.c_str(), &parse_end, 10);
  if (errno == ERANGE || parse_end == nullptr || *parse_end != '\0' ||
      user_slice_upper_bound > std::numeric_limits<int64_t>::max()) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "User cgroup memory limit is outside int64 range: \""
        << user_slice_upper_bound_str << "\" from " << cgroup_manager_.GetUserCgroupPath()
        << ". Skipping this resource-isolation memory monitor poll instead of "
           "falling back to host-based threshold "
        << resolved_threshold_bytes << ".";
    return MemoryMonitorInterface::kNull;
  }
  return static_cast<int64_t>(user_slice_upper_bound);
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

std::optional<MemoryUsageSnapshot>
ThresholdMemoryMonitor::IsHostMemoryThresholdExceeded() {
  MemoryUsageSnapshot cur_memory_snapshot =
      MemoryMonitorUtils::TakeSystemMemoryUsageSnapshot(root_cgroup_path_);
  int64_t used_memory_bytes = cur_memory_snapshot.used_bytes;
  int64_t total_memory_bytes = cur_memory_snapshot.total_bytes;
  if (total_memory_bytes == MemoryMonitorInterface::kNull ||
      used_memory_bytes == MemoryMonitorInterface::kNull) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs)
        << "Unable to capture node memory. Monitor will not be able "
        << "to detect memory usage above threshold.";
    return std::nullopt;
  }
  int64_t threshold_bytes = ComputeMemoryThresholdBytes(total_memory_bytes);
  if (threshold_bytes == MemoryMonitorInterface::kNull) {
    // Threshold could not be resolved this poll (e.g. transient cgroup read
    // failure). Skip; do NOT fall through into a `used > -1` comparison that
    // would always trigger.
    return std::nullopt;
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
  return is_usage_above_threshold
             ? std::optional<MemoryUsageSnapshot>(cur_memory_snapshot)
             : std::nullopt;
}

std::optional<MemoryUsageSnapshot>
ThresholdMemoryMonitor::IsResourceIsolationThresholdExceeded() {
  StatusSetOr<MemoryUsageSnapshot, StatusT::NotFound> user_slice_memory_snapshot_or =
      MemoryMonitorUtils::TakeUserSliceMemoryUsageSnapshot(user_cgroup_path_,
                                                           system_cgroup_path_);
  if (!user_slice_memory_snapshot_or.has_value()) {
    RAY_LOG_EVERY_MS(WARNING, MemoryMonitorInterface::kLogIntervalMs) << absl::StrFormat(
        "Failed to take user slice memory snapshot due to: %s. "
        "The threshold memory monitor will not be able to provide resource isolation "
        "protection.",
        user_slice_memory_snapshot_or.message());
    return std::nullopt;
  }
  MemoryUsageSnapshot user_slice_memory_snapshot = user_slice_memory_snapshot_or.value();
  int64_t threshold_bytes =
      ComputeMemoryThresholdBytes(user_slice_memory_snapshot.total_bytes);
  if (threshold_bytes == MemoryMonitorInterface::kNull) {
    // Threshold could not be resolved this poll. Skip rather than treat
    // every non-negative usage as exceeding kNull (-1).
    return std::nullopt;
  }
  bool is_usage_above_threshold = user_slice_memory_snapshot.used_bytes > threshold_bytes;
  if (is_usage_above_threshold) {
    RAY_LOG_EVERY_MS(INFO, MemoryMonitorInterface::kLogIntervalMs) << absl::StrFormat(
        "User slice memory usage above threshold, used: %d, threshold_bytes: %d, "
        "total bytes: %d, threshold fraction: %f",
        user_slice_memory_snapshot.used_bytes,
        threshold_bytes,
        user_slice_memory_snapshot.total_bytes,
        static_cast<float>(threshold_bytes) /
            static_cast<float>(user_slice_memory_snapshot.total_bytes));
  }
  return is_usage_above_threshold
             ? std::optional<MemoryUsageSnapshot>(user_slice_memory_snapshot)
             : std::nullopt;
}

}  // namespace ray
