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

#include "ray/common/monitors/cpu_monitor_utils.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <string>
#include <thread>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"

namespace ray {
namespace {

using CpuCountOr = StatusSetOr<int64_t, StatusT::NotFound, StatusT::Invalid>;

/**
 * @brief Gets the cgroup v2 cpu limit from the given file path.
 *
 * @param cpu_max_path File path to the cpu.max file tracking the
 *        CPU limit for the cgroup.
 *        https://docs.kernel.org/scheduler/sched-bwc.html
 * @return The number of CPUs the cgroup is limited to. If the cpu is fractional,
 *         it is rounded up to the nearest integer. Returns StatusT::NotFound if the
 *         file cannot be read, or StatusT::Invalid if it contains invalid values.
 */
CpuCountOr GetCgroupV2CpuCount(const std::string &cpu_max_path) {
  std::ifstream cpu_max_ifs(cpu_max_path, std::ios::in | std::ios::binary);
  if (!cpu_max_ifs) {
    return StatusT::NotFound(absl::StrCat("cpu.max file not found: ", cpu_max_path));
  }

  // cpu.max contains "<quota> <period>" in microseconds.
  std::string quota_str;
  int64_t period = 0;
  if (!(cpu_max_ifs >> quota_str >> period)) {
    return StatusT::Invalid(absl::StrCat("Malformed cpu.max file: ", cpu_max_path));
  }

  if (quota_str == "max") {
    return std::thread::hardware_concurrency();
  }

  int64_t quota = 0;
  if (!absl::SimpleAtoi(quota_str, &quota)) {
    return StatusT::Invalid(
        absl::StrCat("Malformed cpu.max quota \"", quota_str, "\": ", cpu_max_path));
  }

  if (quota > 0 && period > 0) {
    return static_cast<int64_t>(
        std::ceil(static_cast<double>(quota) / static_cast<double>(period)));
  }
  return StatusT::Invalid(
      absl::StrCat("Invalid cpu.max values (quota=", quota, ", period=", period, ")"));
}

/**
 * @brief Gets the cgroup v1 cpu count from the given quota and period files.
 *
 * @param cfs_quota_path File path to the cpu.cfs_quota_us file. A value of -1
 *        means no cpu limit is set.
 * @param cfs_period_path File path to the cpu.cfs_period_us file.
 * @return The number of CPUs the cgroup is limited to. If the cpu is fractional,
 *         it is rounded up to the nearest integer. Returns StatusT::NotFound if the
 *         either file cannot be read, or StatusT::Invalid if a file contains
 *         invalid values.
 */
CpuCountOr GetCgroupV1CpuCount(const std::string &cfs_quota_path,
                               const std::string &cfs_period_path) {
  std::ifstream quota_ifs(cfs_quota_path, std::ios::in | std::ios::binary);
  std::ifstream period_ifs(cfs_period_path, std::ios::in | std::ios::binary);
  if (!quota_ifs || !period_ifs) {
    return StatusT::NotFound(absl::StrCat(
        "cpu cfs quota/period file not found: ", cfs_quota_path, ", ", cfs_period_path));
  }

  int64_t quota = 0;
  int64_t period = 0;
  if (!(quota_ifs >> quota) || !(period_ifs >> period)) {
    return StatusT::Invalid(absl::StrCat(
        "Malformed cpu cfs quota/period file: ", cfs_quota_path, ", ", cfs_period_path));
  }

  if (quota == -1) {
    return std::thread::hardware_concurrency();
  }
  if (quota > 0 && period > 0) {
    return static_cast<int64_t>(
        std::ceil(static_cast<double>(quota) / static_cast<double>(period)));
  }
  return StatusT::Invalid(
      absl::StrCat("Invalid cfs quota/period (quota=", quota, ", period=", period, ")"));
}

}  // namespace

int64_t CpuMonitorUtils::GetCpuLimit(const std::string &root_cgroup_path) {
  std::string cgroupv2_cpu_max_path =
      absl::StrCat(root_cgroup_path, "/", kCgroupsV2CpuMaxPath);
  std::string cgroupv1_cpu_quota_path =
      absl::StrCat(root_cgroup_path, "/", kCgroupsV1CpuQuotaPath);
  std::string cgroupv1_cpu_period_path =
      absl::StrCat(root_cgroup_path, "/", kCgroupsV1CpuPeriodPath);

  CpuCountOr cpu_count = GetCgroupV2CpuCount(cgroupv2_cpu_max_path);
  std::string cgroupv2_error;
  std::string cgroupv1_error;
  if (cpu_count.has_value()) {
    return std::min<int64_t>(std::max<int64_t>(1, cpu_count.value()),
                             std::thread::hardware_concurrency());
  }
  cgroupv2_error = cpu_count.message();
  cpu_count = GetCgroupV1CpuCount(cgroupv1_cpu_quota_path, cgroupv1_cpu_period_path);
  if (cpu_count.has_value()) {
    return std::min<int64_t>(std::max<int64_t>(1, cpu_count.value()),
                             std::thread::hardware_concurrency());
  }
  cgroupv1_error = cpu_count.message();

  if (cpu_count.has_error()) {
    RAY_LOG(DEBUG) << absl::StrFormat(
        "Failed to get CPU count from cgroup for both cgroupv1 and cgroupv2: "
        "cgroupv2 cpu max path: %s, error: %s, "
        "cgroupv1 cpu quota path: %s, cpu period path: %s, error: %s. "
        "Falling back to physical cores.",
        cgroupv2_cpu_max_path,
        cgroupv2_error,
        cgroupv1_cpu_quota_path,
        cgroupv1_cpu_period_path,
        cgroupv1_error);
  }
  return std::thread::hardware_concurrency();
}

}  // namespace ray
