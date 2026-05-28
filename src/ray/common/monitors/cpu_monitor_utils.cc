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

#include <cmath>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

namespace ray {

int64_t CpuMonitorUtils::GetCpuLimit(const std::string root_cgroup_path) {
  std::string cgroupV2CpuMaxPath = root_cgroup_path + "/" + kCgroupsV2CpuMaxPath;
  std::string cgroupV1CpuQuotaPath = root_cgroup_path + "/" + kCgroupsV1CpuQuotaPath;
  std::string cgroupV1CpuPeriodPath = root_cgroup_path + "/" + kCgroupsV1CpuPeriodPath;

  if (std::filesystem::exists(cgroupV2CpuMaxPath)) {
    CpuCountOr cpu_count = GetCpuCountV2(cgroupV2CpuMaxPath);
    if (cpu_count.has_value()) {
      return cpu_count.value();
    } else {
      RAY_LOG(DEBUG) << absl::StrCat("Failed to get CPU count for: ",
                                     cgroupV2CpuMaxPath,
                                     ", error: ",
                                     cpu_count.message(),
                                     ". Is the cgroupv2 cpu limit expected to be set?");
    }
  } else if (std::filesystem::exists(cgroupV1CpuQuotaPath) &&
             std::filesystem::exists(cgroupV1CpuPeriodPath)) {
    CpuCountOr cpu_count = GetCpuCountV1(cgroupV1CpuQuotaPath, cgroupV1CpuPeriodPath);
    if (cpu_count.has_value()) {
      return cpu_count.value();
    } else {
      RAY_LOG(DEBUG) << absl::StrCat("Failed to get CPU count for: ",
                                     cgroupV1CpuQuotaPath,
                                     ", ",
                                     cgroupV1CpuPeriodPath,
                                     ", error: ",
                                     cpu_count.message(),
                                     ". Is the cgroupv1 cpu limit expected to be set?");
    }
  }
  return std::thread::hardware_concurrency();
}

CpuMonitorUtils::CpuCountOr CpuMonitorUtils::GetCpuCountV2(
    const std::string &cpu_max_path) {
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

CpuMonitorUtils::CpuCountOr CpuMonitorUtils::GetCpuCountV1(
    const std::string &cfs_quota_path, const std::string &cfs_period_path) {
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

}  // namespace ray
