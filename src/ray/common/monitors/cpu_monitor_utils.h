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

#include <cstdint>
#include <string>

#include "ray/common/status.h"

namespace ray {

class CpuMonitorUtils {
 public:
  static constexpr std::string_view kCgroupsV1CpuQuotaPath = "cpu/cpu.cfs_quota_us";
  static constexpr std::string_view kCgroupsV1CpuPeriodPath = "cpu/cpu.cfs_period_us";
  static constexpr std::string_view kCgroupsV2CpuMaxPath = "cpu.max";
  static constexpr std::string_view kRootCgroupPath = "/sys/fs/cgroup";

  /**
   * @brief Gets the effective CPU limit (number of CPUs) for the given cgroup.
   *
   * If no CPU limit is configured for the given root_cgroup_path, returns physical_cores.
   *
   * @param root_cgroup_path The path to the root cgroup to read the cpu limit from.
   * @return The number of CPUs the cgroup is limited to, or physical_cores if
   *         no limit is set.
   */
  static int64_t GetCpuLimit(const std::string &root_cgroup_path);

 private:
  using CpuCountOr = StatusSetOr<int64_t, StatusT::NotFound, StatusT::Invalid>;

  /**
   * @brief Gets the cgroup v2 cpu limit from the given file path.
   *
   * @param cpu_max_path File path to the cpu.max file tracking the
   *        CPU limit for the cgroup.
   *        https://docs.kernel.org/scheduler/sched-bwc.html
   * @return The number of CPUs the cgroup is limited to. If the cpu is fractional,
   *         it is rounded down to the nearest integer. Returns StatusT::NotFound if the
   *         file cannot be read, or StatusT::Invalid if it contains invalid values.
   */
  static CpuCountOr GetCpuCountV2(const std::string &cpu_max_path);

  /**
   * @brief Gets the cgroup v1 cpu count from the given quota and period files.
   *
   * @param cfs_quota_path File path to the cpu.cfs_quota_us file. A value of -1
   *        means no cpu limit is set.
   * @param cfs_period_path File path to the cpu.cfs_period_us file.
   * @return The number of CPUs the cgroup is limited to. If the cpu is fractional,
   *         it is rounded down to the nearest integer. Returns StatusT::NotFound if the
   *         either file cannot be read, or StatusT::Invalid if a file contains
   *         invalid values.
   */
  static CpuCountOr GetCpuCountV1(const std::string &cfs_quota_path,
                                  const std::string &cfs_period_path);
};

}  // namespace ray
