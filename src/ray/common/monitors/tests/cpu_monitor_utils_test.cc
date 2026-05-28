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

#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/common/status_or.h"

namespace ray {

class CpuMonitorUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StatusOr<std::unique_ptr<TempDirectory>> root_or = TempDirectory::Create();
    RAY_CHECK(root_or.ok()) << absl::StrFormat(
        "Failed to create temp directory due to error: %s. "
        "CPU monitor tests expect tmpfs to be mounted.",
        root_or.status().ToString());
    root_ = std::move(root_or.value());
  }

  const std::string &RootPath() const { return root_->GetPath(); }

  /**
   * @brief Writes a fake cgroup v2 "cpu.max" file with the given contents.
   *
   * @param contents The "<quota> <period>" string in microseconds to write to the file.
   */
  void WriteCpuMax(const std::string &contents) {
    cpu_max_ = std::make_unique<TempFile>(
        absl::StrCat(RootPath(), "/", CpuMonitorUtils::kCgroupsV2CpuMaxPath));
    cpu_max_->AppendLine(contents);
  }

  /**
   * @brief Writes a fake cgroup v1 "cpu.cfs_quota_us" and "cpu.cfs_period_us" files
   *        with the given values.
   *
   * @param quota The quota value to write to the "cpu.cfs_quota_us" file.
   * @param period The period value to write to the "cpu.cfs_period_us" file.
   */
  void WriteCpuV1(const std::string &quota, const std::string &period) {
    StatusOr<std::unique_ptr<TempDirectory>> cpu_dir_or =
        TempDirectory::Create(absl::StrCat(RootPath(), "/cpu"));
    RAY_CHECK(cpu_dir_or.ok()) << absl::StrFormat(
        "Failed to create temp directory due to error: %s. "
        "CPU monitor tests expect tmpfs to be mounted.",
        cpu_dir_or.status().ToString());
    cpu_dir_ = std::move(cpu_dir_or.value());
    cpu_quota_ = std::make_unique<TempFile>(
        absl::StrCat(RootPath(), "/", CpuMonitorUtils::kCgroupsV1CpuQuotaPath));
    cpu_period_ = std::make_unique<TempFile>(
        absl::StrCat(RootPath(), "/", CpuMonitorUtils::kCgroupsV1CpuPeriodPath));
    cpu_quota_->AppendLine(quota);
    cpu_period_->AppendLine(period);
  }

  std::unique_ptr<TempDirectory> root_;
  std::unique_ptr<TempDirectory> cpu_dir_;
  std::unique_ptr<TempFile> cpu_max_;
  std::unique_ptr<TempFile> cpu_quota_;
  std::unique_ptr<TempFile> cpu_period_;
};

TEST_F(CpuMonitorUtilsTest, V2QuotaAndPeriodReturnsCpuCount) {
  WriteCpuMax("200000 100000");
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()), 2);
}

TEST_F(CpuMonitorUtilsTest, V2FractionalQuotaRoundsUp) {
  WriteCpuMax("250000 100000");
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()), 3);
}

TEST_F(CpuMonitorUtilsTest, V2FractionalQuotaCannotBeZero) {
  WriteCpuMax("100 100000");
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()), 1);
}

TEST_F(CpuMonitorUtilsTest, V2MaxQuotaFallsBackToPhysicalCores) {
  WriteCpuMax("max 100000");
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()),
            std::thread::hardware_concurrency());
}

TEST_F(CpuMonitorUtilsTest, V2MalformedFileFallsBackToPhysicalCores) {
  WriteCpuMax("garbage");
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()),
            std::thread::hardware_concurrency());
}

TEST_F(CpuMonitorUtilsTest, V1QuotaAndPeriodReturnsCpuCount) {
  WriteCpuV1("300000", "100000");
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()), 3);
}

TEST_F(CpuMonitorUtilsTest, V1FractionalQuotaRoundsUp) {
  WriteCpuV1("250000", "100000");
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()), 3);
}

TEST_F(CpuMonitorUtilsTest, V1FractionalQuotaCannotBeZero) {
  WriteCpuV1("100", "100000");
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()), 1);
}

TEST_F(CpuMonitorUtilsTest, V1UnlimitedQuotaFallsBackToPhysicalCores) {
  WriteCpuV1("-1", "100000");
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()),
            std::thread::hardware_concurrency());
}

TEST_F(CpuMonitorUtilsTest, NoCgroupFilesFallsBackToPhysicalCores) {
  ASSERT_EQ(CpuMonitorUtils::GetCpuLimit(RootPath()),
            std::thread::hardware_concurrency());
}

}  // namespace ray
