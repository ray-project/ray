// Copyright 2022 The Ray Authors.
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

#include "ray/common/memory_monitor_utils.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/common/id.h"
#include "ray/common/memory_monitor_test_fixture.h"
#include "ray/util/process.h"

namespace ray {

class MemoryMonitorUtilsTest : public MemoryMonitorTestFixture {};

TEST_F(MemoryMonitorUtilsTest, TestGetNodeAvailableMemoryAlwaysPositive) {
  {
    auto system_memory = MemoryMonitorUtils::TakeSystemMemorySnapshot("");
    ASSERT_GT(system_memory.total_bytes, 0);
    ASSERT_GT(system_memory.total_bytes, system_memory.used_bytes);
  }
}

TEST_F(MemoryMonitorUtilsTest,
       TestTakeSystemMemorySnapshotUsesCgroupWhenLowerThanSystem) {
  int64_t cgroup_total_bytes = 1024 * 1024 * 1024;   // 1 GB
  int64_t cgroup_current_bytes = 500 * 1024 * 1024;  // 500 MB current usage
  int64_t inactive_file_bytes = 50 * 1024 * 1024;    // 50 MB inactive file cache
  int64_t active_file_bytes = 30 * 1024 * 1024;      // 30 MB active file cache
  int64_t expected_used_bytes =
      cgroup_current_bytes - inactive_file_bytes - active_file_bytes;

  std::string cgroup_dir = MockCgroupMemoryUsage(
      cgroup_total_bytes, cgroup_current_bytes, inactive_file_bytes, active_file_bytes);

  auto system_memory = MemoryMonitorUtils::TakeSystemMemorySnapshot(cgroup_dir);

  ASSERT_EQ(system_memory.total_bytes, cgroup_total_bytes);
  ASSERT_EQ(system_memory.used_bytes, expected_used_bytes);
}

TEST_F(MemoryMonitorUtilsTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup) {
  {
    auto system_memory = MemoryMonitorUtils::TakeSystemMemorySnapshot("");
    auto [cgroup_used_bytes, cgroup_total_bytes] =
        MemoryMonitorUtils::GetCGroupMemoryBytes("");

    auto cmd_out = Process::Exec("free -b");
    std::string title;
    std::string total;
    std::string used;
    std::string free;
    std::string shared;
    std::string cache;
    std::string available;
    std::istringstream cmd_out_ss(cmd_out);
    cmd_out_ss >> total >> used >> free >> shared >> cache >> available;
    cmd_out_ss >> title >> total >> used >> free >> shared >> cache >> available;

    int64_t free_total_bytes;
    std::istringstream total_ss(total);
    total_ss >> free_total_bytes;

    ASSERT_TRUE(system_memory.total_bytes == free_total_bytes ||
                system_memory.total_bytes == cgroup_total_bytes);
  }
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupFilesValidReturnsWorkingSet) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "123" << std::endl;
  stat_file << "active_file "
            << "88" << std::endl;
  stat_file << "another_random_key "
            << "some_value" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 300 - 123 - 88);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupFilesValidKeyLastReturnsWorkingSet) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "123" << std::endl;
  stat_file << "active_file "
            << "88" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 300 - 123 - 88);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupFilesValidNegativeWorkingSet) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "300" << std::endl;
  stat_file << "active_file "
            << "100" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "123" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 123 - 300 - 100);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupFilesValidMissingFieldReturnskNull) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "another_random_key "
            << "123" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitorInterface::kNull);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupNonexistentStatFileReturnskNull) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitorInterface::kNull);
}

TEST_F(MemoryMonitorUtilsTest, TestCgroupNonexistentUsageFileReturnskNull) {
  std::string curr_file_name = UniqueID::FromRandom().Hex();

  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "300" << std::endl;
  stat_file << "active_file "
            << "88" << std::endl;
  stat_file.close();

  int64_t used_bytes = MemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file_name.c_str(), curr_file_name.c_str(), "inactive_file", "active_file");
  std::remove(stat_file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitorInterface::kNull);
}

TEST_F(MemoryMonitorUtilsTest, TestGetMemoryThresholdTakeGreaterOfTheTwoValues) {
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(100, 0.5, 0), 100);
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(100, 0.5, 60), 50);

  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(100, 1, 10), 100);
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(100, 1, 100), 100);

  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(100, 0.1, 100), 10);
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(100, 0, 10), 90);
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(100, 0, 100), 0);

  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(100, 0, MemoryMonitorInterface::kNull),
            0);
  ASSERT_EQ(
      MemoryMonitorUtils::GetMemoryThreshold(100, 0.5, MemoryMonitorInterface::kNull),
      50);
  ASSERT_EQ(MemoryMonitorUtils::GetMemoryThreshold(100, 1, MemoryMonitorInterface::kNull),
            100);
}

TEST_F(MemoryMonitorUtilsTest, TestGetPidsFromDirOnlyReturnsNumericFilenames) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  boost::filesystem::create_directory(proc_dir);

  std::string num_filename = proc_dir + "/123";
  std::string non_num_filename = proc_dir + "/123b";

  std::ofstream num_file;
  num_file.open(num_filename);
  num_file << num_filename;
  num_file.close();

  std::ofstream non_num_file;
  non_num_file.open(non_num_filename);
  non_num_file << non_num_filename;
  non_num_file.close();

  auto pids = MemoryMonitorUtils::GetPidsFromDir(proc_dir);

  boost::filesystem::remove_all(proc_dir);

  ASSERT_EQ(pids.size(), 1);
  ASSERT_EQ(pids[0], 123);
}

TEST_F(MemoryMonitorUtilsTest, TestGetPidsFromNonExistentDirReturnsEmpty) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  auto pids = MemoryMonitorUtils::GetPidsFromDir(proc_dir);
  ASSERT_EQ(pids.size(), 0);
}

TEST_F(MemoryMonitorUtilsTest, TestGetCommandLinePidExistReturnsValid) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  std::string pid_dir = proc_dir + "/123";
  boost::filesystem::create_directories(pid_dir);

  std::string cmdline_filename = pid_dir + "/" + MemoryMonitorUtils::kCommandlinePath;

  std::ofstream cmdline_file;
  cmdline_file.open(cmdline_filename);
  cmdline_file << "/my/very/custom/command --test passes!     ";
  cmdline_file.close();

  std::string commandline = MemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);

  boost::filesystem::remove_all(proc_dir);

  ASSERT_EQ(commandline, "/my/very/custom/command --test passes!");
}

TEST_F(MemoryMonitorUtilsTest, TestGetCommandLineMissingFileReturnsEmpty) {
  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    std::string commandline = MemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }

  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    boost::filesystem::create_directory(proc_dir);
    std::string commandline = MemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }

  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    std::string pid_dir = proc_dir + "/123";
    boost::filesystem::create_directories(pid_dir);
    std::string commandline = MemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }
}

TEST_F(MemoryMonitorUtilsTest, TestShortStringNotTruncated) {
  std::string out = MemoryMonitorUtils::TruncateString("im short", 20);
  ASSERT_EQ(out, "im short");
}

TEST_F(MemoryMonitorUtilsTest, TestLongStringTruncated) {
  std::string out = MemoryMonitorUtils::TruncateString(std::string(7, 'k'), 5);
  ASSERT_EQ(out, "kkkkk...");
}

TEST_F(MemoryMonitorUtilsTest, TestTopNLessThanNReturnsMemoryUsedDesc) {
  absl::flat_hash_map<pid_t, int64_t> usage;
  usage.insert({1, 111});
  usage.insert({2, 222});
  usage.insert({3, 333});

  auto list = MemoryMonitorUtils::GetTopNMemoryUsage(2, usage);

  ASSERT_EQ(list.size(), 2);
  ASSERT_EQ(std::get<0>(list[0]), 3);
  ASSERT_EQ(std::get<1>(list[0]), 333);
  ASSERT_EQ(std::get<0>(list[1]), 2);
  ASSERT_EQ(std::get<1>(list[1]), 222);
}

TEST_F(MemoryMonitorUtilsTest, TestTopNMoreThanNReturnsAllDesc) {
  absl::flat_hash_map<pid_t, int64_t> usage;
  usage.insert({1, 111});
  usage.insert({2, 222});

  auto list = MemoryMonitorUtils::GetTopNMemoryUsage(3, usage);

  ASSERT_EQ(list.size(), 2);
  ASSERT_EQ(std::get<0>(list[0]), 2);
  ASSERT_EQ(std::get<1>(list[0]), 222);
  ASSERT_EQ(std::get<0>(list[1]), 1);
  ASSERT_EQ(std::get<1>(list[1]), 111);
}

TEST_F(MemoryMonitorUtilsTest, TestTakePerProcessMemorySnapshotFiltersBadPids) {
  std::string proc_dir = MockProcMemoryUsage(1, "111");

  // Invalid pids with no memory usage file.
  auto proc_2_memory_usage_file_or = TempDirectory::Create(proc_dir + "/2");
  RAY_CHECK(proc_2_memory_usage_file_or.ok())
      << "Failed to create temp directory: "
      << proc_2_memory_usage_file_or.status().message();
  std::unique_ptr<TempFile> proc_2_memory_usage_file =
      std::make_unique<TempFile>(proc_dir + "/2/smaps_rollup");
  auto proc_3_memory_usage_file_or = TempDirectory::Create(proc_dir + "/3");
  RAY_CHECK(proc_3_memory_usage_file_or.ok())
      << "Failed to create temp directory: "
      << proc_3_memory_usage_file_or.status().message();
  std::unique_ptr<TempFile> proc_3_memory_usage_file =
      std::make_unique<TempFile>(proc_dir + "/3/smaps_rollup");

  auto usage = MemoryMonitorUtils::TakePerProcessMemorySnapshot(proc_dir);

  ASSERT_EQ(usage.size(), 1);
  ASSERT_TRUE(usage.contains(1));
}

}  // namespace ray
