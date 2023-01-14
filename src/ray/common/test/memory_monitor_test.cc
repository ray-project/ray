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

#include "ray/common/memory_monitor.h"

#include <sys/sysinfo.h>

#include <boost/filesystem.hpp>
#include <boost/thread/latch.hpp>
#include <filesystem>
#include <fstream>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/util/process.h"

namespace ray {

class MemoryMonitorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::io_context::work work(io_context_);
      io_context_.run();
    });
  }
  void TearDown() override {
    io_context_.stop();
    thread_->join();
  }
  std::unique_ptr<std::thread> thread_;
  instrumented_io_context io_context_;

  void MakeMemoryUsage(pid_t pid,
                       const std::string usage_kb,
                       const std::string proc_dir) {
    boost::filesystem::create_directory(proc_dir);
    boost::filesystem::create_directory(proc_dir + "/" + std::to_string(pid));

    std::string usage_filename = proc_dir + "/" + std::to_string(pid) + "/smaps_rollup";

    std::ofstream usage_file;
    usage_file.open(usage_filename);
    usage_file << "SomeHeader" << std::endl;
    usage_file << "Private_Clean: " << usage_kb << " kB" << std::endl;
    usage_file.close();
  }
};

TEST_F(MemoryMonitorTest, TestThresholdZeroMonitorAlwaysAboveThreshold) {
  ASSERT_TRUE(MemoryMonitor::IsUsageAboveThreshold({1, 10}, 0));
}

TEST_F(MemoryMonitorTest, TestThresholdOneMonitorAlwaysBelowThreshold) {
  ASSERT_FALSE(MemoryMonitor::IsUsageAboveThreshold({9, 10}, 10));
}

TEST_F(MemoryMonitorTest, TestUsageAtThresholdReportsFalse) {
  ASSERT_FALSE(MemoryMonitor::IsUsageAboveThreshold({4, 10}, 5));
  ASSERT_FALSE(MemoryMonitor::IsUsageAboveThreshold({5, 10}, 5));
  ASSERT_TRUE(MemoryMonitor::IsUsageAboveThreshold({6, 10}, 5));
}

TEST_F(MemoryMonitorTest, TestGetNodeAvailableMemoryAlwaysPositive) {
  {
    MemoryMonitor monitor(
        MemoryMonitorTest::io_context_,
        0 /*usage_threshold*/,
        -1 /*min_memory_free_bytes*/,
        0 /*refresh_interval_ms*/,
        [](bool is_usage_above_threshold,
           MemorySnapshot system_memory,
           float usage_threshold) { FAIL() << "Expected monitor to not run"; });
    auto [used_bytes, total_bytes] = monitor.GetMemoryBytes();
    ASSERT_GT(total_bytes, 0);
    ASSERT_GT(total_bytes, used_bytes);
  }
}

TEST_F(MemoryMonitorTest, TestGetNodeTotalMemoryEqualsFreeOrCGroup) {
  {
    MemoryMonitor monitor(
        MemoryMonitorTest::io_context_,
        0 /*usage_threshold*/,
        -1 /*min_memory_free_bytes*/,
        0 /*refresh_interval_ms*/,
        [](bool is_usage_above_threshold,
           MemorySnapshot system_memory,
           float usage_threshold) { FAIL() << "Expected monitor to not run"; });
    auto [used_bytes, total_bytes] = monitor.GetMemoryBytes();
    auto [cgroup_used_bytes, cgroup_total_bytes] = monitor.GetCGroupMemoryBytes();

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

    ASSERT_TRUE(total_bytes == free_total_bytes || total_bytes == cgroup_total_bytes);
  }
}

TEST_F(MemoryMonitorTest, TestMonitorPeriodSetMaxUsageThresholdCallbackExecuted) {
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MemoryMonitor monitor(MemoryMonitorTest::io_context_,
                        1 /*usage_threshold*/,
                        -1 /*min_memory_free_bytes*/,
                        1 /*refresh_interval_ms*/,
                        [has_checked_once](bool is_usage_above_threshold,
                                           MemorySnapshot system_memory,
                                           float usage_threshold) {
                          ASSERT_EQ(1.0f, usage_threshold);
                          ASSERT_GT(system_memory.total_bytes, 0);
                          ASSERT_GT(system_memory.used_bytes, 0);
                          has_checked_once->count_down();
                        });
  has_checked_once->wait();
  // Stop io context here to avoid race conditions.
  io_context_.stop();
}

TEST_F(MemoryMonitorTest, TestMonitorPeriodDisableMinMemoryCallbackExecuted) {
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MemoryMonitor monitor(MemoryMonitorTest::io_context_,
                        0.4 /*usage_threshold*/,
                        -1 /*min_memory_free_bytes*/,
                        1 /*refresh_interval_ms*/,
                        [has_checked_once](bool is_usage_above_threshold,
                                           MemorySnapshot system_memory,
                                           float usage_threshold) {
                          ASSERT_EQ(0.4f, usage_threshold);
                          ASSERT_GT(system_memory.total_bytes, 0);
                          ASSERT_GT(system_memory.used_bytes, 0);
                          has_checked_once->count_down();
                        });

  has_checked_once->wait();
}

TEST_F(MemoryMonitorTest, TestMonitorMinFreeZeroThresholdIsOne) {
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MemoryMonitor monitor(MemoryMonitorTest::io_context_,
                        0.4 /*usage_threshold*/,
                        0 /*min_memory_free_bytes*/,
                        1 /*refresh_interval_ms*/,
                        [has_checked_once](bool is_usage_above_threshold,
                                           MemorySnapshot system_memory,
                                           float usage_threshold) {
                          ASSERT_EQ(1.0f, usage_threshold);
                          ASSERT_GT(system_memory.total_bytes, 0);
                          ASSERT_GT(system_memory.used_bytes, 0);
                          has_checked_once->count_down();
                        });

  has_checked_once->wait();
}

TEST_F(MemoryMonitorTest, TestCgroupV1MemFileValidReturnsWorkingSet) {
  std::string file_name = UniqueID::FromRandom().Hex();

  std::ofstream mem_file;
  mem_file.open(file_name);
  mem_file << "total_cache "
           << "918757" << std::endl;
  mem_file << "unknown "
           << "9" << std::endl;
  mem_file << "total_rss "
           << "8571" << std::endl;
  mem_file << "total_inactive_file "
           << "821" << std::endl;
  mem_file.close();

  int64_t used_bytes = MemoryMonitor::GetCGroupV1MemoryUsedBytes(file_name.c_str());

  std::remove(file_name.c_str());

  ASSERT_EQ(used_bytes, 8571 + 918757 - 821);
}

TEST_F(MemoryMonitorTest, TestCgroupV1MemFileMissingFieldReturnskNull) {
  std::string file_name = UniqueID::FromRandom().Hex();

  std::ofstream mem_file;
  mem_file.open(file_name);
  mem_file << "total_cache "
           << "918757" << std::endl;
  mem_file << "unknown "
           << "9" << std::endl;
  mem_file << "total_rss "
           << "8571" << std::endl;
  mem_file.close();

  int64_t used_bytes = MemoryMonitor::GetCGroupV1MemoryUsedBytes(file_name.c_str());

  std::remove(file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitor::kNull);
}

TEST_F(MemoryMonitorTest, TestCgroupV1NonexistentMemFileReturnskNull) {
  std::string file_name = UniqueID::FromRandom().Hex();

  int64_t used_bytes = MemoryMonitor::GetCGroupV1MemoryUsedBytes(file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitor::kNull);
}

TEST_F(MemoryMonitorTest, TestCgroupV2FilesValidReturnsWorkingSet) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "123" << std::endl;
  stat_file << "another_random_key "
            << "some_value" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitor::GetCGroupV2MemoryUsedBytes(stat_file_name.c_str(),
                                                                 curr_file_name.c_str());

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 300 - 123);
}

TEST_F(MemoryMonitorTest, TestCgroupV2FilesValidKeyLastReturnsWorkingSet) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "123" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitor::GetCGroupV2MemoryUsedBytes(stat_file_name.c_str(),
                                                                 curr_file_name.c_str());

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 300 - 123);
}

TEST_F(MemoryMonitorTest, TestCgroupV2FilesValidNegativeWorkingSet) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "300" << std::endl;
  stat_file.close();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "123" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitor::GetCGroupV2MemoryUsedBytes(stat_file_name.c_str(),
                                                                 curr_file_name.c_str());

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, 123 - 300);
}

TEST_F(MemoryMonitorTest, TestCgroupV2FilesValidMissingFieldReturnskNull) {
  std::string file_name = UniqueID::FromRandom().Hex();
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

  int64_t used_bytes = MemoryMonitor::GetCGroupV2MemoryUsedBytes(stat_file_name.c_str(),
                                                                 curr_file_name.c_str());

  std::remove(stat_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitor::kNull);
}

TEST_F(MemoryMonitorTest, TestCgroupV2NonexistentStatFileReturnskNull) {
  std::string stat_file_name = UniqueID::FromRandom().Hex();

  std::string curr_file_name = UniqueID::FromRandom().Hex();
  std::ofstream curr_file;
  curr_file.open(curr_file_name);
  curr_file << "300" << std::endl;
  curr_file.close();

  int64_t used_bytes = MemoryMonitor::GetCGroupV2MemoryUsedBytes(stat_file_name.c_str(),
                                                                 curr_file_name.c_str());
  std::remove(curr_file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitor::kNull);
}

TEST_F(MemoryMonitorTest, TestCgroupV2NonexistentUsageFileReturnskNull) {
  std::string curr_file_name = UniqueID::FromRandom().Hex();

  std::string stat_file_name = UniqueID::FromRandom().Hex();
  std::ofstream stat_file;
  stat_file.open(stat_file_name);
  stat_file << "random_key "
            << "random_value" << std::endl;
  stat_file << "inactive_file "
            << "300" << std::endl;
  stat_file.close();

  int64_t used_bytes = MemoryMonitor::GetCGroupV2MemoryUsedBytes(stat_file_name.c_str(),
                                                                 curr_file_name.c_str());
  std::remove(stat_file_name.c_str());

  ASSERT_EQ(used_bytes, MemoryMonitor::kNull);
}

TEST_F(MemoryMonitorTest, TestGetMemoryThresholdTakeGreaterOfTheTwoValues) {
  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 0.5, 0), 100);
  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 0.5, 60), 50);

  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 1, 10), 100);
  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 1, 100), 100);

  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 0.1, 100), 10);
  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 0, 10), 90);
  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 0, 100), 0);

  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 0, MemoryMonitor::kNull), 0);
  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 0.5, MemoryMonitor::kNull), 50);
  ASSERT_EQ(MemoryMonitor::GetMemoryThreshold(100, 1, MemoryMonitor::kNull), 100);
}

TEST_F(MemoryMonitorTest, TestGetPidsFromDirOnlyReturnsNumericFilenames) {
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

  auto pids = MemoryMonitor::GetPidsFromDir(proc_dir);

  boost::filesystem::remove_all(proc_dir);

  ASSERT_EQ(pids.size(), 1);
  ASSERT_EQ(pids[0], 123);
}

TEST_F(MemoryMonitorTest, TestGetPidsFromNonExistentDirReturnsEmpty) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  auto pids = MemoryMonitor::GetPidsFromDir(proc_dir);
  ASSERT_EQ(pids.size(), 0);
}

TEST_F(MemoryMonitorTest, TestGetCommandLinePidExistReturnsValid) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  std::string pid_dir = proc_dir + "/123";
  boost::filesystem::create_directories(pid_dir);

  std::string cmdline_filename = pid_dir + "/" + MemoryMonitor::kCommandlinePath;

  std::ofstream cmdline_file;
  cmdline_file.open(cmdline_filename);
  cmdline_file << "/my/very/custom/command --test passes!     ";
  cmdline_file.close();

  std::string commandline = MemoryMonitor::GetCommandLineForPid(123, proc_dir);

  boost::filesystem::remove_all(proc_dir);

  ASSERT_EQ(commandline, "/my/very/custom/command --test passes!");
}

TEST_F(MemoryMonitorTest, TestGetCommandLineMissingFileReturnsEmpty) {
  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    std::string commandline = MemoryMonitor::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }

  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    boost::filesystem::create_directory(proc_dir);
    std::string commandline = MemoryMonitor::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }

  {
    std::string proc_dir = UniqueID::FromRandom().Hex();
    std::string pid_dir = proc_dir + "/123";
    boost::filesystem::create_directories(pid_dir);
    std::string commandline = MemoryMonitor::GetCommandLineForPid(123, proc_dir);
    boost::filesystem::remove_all(proc_dir);
    ASSERT_EQ(commandline, "");
  }
}

TEST_F(MemoryMonitorTest, TestShortStringNotTruncated) {
  std::string out = MemoryMonitor::TruncateString("im short", 20);
  ASSERT_EQ(out, "im short");
}

TEST_F(MemoryMonitorTest, TestLongStringTruncated) {
  std::string out = MemoryMonitor::TruncateString(std::string(7, 'k'), 5);
  ASSERT_EQ(out, "kkkkk...");
}

TEST_F(MemoryMonitorTest, TestTopNLessThanNReturnsMemoryUsedDesc) {
  absl::flat_hash_map<pid_t, int64_t> usage;
  usage.insert({1, 111});
  usage.insert({2, 222});
  usage.insert({3, 333});

  auto list = MemoryMonitor::GetTopNMemoryUsage(2, usage);

  ASSERT_EQ(list.size(), 2);
  ASSERT_EQ(std::get<0>(list[0]), 3);
  ASSERT_EQ(std::get<1>(list[0]), 333);
  ASSERT_EQ(std::get<0>(list[1]), 2);
  ASSERT_EQ(std::get<1>(list[1]), 222);
}

TEST_F(MemoryMonitorTest, TestTopNMoreThanNReturnsAllDesc) {
  absl::flat_hash_map<pid_t, int64_t> usage;
  usage.insert({1, 111});
  usage.insert({2, 222});

  auto list = MemoryMonitor::GetTopNMemoryUsage(3, usage);

  ASSERT_EQ(list.size(), 2);
  ASSERT_EQ(std::get<0>(list[0]), 2);
  ASSERT_EQ(std::get<1>(list[0]), 222);
  ASSERT_EQ(std::get<0>(list[1]), 1);
  ASSERT_EQ(std::get<1>(list[1]), 111);
}

TEST_F(MemoryMonitorTest, TestGetProcessMemoryUsageFiltersBadPids) {
  std::string proc_dir = UniqueID::FromRandom().Hex();
  MakeMemoryUsage(1, "111", proc_dir);

  // Invalid pids with no memory usage file.
  boost::filesystem::create_directory(proc_dir + "/2");
  boost::filesystem::create_directory(proc_dir + "/3");

  auto usage = MemoryMonitor::GetProcessMemoryUsage(proc_dir);

  ASSERT_EQ(usage.size(), 1);
  ASSERT_TRUE(usage.contains(1));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
