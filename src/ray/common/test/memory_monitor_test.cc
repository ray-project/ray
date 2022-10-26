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
  std::condition_variable callback_ran;
  std::mutex callback_ran_mutex;

  MemoryMonitor monitor(MemoryMonitorTest::io_context_,
                        1 /*usage_threshold*/,
                        -1 /*min_memory_free_bytes*/,
                        1 /*refresh_interval_ms*/,
                        [&callback_ran](bool is_usage_above_threshold,
                                        MemorySnapshot system_memory,
                                        float usage_threshold) {
                          ASSERT_EQ(1.0f, usage_threshold);
                          ASSERT_GT(system_memory.total_bytes, 0);
                          ASSERT_GT(system_memory.used_bytes, 0);
                          callback_ran.notify_all();
                        });
  std::unique_lock<std::mutex> callback_ran_mutex_lock(callback_ran_mutex);
  callback_ran.wait(callback_ran_mutex_lock);
}

TEST_F(MemoryMonitorTest, TestMonitorPeriodDisableMinMemoryCallbackExecuted) {
  std::condition_variable callback_ran;
  std::mutex callback_ran_mutex;

  MemoryMonitor monitor(MemoryMonitorTest::io_context_,
                        0.4 /*usage_threshold*/,
                        -1 /*min_memory_free_bytes*/,
                        1 /*refresh_interval_ms*/,
                        [&callback_ran](bool is_usage_above_threshold,
                                        MemorySnapshot system_memory,
                                        float usage_threshold) {
                          ASSERT_EQ(0.4f, usage_threshold);
                          ASSERT_GT(system_memory.total_bytes, 0);
                          ASSERT_GT(system_memory.used_bytes, 0);
                          callback_ran.notify_all();
                        });
  std::unique_lock<std::mutex> callback_ran_mutex_lock(callback_ran_mutex);
  callback_ran.wait(callback_ran_mutex_lock);
}

TEST_F(MemoryMonitorTest, TestMonitorMinFreeZeroThresholdIsOne) {
  std::condition_variable callback_ran;
  std::mutex callback_ran_mutex;

  MemoryMonitor monitor(MemoryMonitorTest::io_context_,
                        0.4 /*usage_threshold*/,
                        0 /*min_memory_free_bytes*/,
                        1 /*refresh_interval_ms*/,
                        [&callback_ran](bool is_usage_above_threshold,
                                        MemorySnapshot system_memory,
                                        float usage_threshold) {
                          ASSERT_EQ(1.0f, usage_threshold);
                          ASSERT_GT(system_memory.total_bytes, 0);
                          ASSERT_GT(system_memory.used_bytes, 0);
                          callback_ran.notify_all();
                        });
  std::unique_lock<std::mutex> callback_ran_mutex_lock(callback_ran_mutex);
  callback_ran.wait(callback_ran_mutex_lock);
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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
