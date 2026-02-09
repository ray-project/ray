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

#include <sys/mman.h>
#include <sys/sysinfo.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <boost/filesystem.hpp>
#include <boost/thread/latch.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/common/id.h"
#include "ray/common/memory_monitor.h"
#include "ray/util/process.h"

namespace ray {

class ThresholdMemoryMonitorTest : public ::testing::Test {
 protected:
  void TearDown() override { instance.reset(); }

  /**
   * Sets up a mock cgroup v2 directory for emulating memory usage and populates
   * the files with the provided mock memory values.
   *
   * @param total_bytes the value to write to memory.max (total memory limit)
   * @param current_bytes the value to write to memory.current (current usage)
   * @param inactive_file_bytes the inactive_file value in memory.stat
   * @param active_file_bytes the active_file value in memory.stat
   * @return the path to the created mock cgroup directory
   */
  std::string MockCgroupMemoryUsage(int64_t total_bytes,
                                    int64_t current_bytes,
                                    int64_t inactive_file_bytes,
                                    int64_t active_file_bytes) {
    auto temp_dir_or = TempDirectory::Create();
    RAY_CHECK(temp_dir_or.ok())
        << "Failed to create temp directory: " << temp_dir_or.status().message();
    mock_cgroup_dir_ = std::move(temp_dir_or.value());

    const std::string &cgroup_path = mock_cgroup_dir_->GetPath();

    mock_memory_max_file_ = std::make_unique<TempFile>(cgroup_path + "/memory.max");
    mock_memory_max_file_->AppendLine(std::to_string(total_bytes) + "\n");

    mock_memory_current_file_ =
        std::make_unique<TempFile>(cgroup_path + "/memory.current");
    mock_memory_current_file_->AppendLine(std::to_string(current_bytes) + "\n");

    mock_memory_stat_file_ = std::make_unique<TempFile>(cgroup_path + "/memory.stat");
    mock_memory_stat_file_->AppendLine("anon 123456\n");
    mock_memory_stat_file_->AppendLine("inactive_file " +
                                       std::to_string(inactive_file_bytes) + "\n");
    mock_memory_stat_file_->AppendLine("active_file " +
                                       std::to_string(active_file_bytes) + "\n");
    mock_memory_stat_file_->AppendLine("some_other_key 789\n");

    return cgroup_path;
  }

  ThresholdMemoryMonitor &MakeThresholdMemoryMonitor(
      float usage_threshold,
      int64_t min_memory_free_bytes,
      uint64_t monitor_interval_ms,
      KillWorkersCallback kill_workers_callback,
      const std::string &root_cgroup_path) {
    instance = std::make_unique<ThresholdMemoryMonitor>(std::move(kill_workers_callback),
                                                        usage_threshold,
                                                        min_memory_free_bytes,
                                                        monitor_interval_ms,
                                                        root_cgroup_path);
    return *instance;
  }
  std::unique_ptr<ThresholdMemoryMonitor> instance;

  // Mock cgroup directory and files
  std::unique_ptr<TempDirectory> mock_cgroup_dir_;
  std::unique_ptr<TempFile> mock_memory_max_file_;
  std::unique_ptr<TempFile> mock_memory_current_file_;
  std::unique_ptr<TempFile> mock_memory_stat_file_;
};

TEST_F(ThresholdMemoryMonitorTest, TestMonitorTriggerCanDetectMemoryUsage) {
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MakeThresholdMemoryMonitor(
      0.0 /*usage_threshold*/,
      -1 /*min_memory_free_bytes*/,
      1 /*refresh_interval_ms*/,
      [has_checked_once](const SystemMemorySnapshot &system_memory) {
        ASSERT_GT(system_memory.total_bytes, 0);
        ASSERT_GT(system_memory.used_bytes, 0);
        has_checked_once->count_down();
      },
      "" /*root_cgroup_path*/);
  has_checked_once->wait();
}

TEST_F(ThresholdMemoryMonitorTest,
       TestMonitorDetectsMemoryAboveThresholdCallbackExecuted) {
  int64_t cgroup_total_bytes = 1024 * 1024 * 1024;   // 1 GB
  int64_t cgroup_current_bytes = 850 * 1024 * 1024;  // 850 MB current usage
  int64_t inactive_file_bytes = 30 * 1024 * 1024;    // 30 MB inactive file cache
  int64_t active_file_bytes = 20 * 1024 * 1024;      // 20 MB active file cache
  // Working set = 850 - 30 - 20 = 800 MB (80% of 1GB, above 70% threshold)

  std::string cgroup_dir = MockCgroupMemoryUsage(
      cgroup_total_bytes, cgroup_current_bytes, inactive_file_bytes, active_file_bytes);

  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MakeThresholdMemoryMonitor(
      0.7 /*usage_threshold (70%)*/,
      -1 /*min_memory_free_bytes*/,
      1 /*refresh_interval_ms*/,
      [has_checked_once, cgroup_total_bytes](const SystemMemorySnapshot &system_memory) {
        ASSERT_EQ(system_memory.total_bytes, cgroup_total_bytes);
        has_checked_once->count_down();
      },
      cgroup_dir /*root_cgroup_path*/);

  has_checked_once->wait();
}

TEST_F(ThresholdMemoryMonitorTest,
       TestMonitorDetectsMemoryBelowThresholdCallbackNotExecuted) {
  int64_t cgroup_total_bytes = 1024 * 1024 * 1024;   // 1 GB
  int64_t cgroup_current_bytes = 500 * 1024 * 1024;  // 500 MB current usage
  int64_t inactive_file_bytes = 30 * 1024 * 1024;    // 30 MB inactive file cache
  int64_t active_file_bytes = 20 * 1024 * 1024;      // 20 MB active file cache
  // Working set = 500 - 30 - 20 = 450 MB (45% of 1GB, below 70% threshold)

  std::string cgroup_dir = MockCgroupMemoryUsage(
      cgroup_total_bytes, cgroup_current_bytes, inactive_file_bytes, active_file_bytes);

  std::shared_ptr<std::atomic<bool>> callback_triggered =
      std::make_shared<std::atomic<bool>>(false);

  MakeThresholdMemoryMonitor(
      0.7 /*usage_threshold (70%)*/,
      -1 /*min_memory_free_bytes*/,
      1 /*refresh_interval_ms*/,
      [callback_triggered](const SystemMemorySnapshot &system_memory) {
        callback_triggered->store(true);
      },
      cgroup_dir /*root_cgroup_path*/);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  ASSERT_FALSE(callback_triggered->load())
      << "Callback should not have been triggered when memory is below threshold";
}

}  // namespace ray
