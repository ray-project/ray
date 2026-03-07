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

#include <atomic>
#include <boost/thread/latch.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/common/memory_monitor_test_fixture.h"

namespace ray {

class ThresholdMemoryMonitorTest : public MemoryMonitorTestFixture {
 protected:
  void TearDown() override { instance.reset(); }

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
};

TEST_F(ThresholdMemoryMonitorTest, TestMonitorTriggerCanDetectMemoryUsage) {
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MakeThresholdMemoryMonitor(
      0.0 /*usage_threshold*/,
      -1 /*min_memory_free_bytes*/,
      1 /*refresh_interval_ms*/,
      [has_checked_once](const SystemMemorySnapshot &system_memory) {
        ASSERT_GT(system_memory.total_bytes, 0)
            << "Reported total bytes from cgroup is <= 0. Is the system memory snapshot "
               "taken correctly?";
        ASSERT_GE(system_memory.used_bytes, 0)
            << "Reported used bytes from cgroup is < 0. Is the system memory snapshot "
               "taken correctly?";
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
        ASSERT_EQ(system_memory.total_bytes, cgroup_total_bytes)
            << "Unexpected total bytes read from cgroup. Are we correctly reading memory "
               "from the cgroup?";
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
      << "Callback should not have been triggered when memory is below threshold. "
         "Are is the memory monitor correctly reading memory from the system?";
}

}  // namespace ray
