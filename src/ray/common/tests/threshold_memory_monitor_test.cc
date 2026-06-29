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

#include <memory>
#include <string>
#include <utility>

#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "ray/asio/fake_periodical_runner.h"
#include "ray/common/cgroup2/noop_cgroup_manager.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/common/memory_monitor_test_fixture.h"
#include "ray/common/memory_monitor_utils.h"
#include "ray/util/clock.h"

namespace ray {

// These tests drive the monitor's periodic memory check deterministically by
// injecting a FakePeriodicalRunner backed by a FakeClock. Advancing the clock by
// one monitor interval runs the check exactly once, so there is no need to spawn
// a real IO thread or sleep waiting for a wall-clock timer to fire.
class ThresholdMemoryMonitorTest : public MemoryMonitorTestFixture {
 protected:
  void TearDown() override {
    // Destroy the monitor (which holds a callback registered with the runner)
    // before the runner and clock are torn down.
    instance.reset();
  }

  ThresholdMemoryMonitor &MakeThresholdMemoryMonitor(
      int64_t memory_usage_threshold_bytes,
      uint64_t monitor_interval_ms,
      KillWorkersCallback kill_workers_callback,
      const std::string &root_cgroup_path) {
    monitor_interval_ms_ = monitor_interval_ms;
    instance =
        std::make_unique<ThresholdMemoryMonitor>(runner_,
                                                 std::move(kill_workers_callback),
                                                 memory_usage_threshold_bytes,
                                                 monitor_interval_ms,
                                                 /*resource_isolation_enabled=*/false,
                                                 root_cgroup_path);
    return *instance;
  }

  ThresholdMemoryMonitor &MakeResourceIsolatedThresholdMemoryMonitor(
      int64_t memory_usage_threshold_bytes,
      uint64_t monitor_interval_ms,
      KillWorkersCallback kill_workers_callback,
      const std::string &root_cgroup_path,
      const std::string &user_cgroup_path,
      const std::string &system_cgroup_path) {
    monitor_interval_ms_ = monitor_interval_ms;
    instance =
        std::make_unique<ThresholdMemoryMonitor>(runner_,
                                                 std::move(kill_workers_callback),
                                                 memory_usage_threshold_bytes,
                                                 monitor_interval_ms,
                                                 /*resource_isolation_enabled=*/true,
                                                 root_cgroup_path,
                                                 user_cgroup_path,
                                                 system_cgroup_path);
    return *instance;
  }

  // Advances the fake clock by `num_intervals` monitor intervals, running the
  // periodic memory check once per interval.
  void RunMemoryChecks(int num_intervals = 1) {
    clock_.AdvanceTime(absl::Milliseconds(monitor_interval_ms_ * num_intervals));
  }

  FakeClock clock_;
  FakePeriodicalRunner runner_{clock_};
  uint64_t monitor_interval_ms_ = 1;
  std::unique_ptr<ThresholdMemoryMonitor> instance;
};

TEST_F(ThresholdMemoryMonitorTest, TestMonitorTriggerCanDetectMemoryUsage) {
  bool callback_triggered = false;

  MakeThresholdMemoryMonitor(
      0 /*memory_usage_threshold_bytes*/,
      1 /*refresh_interval_ms*/,
      [&callback_triggered](std::string) { callback_triggered = true; },
      "" /*root_cgroup_path*/);

  RunMemoryChecks();
  ASSERT_TRUE(callback_triggered);
}

TEST_F(ThresholdMemoryMonitorTest,
       TestMonitorDetectsMemoryAboveThresholdCallbackExecuted) {
  int64_t cgroup_total_bytes = 1024 * 1024 * 1024;   // 1 GB
  int64_t cgroup_current_bytes = 850 * 1024 * 1024;  // 850 MB current usage
  int64_t anon_memory_bytes = 200 * 1024 * 1024;     // 200 MB anonymous memory usage
  int64_t shmem_memory_bytes = 100 * 1024 * 1024;    // 100 MB shared memory usage
  int64_t inactive_file_bytes = 30 * 1024 * 1024;    // 30 MB inactive file cache
  int64_t active_file_bytes = 20 * 1024 * 1024;      // 20 MB active file cache
  // Working set = 850 - 30 - 20 = 800 MB (80% of 1GB, above 70% threshold)

  std::string cgroup_dir = MockCgroupv2MemoryUsage(cgroup_total_bytes,
                                                   cgroup_current_bytes,
                                                   anon_memory_bytes,
                                                   shmem_memory_bytes,
                                                   inactive_file_bytes,
                                                   active_file_bytes);

  bool callback_triggered = false;

  NoopCgroupManager noop_cgroup_manager;
  int64_t memory_usage_threshold_bytes = MemoryMonitorUtils::GetMemoryThreshold(
      cgroup_total_bytes, 0.7f, -1, false, noop_cgroup_manager);
  MakeThresholdMemoryMonitor(
      memory_usage_threshold_bytes,  // (70%)
      1 /*refresh_interval_ms*/,
      [&callback_triggered](std::string) { callback_triggered = true; },
      cgroup_dir /*root_cgroup_path*/);

  RunMemoryChecks();
  ASSERT_TRUE(callback_triggered);
}

TEST_F(ThresholdMemoryMonitorTest,
       TestMonitorDetectsMemoryBelowThresholdCallbackNotExecuted) {
  int64_t cgroup_total_bytes = 1024 * 1024 * 1024;   // 1 GB
  int64_t cgroup_current_bytes = 500 * 1024 * 1024;  // 500 MB current usage
  int64_t anon_memory_bytes = 200 * 1024 * 1024;     // 200 MB anonymous memory usage
  int64_t shmem_memory_bytes = 100 * 1024 * 1024;    // 100 MB shared memory usage
  int64_t inactive_file_bytes = 30 * 1024 * 1024;    // 30 MB inactive file cache
  int64_t active_file_bytes = 20 * 1024 * 1024;      // 20 MB active file cache
  // Working set = 500 - 30 - 20 = 450 MB (45% of 1GB, below 70% threshold)

  std::string cgroup_dir = MockCgroupv2MemoryUsage(cgroup_total_bytes,
                                                   cgroup_current_bytes,
                                                   anon_memory_bytes,
                                                   shmem_memory_bytes,
                                                   inactive_file_bytes,
                                                   active_file_bytes);

  bool callback_triggered = false;

  NoopCgroupManager noop_cgroup_manager;
  int64_t memory_usage_threshold_bytes = MemoryMonitorUtils::GetMemoryThreshold(
      cgroup_total_bytes, 0.7f, -1, false, noop_cgroup_manager);
  MakeThresholdMemoryMonitor(
      memory_usage_threshold_bytes,  // (70%)
      1 /*refresh_interval_ms*/,
      [&callback_triggered](std::string) { callback_triggered = true; },
      cgroup_dir /*root_cgroup_path*/);

  // Run the check several times; it should never trigger since usage is below
  // the threshold.
  RunMemoryChecks(/*num_intervals=*/5);

  ASSERT_FALSE(callback_triggered)
      << "Callback should not have been triggered when memory is below threshold. "
         "Are is the memory monitor correctly reading memory from the system?";
}

TEST_F(ThresholdMemoryMonitorTest,
       TestUserSliceAboveThresholdDuringResourceIsolationCallbackExecuted) {
  int64_t total_memory_bytes = 1024 * 1024 * 1024;  // 1 GB
  int64_t threshold_bytes = static_cast<int64_t>(total_memory_bytes * 0.7f);

  // User cgroup: anon=600 MB, shmem=200 MB
  int64_t user_anon_bytes = 600 * 1024 * 1024;
  int64_t user_shmem_bytes = 200 * 1024 * 1024;
  std::string user_cgroup_dir =
      MockCgroupv2MemoryUsage(total_memory_bytes,
                              user_anon_bytes + user_shmem_bytes,
                              user_anon_bytes,
                              user_shmem_bytes,
                              0 /*inactive_file*/,
                              0 /*active_file*/);

  std::string system_cgroup_dir = MockCgroupv2MemoryUsage(
      total_memory_bytes, 0, 0 /*anon*/, 0, 0 /*inactive_file*/, 0 /*active_file*/);

  // Total monitored = user_anon + user_shmem = 600+200 = 800 MB > threshold
  bool callback_triggered = false;

  MakeResourceIsolatedThresholdMemoryMonitor(
      threshold_bytes,
      1 /*refresh_interval_ms*/,
      [&callback_triggered](std::string) { callback_triggered = true; },
      "" /*root_cgroup_path*/,
      user_cgroup_dir,
      system_cgroup_dir);

  RunMemoryChecks();
  ASSERT_TRUE(callback_triggered);
}

TEST_F(
    ThresholdMemoryMonitorTest,
    TestUserSliceWithObjectStoreAboveThresholdDuringResourceIsolationCallbackExecuted) {
  int64_t total_memory_bytes = 1024 * 1024 * 1024;  // 1 GB
  int64_t threshold_bytes = static_cast<int64_t>(total_memory_bytes * 0.7f);

  // User cgroup: anon=400 MB, shmem=200 MB
  int64_t user_anon_bytes = 400 * 1024 * 1024;
  int64_t user_shmem_bytes = 200 * 1024 * 1024;
  std::string user_cgroup_dir =
      MockCgroupv2MemoryUsage(total_memory_bytes,
                              user_anon_bytes + user_shmem_bytes,
                              user_anon_bytes,
                              user_shmem_bytes,
                              0 /*inactive_file*/,
                              0 /*active_file*/);

  // System cgroup: shmem=200 MB (object store)
  int64_t system_shmem_bytes = 200 * 1024 * 1024;
  std::string system_cgroup_dir = MockCgroupv2MemoryUsage(total_memory_bytes,
                                                          system_shmem_bytes,
                                                          0 /*anon*/,
                                                          system_shmem_bytes,
                                                          0 /*inactive_file*/,
                                                          0 /*active_file*/);

  // Total monitored = user_anon + user_shmem + system_shmem = 400+200+200 = 800 MB >
  // threshold
  bool callback_triggered = false;

  MakeResourceIsolatedThresholdMemoryMonitor(
      threshold_bytes,
      1 /*refresh_interval_ms*/,
      [&callback_triggered](std::string) { callback_triggered = true; },
      "" /*root_cgroup_path*/,
      user_cgroup_dir,
      system_cgroup_dir);

  RunMemoryChecks();
  ASSERT_TRUE(callback_triggered);
}

TEST_F(ThresholdMemoryMonitorTest,
       TestResourceIsolationBelowThresholdCallbackNotExecuted) {
  int64_t total_memory_bytes = 1024 * 1024 * 1024;  // 1 GB
  int64_t threshold_bytes = static_cast<int64_t>(total_memory_bytes * 0.7f);

  // User cgroup: anon=200 MB, shmem=100 MB
  int64_t user_anon_bytes = 200 * 1024 * 1024;
  int64_t user_shmem_bytes = 100 * 1024 * 1024;
  std::string user_cgroup_dir =
      MockCgroupv2MemoryUsage(total_memory_bytes,
                              user_anon_bytes + user_shmem_bytes,
                              user_anon_bytes,
                              user_shmem_bytes,
                              0 /*inactive_file*/,
                              0 /*active_file*/);

  // System cgroup: shmem=100 MB
  int64_t system_shmem_bytes = 100 * 1024 * 1024;
  std::string system_cgroup_dir = MockCgroupv2MemoryUsage(total_memory_bytes,
                                                          system_shmem_bytes,
                                                          0 /*anon*/,
                                                          system_shmem_bytes,
                                                          0 /*inactive_file*/,
                                                          0 /*active_file*/);

  // Total monitored = 200+100+100 = 400 MB < threshold
  bool callback_triggered = false;

  MakeResourceIsolatedThresholdMemoryMonitor(
      threshold_bytes,
      1 /*refresh_interval_ms*/,
      [&callback_triggered](std::string) { callback_triggered = true; },
      "" /*root_cgroup_path*/,
      user_cgroup_dir,
      system_cgroup_dir);

  // Run the check several times; it should never trigger since usage is below
  // the threshold.
  RunMemoryChecks(/*num_intervals=*/5);

  ASSERT_FALSE(callback_triggered)
      << "Callback should not have been triggered when user slice memory is below "
         "threshold.";
}

}  // namespace ray
