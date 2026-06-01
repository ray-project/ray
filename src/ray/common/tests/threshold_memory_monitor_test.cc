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
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/cgroup2/noop_cgroup_manager.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/common/memory_monitor_test_fixture.h"
#include "ray/common/memory_monitor_utils.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {

// A fake CgroupManagerInterface that reports a fixed numeric upper bound for
// the user-slice memory, so resource-isolation tests can exercise the
// threshold computation without needing real cgroup files.
class FakeBoundedCgroupManager : public CgroupManagerInterface {
 public:
  explicit FakeBoundedCgroupManager(int64_t user_upper_bound_bytes)
      : user_upper_bound_bytes_(user_upper_bound_bytes) {}

  Status AddProcessToWorkersCgroup(const std::string &) override { return Status::OK(); }
  Status AddProcessToSystemCgroup(const std::string &) override { return Status::OK(); }
  std::string GetSystemCgroupPath() const override { return ""; }
  std::string GetUserCgroupPath() const override { return ""; }
  StatusOr<std::string> GetSystemCgroupConstraintValue(
      const std::string &) const override {
    return std::string{};
  }
  StatusOr<std::string> GetUserCgroupConstraintValue(const std::string &) const override {
    return std::to_string(user_upper_bound_bytes_);
  }

 private:
  int64_t user_upper_bound_bytes_;
};

class ThresholdMemoryMonitorTest : public MemoryMonitorTestFixture {
 protected:
  void TearDown() override { instance.reset(); }

  ThresholdMemoryMonitor &MakeThresholdMemoryMonitor(
      float usage_threshold,
      uint64_t monitor_interval_ms,
      KillWorkersCallback kill_workers_callback,
      const std::string &root_cgroup_path) {
    instance = std::make_unique<ThresholdMemoryMonitor>(
        std::move(kill_workers_callback),
        usage_threshold,
        /*min_memory_free_bytes=*/MemoryMonitorInterface::kNull,
        monitor_interval_ms,
        /*resource_isolation_enabled=*/false,
        noop_cgroup_manager_,
        root_cgroup_path);
    return *instance;
  }

  ThresholdMemoryMonitor &MakeResourceIsolatedThresholdMemoryMonitor(
      float usage_threshold,
      uint64_t monitor_interval_ms,
      KillWorkersCallback kill_workers_callback,
      const std::string &root_cgroup_path,
      const std::string &user_cgroup_path,
      const std::string &system_cgroup_path,
      const CgroupManagerInterface &cgroup_manager) {
    instance = std::make_unique<ThresholdMemoryMonitor>(
        std::move(kill_workers_callback),
        usage_threshold,
        /*min_memory_free_bytes=*/MemoryMonitorInterface::kNull,
        monitor_interval_ms,
        /*resource_isolation_enabled=*/true,
        cgroup_manager,
        root_cgroup_path,
        user_cgroup_path,
        system_cgroup_path);
    return *instance;
  }

  NoopCgroupManager noop_cgroup_manager_;
  std::unique_ptr<ThresholdMemoryMonitor> instance;
};

TEST_F(ThresholdMemoryMonitorTest, TestMonitorTriggerCanDetectMemoryUsage) {
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MakeThresholdMemoryMonitor(
      0.0f /*usage_threshold*/,
      1 /*refresh_interval_ms*/,
      [has_checked_once](std::string) { has_checked_once->count_down(); },
      "" /*root_cgroup_path*/);
  has_checked_once->wait();
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

  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  MakeThresholdMemoryMonitor(
      0.7f /*usage_threshold*/,
      1 /*refresh_interval_ms*/,
      [has_checked_once](std::string) { has_checked_once->count_down(); },
      cgroup_dir /*root_cgroup_path*/);

  has_checked_once->wait();
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

  std::shared_ptr<std::atomic<bool>> callback_triggered =
      std::make_shared<std::atomic<bool>>(false);

  MakeThresholdMemoryMonitor(
      0.7f /*usage_threshold*/,
      1 /*refresh_interval_ms*/,
      [callback_triggered](std::string) { callback_triggered->store(true); },
      cgroup_dir /*root_cgroup_path*/);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  ASSERT_FALSE(callback_triggered->load())
      << "Callback should not have been triggered when memory is below threshold. "
         "Are is the memory monitor correctly reading memory from the system?";
}

TEST_F(ThresholdMemoryMonitorTest,
       TestUserSliceAboveThresholdDuringResourceIsolationCallbackExecuted) {
  int64_t total_memory_bytes = 1024 * 1024 * 1024;  // 1 GB

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
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  FakeBoundedCgroupManager cgroup_manager(
      static_cast<int64_t>(total_memory_bytes * 0.7f));
  MakeResourceIsolatedThresholdMemoryMonitor(
      0.7f /*usage_threshold*/,
      1 /*refresh_interval_ms*/,
      [has_checked_once](std::string) { has_checked_once->count_down(); },
      "" /*root_cgroup_path*/,
      user_cgroup_dir,
      system_cgroup_dir,
      cgroup_manager);

  has_checked_once->wait();
}

TEST_F(
    ThresholdMemoryMonitorTest,
    TestUserSliceWithObjectStoreAboveThresholdDuringResourceIsolationCallbackExecuted) {
  int64_t total_memory_bytes = 1024 * 1024 * 1024;  // 1 GB

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
  std::shared_ptr<boost::latch> has_checked_once = std::make_shared<boost::latch>(1);

  FakeBoundedCgroupManager cgroup_manager(
      static_cast<int64_t>(total_memory_bytes * 0.7f));
  MakeResourceIsolatedThresholdMemoryMonitor(
      0.7f /*usage_threshold*/,
      1 /*refresh_interval_ms*/,
      [has_checked_once](std::string) { has_checked_once->count_down(); },
      "" /*root_cgroup_path*/,
      user_cgroup_dir,
      system_cgroup_dir,
      cgroup_manager);

  has_checked_once->wait();
}

// Verify that the threshold tracks the cgroup memory limit at runtime: if the
// cgroup memory.max shrinks (e.g. Kubernetes in-place pod resize downsizes the
// container), the monitor must recompute the threshold against the new limit
// and start triggering, even though the original limit was high enough that
// the same usage was previously safe.
TEST_F(ThresholdMemoryMonitorTest, TestThresholdTracksRuntimeCgroupLimitChanges) {
  const int64_t initial_total_bytes = 2L * 1024 * 1024 * 1024;  // 2 GB
  const int64_t shrunk_total_bytes = 1L * 1024 * 1024 * 1024;   // 1 GB
  const int64_t cgroup_current_bytes = 850 * 1024 * 1024;       // 850 MB
  const int64_t anon_memory_bytes = 800 * 1024 * 1024;
  const int64_t shmem_memory_bytes = 0;
  const int64_t inactive_file_bytes = 30 * 1024 * 1024;
  const int64_t active_file_bytes = 20 * 1024 * 1024;
  // Working set = 850 - 30 - 20 = 800 MB.
  // Initial: 800 MB / 2 GB = 40% < 70% threshold → no trigger.
  // After shrink: 800 MB / 1 GB = 80% > 70% threshold → trigger.

  std::string cgroup_dir = MockCgroupv2MemoryUsage(initial_total_bytes,
                                                   cgroup_current_bytes,
                                                   anon_memory_bytes,
                                                   shmem_memory_bytes,
                                                   inactive_file_bytes,
                                                   active_file_bytes);

  std::shared_ptr<boost::latch> has_triggered = std::make_shared<boost::latch>(1);

  MakeThresholdMemoryMonitor(
      0.7f /*usage_threshold*/,
      1 /*refresh_interval_ms*/,
      [has_triggered](std::string) { has_triggered->count_down(); },
      cgroup_dir);

  // Confirm no trigger under the original limit.
  ASSERT_EQ(boost::cv_status::timeout,
            has_triggered->wait_for(boost::chrono::milliseconds(500)))
      << "Callback should not fire while cgroup memory.max is large enough that "
         "the working set stays below the threshold.";

  // Simulate an IPPR downsize: rewrite cgroup memory.max in place. The next
  // poll should recompute the threshold against the smaller limit and trigger.
  {
    std::ofstream out(cgroup_dir + "/" + MemoryMonitorUtils::kCgroupsV2MemoryMaxPath,
                      std::ios::trunc);
    out << shrunk_total_bytes << "\n";
  }

  has_triggered->wait();
}

TEST_F(ThresholdMemoryMonitorTest,
       TestResourceIsolationBelowThresholdCallbackNotExecuted) {
  int64_t total_memory_bytes = 1024 * 1024 * 1024;  // 1 GB

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
  std::shared_ptr<std::atomic<bool>> callback_triggered =
      std::make_shared<std::atomic<bool>>(false);

  FakeBoundedCgroupManager cgroup_manager(
      static_cast<int64_t>(total_memory_bytes * 0.7f));
  MakeResourceIsolatedThresholdMemoryMonitor(
      0.7f /*usage_threshold*/,
      1 /*refresh_interval_ms*/,
      [callback_triggered](std::string) { callback_triggered->store(true); },
      "" /*root_cgroup_path*/,
      user_cgroup_dir,
      system_cgroup_dir,
      cgroup_manager);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  ASSERT_FALSE(callback_triggered->load())
      << "Callback should not have been triggered when user slice memory is below "
         "threshold.";
}

}  // namespace ray
