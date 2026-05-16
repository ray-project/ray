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

#include "ray/common/memory_monitor_factory.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/common/threshold_memory_monitor.h"

namespace ray {

class FakeCgroupManager : public CgroupManagerInterface {
 public:
  explicit FakeCgroupManager(int64_t user_memory_max_bytes,
                             int64_t user_memory_high_bytes)
      : user_memory_max_bytes_(user_memory_max_bytes),
        user_memory_high_bytes_(user_memory_high_bytes) {
    StatusOr<std::unique_ptr<TempDirectory>> temp_dir_or = TempDirectory::Create();
    RAY_CHECK(temp_dir_or.ok()) << temp_dir_or.status().ToString();
    temp_dir_ = std::move(temp_dir_or.value());
  }

  Status AddProcessToWorkersCgroup(const std::string &) override { return Status::OK(); }
  Status AddProcessToSystemCgroup(const std::string &) override { return Status::OK(); }

  std::string GetUserCgroupPath() const override { return temp_dir_->GetPath(); }
  std::string GetSystemCgroupPath() const override { return temp_dir_->GetPath(); }

  StatusOr<std::string> GetSystemCgroupConstraintValue(
      const std::string &) const override {
    return Status::IOError("not implemented");
  }

  StatusOr<std::string> GetUserCgroupConstraintValue(
      const std::string &constraint_name) const override {
    if (constraint_name == "memory.max") {
      return std::to_string(user_memory_max_bytes_);
    }
    if (constraint_name == "memory.high") {
      return std::to_string(user_memory_high_bytes_);
    }
    return Status::IOError("constraint not found: " + constraint_name);
  }

  const std::string &GetPath() const { return temp_dir_->GetPath(); }

 private:
  std::unique_ptr<TempDirectory> temp_dir_;
  int64_t user_memory_max_bytes_;
  int64_t user_memory_high_bytes_;
};

class MemoryMonitorFactoryTest : public ::testing::Test {
 protected:
  static constexpr int64_t kUserMemoryMaxBytes = 10LL * 1024 * 1024 * 1024;  // 10 GB
  static constexpr int64_t kUserMemoryHighBytes = 8LL * 1024 * 1024 * 1024;  // 8 GB
};

TEST_F(MemoryMonitorFactoryTest,
       TestCreateWithResourceIsolationDisabledReturnsOnlyThresholdMonitor) {
  FakeCgroupManager cgroup_manager(kUserMemoryMaxBytes, kUserMemoryHighBytes);

  std::vector<std::unique_ptr<MemoryMonitorInterface>> monitors =
      MemoryMonitorFactory::Create([](std::string) {},
                                   /*resource_isolation_enabled=*/false,
                                   cgroup_manager);

  ASSERT_EQ(monitors.size(), 1u) << "Expected exactly one monitor";
  EXPECT_NE(dynamic_cast<ThresholdMemoryMonitor *>(monitors[0].get()), nullptr)
      << "Expected the sole monitor to be a ThresholdMemoryMonitor";
}

TEST_F(MemoryMonitorFactoryTest,
       TestCreateWithResourceIsolationEnabledReturnsThresholdMonitors) {
  FakeCgroupManager cgroup_manager(kUserMemoryMaxBytes, kUserMemoryHighBytes);
  TempFile pressure_file(cgroup_manager.GetPath() + "/memory.pressure");

  std::vector<std::unique_ptr<MemoryMonitorInterface>> monitors =
      MemoryMonitorFactory::Create([](std::string) {},
                                   /*resource_isolation_enabled=*/true,
                                   cgroup_manager);

  ASSERT_EQ(monitors.size(), 1u) << "Expected exactly one monitor";
  EXPECT_NE(dynamic_cast<ThresholdMemoryMonitor *>(monitors[0].get()), nullptr)
      << "Expected the sole monitor to be a ThresholdMemoryMonitor";
}

}  // namespace ray
