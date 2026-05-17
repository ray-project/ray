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
#include "ray/common/cgroup2/fake_cgroup_manager.h"
#include "ray/common/event_memory_monitor.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/common/threshold_memory_monitor.h"

namespace ray {

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
       TestCreateWithResourceIsolationEnabledReturnsThresholdAndEventMonitors) {
  FakeCgroupManager cgroup_manager(kUserMemoryMaxBytes, kUserMemoryHighBytes);

  std::vector<std::unique_ptr<MemoryMonitorInterface>> monitors =
      MemoryMonitorFactory::Create([](std::string) {},
                                   /*resource_isolation_enabled=*/true,
                                   cgroup_manager);

  ASSERT_EQ(monitors.size(), 2u) << "Expected exactly two monitors";
  EXPECT_NE(dynamic_cast<EventMemoryMonitor *>(monitors[0].get()), nullptr)
      << "Expected the second monitor to be an EventMemoryMonitor";
  EXPECT_NE(dynamic_cast<ThresholdMemoryMonitor *>(monitors[1].get()), nullptr)
      << "Expected the first monitor to be a ThresholdMemoryMonitor";
}

}  // namespace ray
