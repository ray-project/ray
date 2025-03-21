// Copyright 2025 The Ray Authors.
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

#include "ray/common/cgroup/fake_cgroup_setup.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

namespace ray {

namespace {

// Add and remove a few system and application cgroup from fake cgroup accessor.
TEST(FakeCgroupSetupTest, AddAndRemoveTest) {
  {
    FakeCgroupSetup fake_cgroup_setup{"node-id"};
    auto system_handler = fake_cgroup_setup.AddSystemProcess(0);

    AppProcCgroupMetadata meta1;
    meta1.pid = 1;
    meta1.max_memory = 10;
    auto application_handler1 = fake_cgroup_setup.ApplyCgroupContext(meta1);

    AppProcCgroupMetadata meta2;
    meta2.pid = 2;
    meta2.max_memory = 10;
    auto application_handler2 = fake_cgroup_setup.ApplyCgroupContext(meta2);

    AppProcCgroupMetadata meta3;
    meta3.pid = 2;
    meta3.max_memory = 5;  // Different max memory with previous applications.
    auto application_handler3 = fake_cgroup_setup.ApplyCgroupContext(meta3);
  }
  // Make sure fake cgroup setup destructs with no problem.

  // Use multiple thread to apply cgroup context.
  constexpr int kThdNum = 100;
  {
    FakeCgroupSetup fake_cgroup_setup{"node-id"};
    std::vector<std::thread> thds;
    thds.reserve(kThdNum);
    auto system_handler = fake_cgroup_setup.AddSystemProcess(0);
    for (int idx = 0; idx < kThdNum; ++idx) {
      thds.emplace_back([pid = idx, &fake_cgroup_setup]() {
        AppProcCgroupMetadata meta;
        meta.pid = pid;
        meta.max_memory = 10;
        fake_cgroup_setup.ApplyCgroupContext(meta);
      });
    }
    for (auto &cur_thd : thds) {
      cur_thd.join();
    }
  }
  // Make sure fake cgroup setup destructs with no problem.
}

}  // namespace

}  // namespace ray
