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

#include "ray/common/cgroup/cgroup_utils.h"

#include <gtest/gtest.h>

#ifdef __linux__

#include <sys/types.h>
#include <sys/wait.h>

#include <chrono>
#include <thread>

#include "ray/common/cgroup/test/cgroup_test_utils.h"
#include "ray/common/test/testing.h"

namespace ray {

namespace {

TEST(CgroupUtilsTest, AddCurrentProcessToCgroup) {
  CgroupSetupConfig setup_config;
  setup_config.type = CgroupSetupType::kProd;
  setup_config.directory = "/sys/fs/cgroup";
  setup_config.node_id = "node_id";

  pid_t pid = fork();
  ASSERT_NE(pid, -1);

  // Child process.
  if (pid == 0) {
    // Spawn a process running long enough, so it could be added into system cgroup.
    // It won't affect test runtime, because it will be killed later.
    std::this_thread::sleep_for(std::chrono::seconds(3600));
    // Exit without flushing the buffer.
    std::_Exit(0);
  }

  AppProcCgroupMetadata ctx;
  ctx.pid = pid;
  ctx.max_memory = 0;  // No limit specified.
  auto handle = AddCurrentProcessToCgroup(setup_config, ctx);

  AssertPidInCgroup(
      pid, "/sys/fs/cgroup/ray_node_node_id/ray_application/default/cgroup.procs");

  // Kill testing process.
  RAY_ASSERT_OK(KillAllProcAndWait("/sys/fs/cgroup/ray_node_node_id/ray_application"));
}

}  // namespace

}  // namespace ray

#endif  // __linux__
