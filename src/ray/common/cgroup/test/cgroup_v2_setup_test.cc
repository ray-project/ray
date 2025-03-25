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
//
// Precondition: cgroup V2 has already been mounted as rw.
//
// Setup command:
// sudo umount /sys/fs/cgroup/unified
// sudo mount -t cgroup2 cgroup2 /sys/fs/cgroup/unified -o rw
//
// Execution command:
// sudo bazel-bin/src/ray/common/cgroup/test/cgroup_v2_setup_test

#include <gtest/gtest.h>

#include <filesystem>

#include "ray/common/cgroup/cgroup_setup.h"

namespace ray {

namespace {

#ifndef __linux__
TEST(Cgroupv2SetupTest, NonLinuxCrashTest) {
  EXPECT_EXIT(CgroupSetup{},
              testing::ExitedWithCode(EXIT_FAILURE),
              "cgroupv2 doesn't work on non linux platform.");
}
#else
TEST(Cgroupv2SetupTest, SetupTest) {
  CgroupSetup cgroup_setup{"/sys/fs/cgroup", "node_id"};

  // Check internal cgroup is created successfully.
  std::error_code err_code;
  bool exists =
      std::filesystem::exists("/sys/fs/cgroup/ray_node_node_id/internal", err_code);
  EXPECT_TRUE(err_code);
  ASSERT_TRUE(exists);

  // Check application cgroup is created successfully.
  exists = std::filesystem::exists("/sys/fs/cgroup/ray_node_node_id/ray_application",
                                   err_code);
  EXPECT_TRUE(err_code);
  ASSERT_TRUE(exists);
}
#endif

}  // namespace

}  // namespace ray
