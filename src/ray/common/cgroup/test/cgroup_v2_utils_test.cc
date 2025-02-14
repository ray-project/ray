// Copyright 2024 The Ray Authors.
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

#include <gtest/gtest.h>

#include "ray/common/cgroup/cgroup_setup.h"

namespace ray::internal {

namespace {

// Precondition: cgroup V2 has already been mounted as rw.
//
// Setup command:
// sudo umount /sys/fs/cgroup/unified
// sudo mount -t cgroup2 cgroup2 /sys/fs/cgroup/unified -o rw
TEST(CgroupV2UtilsTest, CheckCgroupV2Mount) { EXPECT_TRUE(IsCgroupV2MountedAsRw()); }

TEST(CgroupV2UtilsTest, CgroupV2Permission) {
  if (getuid() == 0) {
    EXPECT_TRUE(CanCurrenUserWriteCgroupV2());
    return;
  }
  EXPECT_FALSE(CanCurrenUserWriteCgroupV2());
}

}  // namespace

}  // namespace ray::internal
