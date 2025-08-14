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

// Precondition for the test suite:
// - If run on local dev environment, don't mount cgroupv2 as rw mode.
// - If run on remote CI, run in non-privileged container mode (where cgroup is mounted as
// read-only mode).

#include <gtest/gtest.h>

#include "ray/common/cgroup/cgroup_setup.h"
#include "ray/common/test/testing.h"

namespace ray::internal {

namespace {

TEST(CgroupV2UtilsTest, CheckCgroupV2Mount) {
#ifndef __linux__
  // Error case: cgroup feature is not supported on non-linux platforms.
  EXPECT_EQ(CheckCgroupV2MountedRW("/sys/fs/cgroup").code(), StatusCode::Invalid);
#else
  // Error case: cgroup directory exists, but not writable.
  EXPECT_EQ(CheckCgroupV2MountedRW("/sys/fs/cgroup").code(), StatusCode::InvalidArgument);
#endif  // __linux__
}

}  // namespace

}  // namespace ray::internal
