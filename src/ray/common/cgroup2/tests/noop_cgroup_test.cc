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

#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_manager.h"
#include "ray/common/cgroup2/sysfs_cgroup_driver.h"
#include "ray/common/status.h"
namespace ray {

TEST(NoopCgroupTest, NoopCgroupDriverAndManagerBuildSuccessfullyOnAllPlatforms) {
  std::unique_ptr<SysFsCgroupDriver> sysfs_cgroup_driver =
      std::make_unique<SysFsCgroupDriver>();
  auto cgroup_manager =
      CgroupManager::Create("", "", 1, 1, std::move(sysfs_cgroup_driver));
}

}  // namespace ray
