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

#include "ray/common/cgroup2/cgroup_manager.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/fake_cgroup_driver.h"
#include "ray/common/status.h"
namespace ray {

TEST(CgroupManagerTest, CreateReturnsInvalidIfCgroupv2NotAvailable) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace("/sys/fs/cgroup", FakeCgroup{"/sys/fs/cgroup"});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};
  FakeCgroupDriver *driver = new FakeCgroupDriver(cgroups);
  driver->check_cgroup_enabled_s_ = Status::Invalid("");
  auto cgroup_manager_s =
      CgroupManager::Create("/sys/fs/cgroup/ray",
                            "node_id_123",
                            100,
                            1000000,
                            std::unique_ptr<CgroupDriverInterface>(driver));
  ASSERT_TRUE(cgroup_manager_s.IsInvalid()) << cgroup_manager_s.ToString();
  // No visible side-effects
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_EQ(cgroups->begin()->second, base_cgroup);
}

TEST(CgroupManagerTest, CreateReturnsNotFoundIfBaseCgroupDoesNotExist) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  FakeCgroupDriver *driver = new FakeCgroupDriver(cgroups);
  driver->check_cgroup_s_ = Status::NotFound("");
  auto cgroup_manager_s =
      CgroupManager::Create("/sys/fs/cgroup/ray",
                            "node_id_123",
                            100,
                            1000000,
                            std::unique_ptr<CgroupDriverInterface>(driver));
  ASSERT_TRUE(cgroup_manager_s.IsNotFound()) << cgroup_manager_s.ToString();
  // No visible side-effects
  ASSERT_EQ(cgroups->size(), 0);
}

TEST(CgroupManagerTest,
     CreateReturnsNotFoundIfProcessDoesNotHavePermissionsForBaseCgroup) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace("/sys/fs/cgroup", FakeCgroup{"/sys/fs/cgroup"});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};
  FakeCgroupDriver *driver = new FakeCgroupDriver(cgroups);
  driver->check_cgroup_s_ = Status::PermissionDenied("");
  auto cgroup_manager_s =
      CgroupManager::Create("/sys/fs/cgroup/ray",
                            "node_id_123",
                            100,
                            1000000,
                            std::unique_ptr<CgroupDriverInterface>(driver));
  ASSERT_TRUE(cgroup_manager_s.IsPermissionDenied()) << cgroup_manager_s.ToString();
  // No visible side-effects
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_EQ(cgroups->begin()->second, base_cgroup);
}

TEST(CgroupManagerTest, CreateReturnsInvalidIfSupportedControllersAreNotAvailable) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  // By default no controllers are available.
  cgroups->emplace("/sys/fs/cgroup", FakeCgroup{"/sys/fs/cgroup"});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};
  FakeCgroupDriver *driver = new FakeCgroupDriver(cgroups);
  auto cgroup_manager_s =
      CgroupManager::Create("/sys/fs/cgroup",
                            "node_id_123",
                            100,
                            1000000,
                            std::unique_ptr<CgroupDriverInterface>(driver));
  ASSERT_TRUE(cgroup_manager_s.IsInvalid()) << cgroup_manager_s.ToString();
  // No visible side-effects
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_EQ(cgroups->begin()->second, base_cgroup);
}

}  // namespace ray
