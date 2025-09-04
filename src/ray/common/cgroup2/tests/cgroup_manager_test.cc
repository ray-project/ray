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

TEST(CgroupManagerTest, CreateReturnsInvalidArgumentIfConstraintValuesOutOfBounds) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  // By default no controllers are available.
  cgroups->emplace("/sys/fs/cgroup", FakeCgroup{"/sys/fs/cgroup"});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};
  FakeCgroupDriver *driver = new FakeCgroupDriver(cgroups);
  auto cgroup_manager_s =
      CgroupManager::Create("/sys/fs/cgroup",
                            "node_id_123",
                            -1,
                            -1,
                            std::unique_ptr<CgroupDriverInterface>(driver));
  ASSERT_TRUE(cgroup_manager_s.IsInvalidArgument()) << cgroup_manager_s.ToString();
  // No visible side-effects
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_EQ(cgroups->begin()->second, base_cgroup);
}

// I'll write this as one long happy path test that also tests the destructor
// FakeCgroupDriver needs to record all operations it's required to do in order
// then track them in reverse :)
TEST(CgroupManagerTest, CreateSucceedsWithCleanupInOrder) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();

  // By default no controllers are available.
  cgroups->emplace("/sys/fs/cgroup",
                   FakeCgroup{"/sys/fs/cgroup", {5}, {}, {"cpu", "memory"}, {}});
  FakeCgroupDriver *driver = new FakeCgroupDriver(cgroups);

  // node, system, and application cgroups were created in the fake
  std::string node_id = "id_123";
  std::string base_cgroup_path = "/sys/fs/cgroup";
  std::string node_cgroup_path = "/sys/fs/cgroup/ray_node_id_123";
  std::string system_cgroup_path = "/sys/fs/cgroup/ray_node_id_123/system";
  std::string application_cgroup_path = "/sys/fs/cgroup/ray_node_id_123/application";
  int64_t system_reserved_cpu_weight = 1000;
  int64_t system_reserved_memory_bytes = 1024 * 1024 * 1024;

  auto cgroup_manager_s =
      CgroupManager::Create(base_cgroup_path,
                            node_id,
                            system_reserved_cpu_weight,
                            system_reserved_memory_bytes,
                            std::unique_ptr<CgroupDriverInterface>(driver));

  // The cgroup hierarchy was created correctly.
  ASSERT_EQ(cgroups->size(), 4);
  ASSERT_NE(cgroups->find(base_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(node_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(system_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(application_cgroup_path), cgroups->end());

  std::array<FakeCgroup *, 4> created_cgroups{&cgroups->at(base_cgroup_path),
                                              &cgroups->at(node_cgroup_path),
                                              &cgroups->at(system_cgroup_path),
                                              &cgroups->at(application_cgroup_path)};

  // Controllers are enabled on base, node, application, and system cgroups.
  for (const FakeCgroup *cg : created_cgroups) {
    ASSERT_EQ(cg->enabled_controllers_.size(), 2);
    ASSERT_NE(cg->enabled_controllers_.find("cpu"), cg->enabled_controllers_.end());
    ASSERT_NE(cg->enabled_controllers_.find("memory"), cg->enabled_controllers_.end());
  }

  // Processes were moved out of the base cgroup into the system cgroup.
  const FakeCgroup &base_cgroup = cgroups->find(base_cgroup_path)->second;
  const FakeCgroup &system_cgroup = cgroups->find(system_cgroup_path)->second;
  ASSERT_TRUE(base_cgroup.processes_.empty());
  ASSERT_EQ(system_cgroup.processes_.size(), 1);

  // Check to see that the memory and cpu constraints were enabled correctly
  // for the system and application cgroups.
  ASSERT_EQ(system_cgroup.constraints_.size(), 2);
  ASSERT_NE(system_cgroup.constraints_.find("cpu.weight"),
            system_cgroup.constraints_.end());
  ASSERT_EQ(system_cgroup.constraints_.at("cpu.weight"),
            std::to_string(system_reserved_cpu_weight));
  ASSERT_EQ(system_cgroup.constraints_.at("memory.min"),
            std::to_string(system_reserved_memory_bytes));

  const FakeCgroup &app_cgroup = cgroups->find(application_cgroup_path)->second;
  ASSERT_EQ(app_cgroup.constraints_.size(), 1);
  ASSERT_NE(app_cgroup.constraints_.find("cpu.weight"), app_cgroup.constraints_.end());
  ASSERT_EQ(app_cgroup.constraints_.at("cpu.weight"),
            std::to_string(10000 - system_reserved_cpu_weight));

  // Destroying the cgroup manager triggers automatic cleanup.
  std::unique_ptr<CgroupManager> cgroup_manager = std::move(cgroup_manager_s.value());
  cgroup_manager.reset();
  // Only the base cgroup is left after the cgroup_manager is destroyed.
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_NE(cgroups->find(base_cgroup_path), cgroups->end());
}

}  // namespace ray
