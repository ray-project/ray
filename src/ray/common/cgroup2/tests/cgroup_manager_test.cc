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

  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);

  driver->check_cgroup_enabled_s_ = Status::Invalid("");
  auto cgroup_manager_s = CgroupManager::Create(
      "/sys/fs/cgroup/ray", "node_id_123", 100, 1000000, std::move(driver));
  ASSERT_TRUE(cgroup_manager_s.IsInvalid()) << cgroup_manager_s.ToString();
  // No visible side-effects
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_EQ(cgroups->begin()->second, base_cgroup);
}

TEST(CgroupManagerTest, CreateReturnsNotFoundIfBaseCgroupDoesNotExist) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);
  driver->check_cgroup_s_ = Status::NotFound("");
  auto cgroup_manager_s = CgroupManager::Create(
      "/sys/fs/cgroup/ray", "node_id_123", 100, 1000000, std::move(driver));
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
  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);
  driver->check_cgroup_s_ = Status::PermissionDenied("");
  auto cgroup_manager_s = CgroupManager::Create(
      "/sys/fs/cgroup/ray", "node_id_123", 100, 1000000, std::move(driver));
  ASSERT_TRUE(cgroup_manager_s.IsPermissionDenied()) << cgroup_manager_s.ToString();
  // No visible side-effects
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_EQ(cgroups->begin()->second, base_cgroup);
}

TEST(CgroupManagerTest, CreateReturnsInvalidIfSupportedControllersAreNotAvailable) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace("/sys/fs/cgroup", FakeCgroup{"/sys/fs/cgroup"});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};
  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);
  auto cgroup_manager_s = CgroupManager::Create(
      "/sys/fs/cgroup", "node_id_123", 100, 1000000, std::move(driver));
  ASSERT_TRUE(cgroup_manager_s.IsInvalid()) << cgroup_manager_s.ToString();
  // No visible side-effects
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_EQ(cgroups->begin()->second, base_cgroup);
}

TEST(CgroupManagerTest, CreateReturnsInvalidArgumentIfConstraintValuesOutOfBounds) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace("/sys/fs/cgroup", FakeCgroup{"/sys/fs/cgroup"});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};
  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);
  auto cgroup_manager_s =
      CgroupManager::Create("/sys/fs/cgroup", "node_id_123", -1, -1, std::move(driver));
  ASSERT_TRUE(cgroup_manager_s.IsInvalidArgument()) << cgroup_manager_s.ToString();
  // No visible side-effects
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_EQ(cgroups->begin()->second, base_cgroup);
}

TEST(CgroupManagerTest, CreateSucceedsWithCleanupInOrder) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();

  cgroups->emplace("/sys/fs/cgroup",
                   FakeCgroup{"/sys/fs/cgroup", {5}, {}, {"cpu", "memory"}, {}});

  auto deleted_cgroups = std::make_shared<std::vector<std::pair<int, std::string>>>();
  auto constraints_disabled =
      std::make_shared<std::vector<std::pair<int, FakeConstraint>>>();
  auto controllers_disabled =
      std::make_shared<std::vector<std::pair<int, FakeController>>>();
  auto processes_moved =
      std::make_shared<std::vector<std::pair<int, FakeMoveProcesses>>>();

  std::unique_ptr<FakeCgroupDriver> owned_driver =
      FakeCgroupDriver::Create(cgroups,
                               deleted_cgroups,
                               constraints_disabled,
                               controllers_disabled,
                               processes_moved);

  FakeCgroupDriver *driver = owned_driver.get();

  // node, system, and application cgroups were created in the fake
  std::string node_id = "id_123";
  std::string base_cgroup_path = "/sys/fs/cgroup";
  std::string node_cgroup_path = "/sys/fs/cgroup/ray_node_id_123";
  std::string system_cgroup_path = "/sys/fs/cgroup/ray_node_id_123/system";
  std::string system_leaf_cgroup_path = "/sys/fs/cgroup/ray_node_id_123/system/leaf";
  std::string application_cgroup_path = "/sys/fs/cgroup/ray_node_id_123/application";
  std::string application_leaf_cgroup_path =
      "/sys/fs/cgroup/ray_node_id_123/application/leaf";
  int64_t system_reserved_cpu_weight = 1000;
  int64_t system_reserved_memory_bytes = 1024 * 1024 * 1024;

  auto cgroup_manager_s = CgroupManager::Create(base_cgroup_path,
                                                node_id,
                                                system_reserved_cpu_weight,
                                                system_reserved_memory_bytes,
                                                std::move(owned_driver));

  // The cgroup hierarchy was created correctly.
  ASSERT_EQ(cgroups->size(), 6);
  ASSERT_NE(cgroups->find(base_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(node_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(system_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(system_leaf_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(application_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(application_leaf_cgroup_path), cgroups->end());

  std::array<FakeCgroup *, 4> controlled_cgroups{&cgroups->at(base_cgroup_path),
                                                 &cgroups->at(node_cgroup_path),
                                                 &cgroups->at(system_cgroup_path),
                                                 &cgroups->at(application_cgroup_path)};

  // Controllers are enabled on base, node, application, and system cgroups.
  for (const FakeCgroup *cg : controlled_cgroups) {
    ASSERT_EQ(cg->enabled_controllers_.size(), 2);
    ASSERT_NE(cg->enabled_controllers_.find("cpu"), cg->enabled_controllers_.end());
    ASSERT_NE(cg->enabled_controllers_.find("memory"), cg->enabled_controllers_.end());
  }

  // Processes were moved out of the base cgroup into the system leaf cgroup.
  const FakeCgroup &base_cgroup = cgroups->find(base_cgroup_path)->second;
  const FakeCgroup &system_cgroup = cgroups->find(system_cgroup_path)->second;
  const FakeCgroup &system_leaf_cgroup = cgroups->find(system_leaf_cgroup_path)->second;
  ASSERT_TRUE(base_cgroup.processes_.empty());
  ASSERT_EQ(system_leaf_cgroup.processes_.size(), 1);

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

  // Switching the mode of the FakeCgroupDriver to cleanup to record cleanup
  // operations
  driver->cleanup_mode_ = true;
  // Destroying the cgroup manager triggers automatic cleanup.
  std::unique_ptr<CgroupManager> cgroup_manager = std::move(cgroup_manager_s.value());
  cgroup_manager.reset();

  // Only the base cgroup is left after the cgroup_manager is destroyed.
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_NE(cgroups->find(base_cgroup_path), cgroups->end());

  // Since the order of operation matters during cleanup for cgroups, we're going
  // to have to check the fake for side-effects extensively:
  //
  // Constraints have to be disabled before controllers are disabled.
  ASSERT_EQ(constraints_disabled->size(), 3);
  // Since constraints were only enabled on leaf nodes, the order does not matter.
  ASSERT_EQ(
      std::count_if(constraints_disabled->begin(),
                    constraints_disabled->end(),
                    [&system_cgroup_path](const std::pair<int, FakeConstraint> &item) {
                      return item.second.cgroup_ == system_cgroup_path &&
                             item.second.name_ == "cpu.weight";
                    }),
      1);
  ASSERT_EQ(
      std::count_if(constraints_disabled->begin(),
                    constraints_disabled->end(),
                    [&system_cgroup_path](const std::pair<int, FakeConstraint> &item) {
                      return item.second.cgroup_ == system_cgroup_path &&
                             item.second.name_ == "memory.min";
                    }),
      1);
  ASSERT_EQ(std::count_if(
                constraints_disabled->begin(),
                constraints_disabled->end(),
                [&application_cgroup_path](const std::pair<int, FakeConstraint> &item) {
                  return item.second.cgroup_ == application_cgroup_path &&
                         item.second.name_ == "cpu.weight";
                }),
            1);

  // Controllers were disabled second.
  ASSERT_EQ(controllers_disabled->size(), 8);
  // Controllers must be disabled after the constraints are removed.
  ASSERT_LT(constraints_disabled->back().first, controllers_disabled->front().first);
  // Check to see controllers are disabled on all cgroups from the leaves to
  // the root.
  ASSERT_EQ((*controllers_disabled)[0].second.cgroup_, application_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[1].second.cgroup_, system_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[2].second.cgroup_, node_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[3].second.cgroup_, base_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[4].second.cgroup_, application_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[5].second.cgroup_, system_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[6].second.cgroup_, node_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[7].second.cgroup_, base_cgroup_path);

  // The memory and cpu controller are both disabled for each cgroup
  std::array<std::string, 4> cgroup_names{
      base_cgroup_path,
      node_cgroup_path,
      system_cgroup_path,
      application_cgroup_path,
  };

  for (const auto &cgroup_name : cgroup_names) {
    ASSERT_EQ(std::count_if(controllers_disabled->begin(),
                            controllers_disabled->end(),
                            [&cgroup_name](const std::pair<int, FakeController> &item) {
                              return item.second.cgroup_ == cgroup_name &&
                                     item.second.name_ == "cpu";
                            }),
              1);
    ASSERT_EQ(std::count_if(controllers_disabled->begin(),
                            controllers_disabled->end(),
                            [&cgroup_name](const std::pair<int, FakeController> &item) {
                              return item.second.cgroup_ == cgroup_name &&
                                     item.second.name_ == "memory";
                            }),
              1);
  }

  // Processes were moved third.
  ASSERT_EQ(processes_moved->size(), 1);

  ASSERT_EQ((*processes_moved)[0].second.from_, system_leaf_cgroup_path);

  ASSERT_EQ((*processes_moved)[0].second.to_, base_cgroup_path);
  ASSERT_LT(constraints_disabled->back().first, processes_moved->front().first);

  // Cgroups were deleted last and in reverse order i.e. application, system, node.
  ASSERT_EQ(deleted_cgroups->size(), 5);
  ASSERT_LT(processes_moved->back().first, deleted_cgroups->front().first);
  ASSERT_EQ((*deleted_cgroups)[0].second, application_leaf_cgroup_path);
  ASSERT_EQ((*deleted_cgroups)[1].second, application_cgroup_path);
  ASSERT_EQ((*deleted_cgroups)[2].second, system_leaf_cgroup_path);
  ASSERT_EQ((*deleted_cgroups)[3].second, system_cgroup_path);
  ASSERT_EQ((*deleted_cgroups)[4].second, node_cgroup_path);
}

TEST(CgroupManagerTest, AddProcessToSystemCgroupFailsIfInvalidProcess) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace("/sys/fs/cgroup",
                   FakeCgroup{"/sys/fs/cgroup", {5}, {}, {"cpu", "memory"}, {}});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};

  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);
  driver->add_process_to_cgroup_s_ = Status::InvalidArgument("");

  auto cgroup_manager_s = CgroupManager::Create(
      "/sys/fs/cgroup", "node_id_123", 100, 1000000, std::move(driver));
  ASSERT_TRUE(cgroup_manager_s.ok()) << cgroup_manager_s.ToString();

  std::unique_ptr<CgroupManager> cgroup_manager = std::move(cgroup_manager_s.value());
  Status s = cgroup_manager->AddProcessToSystemCgroup("-1");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(CgroupManagerTest, AddProcessToSystemCgroupIsFatalIfSystemCgroupDoesNotExist) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace("/sys/fs/cgroup",
                   FakeCgroup{"/sys/fs/cgroup", {5}, {}, {"cpu", "memory"}, {}});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};

  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);
  driver->add_process_to_cgroup_s_ = Status::NotFound("");

  auto cgroup_manager_s = CgroupManager::Create(
      "/sys/fs/cgroup", "node_id_123", 100, 1000000, std::move(driver));
  ASSERT_TRUE(cgroup_manager_s.ok()) << cgroup_manager_s.ToString();

  std::unique_ptr<CgroupManager> cgroup_manager = std::move(cgroup_manager_s.value());

  EXPECT_DEATH((void)cgroup_manager->AddProcessToSystemCgroup("-1"),
               "Failed to move.*not found");
}

TEST(CgroupManagerTest,
     AddProcessToSystemCgroupIsFatalIfProcessDoesNotHavePermissionsForSystemCgroup) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace("/sys/fs/cgroup",
                   FakeCgroup{"/sys/fs/cgroup", {5}, {}, {"cpu", "memory"}, {}});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};

  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);
  driver->add_process_to_cgroup_s_ = Status::PermissionDenied("");

  auto cgroup_manager_s = CgroupManager::Create(
      "/sys/fs/cgroup", "node_id_123", 100, 1000000, std::move(driver));
  ASSERT_TRUE(cgroup_manager_s.ok()) << cgroup_manager_s.ToString();

  std::unique_ptr<CgroupManager> cgroup_manager = std::move(cgroup_manager_s.value());

  EXPECT_DEATH((void)cgroup_manager->AddProcessToSystemCgroup("-1"),
               "Failed to move.*permissions");
}

TEST(
    CgroupManagerTest,
    AddProcessToSystemCgroupSucceedsIfSystemCgroupExistsWithCorrectPermissionsAndValidProcess) {
  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups =
      std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
  cgroups->emplace("/sys/fs/cgroup",
                   FakeCgroup{"/sys/fs/cgroup", {5}, {}, {"cpu", "memory"}, {}});
  FakeCgroup base_cgroup{"/sys/fs/cgroup"};

  std::unique_ptr<FakeCgroupDriver> driver = FakeCgroupDriver::Create(cgroups);

  auto cgroup_manager_s = CgroupManager::Create(
      "/sys/fs/cgroup", "node_id_123", 100, 1000000, std::move(driver));
  ASSERT_TRUE(cgroup_manager_s.ok()) << cgroup_manager_s.ToString();

  std::unique_ptr<CgroupManager> cgroup_manager = std::move(cgroup_manager_s.value());

  Status s = cgroup_manager->AddProcessToSystemCgroup("5");
  ASSERT_TRUE(s.ok()) << s.ToString();
}

}  // namespace ray
