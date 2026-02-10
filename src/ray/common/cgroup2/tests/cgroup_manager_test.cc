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

  // node, system, workers, and user cgroups were created in the fake
  // the cgroup heirarchy.
  //      sys/fs/cgroup
  //            |
  //    ray-node_id_123
  //   |              |
  // system          user
  //   |            |    |
  //  leaf     workers  non-ray
  std::string node_id = "id_123";
  std::string base_cgroup_path = "/sys/fs/cgroup";
  std::string node_cgroup_path = "/sys/fs/cgroup/ray-node_id_123";
  std::string system_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/system";
  std::string system_leaf_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/system/leaf";
  std::string user_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/user";
  std::string workers_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/user/workers";
  std::string non_ray_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/user/non-ray";
  int64_t system_reserved_cpu_weight = 1000;
  int64_t system_reserved_memory_bytes = 1024 * 1024 * 1024;

  auto cgroup_manager_s = CgroupManager::Create(base_cgroup_path,
                                                node_id,
                                                system_reserved_cpu_weight,
                                                system_reserved_memory_bytes,
                                                std::move(owned_driver));

  // The cgroup hierarchy was created correctly.
  ASSERT_EQ(cgroups->size(), 7);
  ASSERT_NE(cgroups->find(base_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(node_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(system_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(system_leaf_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(user_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(workers_cgroup_path), cgroups->end());
  ASSERT_NE(cgroups->find(non_ray_cgroup_path), cgroups->end());

  FakeCgroup &base_cgroup = cgroups->at(base_cgroup_path);
  FakeCgroup &node_cgroup = cgroups->at(node_cgroup_path);
  FakeCgroup &system_cgroup = cgroups->at(system_cgroup_path);
  FakeCgroup &user_cgroup = cgroups->at(user_cgroup_path);
  FakeCgroup &non_ray_cgroup = cgroups->at(non_ray_cgroup_path);

  ASSERT_EQ(base_cgroup.enabled_controllers_.size(), 2);
  ASSERT_EQ(node_cgroup.enabled_controllers_.size(), 2);
  ASSERT_EQ(system_cgroup.enabled_controllers_.size(), 1);

  // cpu controllers are enabled on base, and node.
  std::array<const std::string *, 2> cpu_controlled_cgroup_paths{&base_cgroup_path,
                                                                 &node_cgroup_path};

  for (const auto cg_path : cpu_controlled_cgroup_paths) {
    const FakeCgroup &cg = cgroups->at(*cg_path);
    ASSERT_NE(cg.enabled_controllers_.find("cpu"), cg.enabled_controllers_.end());
  }

  // memory controllers are enabled on base, node, and system
  std::array<const std::string *, 3> memory_controlled_cgroup_paths{
      &base_cgroup_path, &node_cgroup_path, &system_cgroup_path};

  for (const auto cg_path : memory_controlled_cgroup_paths) {
    const FakeCgroup &cg = cgroups->at(*cg_path);
    ASSERT_NE(cg.enabled_controllers_.find("memory"), cg.enabled_controllers_.end());
  }

  // Processes were moved out of the base cgroup into the non-ray cgroup.
  ASSERT_TRUE(base_cgroup.processes_.empty());
  ASSERT_EQ(non_ray_cgroup.processes_.size(), 1);

  // The memory and cpu constraints were enabled correctly on the system cgroup.
  ASSERT_EQ(system_cgroup.constraints_.size(), 2);
  ASSERT_NE(system_cgroup.constraints_.find("cpu.weight"),
            system_cgroup.constraints_.end());
  ASSERT_EQ(system_cgroup.constraints_.at("cpu.weight"),
            std::to_string(system_reserved_cpu_weight));
  ASSERT_EQ(system_cgroup.constraints_.at("memory.min"),
            std::to_string(system_reserved_memory_bytes));

  // The cpu constraints were enabled correctly on the user cgroup.
  ASSERT_EQ(user_cgroup.constraints_.size(), 1);
  ASSERT_NE(user_cgroup.constraints_.find("cpu.weight"), user_cgroup.constraints_.end());
  // (10000 - system_reserved_cpu_weight)
  ASSERT_EQ(user_cgroup.constraints_.at("cpu.weight"), "9000");

  // Switching to cleanup mode to record cleanup operations.
  driver->cleanup_mode_ = true;

  // Destroying the cgroup manager triggers automatic cleanup.
  std::unique_ptr<CgroupManager> cgroup_manager = std::move(cgroup_manager_s.value());
  cgroup_manager.reset();

  // Only the base cgroup is left after the cgroup_manager is destroyed.
  ASSERT_EQ(cgroups->size(), 1);
  ASSERT_NE(cgroups->find(base_cgroup_path), cgroups->end());

  // Cleanup involves recursively deleting directories, disabling controllers, moving
  // processes etc. Therefore, the rest of the test asserts that the order of
  // operations was correct.
  //
  // Constraints have to be disabled before controllers are disabled.
  ASSERT_EQ(constraints_disabled->size(), 3);

  // Since constraints were enabled on sibling nodes, the order in which you disable
  // them does not matter.
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
  ASSERT_EQ(
      std::count_if(constraints_disabled->begin(),
                    constraints_disabled->end(),
                    [&user_cgroup_path](const std::pair<int, FakeConstraint> &item) {
                      return item.second.cgroup_ == user_cgroup_path &&
                             item.second.name_ == "cpu.weight";
                    }),
      1);

  // Controllers were disabled second.
  ASSERT_EQ(controllers_disabled->size(), 5);
  // Controllers must be disabled after the constraints are removed.
  ASSERT_LT(constraints_disabled->back().first, controllers_disabled->front().first);
  // Check to see controllers are disabled.
  ASSERT_EQ((*controllers_disabled)[0].second.cgroup_, system_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[1].second.cgroup_, node_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[2].second.cgroup_, base_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[3].second.cgroup_, node_cgroup_path);
  ASSERT_EQ((*controllers_disabled)[4].second.cgroup_, base_cgroup_path);

  // The memory and cpu controller are both disabled for each cgroup
  for (const auto cg_path : cpu_controlled_cgroup_paths) {
    ASSERT_EQ(std::count_if(controllers_disabled->begin(),
                            controllers_disabled->end(),
                            [&cg_path](const std::pair<int, FakeController> &item) {
                              return item.second.cgroup_ == *cg_path &&
                                     item.second.name_ == "cpu";
                            }),
              1);
  }

  for (const auto cg_path : memory_controlled_cgroup_paths) {
    ASSERT_EQ(std::count_if(controllers_disabled->begin(),
                            controllers_disabled->end(),
                            [cg_path](const std::pair<int, FakeController> &item) {
                              return item.second.cgroup_ == *cg_path &&
                                     item.second.name_ == "memory";
                            }),
              1);
  }

  // Processes must be moved third.
  // Processes were moved both out of the system_leaf, workers, and non_ray
  // cgroups.
  ASSERT_EQ(processes_moved->size(), 3);
  std::array<std::string, 3> process_moved_cgroups{
      system_leaf_cgroup_path, non_ray_cgroup_path, workers_cgroup_path};

  // The order in which processes were moved back from leaf nodes to the base_cgroup
  // does not matter.
  for (const auto &process_moved_cgroup : process_moved_cgroups) {
    ASSERT_EQ(std::count_if(processes_moved->begin(),
                            processes_moved->end(),
                            [&process_moved_cgroup, &base_cgroup_path](
                                const std::pair<int, FakeMoveProcesses> &item) {
                              return item.second.from_ == process_moved_cgroup &&
                                     item.second.to_ == base_cgroup_path;
                            }),
              1);
  }

  ASSERT_EQ((*processes_moved)[0].second.to_, base_cgroup_path);
  ASSERT_LT(constraints_disabled->back().first, processes_moved->front().first);

  // Cgroups were deleted last and in reverse order i.e. application, system, node.
  ASSERT_EQ(deleted_cgroups->size(), 6);
  ASSERT_LT(processes_moved->back().first, deleted_cgroups->front().first);
  ASSERT_EQ((*deleted_cgroups)[0].second, non_ray_cgroup_path);
  ASSERT_EQ((*deleted_cgroups)[1].second, workers_cgroup_path);
  ASSERT_EQ((*deleted_cgroups)[2].second, user_cgroup_path);
  ASSERT_EQ((*deleted_cgroups)[3].second, system_leaf_cgroup_path);
  ASSERT_EQ((*deleted_cgroups)[4].second, system_cgroup_path);
  ASSERT_EQ((*deleted_cgroups)[5].second, node_cgroup_path);
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
