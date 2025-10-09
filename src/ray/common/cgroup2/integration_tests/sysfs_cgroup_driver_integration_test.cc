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
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>

#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/common/cgroup2/sysfs_cgroup_driver.h"
#include "ray/common/status.h"

constexpr const char *ENV_VAR_TEST_CGROUP_PATH = "CGROUP_PATH";

namespace ray {

class SysFsCgroupDriverIntegrationTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    const char *cgroup_env = std::getenv(ENV_VAR_TEST_CGROUP_PATH);
    if (!cgroup_env || std::string(cgroup_env).empty()) {
      throw std::runtime_error("Environment variable CGROUP_PATH not set or empty");
    }
    test_cgroup_path_ = cgroup_env;
  }

  static const std::string &GetTestCgroupPath() { return test_cgroup_path_; }

  inline static std::string test_cgroup_path_;
};

TEST_F(SysFsCgroupDriverIntegrationTest,
       SysFsCgroupDriverIntegrationTestFailsIfNoCgroupTestPathSpecified) {
  ASSERT_FALSE(test_cgroup_path_.empty())
      << "These integration tests cannot be run without the "
         "environment variable CGROUP_TEST_PATH";
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       CheckCgroupFailsIfCgroupv2PathButNoReadPermissions) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, 0000);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup(cgroup_dir->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       CheckCgroupFailsIfCgroupv2PathButNoWritePermissions) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup(cgroup_dir->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       CheckCgroupFailsIfCgroupv2PathButNoExecPermissions) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup(cgroup_dir->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       CheckCgroupSucceedsIfCgroupv2PathAndReadWriteExecPermissions) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup(cgroup_dir->GetPath());
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, CreateCgroupFailsIfAlreadyExists) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CreateCgroup(cgroup_dir->GetPath());
  ASSERT_TRUE(s.IsAlreadyExists()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, CreateCgroupFailsIfAncestorCgroupDoesNotExist) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  std::string non_existent_path = cgroup_dir->GetPath() +
                                  std::filesystem::path::preferred_separator + "no" +
                                  std::filesystem::path::preferred_separator + "bueno";
  Status s = driver.CreateCgroup(non_existent_path);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, CreateCgroupFailsIfOnlyReadPermissions) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  std::string child_cgroup_path =
      cgroup_dir->GetPath() + std::filesystem::path::preferred_separator + "child";
  Status s = driver.CreateCgroup(child_cgroup_path);
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, CreateCgroupFailsIfOnlyReadWritePermissions) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  std::string child_cgroup_path =
      cgroup_dir->GetPath() + std::filesystem::path::preferred_separator + "child";
  Status s = driver.CreateCgroup(child_cgroup_path);
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       CreateCgroupSucceedsIfParentExistsAndReadWriteExecPermissions) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  std::string child_cgroup_path =
      cgroup_dir->GetPath() + std::filesystem::path::preferred_separator + "child";
  Status s = driver.CreateCgroup(child_cgroup_path);
  EXPECT_TRUE(s.ok()) << s.ToString();
  Status check_status = driver.CheckCgroup(child_cgroup_path);
  EXPECT_TRUE(check_status.ok()) << check_status.ToString();
  ASSERT_EQ(rmdir(child_cgroup_path.c_str()), 0)
      << "Failed to cleanup test cgroup at path " << child_cgroup_path << ".\n"
      << "Error: " << strerror(errno);
}

// Tests for DeleteCgroup
TEST_F(SysFsCgroupDriverIntegrationTest, DeleteCgroupFailsIfDoesNotExist) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup = std::move(cgroup_dir_or_status.value());
  std::string cgroup_to_delete =
      cgroup->GetPath() + std::filesystem::path::preferred_separator + "cool_group";
  SysFsCgroupDriver driver;
  Status s = driver.DeleteCgroup(cgroup_to_delete);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, DeleteCgroupFailsIfAncestorCgroupDoesNotExist) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  std::string non_existent_path = cgroup_dir->GetPath() +
                                  std::filesystem::path::preferred_separator + "no" +
                                  std::filesystem::path::preferred_separator + "bueno";
  Status s = driver.DeleteCgroup(non_existent_path);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, DeleteCgroupFailsIfOnlyReadPermissions) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  std::string child_cgroup_path =
      cgroup_dir->GetPath() + std::filesystem::path::preferred_separator + "child";
  Status s = driver.DeleteCgroup(child_cgroup_path);
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, DeleteCgroupFailsIfOnlyReadWritePermissions) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  std::string child_cgroup_path =
      cgroup_dir->GetPath() + std::filesystem::path::preferred_separator + "child";
  Status s = driver.DeleteCgroup(child_cgroup_path);
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, DeleteCgroupFailsIfCgroupHasChildren) {
  auto parent_cgroup_dir_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(parent_cgroup_dir_or_status.ok()) << parent_cgroup_dir_or_status.ToString();
  std::unique_ptr<TempCgroupDirectory> parent_cgroup =
      std::move(parent_cgroup_dir_or_status.value());
  auto child_cgroup_dir_or_status =
      TempCgroupDirectory::Create(parent_cgroup->GetPath(), S_IRWXU);
  ASSERT_TRUE(child_cgroup_dir_or_status.ok()) << child_cgroup_dir_or_status.ToString();
  SysFsCgroupDriver driver;
  Status s = driver.DeleteCgroup(parent_cgroup->GetPath());
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, DeleteCgroupFailsIfCgroupHasProcesses) {
  auto cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  StatusOr<std::pair<pid_t, int>> child_process =
      StartChildProcessInCgroup(cgroup->GetPath());
  ASSERT_TRUE(child_process.ok()) << child_process.ToString();
  auto [child_pid, child_pidfd] = *child_process;
  SysFsCgroupDriver driver;
  // Delete fails while process is alive.
  Status failed_s = driver.DeleteCgroup(cgroup->GetPath());
  EXPECT_TRUE(failed_s.IsInvalidArgument()) << failed_s.ToString();
  Status terminate_child =
      TerminateChildProcessAndWaitForTimeout(child_pid, child_pidfd, 5000);
  ASSERT_TRUE(terminate_child.ok()) << terminate_child.ToString();
  // Delete succeeds after child process terminates.
  Status succeeded_s = driver.DeleteCgroup(cgroup->GetPath());
  EXPECT_TRUE(succeeded_s.ok()) << succeeded_s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       DeleteCgroupSucceedsIfLeafCgroupExistsWithNoProcessesAndCorrectPermissions) {
  auto cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.DeleteCgroup(cgroup->GetPath());
  EXPECT_TRUE(s.ok()) << s.ToString();
}

// RemoveController tests

TEST_F(SysFsCgroupDriverIntegrationTest,
       GetAvailableControllersFailsIfCgroupDoesNotExist) {
  std::string non_existent_path = test_cgroup_path_ +
                                  std::filesystem::path::preferred_separator + "no" +
                                  std::filesystem::path::preferred_separator + "bueno";
  SysFsCgroupDriver driver;
  StatusOr<std::unordered_set<std::string>> s =
      driver.GetAvailableControllers(non_existent_path);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       GetAvailableControllersFailsIfReadWriteButNotExecutePermissions) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  std::unique_ptr<TempCgroupDirectory> cgroup_dir =
      std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  StatusOr<std::unordered_set<std::string>> s =
      driver.GetAvailableControllers(cgroup_dir->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       GetAvailableControllersSucceedsWithCPUAndMemoryControllersOnBaseCgroup) {
  SysFsCgroupDriver driver;
  StatusOr<std::unordered_set<std::string>> s =
      driver.GetAvailableControllers(test_cgroup_path_);
  EXPECT_TRUE(s.ok()) << s.ToString();
  std::unordered_set<std::string> controllers = std::move(s.value());
  EXPECT_TRUE(controllers.find("cpu") != controllers.end())
      << "Cgroup integration tests expect the base cgroup at " << test_cgroup_path_
      << " has the cpu controller available";
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       GetAvailableControllersSucceedsWithNoAvailableControllers) {
  auto parent_cgroup_dir_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(parent_cgroup_dir_or_status.ok()) << parent_cgroup_dir_or_status.ToString();
  std::unique_ptr<TempCgroupDirectory> parent_cgroup =
      std::move(parent_cgroup_dir_or_status.value());
  auto child_cgroup_dir_or_status =
      TempCgroupDirectory::Create(parent_cgroup->GetPath(), S_IRWXU);
  ASSERT_TRUE(child_cgroup_dir_or_status.ok()) << child_cgroup_dir_or_status.ToString();
  std::unique_ptr<TempCgroupDirectory> child_cgroup =
      std::move(child_cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  StatusOr<std::unordered_set<std::string>> s =
      driver.GetAvailableControllers(child_cgroup->GetPath());
  EXPECT_TRUE(s.ok()) << s.ToString();
  std::unordered_set<std::string> controllers = std::move(s.value());
  EXPECT_EQ(controllers.size(), 0);
}

TEST_F(SysFsCgroupDriverIntegrationTest, MoveAllProcessesFailsIfSourceDoesntExist) {
  auto ancestor_cgroup_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(ancestor_cgroup_or_status.ok()) << ancestor_cgroup_or_status.ToString();
  auto ancestor_cgroup = std::move(ancestor_cgroup_or_status.value());
  auto dest_cgroup_or_status =
      TempCgroupDirectory::Create(ancestor_cgroup->GetPath(), S_IRWXU);
  ASSERT_TRUE(dest_cgroup_or_status.ok()) << dest_cgroup_or_status.ToString();
  auto dest_cgroup = std::move(dest_cgroup_or_status.value());
  // Do not create the source cgroup
  std::string non_existent_path =
      ancestor_cgroup->GetPath() + std::filesystem::path::preferred_separator + "nope";
  SysFsCgroupDriver driver;
  Status s = driver.MoveAllProcesses(non_existent_path, dest_cgroup->GetPath());
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, MoveAllProcessesFailsIfDestDoesntExist) {
  auto ancestor_cgroup_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(ancestor_cgroup_or_status.ok()) << ancestor_cgroup_or_status.ToString();
  auto ancestor_cgroup = std::move(ancestor_cgroup_or_status.value());
  auto source_cgroup_or_status =
      TempCgroupDirectory::Create(ancestor_cgroup->GetPath(), S_IRWXU);
  ASSERT_TRUE(source_cgroup_or_status.ok()) << source_cgroup_or_status.ToString();
  auto source_cgroup = std::move(source_cgroup_or_status.value());
  // Do not create the dest cgroup.
  std::string non_existent_path =
      ancestor_cgroup->GetPath() + std::filesystem::path::preferred_separator + "nope";
  SysFsCgroupDriver driver;
  Status s = driver.MoveAllProcesses(source_cgroup->GetPath(), non_existent_path);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       MoveAllProcessesFailsIfNotReadWriteExecPermissionsForSource) {
  auto ancestor_cgroup_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(ancestor_cgroup_or_status.ok()) << ancestor_cgroup_or_status.ToString();
  auto ancestor_cgroup = std::move(ancestor_cgroup_or_status.value());
  auto source_cgroup_or_status =
      TempCgroupDirectory::Create(ancestor_cgroup->GetPath(), S_IRUSR | S_IWUSR);
  ASSERT_TRUE(source_cgroup_or_status.ok()) << source_cgroup_or_status.ToString();
  auto source_cgroup = std::move(source_cgroup_or_status.value());
  auto dest_cgroup_or_status =
      TempCgroupDirectory::Create(ancestor_cgroup->GetPath(), S_IRWXU);
  ASSERT_TRUE(dest_cgroup_or_status.ok()) << dest_cgroup_or_status.ToString();
  auto dest_cgroup = std::move(dest_cgroup_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.MoveAllProcesses(source_cgroup->GetPath(), dest_cgroup->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       MoveAllProcessesFailsIfNotReadWriteExecPermissionsForDest) {
  auto ancestor_cgroup_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(ancestor_cgroup_or_status.ok()) << ancestor_cgroup_or_status.ToString();
  auto ancestor_cgroup = std::move(ancestor_cgroup_or_status.value());
  auto source_cgroup_or_status =
      TempCgroupDirectory::Create(ancestor_cgroup->GetPath(), S_IRWXU);
  ASSERT_TRUE(source_cgroup_or_status.ok()) << source_cgroup_or_status.ToString();
  auto source_cgroup = std::move(source_cgroup_or_status.value());
  auto dest_cgroup_or_status =
      TempCgroupDirectory::Create(ancestor_cgroup->GetPath(), S_IRUSR | S_IWUSR);
  ASSERT_TRUE(dest_cgroup_or_status.ok()) << dest_cgroup_or_status.ToString();
  auto dest_cgroup = std::move(dest_cgroup_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.MoveAllProcesses(source_cgroup->GetPath(), dest_cgroup->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       MoveAllProcessesFailsIfNotReadWriteExecPermissionsForAncestor) {
  auto ancestor_cgroup_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(ancestor_cgroup_or_status.ok()) << ancestor_cgroup_or_status.ToString();
  auto ancestor_cgroup = std::move(ancestor_cgroup_or_status.value());
  auto source_cgroup_or_status =
      TempCgroupDirectory::Create(ancestor_cgroup->GetPath(), S_IRWXU);
  ASSERT_TRUE(source_cgroup_or_status.ok()) << source_cgroup_or_status.ToString();
  auto source_cgroup = std::move(source_cgroup_or_status.value());
  auto dest_cgroup_or_status =
      TempCgroupDirectory::Create(ancestor_cgroup->GetPath(), S_IRWXU);
  ASSERT_TRUE(dest_cgroup_or_status.ok()) << dest_cgroup_or_status.ToString();
  auto dest_cgroup = std::move(dest_cgroup_or_status.value());
  ASSERT_EQ(chmod(ancestor_cgroup->GetPath().c_str(), S_IRUSR), 0)
      << "Failed to chmod cgroup directory " << ancestor_cgroup->GetPath()
      << "\n Error: " << strerror(errno);
  SysFsCgroupDriver driver;
  Status s = driver.MoveAllProcesses(source_cgroup->GetPath(), dest_cgroup->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
  // Change the permissions back read, write, and execute so cgroup can be deleted.
  ASSERT_EQ(chmod(ancestor_cgroup->GetPath().c_str(), S_IRWXU), 0)
      << "Failed to chmod cgroup directory " << ancestor_cgroup->GetPath()
      << "\n Error: " << strerror(errno);
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       MoveAllProcessesSucceedsWithCorrectPermissionsAndValidCgroups) {
  auto source_cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(source_cgroup_or_status.ok()) << source_cgroup_or_status.ToString();
  auto source_cgroup = std::move(source_cgroup_or_status.value());
  auto dest_cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(dest_cgroup_or_status.ok()) << dest_cgroup_or_status.ToString();
  auto dest_cgroup = std::move(dest_cgroup_or_status.value());
  StatusOr<std::pair<pid_t, int>> child_process_s =
      StartChildProcessInCgroup(source_cgroup->GetPath());
  ASSERT_TRUE(child_process_s.ok()) << child_process_s.ToString();
  auto [child_pid, child_pidfd] = child_process_s.value();
  SysFsCgroupDriver driver;
  Status s = driver.MoveAllProcesses(source_cgroup->GetPath(), dest_cgroup->GetPath());
  ASSERT_TRUE(s.ok()) << s.ToString();
  // Assert that the child's pid is actually in the new file.
  std::string dest_cgroup_procs_file_path = dest_cgroup->GetPath() +
                                            std::filesystem::path::preferred_separator +
                                            "cgroup.procs";
  std::ifstream dest_cgroup_procs_file(dest_cgroup_procs_file_path);
  ASSERT_TRUE(dest_cgroup_procs_file.is_open())
      << "Could not open file " << dest_cgroup_procs_file_path << ".";
  std::unordered_set<int> dest_cgroup_pids;
  int pid = -1;
  while (dest_cgroup_procs_file >> pid) {
    ASSERT_FALSE(dest_cgroup_procs_file.fail())
        << "Unable to read pid from file " << dest_cgroup_procs_file_path;
    dest_cgroup_pids.emplace(pid);
  }
  EXPECT_EQ(dest_cgroup_pids.size(), 1);
  EXPECT_TRUE(dest_cgroup_pids.find(child_pid) != dest_cgroup_pids.end());
  Status terminate_s =
      TerminateChildProcessAndWaitForTimeout(child_pid, child_pidfd, 5000);
  ASSERT_TRUE(terminate_s.ok()) << terminate_s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       EnableControllerFailsIfReadOnlyPermissionsForCgroup) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.EnableController(cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       EnableControllerFailsIfReadWriteOnlyPermissionsForCgroup) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.EnableController(cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, EnableControllerFailsIfCgroupDoesNotExist) {
  std::string non_existent_path =
      test_cgroup_path_ + std::filesystem::path::preferred_separator + "nope";
  SysFsCgroupDriver driver;
  Status s = driver.EnableController(non_existent_path, "memory");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       EnableControllerFailsIfControllerNotAvailableForCgroup) {
  // This will inherit controllers available because testing_cgroup_ has
  // CPU and Memory controllers available.
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  auto nested_cgroup_dir_or_status =
      TempCgroupDirectory::Create(cgroup_dir->GetPath(), S_IRWXU);
  ASSERT_TRUE(nested_cgroup_dir_or_status.ok()) << nested_cgroup_dir_or_status.ToString();
  auto nested_cgroup_dir = std::move(nested_cgroup_dir_or_status.value());
  // Make sure that the cgroup has 0 available controllers.
  SysFsCgroupDriver driver;
  auto available_controllers_s =
      driver.GetAvailableControllers(nested_cgroup_dir->GetPath());
  ASSERT_TRUE(available_controllers_s.ok()) << available_controllers_s.ToString();
  auto available_controllers = std::move(available_controllers_s.value());
  ASSERT_EQ(available_controllers.size(), 0);
  Status s = driver.EnableController(nested_cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, DisableControllerFailsIfControllerNotEnabled) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  auto enabled_controllers_s = driver.GetEnabledControllers(cgroup_dir->GetPath());
  ASSERT_TRUE(enabled_controllers_s.ok()) << enabled_controllers_s.ToString();
  auto enabled_controllers = std::move(enabled_controllers_s.value());
  ASSERT_EQ(enabled_controllers.size(), 0);
  Status s = driver.DisableController(cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       DisableControllerFailsIfReadOnlyPermissionsForCgroup) {
  auto cgroup_dir_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.DisableController(cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       DisableControllerFailsIfReadWriteOnlyPermissionsForCgroup) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.DisableController(cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, DisableControllerFailsIfCgroupDoesNotExist) {
  std::string non_existent_path =
      test_cgroup_path_ + std::filesystem::path::preferred_separator + "nope";
  SysFsCgroupDriver driver;
  Status s = driver.DisableController(non_existent_path, "memory");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       EnableAndDisableControllerSucceedWithCorrectInputAndPermissions) {
  auto parent_cgroup_dir_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(parent_cgroup_dir_or_status.ok()) << parent_cgroup_dir_or_status.ToString();
  auto parent_cgroup_dir = std::move(parent_cgroup_dir_or_status.value());
  auto child_cgroup_dir_or_status =
      TempCgroupDirectory::Create(parent_cgroup_dir->GetPath(), S_IRWXU);
  ASSERT_TRUE(child_cgroup_dir_or_status.ok()) << child_cgroup_dir_or_status.ToString();
  auto child_cgroup_dir = std::move(child_cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;

  // There should be no enabled controllers on the parent cgroup so enabling the memory
  // controller should fail.
  Status invalid_argument_s = driver.EnableController(child_cgroup_dir->GetPath(), "cpu");
  ASSERT_TRUE(invalid_argument_s.IsInvalidArgument()) << invalid_argument_s.ToString();

  // Enable the controller on the parent cgroup to make it available on the child
  Status enable_parent_s = driver.EnableController(parent_cgroup_dir->GetPath(), "cpu");
  ASSERT_TRUE(enable_parent_s.ok()) << enable_parent_s.ToString();

  // Enable the controller on the child cgroup.
  Status enable_child_s = driver.EnableController(child_cgroup_dir->GetPath(), "cpu");
  ASSERT_TRUE(enable_child_s.ok()) << enable_child_s.ToString();

  // Cannot disable the controller on the parent cgroup while the child cgroup
  // still has it enabled.
  Status disable_parent_failure_s =
      driver.DisableController(parent_cgroup_dir->GetPath(), "cpu");
  ASSERT_FALSE(disable_parent_failure_s.ok()) << enable_parent_s.ToString();
  // Disable the controller on the child cgroup.
  Status disable_child_s = driver.DisableController(child_cgroup_dir->GetPath(), "cpu");
  ASSERT_TRUE(disable_child_s.ok()) << disable_child_s.ToString();
  // Can now disable the controller on the parent cgroup.
  Status disable_parent_success_s =
      driver.DisableController(parent_cgroup_dir->GetPath(), "cpu");
  ASSERT_TRUE(disable_parent_success_s.ok()) << disable_parent_success_s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, AddResourceConstraintFailsIfCgroupDoesntExist) {
  std::string non_existent_path =
      test_cgroup_path_ + std::filesystem::path::preferred_separator + "nope";
  SysFsCgroupDriver driver;
  Status s = driver.AddConstraint(non_existent_path, "memory", "memory.min", "1");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       AddResourceConstraintFailsIfReadOnlyPermissions) {
  auto cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.AddConstraint(cgroup->GetPath(), "memory", "memory.min", "1");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       AddResourceConstraintFailsIfReadWriteOnlyPermissions) {
  auto cgroup_or_status =
      TempCgroupDirectory::Create(test_cgroup_path_, S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.AddConstraint(cgroup->GetPath(), "memory", "memory.min", "1");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       AddResourceConstraintFailsIfControllerNotEnabled) {
  auto cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  // Memory controller is not enabled.
  Status s = driver.AddConstraint(cgroup->GetPath(), "memory", "memory.min", "1");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, AddResourceConstraintSucceeds) {
  auto cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  // Enable the cpu controller first.
  Status enable_controller_s = driver.EnableController(cgroup->GetPath(), "cpu");
  ASSERT_TRUE(enable_controller_s.ok()) << enable_controller_s.ToString();
  // cpu.weight must be between [1,10000]
  Status s = driver.AddConstraint(cgroup->GetPath(), "cpu", "cpu.weight", "500");
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, AddProcessToCgroupFailsIfCgroupDoesNotExist) {
  auto cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  std::string non_existent_path =
      cgroup->GetPath() + std::filesystem::path::preferred_separator + "nope";
  SysFsCgroupDriver driver;
  Status s = driver.AddProcessToCgroup(non_existent_path, "123");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       AddProcessToCgroupFailsIfCNotReadWriteExecPermissionsForCgroup) {
  auto cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IREAD);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.AddProcessToCgroup(cgroup->GetPath(), "123");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, AddProcessToCgroupFailsIfProcessDoesNotExist) {
  auto cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.AddProcessToCgroup(cgroup->GetPath(), "123");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       AddProcessToCgroupSucceedsIfProcessExistsAndCorrectPermissions) {
  auto cgroup_or_status = TempCgroupDirectory::Create(test_cgroup_path_, S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  auto child_cgroup_or_status = TempCgroupDirectory::Create(cgroup->GetPath(), S_IRWXU);
  ASSERT_TRUE(child_cgroup_or_status.ok()) << child_cgroup_or_status.ToString();
  auto child_cgroup = std::move(child_cgroup_or_status.value());
  StatusOr<std::pair<pid_t, int>> child_process_s =
      StartChildProcessInCgroup(cgroup->GetPath());
  ASSERT_TRUE(child_process_s.ok()) << child_process_s.ToString();
  auto [child_pid, child_pidfd] = child_process_s.value();
  SysFsCgroupDriver driver;
  Status s =
      driver.AddProcessToCgroup(child_cgroup->GetPath(), std::to_string(child_pid));
  ASSERT_TRUE(s.ok()) << s.ToString();
  // Assert that the child's pid is actually in the new file.
  std::string child_cgroup_procs_file_path = child_cgroup->GetPath() +
                                             std::filesystem::path::preferred_separator +
                                             "cgroup.procs";
  std::ifstream child_cgroup_procs_file(child_cgroup_procs_file_path);
  ASSERT_TRUE(child_cgroup_procs_file.is_open())
      << "Could not open file " << child_cgroup_procs_file_path << ".";
  std::unordered_set<int> child_cgroup_pids;
  int pid = -1;
  while (child_cgroup_procs_file >> pid) {
    ASSERT_FALSE(child_cgroup_procs_file.fail())
        << "Unable to read pid from file " << child_cgroup_procs_file_path;
    child_cgroup_pids.emplace(pid);
  }
  EXPECT_EQ(child_cgroup_pids.size(), 1);
  EXPECT_TRUE(child_cgroup_pids.find(child_pid) != child_cgroup_pids.end());
  Status terminate_s =
      TerminateChildProcessAndWaitForTimeout(child_pid, child_pidfd, 5000);
  ASSERT_TRUE(terminate_s.ok()) << terminate_s.ToString();
}

}  // namespace ray
