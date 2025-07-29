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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/cgroup2/sysfs_cgroup_driver.h"
#include "ray/common/cgroup2/test/cgroup_test_utils.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

/**
    NOTE: Read these instructions before running these tests locally.
    These tests are only supported on linux with cgroupv2 enabled in unified mode.

    See the following documentation for how to enable cgroupv2 properly:
    https://kubernetes.io/docs/concepts/architecture/cgroups/#linux-distribution-cgroup-v2-support

    When running these tests, the process that starts the tests (usually your terminal)
    needs to be inside a cgroup hierarchy that it has ownership of. Run the following
    bash commands to make that happen:

    # Create the cgroup heirarchy (these need to match base_cgroup_path_ and
    # processes_cgroup_path_ which are defined below)
    sudo mkdir /sys/fs/cgroup/testing
    sudo mkdir /sys/fs/cgroup/active

    # Give ownership and permissions to the current user.
    USER=`whoami`
    sudo chown -R USER:USER /sys/fs/cgroup/testing
    sudo chmod -R u+rwx /sys/fs/cgroup/testing

    # Move the current process into a leaf cgroup within the hierarchy.
    echo $$ | sudo tee -a /sys/fs/cgroup/testing/active/cgroup.procs

    # Enable cpu and memory controllers on the parent cgroup
    echo "+cpu" > /sys/fs/cgroup/testing/cgroup.subtree_control
    echo "+memory" > /sys/fs/cgroup/testing/cgroup.subtree_control
*/
// Uncomment the following line to run the tests locally.
#define RUN_LOCALLY

// This the root of the cgroup subtree that has been delegated to the testing user.
std::unique_ptr<TempCgroupDirectory> testing_cgroup_;

// This a sibling to the testing_cgroup_ that will store all processes that were in
// the base_cgroup. This will allow the testing_cgroup_ to have subcgroups with
// controllers enabled to satisfy the no internal process constraint.
// See the no internal process constraint section for more information:
// https://docs.kernel.org/admin-guide/cgroup-v2.html#no-internal-process-constraint
std::unique_ptr<TempCgroupDirectory> leaf_cgroup_;

// base_cgroup_path_ is the root of the cgroup heirarchy that will be used for testing.
// The tests will expect that this cgroup has only the cpu and memory controllers
// enabled because they will be used for testing.
// processes_cgroup_path_ is used when running tests locally to start the test process
// in a leaf node inside the cgroup hierarchy. It's not needed in CI because the user
// inside the container has root access and can manage the entire hierarchy. This implies
// that the CI test process is always started within a cgroup hierarchy that it owns.
#ifdef RUN_LOCALLY
static inline std::string base_cgroup_path_ = "/sys/fs/cgroup/testing";
static inline std::string processes_cgroup_path_ = "/sys/fs/cgroup/testing/active";
#else
static inline std::string base_cgroup_path_ = "/sys/fs/cgroup";
static inline std::string processescgroup_path_ = "/sys/fs/cgroup";
#endif

/**
    This test suite will create the following cgroup hierarchy for the tests
                        base_cgroup
                        /           \
               testing_cgroup     leaf_cgroup

    It expects that root_cgroup has cpu, memory controllers enabled.
    It enables the cpu, memory controllers on the testing_cgroup.
    Tests should not access anything above the testing_cgroup.
    The leaf_cgroup provides a leaf node clone processes into while not
    violating the no internal processes constraint.

    Note: testing_cgroup and leaf_cgroup have randomly generating names
    to isolate test runs from each other.
 */
class SysFsCgroupDriverIntegrationTest : public ::testing::Test {
 protected:
  /**
    This can fail an assertion without running fully. In that case, best-effort
    cleanup will happen inside the TearDownTestSuite method.
    */
  static void SetUpTestSuite() {
    // 1) Create the testing_cgroup_ under base_cgroup. This is the root node of the
    // cgroup subtree used by the tests in this file.
    auto testing_cgroup_or_status =
        TempCgroupDirectory::Create(base_cgroup_path_, S_IRWXU);
    ASSERT_TRUE(testing_cgroup_or_status.ok()) << testing_cgroup_or_status.ToString();
    testing_cgroup_ = std::move(testing_cgroup_or_status.value());

    // 2) Create the leaf_cgroup_ under the base_cgroup. This will be used to store
    // all the processes in the base_cgroup.
    auto leaf_cgroup_or_status = TempCgroupDirectory::Create(base_cgroup_path_, S_IRWXU);
    ASSERT_TRUE(leaf_cgroup_or_status.ok()) << leaf_cgroup_or_status.ToString();
    leaf_cgroup_ = std::move(leaf_cgroup_or_status.value());

    // 3) Move all processes from the <processes_cgroup>/cgroup.procs file into the
    //<leaf_cgroup>/cgroup.procs file. This will allow the base_cgroup to have children.
    std::string processes_cgroup_procs_file_path =
        processes_cgroup_path_ + "/cgroup.procs";
    std::string leaf_cgroup_procs_file_path = leaf_cgroup_->GetPath() + "/cgroup.procs";
    FILE *processes_cgroup_procs = fopen(processes_cgroup_procs_file_path.c_str(), "r+");
    ASSERT_NE(processes_cgroup_procs, nullptr)
        << "Failed to open processes cgroup's proc file at path "
        << processes_cgroup_procs_file_path << " with error " << strerror(errno);
    int leaf_cgroup_procs_fd = open(leaf_cgroup_procs_file_path.c_str(), O_RDWR);
    ASSERT_NE(leaf_cgroup_procs_fd, -1)
        << "Failed to open leaf cgroup's proc file at path "
        << leaf_cgroup_procs_file_path << " with error " << strerror(errno);

    // According to man 5 proc, the maximum value of a pid on a 64-bit system is 2^22
    // i.e./4194304. fgets adds a null terminating character.
    char read_buffer[32];

    while (fgets(read_buffer, sizeof(read_buffer), processes_cgroup_procs)) {
      ssize_t bytes_written =
          write(leaf_cgroup_procs_fd, read_buffer, strlen(read_buffer));

      // If the process exited between the call to fgets and write, write will
      // return a NoSuchProcess error.
      if (bytes_written == -1 && errno == ESRCH) {
        continue;
      }
      ASSERT_EQ(bytes_written, strlen(read_buffer))
          << "Failed to write to leaf cgroups proc file at path "
          << leaf_cgroup_->GetPath() << " with error " << strerror(errno);
    }
    ASSERT_EQ(ferror(processes_cgroup_procs), 0)
        << "Failed to read pid from processes cgroups proc file at path "
        << processes_cgroup_procs_file_path << " with error " << strerror(errno);

    fclose(processes_cgroup_procs);
    close(leaf_cgroup_procs_fd);

    // 4) Enable cpu and memory controllers for the testing_cgroup to allow tests to
    // use those controllers as necessary.
    std::string test_cgroup_subtree_control_path =
        testing_cgroup_->GetPath() + "/cgroup.subtree_control";
    std::unordered_set<std::string> subtree_control_ops = {"+cpu", "+memory"};
    for (const auto &op : subtree_control_ops) {
      int test_cgroup_subtree_control_fd =
          open(test_cgroup_subtree_control_path.c_str(), O_RDWR);
      ASSERT_NE(test_cgroup_subtree_control_fd, -1)
          << "Failed to open test cgroup's subtree_control file at path "
          << test_cgroup_subtree_control_path << " with error " << strerror(errno);
      ssize_t num_written = write(test_cgroup_subtree_control_fd, op.c_str(), op.size());
      ASSERT_EQ(num_written, op.size())
          << "Failed to write to test cgroup's subtree_control file at path "
          << test_cgroup_subtree_control_path << " with error " << strerror(errno);
      close(test_cgroup_subtree_control_fd);
    }
  }

  /**
    This will attempt to do best effort cleanup by reversing the effects of the
    SetUpTestSuite method. Cleanup doesn't need to delete cgroup directories because of
    TempCgroupDirectory's dtor will take care of it. Cleanup doesn't need to disable
    controllers because a cgroup can be deleted even if it has controllers enabled.

    If cleanup fails, cgroup directories may not be cleaned up properly. However, despite
    cleanup failure, tests can be rerun on the same machine/container without interference
    from the previous failed attempt.

    There are a few types of leaks possible
      1) In CI, the tests run inside the container and all processes inside the container
        will have been moved into the <root_cgroup>/leaf/cgroup.procs file. They may stay
        there. This has no impact on subsequent runs because this cgroup should have no
        constriants.
      2) In all environments, cgroups may be leaked and will have to be manually cleaned
      up. Typically, a cgroup cannot be removed iff it has a running process in the
      cgroup.procs file or child cgroups.

    NOTE: The teardown method is called even if assertions fail in the setup or in
    tests in the suite.
    */
  static void TearDownTestSuite() {
    // Move all processes from the <leaf_cgroup>/cgroup.procs file into the
    //<processes_cgroup>/cgroup.procs file. This will allow the leaf_cgroup to be deleted.
    std::string processes_cgroup_procs_file_path =
        processes_cgroup_path_ + "/cgroup.procs";
    std::string leaf_cgroup_procs_file_path = leaf_cgroup_->GetPath() + "/cgroup.procs";
    FILE *leaf_cgroup_procs = fopen(leaf_cgroup_procs_file_path.c_str(), "r+");
    ASSERT_NE(leaf_cgroup_procs, nullptr)
        << "Failed to open leaf cgroup's proc file at path " << leaf_cgroup_
        << " with error " << strerror(errno);
    int processes_cgroup_procs_fd =
        open(processes_cgroup_procs_file_path.c_str(), O_RDWR);
    ASSERT_NE(processes_cgroup_procs_fd, -1)
        << "Failed to open processes cgroup's proc file at path "
        << processes_cgroup_path_ << " with error " << strerror(errno);

    // According to man 5 proc, the maximum value of a pid on a 64-bit system is 2^22
    // i.e./4194304. fgets adds a null terminating character.
    char read_buffer[16];

    while (fgets(read_buffer, sizeof(read_buffer), leaf_cgroup_procs)) {
      ssize_t bytes_written =
          write(processes_cgroup_procs_fd, read_buffer, strlen(read_buffer));
      ASSERT_EQ(bytes_written, strlen(read_buffer))
          << "Failed to write to processes cgroups proc file at path "
          << processes_cgroup_path_ << " with error " << strerror(errno);
    }
    ASSERT_EQ(ferror(leaf_cgroup_procs), 0)
        << "Failed to read pid from leaf cgroups proc file at path " << leaf_cgroup_
        << " with error " << strerror(errno);

    fclose(leaf_cgroup_procs);
    close(processes_cgroup_procs_fd);
  }
};

TEST_F(SysFsCgroupDriverIntegrationTest,
       FixtureCreatesCgroupsAndMigratesProcessToLeafCgroup) {
  ASSERT_TRUE(std::filesystem::exists(testing_cgroup_->GetPath()))
      << "Test fixture did not initialize the base cgroup at " << testing_cgroup_;
  ASSERT_TRUE(std::filesystem::exists(leaf_cgroup_->GetPath()))
      << "Test fixture did not initialize the leaf cgroup at " << leaf_cgroup_;
  std::ifstream curr_proc_group_file("/proc/" + std::to_string(getpid()) + "/cgroup");
  std::string cgroup_path;
  ASSERT_NE(curr_proc_group_file.peek(), EOF)
      << "The current process is not running in any cgroup. This should never happen.";
  std::getline(curr_proc_group_file, cgroup_path);
  ASSERT_TRUE(curr_proc_group_file.good());
  // Extract the cgroup from the path.
  std::string cgroup = cgroup_path.substr(cgroup_path.find_last_of('/') + 1);
  ASSERT_EQ(cgroup, leaf_cgroup_->GetName());
}

namespace ray {

TEST_F(SysFsCgroupDriverIntegrationTest,
       CheckCgroupv2EnabledSucceedsIfOnlyCgroupv2Mounted) {
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroupv2Enabled();
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       CheckCgroupFailsIfCgroupv2PathButNoReadPermissions) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), 0000);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup(cgroup_dir->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       CheckCgroupFailsIfCgroupv2PathButNoWritePermissions) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup(cgroup_dir->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       CheckCgroupFailsIfCgroupv2PathButNoExecPermissions) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup(cgroup_dir->GetPath());
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       CheckCgroupSucceedsIfCgroupv2PathAndReadWriteExecPermissions) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CheckCgroup(cgroup_dir->GetPath());
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, CreateCgroupFailsIfAlreadyExists) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.CreateCgroup(cgroup_dir->GetPath());
  ASSERT_TRUE(s.IsAlreadyExists()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, CreateCgroupFailsIfAncestorCgroupDoesNotExist) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR);
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
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR | S_IWUSR);
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
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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

TEST_F(SysFsCgroupDriverIntegrationTest,
       GetAvailableControllersFailsIfCgroupDoesNotExist) {
  std::string non_existent_path = testing_cgroup_->GetPath() +
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
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR | S_IWUSR);
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
      driver.GetAvailableControllers(testing_cgroup_->GetPath());
  EXPECT_TRUE(s.ok()) << s.ToString();
  std::unordered_set<std::string> controllers = std::move(s.value());
  EXPECT_TRUE(controllers.find("cpu") != controllers.end())
      << "Cgroup integration tests expect the base cgroup at " << testing_cgroup_
      << " has the cpu controller available";
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       GetAvailableControllersSucceedsWithNoAvailableControllers) {
  auto parent_cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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
  auto source_cgroup_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
  ASSERT_TRUE(source_cgroup_or_status.ok()) << source_cgroup_or_status.ToString();
  auto source_cgroup = std::move(source_cgroup_or_status.value());
  auto dest_cgroup_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.EnableController(cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       EnableControllerFailsIfReadWriteOnlyPermissionsForCgroup) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.EnableController(cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, EnableControllerFailsIfCgroupDoesNotExist) {
  std::string non_existent_path =
      testing_cgroup_->GetPath() + std::filesystem::path::preferred_separator + "nope";
  SysFsCgroupDriver driver;
  Status s = driver.EnableController(non_existent_path, "memory");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       EnableControllerFailsIfControllerNotAvailableForCgroup) {
  // This will inherit controllers available because testing_cgroup_ has
  // CPU and Memory controllers available.
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  auto nested_cgroup_dir_or_status =
      TempCgroupDirectory::Create(cgroup_dir->GetPath(), S_IRWXU);
  ASSERT_TRUE(nested_cgroup_dir_or_status.ok()) << nested_cgroup_dir_or_status.ToString();
  auto nested_cgroup_dir = std::move(nested_cgroup_dir_or_status.value());
  // Make sure that the cgroup has 0 available controllers.
  // TODO(irabbani): I think it's okay to call the GetAvailableControllers function
  // from here.
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
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.DisableController(cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       DisableControllerFailsIfReadWriteOnlyPermissionsForCgroup) {
  auto cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_dir_or_status.ok()) << cgroup_dir_or_status.ToString();
  auto cgroup_dir = std::move(cgroup_dir_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.DisableController(cgroup_dir->GetPath(), "memory");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, DisableControllerFailsIfCgroupDoesNotExist) {
  std::string non_existent_path =
      testing_cgroup_->GetPath() + std::filesystem::path::preferred_separator + "nope";
  SysFsCgroupDriver driver;
  Status s = driver.DisableController(non_existent_path, "memory");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

//// /sys/fs/cgroup/testing (2 available, enable cpu controller)
//// enable one in /sys/fs/cgroup/testing/inner (enable cpu controller fails, then
/// succeeds) / enable one in /sys/fs/cgroup/testing/inner / disable one in
////sys/fs/cgroup/testing
TEST_F(SysFsCgroupDriverIntegrationTest,
       EnableAndDisableControllerSucceedWithCorrectInputAndPermissions) {
  auto parent_cgroup_dir_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
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

  // Enable the controller on the parent cgroup to make it available on the child cgroup.
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
      testing_cgroup_->GetPath() + std::filesystem::path::preferred_separator + "nope";
  SysFsCgroupDriver driver;
  Status s = driver.AddConstraint(non_existent_path, "memory.min", "1");
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       AddResourceConstraintFailsIfReadOnlyPermissions) {
  auto cgroup_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.AddConstraint(cgroup->GetPath(), "memory.min", "1");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       AddResourceConstraintFailsIfReadWriteOnlyPermissions) {
  auto cgroup_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRUSR | S_IWUSR);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  Status s = driver.AddConstraint(cgroup->GetPath(), "memory.min", "1");
  ASSERT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest,
       AddResourceConstraintFailsIfConstraintNotSupported) {
  auto cgroup_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  // "memory.max" is not supported.
  Status s = driver.AddConstraint(cgroup->GetPath(), "memory.max", "1");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}
TEST_F(SysFsCgroupDriverIntegrationTest,
       AddResourceConstraintFailsIfControllerNotEnabled) {
  auto cgroup_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  // Memory controller is not enabled.
  Status s = driver.AddConstraint(cgroup->GetPath(), "memory.min", "1");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}
TEST_F(SysFsCgroupDriverIntegrationTest,
       AddResourceConstraintFailsIfInvalidConstraintValue) {
  auto cgroup_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  // Enable the cpu controller first.
  Status enable_controller_s = driver.EnableController(cgroup->GetPath(), "cpu");
  ASSERT_TRUE(enable_controller_s.ok()) << enable_controller_s.ToString();
  // cpu.weight must be between [1,10000]
  Status s_too_low = driver.AddConstraint(cgroup->GetPath(), "cpu.weight", "0");
  ASSERT_TRUE(s_too_low.IsInvalidArgument()) << s_too_low.ToString();
  Status s_too_high = driver.AddConstraint(cgroup->GetPath(), "cpu.weight", "10001");
  ASSERT_TRUE(s_too_high.IsInvalidArgument()) << s_too_high.ToString();
}

TEST_F(SysFsCgroupDriverIntegrationTest, AddResourceConstraintSucceeds) {
  auto cgroup_or_status =
      TempCgroupDirectory::Create(testing_cgroup_->GetPath(), S_IRWXU);
  ASSERT_TRUE(cgroup_or_status.ok()) << cgroup_or_status.ToString();
  auto cgroup = std::move(cgroup_or_status.value());
  SysFsCgroupDriver driver;
  // Enable the cpu controller first.
  Status enable_controller_s = driver.EnableController(cgroup->GetPath(), "cpu");
  ASSERT_TRUE(enable_controller_s.ok()) << enable_controller_s.ToString();
  // cpu.weight must be between [1,10000]
  Status s = driver.AddConstraint(cgroup->GetPath(), "cpu.weight", "500");
  ASSERT_TRUE(s.ok()) << s.ToString();
}
}  // namespace ray
