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
// Precondition: cgroupv2 has already been mounted as rw.
//
// TODO(hjiang): Provide documentation and scripts to check cgroupv2 mount status and
// mount it correctly.
// Link:
// https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/8/html/managing_monitoring_and_updating_the_kernel/using-cgroups-v2-to-control-distribution-of-cpu-time-for-applications_managing-monitoring-and-updating-the-kernel#mounting-cgroups-v2_using-cgroups-v2-to-control-distribution-of-cpu-time-for-applications
//
// Execution command:
// sudo bazel-bin/src/ray/common/cgroup/test/cgroup_v2_setup_test

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <string_view>
#include <thread>
#include <unordered_set>

#include "ray/common/cgroup/cgroup_setup.h"
#include "ray/common/cgroup/cgroup_utils.h"
#include "ray/common/cgroup/test/cgroup_test_utils.h"
#include "ray/common/test/testing.h"

namespace ray {

#ifndef __linux__
TEST(Cgroupv2SetupTest, NonLinuxCrashTest) {
  EXPECT_EXIT(CgroupSetup{"/sys/fs/cgroup", "node_id"},
              testing::ExitedWithCode(EXIT_FAILURE),
              "cgroupv2 doesn't work on non linux platform.");
}
#else

class Cgroupv2SetupTest : public ::testing::Test {
 public:
  Cgroupv2SetupTest()
      : node_id_("node_id"),
        node_cgroup_folder_("/sys/fs/cgroup/ray_node_node_id"),
        system_cgroup_folder_("/sys/fs/cgroup/ray_node_node_id/system"),
        system_cgroup_proc_filepath_(
            "/sys/fs/cgroup/ray_node_node_id/system/cgroup.procs"),
        app_cgroup_folder_("/sys/fs/cgroup/ray_node_node_id/ray_application"),
        app_cgroup_proc_filepath_(
            "/sys/fs/cgroup/ray_node_node_id/ray_application/default/cgroup.procs") {}
  void TearDown() override {
    // Check the application subcgroup folder has been deleted.
    std::error_code err_code;
    bool exists = std::filesystem::exists(app_cgroup_folder_, err_code);
    ASSERT_FALSE(err_code) << "Check file existence failed because "
                           << err_code.message();
    ASSERT_FALSE(exists);
  }

 protected:
  const std::string node_id_;
  const std::string node_cgroup_folder_;
  const std::string system_cgroup_folder_;
  const std::string system_cgroup_proc_filepath_;
  const std::string app_cgroup_folder_;
  const std::string app_cgroup_proc_filepath_;
};

TEST_F(Cgroupv2SetupTest, SetupTest) {
  CgroupSetup cgroup_setup{"/sys/fs/cgroup", "node_id", CgroupSetup::TestTag{}};

  // Check system cgroup is created successfully.
  std::error_code err_code;
  bool exists = std::filesystem::exists(system_cgroup_folder_, err_code);
  ASSERT_FALSE(err_code);
  ASSERT_TRUE(exists);

  // Check application cgroup is created successfully.
  exists = std::filesystem::exists(app_cgroup_folder_, err_code);
  ASSERT_FALSE(err_code);
  ASSERT_TRUE(exists);
}

TEST_F(Cgroupv2SetupTest, AddSystemProcessTest) {
  CgroupSetup cgroup_setup{"/sys/fs/cgroup", "node_id", CgroupSetup::TestTag{}};

  pid_t pid = fork();
  ASSERT_NE(pid, -1);

  // Child process.
  if (pid == 0) {
    // Spawn a process running long enough, so it could be added into system cgroup.
    // It won't affect test runtime, because it will be killed later.
    std::this_thread::sleep_for(std::chrono::seconds(3600));
    // Exit without flushing the buffer.
    std::_Exit(0);
  }

  RAY_ASSERT_OK(cgroup_setup.AddSystemProcess(pid));
  AssertPidInCgroup(pid, system_cgroup_proc_filepath_);

  // Kill testing process.
  RAY_ASSERT_OK(KillAllProcAndWait(system_cgroup_folder_));
}

TEST_F(Cgroupv2SetupTest, AddAppProcessTest) {
  CgroupSetup cgroup_setup{"/sys/fs/cgroup", "node_id", CgroupSetup::TestTag{}};

  pid_t pid = fork();
  ASSERT_NE(pid, -1);

  // Child process.
  if (pid == 0) {
    // Spawn a process running long enough, so it could be added into system cgroup.
    // It won't affect test runtime, because it will be killed later.
    std::this_thread::sleep_for(std::chrono::seconds(3600));
    // Exit without flushing the buffer.
    std::_Exit(0);
  }

  AppProcCgroupMetadata app_metadata;
  app_metadata.pid = pid;
  app_metadata.max_memory = 0;  // No limit specified.
  auto handle = cgroup_setup.ApplyCgroupContext(app_metadata);
  AssertPidInCgroup(pid, app_cgroup_proc_filepath_);

  // Kill testing process.
  RAY_ASSERT_OK(KillAllProcAndWait(app_cgroup_folder_));
}

#endif

}  // namespace ray
