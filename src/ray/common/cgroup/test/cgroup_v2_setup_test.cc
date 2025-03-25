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
// Precondition: cgroup V2 has already been mounted as rw.
//
// Setup command:
// sudo umount /sys/fs/cgroup/unified
// sudo mount -t cgroup2 cgroup2 /sys/fs/cgroup/unified -o rw
//
// Execution command:
// sudo bazel-bin/src/ray/common/cgroup/test/cgroup_v2_setup_test

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <csignal>
#include <filesystem>
#include <string_view>
#include <unordered_set>

#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "ray/common/cgroup/cgroup_setup.h"
#include "ray/common/cgroup/cgroup_utils.h"
#include "ray/common/test/testing.h"
#include "ray/util/container_util.h"
#include "ray/util/filesystem.h"

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
        internal_cgroup_folder_("/sys/fs/cgroup/ray_node_node_id/internal"),
        internal_cgroup_proc_filepath_(
            "/sys/fs/cgroup/ray_node_node_id/internal/cgroup.procs"),
        app_cgroup_folder_("/sys/fs/cgroup/ray_node_node_id/ray_application") {}
  void TearDown() override {
    // Check the node-wise subcgroup folder has been deleted.
    std::error_code err_code;
    bool exists = std::filesystem::exists(app_cgroup_folder_, err_code);
    ASSERT_FALSE(err_code);
    ASSERT_FALSE(exists);
  }

 protected:
  const std::string node_id_;
  const std::string node_cgroup_folder_;
  const std::string internal_cgroup_folder_;
  const std::string internal_cgroup_proc_filepath_;
  const std::string app_cgroup_folder_;
};

TEST_F(Cgroupv2SetupTest, SetupTest) {
  CgroupSetup cgroup_setup{"/sys/fs/cgroup", "node_id", CgroupSetup::Tag{}};

  // Check internal cgroup is created successfully.
  std::error_code err_code;
  bool exists = std::filesystem::exists(internal_cgroup_folder_, err_code);
  ASSERT_FALSE(err_code);
  ASSERT_TRUE(exists);

  // Check application cgroup is created successfully.
  exists = std::filesystem::exists(app_cgroup_folder_, err_code);
  ASSERT_FALSE(err_code);
  ASSERT_TRUE(exists);
}

TEST_F(Cgroupv2SetupTest, AddInternalProcessTest) {
  CgroupSetup cgroup_setup{"/sys/fs/cgroup", "node_id", CgroupSetup::Tag{}};

  pid_t pid = fork();
  ASSERT_NE(pid, -1);

  // Child process.
  if (pid == 0) {
    // Spawn a process running long enough, so it could be added into internal cgroup.
    // It won't affect test runtime, because it will be killed later.
    execlp("sleep", "sleep", "3600", nullptr);
    perror("execlp");
  }

  // Parent process.
  RAY_ASSERT_OK(cgroup_setup.AddInternalProcess(pid));

  // Check process id exists in cgroup.
  auto pids = ReadEntireFile(internal_cgroup_proc_filepath_);
  RAY_ASSERT_OK(pids);
  std::string_view pids_sv = *pids;
  absl::ConsumeSuffix(&pids_sv, "\n");

  const std::unordered_set<std::string_view> pid_parts = absl::StrSplit(pids_sv, ' ');
  EXPECT_TRUE(pid_parts.find(std::to_string(pid)) != pid_parts.end())
      << "All pids include "
      << DebugStringWrapper<std::unordered_set<std::string_view> >(pid_parts)
      << " and new pid is " << pid;

  // Kill testing process.
  RAY_ASSERT_OK(KillAllProc(internal_cgroup_folder_));
}
#endif

}  // namespace ray
