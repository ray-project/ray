// Copyright 2024 The Ray Authors.
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
// TODO(hjiang): Set resource reservation for system cgroup.

#pragma once

#include <gtest/gtest_prod.h>

#include <string>

#include "ray/common/cgroup/base_cgroup_setup.h"
#include "ray/common/status.h"

namespace ray {

namespace internal {

// Checks whether cgroupv2 is properly mounted for read-write operations in the given
// [directory]. Also checks that cgroupv1 is not mounted.
// If not, InvalidArgument status is returned.
//
// This function is exposed in header file for unit test purpose.
//
// \param directory: user provided mounted cgroupv2 directory.
Status CheckCgroupV2MountedRW(const std::string &directory);

// Checks whether root cgroupv2 (whether it's at host machine or inside of container) has
// enabled memory and cpu subtree controller.
//
// \param directory: user provided mounted cgroupv2 directory.
Status CheckBaseCgroupSubtreeController(const std::string &directory);

}  // namespace internal

class CgroupSetup : public BaseCgroupSetup {
 public:
  // This class sets up resource isolation using cgroupv2. It reserves resources on each
  // ray node for system processes on linux. It is expected to work in containers, virtual
  // machines, and bare metal machines. It is expected to be used by the raylet.

  // Creates a cgroup hierarchy under the specified directory.
  // See https://github.com/ray-project/ray/blob/master/src/ray/common/cgroup/README.md
  // for more details about the cgroup hierarchy. If there is an error, it will be logged
  // and the process will exit. NOTE: This constructor is expected to be called only once
  // per raylet instance
  //
  // TODO(hjiang): Implement support for VM/BM. Currently only docker is supported.
  CgroupSetup(const std::string &directory, const std::string &node_id);

  // On destruction, all processes (including spawned child processes) in the managed
  // cgroup will be killed recursively via SIGKILL.
  ~CgroupSetup() override;

  // Add the specified process into the system cgroup.
  Status AddSystemProcess(pid_t pid) override;

  ScopedCgroupHandler ApplyCgroupContext(const AppProcCgroupMetadata &ctx) override;

 private:
  struct TestTag {};
  // Constructor made for unit tests, which allows [CgroupSetup] to be created for
  // multiple times in a process.
  CgroupSetup(const std::string &directory, const std::string &node_id, TestTag);

  FRIEND_TEST(Cgroupv2SetupTest, SetupTest);
  FRIEND_TEST(Cgroupv2SetupTest, AddSystemProcessTest);
  FRIEND_TEST(Cgroupv2SetupTest, AddAppProcessTest);

  // Setup cgroup folders for the given [node_id].
  Status InitializeCgroupV2Directory(const std::string &directory,
                                     const std::string &node_id);

  // Cleans up cgroup after the raylet exits by killing all dangling processes and
  // deleting the node cgroup.
  //
  // NOTE: This function is expected to be called once for each raylet instance at its
  // termination.
  Status CleanupCgroups();

  // Apply cgroup context which addes pid into default cgroup folder.
  //
  // TODO(hjiang): As of now there's a bug for returning StatusOr<> at windows, switch
  // after the issue resolved.
  // Link: https://github.com/ray-project/ray/pull/50761
  ScopedCgroupHandler ApplyCgroupForDefaultAppCgroup(const AppProcCgroupMetadata &ctx);

  // File path of PIDs for root cgroup.
  std::string root_cgroup_procs_filepath_;
  // File path for subtree control for root cgroup.
  std::string root_cgroup_subtree_control_filepath_;
  // Folder for cgroup v2 application processes of the current raylet instance.
  std::string cgroup_v2_app_folder_;
  // Folder for cgroup v2 default application cgroup of the current raylet instance.
  std::string cgroup_v2_default_app_folder_;
  // Process id file for default application cgroup.
  std::string cgroup_v2_default_app_proc_filepath_;
  // Folder for cgroup v2 internal processes of the current raylet instance.
  std::string cgroup_v2_system_folder_;
  // File path for cgroup v2 internal process pids.
  std::string cgroup_v2_system_proc_filepath_;
  // Cgroup folder for the current ray node.
  std::string node_cgroup_v2_folder_;
};

}  // namespace ray
