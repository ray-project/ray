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
// TODO(hjiang): The util class should take a parameter like `allow_proceed_at_error` to
// decide what to do when a non-internal error happens; at the moment we simply log and
// return for these errors.

#pragma once

#include <string>

#include "ray/common/cgroup/base_cgroup_setup.h"

namespace ray {

namespace internal {

// Return whether current user could write to cgroupv2.
bool CanCurrenUserWriteCgroupV2();

// Return whether cgroup V2 is mounted in read and write mode.
bool IsCgroupV2MountedAsRw();

}  // namespace internal

class CgroupSetup : public BaseCgroupSetup {
 public:
  // Util class to setup cgroups to reserve resources on a ray node for ray internal
  // processes on linux, and it works for different environments (VM, bare metal machine
  // and docker). It's expected to call from raylet to setup node level cgroup
  // configurations.
  //
  // If error happens, error will be logged and return.
  // NOTICE: This function is expected to be called once for each raylet instance.
  //
  // TODO(hjiang): Docker and VM/BM take different handlings, here we only implement the
  // docker env. Impact:
  // - Application cgroup will be created, where later worker process will be placed
  // under;
  // - Existing operating internal processes and internal components will be placed under
  // internal cgroup. For more details, see
  // https://github.com/ray-project/ray/blob/master/src/ray/common/cgroup/README.md
  explicit CgroupSetup(const std::string &node_id);

  // On destruction, all processes in the managed cgroup will be killed via SIGKILL.
  ~CgroupSetup() override;

  void AddInternalProcess(pid_t pid) override;

  ScopedCgroupHandler ApplyCgroupContext(const AppProcCgroupMetadata &ctx) override;

 private:
  // Setup cgroup folders for the given [node_id].
  void SetupCgroups(const std::string &node_id);

  // Util function to cleanup cgroup after raylet exits.
  // NOTICE: This function is expected to be called once for each raylet instance at its
  // termination.
  //
  // Impact:
  // - All dangling processes will be killed;
  // - Cgroup for the current node will be deleted.
  void CleanupCgroups();

  // Apply cgroup context with new cgroup folder created.
  ScopedCgroupHandler ApplyCgroupForIndividualAppCgroup(const AppProcCgroupMetadata &ctx);

  // Apply cgroup context which addes pid into default cgroup folder.
  ScopedCgroupHandler ApplyCgroupForDefaultAppCgroup(const AppProcCgroupMetadata &ctx);

  // See README under the current folder for details.
  //
  // Folder for cgroup v2 application processes of the current raylet instance.
  [[maybe_unused]] std::string cgroup_v2_app_folder_;
  // Folder for cgroup v2 internal processes of the current raylet instance.
  [[maybe_unused]] std::string cgroup_v2_internal_folder_;
  // Cgroup folder for the current ray node.
  [[maybe_unused]] std::string cgroup_v2_folder_;
};

}  // namespace ray
