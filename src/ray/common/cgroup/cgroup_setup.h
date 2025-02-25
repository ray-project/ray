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
  // Usage to setup and cleanup cgroup resource in raylet:
  // const auto cgroup_setup = make_unique<CgroupSetup>(node_id);
  //
  // Util class to setup cgroups preparation for resource constraints.
  // It's expected to call from raylet to setup node level cgroup configurations.
  //
  // If error happens, error will be logged and return.
  // Cgroup is not supported on non-linux platforms; for non-linux environment program
  // proceeds with no warning.
  //
  // NOTICE: This function is expected to be called once for each raylet instance.
  //
  // Impact:
  // - Application cgroup will be created, where later worker process will be placed
  // under;
  // - Existing operating system processes and system components (GCS/raylet) will be
  // placed under system cgroup. For more details, see
  // https://github.com/ray-project/ray/blob/master/src/ray/common/cgroup/README.md
  CgroupSetup(std::string node_id);

  // On destruction, all processes in the managed cgroup will be killed via SIGKILL.
  ~CgroupSetup() override;

  ScopedCgroupHandler AddSystemProcess(pid_t pid) override;

  ScopedCgroupHandler ApplyCgroupContext(const AppProcCgroupMetadata &ctx) override;

 private:
  // Setup cgroup folders for the given [node_id].
  bool SetupCgroupsPreparation(const std::string &node_id);

  // Util function to cleanup cgroup after raylet exits.
  // If any error happens, it will be logged and early return.
  //
  // NOTITE: This function is expected to be called once for each raylet instance at its
  // termination.
  //
  // Impact:
  // - All dangling processes will be killed;
  // - Cgroup for the current node will be deleted.
  void CleanupCgroupForNode();

  // Apply cgroup context with new cgroup folder created.
  ScopedCgroupHandler ApplyCgroupForIndividualAppCgroup(const AppProcCgroupMetadata &ctx);

  // Apply cgroup context which addes pid into default cgroup folder.
  ScopedCgroupHandler ApplyCgroupForDefaultAppCgroup(const AppProcCgroupMetadata &ctx);

  [[maybe_unused]] bool cgroup_enabled_ = true;
  [[maybe_unused]] const std::string node_id_;
  // Root folder for cgroup v2 for the current raylet instance.
  // See README under the current folder for details.
  [[maybe_unused]] std::string cgroup_v2_app_folder_;
  [[maybe_unused]] std::string cgroup_v2_system_folder_;

  // Cgroup folder for the current ray node.
  [[maybe_unused]] std::string cgroup_v2_folder_;
};

}  // namespace ray
