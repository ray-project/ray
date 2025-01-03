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

#include "ray/common/cgroup/cgroup_setup.h"

#ifndef __linux__
namespace ray {
bool SetupCgroupsPreparation(const std::string &node_id /*unused*/) { return false; }
}  // namespace ray
#else  // __linux__

#include <sys/stat.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <string_view>
#include <vector>

#include "absl/strings/str_format.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {

namespace {
// TODO(hjiang): Use `absl::NoDestructor` to avoid non-trivially destructible global
// objects. Cgroup path for ray system components.
//
// Root folder for cgroup v2 for the current raylet instance.
// See README under the current folder for details.
std::string cgroup_v2_app_folder;
std::string cgroup_v2_system_folder;

// Parent cgroup path.
constexpr std::string_view kRtootCgroupProcs = "/sys/fs/cgroup/cgroup.procs";
// Cgroup subtree control path.
constexpr std::string_view kRootCgroupSubtreeControl =
    "/sys/fs/cgroup/cgroup.subtree_control";
// Owner can read and write.
constexpr int kCgroupFilePerm = 0600;

// If system processes are running in the container, all processes will be placed in the
// root cgroup. This function will move all PIDs under root cgroup into system cgroup.
void MoveProcsInSystemCgroup() {
  std::ifstream in_file(kRtootCgroupProcs.data());
  std::ofstream out_file(cgroup_v2_system_folder.data());
  int pid = 0;
  while (in_file >> pid) {
    out_file << pid << std::endl;
  }
}

void EnableCgroupSubtreeControl(const char *subtree_control_path) {
  std::ofstream out_file(subtree_control_path);
  // Able to add new PIDs and memory constraint to the system cgroup.
  out_file << "+memory +pids";
}

}  // namespace

bool SetupCgroupsPreparation(const std::string &node_id) {
#ifndef __linux__
  RAY_LOG(ERROR) << "Cgroup is not supported on non-Linux platforms.";
  return true;
#endif

  RAY_CHECK(cgroup_v2_app_folder.empty())
      << "Cgroup v2 for raylet should be only initialized once.";

  cgroup_v2_app_folder = absl::StrFormat("/sys/fs/cgroup/ray_application_%s", node_id);
  cgroup_v2_system_folder = absl::StrFormat("/sys/fs/cgroup/ray_system_%s", node_id);
  const std::string cgroup_v2_app_procs =
      ray::JoinPaths(cgroup_v2_app_folder, "cgroup.procs");
  const std::string cgroup_v2_app_subtree_control =
      ray::JoinPaths(cgroup_v2_app_folder, "cgroup.subtree_control");
  const std::string cgroup_v2_system_procs =
      ray::JoinPaths(cgroup_v2_system_folder, "cgroup.procs");

  // Create the system cgroup.
  int ret_code = mkdir(cgroup_v2_system_folder.data(), kCgroupFilePerm);
  if (ret_code != 0) {
    RAY_LOG(ERROR) << "Failed to create system cgroup: " << strerror(errno);
    return false;
  }

  MoveProcsInSystemCgroup();
  EnableCgroupSubtreeControl(kRootCgroupSubtreeControl.data());

  // Setup application cgroup.
  ret_code = mkdir(cgroup_v2_app_folder.data(), kCgroupFilePerm);
  if (ret_code != 0) {
    RAY_LOG(ERROR) << "Failed to create application cgroup: " << strerror(errno);
    return false;
  }
  EnableCgroupSubtreeControl(cgroup_v2_app_subtree_control.data());

  return true;
}

const std::string &GetCgroupV2AppFolder() { return cgroup_v2_app_folder; }

// Get folder name for system cgroup v2 for current raylet instance.
const std::string &GetCgroupV2SystemFolder() { return cgroup_v2_system_folder; }

}  // namespace ray

#endif  // __linux__
