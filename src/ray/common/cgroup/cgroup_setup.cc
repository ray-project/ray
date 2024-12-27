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
bool SetupCgroupsPreparation() { return false; }
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
// System cgroup related constants.
const std::string kSystemCgroupFolder = []() {
  // Append UUID to system cgroup path to avoid conflict.
  // Chances are that multiple ray cluster runs on the same filesystem.
  return absl::StrFormat("/sys/fs/cgroup/ray_system_%s", GenerateUUIDV4());
}();
// Cgroup PID path for ray system components.
const std::string kSystemCgroupProcs =
    ray::JoinPaths(kSystemCgroupFolder, "cgroup.procs");

// Application cgroup related constants.
const std::string kApplicationCgroupFolder = []() {
  // Append UUID to application cgroup path to avoid conflict.
  // Chances are that multiple ray cluster runs on the same filesystem.
  return absl::StrFormat("/sys/fs/cgroup/ray_application_%s", GenerateUUIDV4());
}();
const std::string kApplicationCgroupSubtreeControl = []() {
  return absl::StrFormat("%s/cgroup.subtree_control", kApplicationCgroupFolder);
}();

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
  std::ofstream out_file(kSystemCgroupProcs.data());
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

bool SetupCgroupsPreparation() {
#ifndef __linux__
  RAY_LOG(ERROR) << "Cgroup is not supported on non-Linux platforms.";
  return true;
#endif

  // Create the system cgroup.
  int ret_code = mkdir(kSystemCgroupFolder.data(), kCgroupFilePerm);
  if (ret_code != 0) {
    RAY_LOG(ERROR) << "Failed to create system cgroup: " << strerror(errno);
    return false;
  }

  MoveProcsInSystemCgroup();
  EnableCgroupSubtreeControl(kRootCgroupSubtreeControl.data());

  // Setup application cgroup.
  ret_code = mkdir(kApplicationCgroupFolder.data(), kCgroupFilePerm);
  if (ret_code != 0) {
    RAY_LOG(ERROR) << "Failed to create application cgroup: " << strerror(errno);
    return false;
  }
  EnableCgroupSubtreeControl(kApplicationCgroupSubtreeControl.data());

  return true;
}

}  // namespace ray

#endif  // __linux__
