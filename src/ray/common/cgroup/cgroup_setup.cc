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
Status InitializeCgroupv2Directory(const std::string &node_id /*unused*/) {
  return Status::Invalid("cgroupv2 operations only support linux platform.");
}
namespace internal {
Status CheckCgroupV2MountedRW(const std::string &directory) {
  return Status::Invalid("cgroupv2 operations only support linux platform.");
}
}  // namespace internal
}  // namespace ray
#else  // __linux__

#include <linux/magic.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <string_view>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
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

// TODO(hjiang): Cleanup all constants in the followup PR.
//
// Parent cgroup path.
constexpr std::string_view kRootCgroupProcs = "/sys/fs/cgroup/cgroup.procs";
// Cgroup subtree control path.
constexpr std::string_view kRootCgroupSubtreeControl =
    "/sys/fs/cgroup/cgroup.subtree_control";
// Owner can read and write.
constexpr int kReadWritePerm = 0600;

// If system processes are running in the container, all processes will be placed in the
// root cgroup. This function will move all PIDs under root cgroup into system cgroup.
//
// Return whether the PIDs move successfully.
bool MoveProcsInSystemCgroup() {
  std::ifstream in_file(kRootCgroupProcs.data());
  std::ofstream out_file(cgroup_v2_system_folder.data());
  int pid = 0;
  while (in_file >> pid) {
    out_file << pid << std::endl;
  }
  return out_file.good();
}

// Return whether cgroup control writes successfully.
bool EnableCgroupSubtreeControl(const char *subtree_control_path) {
  std::ofstream out_file(subtree_control_path);
  // Able to add new PIDs and memory constraint to the system cgroup.
  out_file << "+memory +pids";
  return out_file.good();
}

}  // namespace

namespace internal {

Status CheckCgroupV2MountedRW(const std::string &path) {
  struct statfs fs_stats;
  if (statfs(path.data(), &fs_stats) != 0) {
    return Status::InvalidArgument("")
           << "Failed to stat file " << path << " because " << strerror(errno);
  }
  if (fs_stats.f_type != CGROUP2_SUPER_MAGIC) {
    return Status::InvalidArgument("")
           << "File " << path << " is not of type cgroupv2, which is "
           << static_cast<int>(fs_stats.f_type);
  }

  // Check whether cgroupv2 is mounted in rw mode.
  struct statvfs vfs_stats;
  if (statvfs(path.data(), &vfs_stats) != 0) {
    return Status::InvalidArgument("")
           << "Failed to stat filesystem for " << path << " because " << strerror(errno);
  }
  // There're only two possible modes, either rw mode or read-only mode.
  if ((vfs_stats.f_flag & ST_RDONLY) != 0) {
    return Status::InvalidArgument("")
           << "Filesystem indicated by " << path << " doesn't have write permission.";
  }

  return Status::OK();
}

}  // namespace internal

Status InitializeCgroupV2Directory(const std::string &directory,
                                   const std::string &node_id) {
  RAY_CHECK(cgroup_v2_app_folder.empty())
      << "Cgroup v2 for raylet should be only initialized once.";

  // Cgroup folder for the current ray node.
  const std::string cgroup_folder =
      absl::StrFormat("/sys/fs/cgroup/ray_node_%s", node_id);

  // Check cgroup accessibility before setup.
  if (Status s = internal::CheckCgroupV2MountedRW(directory); !s.ok()) {
    return s;
  }

  cgroup_v2_app_folder = absl::StrFormat("%s/ray_application_%s", cgroup_folder, node_id);
  cgroup_v2_system_folder = absl::StrFormat("%s/ray_system_%s", cgroup_folder, node_id);
  const std::string cgroup_v2_app_procs =
      ray::JoinPaths(cgroup_v2_app_folder, "cgroup.procs");
  const std::string cgroup_v2_app_subtree_control =
      ray::JoinPaths(cgroup_v2_app_folder, "cgroup.subtree_control");
  const std::string cgroup_v2_system_procs =
      ray::JoinPaths(cgroup_v2_system_folder, "cgroup.procs");

  // Create the system cgroup.
  int ret_code = mkdir(cgroup_v2_system_folder.data(), kReadWritePerm);
  if (ret_code != 0) {
    return Status::InvalidArgument("")
           << "Failed to make directory " << cgroup_v2_system_folder << " because "
           << strerror(errno);
  }

  if (!MoveProcsInSystemCgroup()) {
    return Status::UnknownError("") << "Failed to move processes into system cgroup";
  }
  if (!EnableCgroupSubtreeControl(kRootCgroupSubtreeControl.data())) {
    return Status::UnknownError("")
           << "Failed to enable subtree control for cgroup " << kRootCgroupSubtreeControl;
  }

  // Setup application cgroup.
  ret_code = mkdir(cgroup_v2_app_folder.data(), kReadWritePerm);
  if (ret_code != 0) {
    return Status::InvalidArgument("")
           << "Failed to make directory " << cgroup_v2_app_folder << " because "
           << strerror(errno);
  }
  if (!EnableCgroupSubtreeControl(cgroup_v2_app_subtree_control.data())) {
    return Status::UnknownError("")
           << "Failed to enable subtree control for " << cgroup_v2_app_subtree_control;
  }

  return Status::OK();
}

const std::string &GetCgroupV2AppFolder() { return cgroup_v2_app_folder; }

// Get folder name for system cgroup v2 for current raylet instance.
const std::string &GetCgroupV2SystemFolder() { return cgroup_v2_system_folder; }

}  // namespace ray

#endif  // __linux__
