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
CgroupSetup::CgroupSetup() {
  RAY_CHECK(false) << "cgroupv2 doesn't work on non linux platform.";
  RAY_UNUSED(root_cgroup_procs_filepath_);
  RAY_UNUSED(root_cgroup_subtree_control_filepath_);
  RAY_UNUSED(cgroup_v2_app_folder_);
  RAY_UNUSED(cgroup_v2_default_app_folder_);
  RAY_UNUSED(cgroup_v2_default_app_proc_filepath_);
  RAY_UNUSED(cgroup_v2_system_folder_);
  RAY_UNUSED(cgroup_v2_system_proc_filepath_);
  RAY_UNUSED(node_cgroup_v2_folder_);
}
Status CgroupSetup::AddSystemProcess(pid_t pid) { return Status::OK(); }
ScopedCgroupHandler CgroupSetup::ApplyCgroupContext(const AppProcCgroupMetadata &ctx) {
  return {};
}
Status CgroupSetup::CleanupCgroups() { return Status::OK(); }
namespace internal {
Status CheckCgroupV2MountedRW(const std::string &directory) {
  return Status::Invalid("cgroupv2 operations only support linux platform.");
}
}  // namespace internal
}  // namespace ray
#else  // __linux__

#include <fcntl.h>
#include <linux/magic.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string_view>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "ray/common/cgroup/cgroup_macros.h"
#include "ray/common/cgroup/cgroup_utils.h"
#include "ray/common/cgroup/constants.h"
#include "ray/util/filesystem.h"
#include "ray/util/invoke_once_token.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {

namespace {

// Enables controllers for a cgroup, and returns whether cgroup control writes
// successfully.
//
// Note: enabling controllers in a subcgroup requires that its parent cgroup
// also has those controllers enabled.
Status EnableCgroupSubtreeControl(const std::string &subtree_control_path) {
  std::ofstream out_file(subtree_control_path, std::ios::app | std::ios::out);
  RAY_SCHECK_OK_CGROUP(out_file.good())
      << "Failed to open cgroup file " << subtree_control_path;

  out_file << "+memory";
  RAY_SCHECK_OK_CGROUP(out_file.good())
      << "Failed to write to cgroup file " << subtree_control_path;

  out_file << "+cpu";
  RAY_SCHECK_OK_CGROUP(out_file.good())
      << "Failed to write to cgroup file " << subtree_control_path;

  return Status::OK();
}

// Checks to see if the given cgroup directory is mounted as the root cgroup.
// The cgroup.type file only exists in non-root cgroups in cgroupv2.
//
//  \returns true for bare metal/virtual machines and false for containers (since the
//  cgroup within
// container is a subcgroup in the host cgroup hierarchy).
StatusOr<bool> IsRootCgroup(const std::string &directory) {
  const std::string cgroup_type_filepath = ray::JoinPaths(directory, kCgroupTypeFilename);
  std::error_code err_code;
  bool exists = std::filesystem::exists(cgroup_type_filepath, err_code);
  RAY_SCHECK_OK_CGROUP(err_code.value() == 0)
      << "Failed to check file " << cgroup_type_filepath << " exists because of "
      << err_code.message();
  return !exists;
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

// Use unix syscall `mkdir` instead of STL filesystem library because the former provides
// (1) ability to specify permission; (2) better error code and message.
Status MakeDirectory(const std::string &directory) {
  int ret_code = mkdir(directory.data(), kReadWritePerm);
  if (ret_code != 0 && errno != EEXIST) {
    RAY_SCHECK_OK_CGROUP(false)
        << "Failed to make directory for " << directory << " because " << strerror(errno);
  }
  return Status::OK();
}

}  // namespace internal

CgroupSetup::CgroupSetup(const std::string &directory, const std::string &node_id) {
  static InvokeOnceToken token;
  token.CheckInvokeOnce();
  RAY_CHECK_OK(InitializeCgroupV2Directory(directory, node_id));
}

CgroupSetup::CgroupSetup(const std::string &directory,
                         const std::string &node_id,
                         TestTag) {
  RAY_CHECK_OK(InitializeCgroupV2Directory(directory, node_id));
}

Status CgroupSetup::InitializeCgroupV2Directory(const std::string &directory,
                                                const std::string &node_id) {
  // Check cgroup accessibility before setup.
  RAY_RETURN_NOT_OK(internal::CheckCgroupV2MountedRW(directory));

  cgroup_v2_folder_ = directory;
  node_id_ = node_id;

  // Cgroup folders for the current ray node.
  node_cgroup_v2_folder_ =
      ray::JoinPaths(directory, absl::StrFormat("ray_node_%s", node_id));
  root_cgroup_procs_filepath_ = ray::JoinPaths(directory, kProcFilename);
  root_cgroup_subtree_control_filepath_ =
      ray::JoinPaths(node_cgroup_v2_folder_, kSubtreeControlFilename);
  cgroup_v2_app_folder_ = ray::JoinPaths(node_cgroup_v2_folder_, "ray_application");
  cgroup_v2_default_app_folder_ = ray::JoinPaths(cgroup_v2_app_folder_, "default");
  cgroup_v2_default_app_proc_filepath_ =
      ray::JoinPaths(cgroup_v2_default_app_folder_, kProcFilename);
  cgroup_v2_system_folder_ = ray::JoinPaths(node_cgroup_v2_folder_, "system");
  cgroup_v2_system_proc_filepath_ =
      ray::JoinPaths(cgroup_v2_system_folder_, kProcFilename);
  const std::string cgroup_v2_app_subtree_control =
      ray::JoinPaths(cgroup_v2_app_folder_, kSubtreeControlFilename);
  const std::string cgroup_v2_system_procs =
      ray::JoinPaths(cgroup_v2_system_folder_, kProcFilename);

  // Create subcgroup for current node.
  RAY_RETURN_NOT_OK(internal::MakeDirectory(node_cgroup_v2_folder_));

  // Create the system cgroup.
  RAY_RETURN_NOT_OK(internal::MakeDirectory(cgroup_v2_system_folder_));

  // Setup application cgroup.
  // TODO(hjiang): For milestone-2 per-task-based reservation and limitation, we need to
  // add subtree control to subcgroup as well, not needed for milestone-1.
  RAY_RETURN_NOT_OK(internal::MakeDirectory(cgroup_v2_app_folder_));
  RAY_RETURN_NOT_OK(internal::MakeDirectory(cgroup_v2_default_app_folder_));

  // If the given cgroup is not root cgroup (i.e. container environment), we need to move
  // all processes (including operating system processes) into system cgroup, because
  // only leaf cgroups can contain processes for cgroupv2. Otherwise we only move known
  // ray processes into system cgroup.
  RAY_ASSIGN_OR_RETURN(const bool is_root_cgroup, IsRootCgroup(directory));
  if (!is_root_cgroup) {
    RAY_RETURN_NOT_OK(MoveProcsBetweenCgroups(/*from=*/root_cgroup_procs_filepath_,
                                              /*to=*/cgroup_v2_system_proc_filepath_));
  }

  RAY_RETURN_NOT_OK(EnableCgroupSubtreeControl(root_cgroup_subtree_control_filepath_));
  return Status::OK();
}

Status CgroupSetup::AddSystemProcess(pid_t pid) {
  std::ofstream out_file(cgroup_v2_system_proc_filepath_, std::ios::app | std::ios::out);
  RAY_SCHECK_OK_CGROUP(out_file.good())
      << "Failed to open file " << cgroup_v2_system_proc_filepath_;

  out_file << pid;
  RAY_SCHECK_OK_CGROUP(out_file.good())
      << "Failed to add " << pid << " into cgroup process file "
      << cgroup_v2_system_proc_filepath_;
  return Status::OK();
}

}  // namespace ray

#endif  // __linux__
