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

#include <string>

#ifndef __linux__
namespace ray {
CgroupSetup::CgroupSetup(const std::string &directory, const std::string &node_id) {
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
CgroupSetup::CgroupSetup(const std::string &directory,
                         const std::string &node_id,
                         TestTag) {
  RAY_CHECK(false) << "cgroupv2 doesn't work on non linux platform.";
}
CgroupSetup::~CgroupSetup() {}
Status CgroupSetup::InitializeCgroupV2Directory(const std::string &directory,
                                                const std::string &node_id) {
  return Status::OK();
}
ScopedCgroupHandler CgroupSetup::ApplyCgroupForDefaultAppCgroup(
    const AppProcCgroupMetadata &ctx) {
  return {};
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

#include <algorithm>
#include <array>
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
#include "ray/common/cgroup/cgroup_utils.h"
#include "ray/common/cgroup/constants.h"
#include "ray/common/macros.h"
#include "ray/util/filesystem.h"
#include "ray/util/invoke_once_token.h"
#include "ray/util/logging.h"
#include "ray/util/path_utils.h"

namespace ray {

namespace {

#if defined(RAY_SCHECK_OK_CGROUP)
#error "RAY_SCHECK_OK_CGROUP is already defined."
#else
#define __RAY_SCHECK_OK_CGROUP(expr, boolname) \
  auto boolname = (expr);                      \
  if (!boolname) return Status(StatusCode::Invalid, /*msg=*/"", RAY_LOC())

// Invoke the given [expr] which returns a boolean convertible type; and return error
// status if Failed. Cgroup operations on filesystem are not expected to fail after
// precondition checked, so we use INVALID as the status code.
//
// Example usage:
// RAY_SCHECK_OK_CGROUP(DoSomething()) << "DoSomething Failed";
#define RAY_SCHECK_OK_CGROUP(expr) \
  __RAY_SCHECK_OK_CGROUP(expr, RAY_UNIQUE_VARIABLE(cgroup_op))
#endif

Status MoveProcsBetweenCgroups(const std::string &from, const std::string &to) {
  std::ifstream in_file(from.data());
  RAY_SCHECK_OK_CGROUP(in_file.good()) << "Failed to open cgroup file " << from;
  std::ofstream out_file(to.data(), std::ios::app | std::ios::out);
  RAY_SCHECK_OK_CGROUP(out_file.good()) << "Failed to open cgroup file " << to;

  pid_t pid = 0;
  while (in_file >> pid) {
    out_file << pid;
  }
  RAY_SCHECK_OK_CGROUP(out_file.good()) << "Failed to flush cgroup file " << to;

  return Status::OK();
}

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

Status CheckBaseCgroupSubtreeController(const std::string &directory) {
  const auto subtree_control_path = ray::JoinPaths(directory, kSubtreeControlFilename);
  std::ifstream in_file(subtree_control_path, std::ios::app | std::ios::out);
  RAY_SCHECK_OK_CGROUP(in_file.good())
      << "Failed to open cgroup file " << subtree_control_path;

  std::string content((std::istreambuf_iterator<char>(in_file)),
                      std::istreambuf_iterator<char>());
  std::string_view content_sv{content};
  absl::ConsumeSuffix(&content_sv, "\n");

  const std::vector<std::string_view> enabled_subtree_controllers =
      absl::StrSplit(content_sv, ' ');
  for (const auto &cur_controller : kRequiredControllers) {
    if (std::find(enabled_subtree_controllers.begin(),
                  enabled_subtree_controllers.end(),
                  cur_controller) != enabled_subtree_controllers.end()) {
      return Status(StatusCode::Invalid, /*msg=*/"", RAY_LOC())
             << "Base cgroup " << directory << " doesn't enable " << cur_controller
             << " controller for subtree."
             << " Check to see if the parent of " << directory << " has the "
             << cur_controller << " controller enabled.";
    }
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

  // Check cgroup subtree control before setup.
  if (Status s = internal::CheckBaseCgroupSubtreeController(directory); !s.ok()) {
    return s;
  }

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

CgroupSetup::~CgroupSetup() { RAY_CHECK_OK(CleanupCgroups()); }

Status CgroupSetup::CleanupCgroups() {
  // Kill all dangling processes.
  RAY_RETURN_NOT_OK(KillAllProcAndWait(cgroup_v2_app_folder_));

  // Move all internal processes into root cgroup and delete system cgroup.
  RAY_RETURN_NOT_OK(MoveProcsBetweenCgroups(/*from=*/cgroup_v2_system_folder_,
                                            /*to=*/root_cgroup_procs_filepath_));

  // Cleanup all ray application cgroup folders.
  std::error_code err_code;
  for (const auto &dentry :
       std::filesystem::directory_iterator(cgroup_v2_app_folder_, err_code)) {
    RAY_SCHECK_OK_CGROUP(err_code.value() == 0)
        << "Failed to iterate through directory " << cgroup_v2_app_folder_ << " because "
        << err_code.message();
    if (!dentry.is_directory()) {
      continue;
    }
    RAY_SCHECK_OK_CGROUP(std::filesystem::remove(dentry, err_code))
        << "Failed to delete application cgroup folder " << dentry.path().string()
        << " because " << err_code.message();
  }

  RAY_SCHECK_OK_CGROUP(std::filesystem::remove(cgroup_v2_app_folder_, err_code))
      << "Failed to delete application cgroup folder " << cgroup_v2_app_folder_
      << " because " << err_code.message();

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

ScopedCgroupHandler CgroupSetup::ApplyCgroupForDefaultAppCgroup(
    const AppProcCgroupMetadata &ctx) {
  RAY_CHECK_EQ(ctx.max_memory, static_cast<uint64_t>(kUnlimitedCgroupMemory))
      << "Ray doesn't support per-task resource constraint.";

  std::ofstream out_file(cgroup_v2_default_app_proc_filepath_,
                         std::ios::app | std::ios::out);
  out_file << ctx.pid;
  RAY_CHECK(out_file.good()) << "Failed to add process " << ctx.pid << " with max memory "
                             << ctx.max_memory << " into cgroup folder";

  // Default cgroup folder's lifecycle is the same as node-level's cgroup folder, we don't
  // need to clean it up after one process terminates.
  return ScopedCgroupHandler{};
}

ScopedCgroupHandler CgroupSetup::ApplyCgroupContext(const AppProcCgroupMetadata &ctx) {
  // For milestone-1, there's no request and limit set for each task.
  RAY_CHECK_EQ(ctx.max_memory, static_cast<uint64_t>(0));
  return ApplyCgroupForDefaultAppCgroup(ctx);
}

}  // namespace ray

#endif  // __linux__
