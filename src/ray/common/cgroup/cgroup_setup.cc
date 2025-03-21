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
CgroupSetup::CgroupSetup() {}
void CgroupSetup::AddInternalProcess(pid_t pid) {}
ScopedCgroupHandler CgroupSetup::ApplyCgroupContext(const AppProcCgroupMetadata &ctx) {
  return {};
}
void CgroupSetup::CleanupCgroupContext(const AppProcCgroupMetadata &ctx) {}
void CgroupSetup::CleanupCgroups() {}
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
#include "ray/util/filesystem.h"
#include "ray/util/invoke_once_token.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {

namespace {

// TODO(hjiang): Cleanup all constants in the followup PR.
//
// Parent cgroup path.
constexpr std::string_view kRootCgroupProcs = "/sys/fs/cgroup/cgroup.procs";
// Cgroup subtree control path.
constexpr std::string_view kRootCgroupSubtreeControl =
    "/sys/fs/cgroup/cgroup.subtree_control";
// Owner can read and write.
constexpr mode_t kReadWritePerm = S_IRUSR | S_IWUSR;

// Move all pids under [from] to [to].
bool MoveProcsBetweenCgroups(const std::string &from, const std::string &to) {
  std::ifstream in_file(from.data());
  RAY_CHECK(in_file.good()) << "Failed to open cgroup file " << to;
  std::ofstream out_file(to.data(), std::ios::app | std::ios::out);
  RAY_CHECK(out_file.good()) << "Failed to open cgroup file " << from;

  pid_t pid = 0;
  while (in_file >> pid) {
    out_file << pid << std::endl;
  }
  out_file.flush();
  return out_file.good();
}

// Return whether cgroup control writes successfully.
//
// TODO(hjiang): Currently only memory resource is considered, should consider CPU
// resource as well.
bool EnableCgroupSubtreeControl(const char *subtree_control_path) {
  std::ofstream out_file(subtree_control_path, std::ios::app | std::ios::out);
  RAY_CHECK(out_file.good()) << "Failed to open cgroup file " << subtree_control_path;
  // Able to add memory constraint to the internal cgroup.
  out_file << "+memory";
  out_file.flush();
  return out_file.good();
}

// Kill all processes under the given [cgroup_folder].
void KillAllProc(const std::string &cgroup_folder) {
  const std::string kill_proc_file = absl::StrFormat("%s/cgroup.kill", cgroup_folder);
  std::ofstream f{kill_proc_file, std::ios::app | std::ios::out};
  f << "1";
  f.flush();
  RAY_CHECK(f.good()) << "Fails to kill all processes under the cgroup " << cgroup_folder;
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

CgroupSetup::CgroupSetup(const std::string &node_id) {
  static InvokeOnceToken token;
  token.CheckInvokeOnce();
  SetupCgroups(node_id);
}

void CgroupSetup::SetupCgroups(const std::string &node_id) {
  // Check cgroup accessibility before setup.
  RAY_CHECK(internal::IsCgroupV2MountedAsRw())
      << "Cgroup v2 is not mounted in read-write mode.";
  RAY_CHECK(internal::CanCurrenUserWriteCgroupV2())
      << "Current user doesn't have the permission to update cgroup v2.";

  // Cgroup folder for the current ray node.
  cgroup_v2_folder_ = absl::StrFormat("/sys/fs/cgroup/ray_node_%s", node_id);

  cgroup_v2_app_folder_ = absl::StrFormat("%s/ray_application", cgroup_v2_folder_);
  cgroup_v2_internal_folder_ = absl::StrFormat("%s/internal", cgroup_v2_folder_);
  const std::string cgroup_v2_app_procs =
      ray::JoinPaths(cgroup_v2_app_folder_, "cgroup.procs");
  const std::string cgroup_v2_app_subtree_control =
      ray::JoinPaths(cgroup_v2_app_folder_, "cgroup.subtree_control");
  const std::string cgroup_v2_internal_procs =
      ray::JoinPaths(cgroup_v2_internal_folder_, "cgroup.procs");

  // Create the internal cgroup.
  RAY_CHECK_EQ(mkdir(cgroup_v2_internal_folder_.data(), kCgroupFilePerm), 0);

  // TODO(hjiang): Move GCS and raylet into internal cgroup, so we need a way to know
  // internal components PID for raylet.
  RAY_CHECK(MoveProcsBetweenCgroups(/*from=*/kRootCgroupProcs.data(),
                                    /*to=*/cgroup_v2_internal_folder_));
  RAY_CHECK(EnableCgroupSubtreeControl(kRootCgroupSubtreeControl.data()));

  // Setup application cgroup.
  RAY_CHECK_EQ(mkdir(cgroup_v2_app_folder_.data(), kCgroupFilePerm), 0);
  RAY_CHECK(EnableCgroupSubtreeControl(cgroup_v2_app_subtree_control.data()));
}

CgroupSetup::~CgroupSetup() { CleanupCgroups(); }

void CgroupSetup::CleanupCgroups() {
  static InvokeOnceToken token;
  token.CheckInvokeOnce();

  // Kill all dangling processes.
  KillAllProc(cgroup_v2_app_folder_);

  // Move all internal processes into root cgroup and delete internal cgroup.
  RAY_CHECK(MoveProcsBetweenCgroups(/*from=*/cgroup_v2_internal_folder_,
                                    /*to=*/kRootCgroupProcs.data()))
      << "Failed to move internal processes back to root cgroup";
  RAY_CHECK(std::filesystem::remove(cgroup_v2_internal_folder_))
      << "Failed to delete raylet internal cgroup folder";

  // Cleanup cgroup for current node.
  RAY_CHECK(std::filesystem::remove(cgroup_v2_folder_))
      << "Failed to delete raylet internal cgroup folder";
}

void CgroupSetup::AddInternalProcess(pid_t pid) {
  std::ofstream out_file(cgroup_v2_internal_folder_, std::ios::app | std::ios::out);
  // Able to add memory constraint to the internal cgroup.
  out_file << pid;
  out_file.flush();
  RAY_CHECK(out_file.good()) << "Failed to add " << pid << " into cgroup.";
}

ScopedCgroupHandler CgroupSetup::ApplyCgroupForIndividualAppCgroup(
    const AppProcCgroupMetadata &ctx) {
  RAY_CHECK_NE(ctx.max_memory, 0);  // Sanity check.

  // Create a new cgroup folder for the given process.
  const std::string cgroup_folder = ray::JoinPaths(cgroup_v2_app_folder_, ctx.id);
  RAY_CHECK(std::filesystem::create_directory(cgroup_folder))
      << "Failed to create cgroup " << cgroup_folder;

  const std::string cgroup_proc_file = ray::JoinPaths(cgroup_folder, "cgroup.procs");
  std::ofstream out_file(cgroup_proc_file, std::ios::app | std::ios::out);
  out_file << ctx.pid;
  out_file.flush();
  RAY_CHECK(out_file.good()) << "Failed to add process " << ctx.pid << " with max memory "
                             << ctx.max_memory << " into cgroup folder";

  auto delete_cgroup_folder = [folder = cgroup_folder]() {
    RAY_CHECK(std::filesystem::remove(folder));  // Not expected to fail.
  };
  return ScopedCgroupHandler{std::move(delete_cgroup_folder)};
}

ScopedCgroupHandler CgroupSetup::ApplyCgroupForDefaultAppCgroup(
    const AppProcCgroupMetadata &ctx) {
  RAY_CHECK_EQ(ctx.max_memory, 0);  // Sanity check.

  const std::string default_cgroup_folder =
      ray::JoinPaths(cgroup_v2_app_folder_, "default");
  const std::string default_cgroup_proc_file =
      ray::JoinPaths(default_cgroup_folder, "cgroup.procs");

  std::ofstream out_file(default_cgroup_proc_file, std::ios::app | std::ios::out);
  out_file << ctx.pid;
  out_file.flush();
  RAY_CHECK(out_file.good()) << "Failed to add process " << ctx.pid << " with max memory "
                             << ctx.max_memory << " into cgroup folder";

  // Default cgroup folder's lifecycle is the same as node-level's cgroup folder, we don't
  // need to clean it up after one process terminates.
  return ScopedCgroupHandler{};
}

ScopedCgroupHandler CgroupSetup::ApplyCgroupContext(const AppProcCgroupMetadata &ctx) {
  if (ctx.max_memory == 0) {
    return ApplyCgroupForDefaultAppCgroup(ctx);
  }
  return ApplyCgroupForIndividualAppCgroup(ctx);
}

}  // namespace ray

#endif  // __linux__
