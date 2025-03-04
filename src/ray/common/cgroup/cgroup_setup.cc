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

// Cgroup only works for linux platform, proceed with no warning for other platforms.
#ifndef __linux__
namespace ray {
CgroupSetup::CgroupSetup() {}
ScopedCgroupHandler CgroupSetup::AddSystemProcess(pid_t pid) { return {}; }
ScopedCgroupHandler CgroupSetup::ApplyCgroupContext(const AppProcCgroupMetadata &ctx) {
  return {};
}
void CgroupSetup::CleanupSystemProcess(pid_t pid) {}
void CgroupSetup::CleanupCgroupContext(const AppProcCgroupMetadata &ctx) {}
void CgroupSetup::CleanupCgroups() {}
namespace internal {
bool CanCurrenUserWriteCgroupV2() { return false; }
bool IsCgroupV2MountedAsRw() { return false; }
}  // namespace internal
}  // namespace ray
#else  // __linux__

#include <fcntl.h>
#include <sys/stat.h>
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
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {

namespace {

// Parent cgroup path.
constexpr std::string_view kRootCgroupProcs = "/sys/fs/cgroup/cgroup.procs";
// Cgroup subtree control path.
constexpr std::string_view kRootCgroupSubtreeControl =
    "/sys/fs/cgroup/cgroup.subtree_control";
// Owner can read and write.
constexpr mode_t kCgroupFilePerm = 0600;

// Move all pids under [from] to [to].
bool MoveProcsBetweenCgroups(const std::string &from, const std::string &to) {
  std::ifstream in_file(to.data());
  RAY_CHECK(in_file.good()) << "Failed to open cgroup file " << to;
  std::ofstream out_file(from.data());
  RAY_CHECK(out_file.good()) << "Failed to open cgroup file " << from;

  pid_t pid = 0;
  while (in_file >> pid) {
    out_file << pid << std::endl;
  }
  return out_file.good();
}

// Return whether cgroup control writes successfully.
//
// TODO(hjiang): Currently only memory resource is considered, should consider CPU
// resource as well.
bool EnableCgroupSubtreeControl(const char *subtree_control_path) {
  std::ofstream out_file(subtree_control_path);
  RAY_CHECK(out_file.good()) << "Failed to open cgroup file " << subtree_control_path;
  // Able to add memory constraint to the system cgroup.
  out_file << "+memory";
  return out_file.good();
}

// Kill all processes inside of the cgroup.
// If any operation fails, the error is logged and continue.
void KillProcsUnderCgroupFolder(const std::string &folder) {
  const std::string cgroup_v2_app_procs = ray::JoinPaths(folder, "cgroup.procs");
  std::vector<int> dangling_pids;
  std::ifstream in_file(cgroup_v2_app_procs.data());
  RAY_CHECK(in_file.good()) << "Failed to open cgroup file " << cgroup_v2_app_procs;
  pid_t pid = 0;
  while (in_file >> pid) {
    dangling_pids.emplace_back(pid);
  }

  for (pid_t pid : dangling_pids) {
    if (kill(pid, SIGKILL) == -1) {
      // TODO(hjiang): Consider to use command which starts the process to be more
      // user-friendly.
      RAY_LOG(ERROR) << "Failed to kill process " << pid << " because "
                     << strerror(errno);
      continue;
    }
    int status = 0;
    if (waitpid(pid, &status, 0) != -1) {
      RAY_LOG(ERROR) << "Failed to wait for subprocess when clean up cgroup because "
                     << strerror(errno);
      continue;
    }
  }
}

}  // namespace

namespace internal {

bool CanCurrenUserWriteCgroupV2() { return access("/sys/fs/cgroup", W_OK | X_OK) == 0; }

bool IsCgroupV2MountedAsRw() {
  // Checking all mountpoints directly and parse textually is the easiest way, compared
  // with mounted filesystem attributes.
  std::ifstream mounts("/proc/mounts");
  if (!mounts.is_open()) {
    return false;
  }

  // Mount information is formatted as:
  // <fs_spec> <fs_file> <fs_vfstype> <fs_mntopts> <dump-field> <fsck-field>
  std::string line;
  while (std::getline(mounts, line)) {
    std::vector<std::string_view> mount_info_tokens = absl::StrSplit(line, ' ');
    RAY_CHECK_EQ(mount_info_tokens.size(), 6UL);
    // For cgroupv2, `fs_spec` should be `cgroupv2` and there should be only one mount
    // information item.
    if (mount_info_tokens[0] != "cgroup2") {
      continue;
    }
    const auto &fs_mntopts = mount_info_tokens[3];

    // Mount options are formatted as: <opt1,opt2,...>.
    std::vector<std::string_view> mount_opts = absl::StrSplit(fs_mntopts, ',');

    // CgroupV2 has only one mount item, directly returns.
    return std::any_of(mount_opts.begin(),
                       mount_opts.end(),
                       [](const std::string_view cur_opt) { return cur_opt == "rw"; });
  }

  return false;
}

}  // namespace internal

CgroupSetup::CgroupSetup(std::string node_id) {
  static std::atomic<bool> cgroup_already_setup{false};
  bool expected = false;
  RAY_CHECK(cgroup_already_setup.compare_exchange_strong(expected, /*desired=*/true))
      << "Cgroup setup should be only called once per process";

  cgroup_enabled_ = SetupCgroups(node_id);
}

bool CgroupSetup::SetupCgroups(const std::string &node_id) {
  // Check cgroup accessibility before setup.
  if (!internal::CanCurrenUserWriteCgroupV2()) {
    RAY_LOG(ERROR) << "Current user doesn't have the permission to update cgroup v2.";
    return false;
  }
  if (!internal::IsCgroupV2MountedAsRw()) {
    RAY_LOG(ERROR) << "Cgroup v2 is not mounted in read-write mode.";
    return false;
  }

  // Cgroup folder for the current ray node.
  cgroup_v2_folder_ = absl::StrFormat("/sys/fs/cgroup/ray_node_%s", node_id);

  cgroup_v2_app_folder_ =
      absl::StrFormat("%s/ray_application_%s", cgroup_v2_folder_, node_id);
  cgroup_v2_system_folder_ = absl::StrFormat("%s/internal", cgroup_v2_folder_);
  const std::string cgroup_v2_app_procs =
      ray::JoinPaths(cgroup_v2_app_folder_, "cgroup.procs");
  const std::string cgroup_v2_app_subtree_control =
      ray::JoinPaths(cgroup_v2_app_folder_, "cgroup.subtree_control");
  const std::string cgroup_v2_system_procs =
      ray::JoinPaths(cgroup_v2_system_folder_, "cgroup.procs");

  // Create the system cgroup.
  RAY_CHECK_EQ(mkdir(cgroup_v2_system_folder_.data(), kCgroupFilePerm), 0);

  // TODO(hjiang): Move GCS and raylet into system cgroup, so we need a way to know system
  // components PID for raylet.
  RAY_CHECK(MoveProcsBetweenCgroups(/*from=*/kRootCgroupProcs.data(),
                                    /*to=*/cgroup_v2_system_folder_));
  RAY_CHECK(EnableCgroupSubtreeControl(kRootCgroupSubtreeControl.data()));

  // Setup application cgroup.
  RAY_CHECK_EQ(mkdir(cgroup_v2_app_folder_.data(), kCgroupFilePerm), 0);
  RAY_CHECK(EnableCgroupSubtreeControl(cgroup_v2_app_subtree_control.data()));

  return true;
}

CgroupSetup::~CgroupSetup() { CleanupCgroups(); }

void CgroupSetup::CleanupCgroups() {
  static std::atomic<bool> cgroup_already_cleaned{false};
  bool expected = false;
  RAY_CHECK(cgroup_already_cleaned.compare_exchange_strong(expected, /*desired=*/true))
      << "Cgroup should be only cleaned once per process";

  for (const auto &entry : std::filesystem::directory_iterator{cgroup_v2_app_folder_}) {
    // Search for all subcgroup, terminate all dangling processes and delete the empty
    // cgroup folder.
    if (!entry.is_directory()) {
      continue;
    }
    const std::string subcgroup_directory = std::filesystem::absolute(entry.path());
    KillProcsUnderCgroupFolder(subcgroup_directory);
    RAY_CHECK(std::filesystem::remove(subcgroup_directory))
        << "Failed to remove cgroup directory " << subcgroup_directory;
  }

  // Move all system processes into root cgroup and delete system cgroup.
  RAY_CHECK(MoveProcsBetweenCgroups(/*from=*/cgroup_v2_system_folder_,
                                    /*to=*/kRootCgroupProcs.data()))
      << "Failed to move system processes back to root cgroup";
  RAY_CHECK(std::filesystem::remove(cgroup_v2_system_folder_))
      << "Failed to delete raylet system cgroup folder";

  // Cleanup cgroup for current node.
  RAY_CHECK(std::filesystem::remove(cgroup_v2_folder_))
      << "Failed to delete raylet system cgroup folder";
}

ScopedCgroupHandler CgroupSetup::AddSystemProcess(pid_t pid) {
  if (!cgroup_enabled_) {
    return {};
  }
  std::ofstream out_file(cgroup_v2_system_folder_);
  // Able to add memory constraint to the system cgroup.
  out_file << pid;
  RAY_CHECK(out_file.good()) << "Failed to add " << pid << " into cgroup.";

  // Nothing to do at destruction.
  return ScopedCgroupHandler{};
}

ScopedCgroupHandler CgroupSetup::ApplyCgroupForIndividualAppCgroup(
    const AppProcCgroupMetadata &ctx) {
  RAY_CHECK_NE(ctx.max_memory, 0);  // Sanity check.

  // Create a new cgroup folder for the given process.
  const std::string cgroup_folder = ray::JoinPaths(cgroup_v2_app_folder_, ctx.id);
  // Create cgroup folder.
  const std::string default_cgroup_folder =
      ray::JoinPaths(cgroup_v2_app_folder_, "default");
  RAY_CHECK(std::filesystem::create_directory(default_cgroup_folder))
      << "Failed to create cgroup " << default_cgroup_folder;

  const std::string cgroup_proc_file = ray::JoinPaths(cgroup_folder, "cgroup.procs");
  std::ofstream out_file(cgroup_proc_file);
  out_file << ctx.pid;
  RAY_CHECK(out_file.good()) << "Failed to add process " << ctx.pid << " with max memory "
                             << ctx.max_memory << " into cgroup folder";

  auto delete_cgroup_folder = [folder = default_cgroup_folder]() {
    RAY_CHECK(std::filesystem::remove(folder));  // Not expected to fail.
  };
  return ScopedCgroupHandler{std::move(delete_cgroup_folder)};
}

// TODO(hjiang): Make a few constants for cgroup.
ScopedCgroupHandler CgroupSetup::ApplyCgroupForDefaultAppCgroup(
    const AppProcCgroupMetadata &ctx) {
  RAY_CHECK_EQ(ctx.max_memory, 0);  // Sanity check.

  const std::string default_cgroup_folder =
      ray::JoinPaths(cgroup_v2_app_folder_, "default");
  const std::string default_cgroup_proc_file =
      ray::JoinPaths(default_cgroup_folder, "cgroup.procs");

  std::ofstream out_file(default_cgroup_proc_file);
  out_file << ctx.pid;
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
