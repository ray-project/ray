// Copyright 2025 The Ray Authors.
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

#include "ray/common/cgroup2/sysfs_cgroup_driver.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/magic.h>
#include <mntent.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {
Status SysFsCgroupDriver::CheckCgroupv2Enabled() {
  FILE *fp = setmntent(mount_file_path_.c_str(), "r");

  if (!fp) {
    return Status::Invalid(
        absl::StrFormat("Failed to open mount file at %s. Could not verify that "
                        "cgroupv2 was mounted correctly. \n%s",
                        mount_file_path_,
                        strerror(errno)));
  }

  bool found_cgroupv1 = false;
  bool found_cgroupv2 = false;

  struct mntent *mnt;
  while ((mnt = getmntent(fp)) != nullptr) {
    found_cgroupv1 = found_cgroupv1 || strcmp(mnt->mnt_fsname, "cgroup") == 0;
    found_cgroupv2 = found_cgroupv2 || strcmp(mnt->mnt_fsname, "cgroup2") == 0;
  }

  // After parsing the mount file, the file should be at the EOF position.
  // If it's not, getmntent encountered an error.
  if (!feof(fp) || !endmntent(fp)) {
    return Status::Invalid(
        absl::StrFormat("Failed to parse mount file at %s. Could not verify that "
                        "cgroupv2 was mounted correctly.",
                        mount_file_path_));
  }

  if (found_cgroupv1 && found_cgroupv2) {
    return Status::Invalid("Cgroupv1 and cgroupv2 are both mounted. Unmount cgroupv1.");
  } else if (found_cgroupv1 && !found_cgroupv2) {
    // TODO(#54703): provide a link to the ray documentation once it's been written
    // for how to troubleshoot these.
    return Status::Invalid(
        "Cgroupv1 is mounted and cgroupv2 is not mounted. "
        "Unmount cgroupv1 and mount cgroupv2.");
  } else if (!found_cgroupv2) {
    return Status::Invalid("Cgroupv2 is not mounted. Mount cgroupv2.");
  }
  return Status::OK();
}

Status SysFsCgroupDriver::CheckCgroup(const std::string &cgroup_path) {
  struct statfs fs_stats {};
  if (statfs(cgroup_path.c_str(), &fs_stats) != 0) {
    if (errno == ENOENT) {
      return Status::NotFound(
          absl::StrFormat("Cgroup at %s does not exist.", cgroup_path));
    }
    if (errno == EACCES) {
      return Status::PermissionDenied(
          absl::StrFormat("The current user does not have read, write, and execute "
                          "permissions for the directory at path %s.\n%s",
                          cgroup_path,
                          strerror(errno)));
    }
    return Status::InvalidArgument(
        absl::StrFormat("Failed to stat cgroup directory at path %s because of %s",
                        cgroup_path,
                        strerror(errno)));
  }
  if (fs_stats.f_type != CGROUP2_SUPER_MAGIC) {
    return Status::InvalidArgument(
        absl::StrFormat("Directory at path %s is not of type cgroupv2. "
                        "For instructions to mount cgroupv2 correctly, see:\n"
                        "https://kubernetes.io/docs/concepts/architecture/cgroups/"
                        "#linux-distribution-cgroup-v2-support.",
                        cgroup_path));
  }

  // NOTE: the process needs execute permissions for the cgroup directory
  // to traverse the filesystem.
  if (access(cgroup_path.c_str(), R_OK | W_OK | X_OK) == -1) {
    return Status::PermissionDenied(
        absl::StrFormat("The current user does not have read, write, and execute "
                        "permissions for the directory at path %s.\n%s",
                        cgroup_path,
                        strerror(errno)));
  }

  return Status::OK();
}

Status SysFsCgroupDriver::CreateCgroup(const std::string &cgroup_path) {
  if (mkdir(cgroup_path.c_str(), S_IRWXU) == -1) {
    if (errno == ENOENT) {
      return Status::NotFound(
          absl::StrFormat("Failed to create cgroup at path %s with permissions %#o. "
                          "The parent cgroup does not exist.\n"
                          "Error: %s.",
                          cgroup_path,
                          S_IRWXU,
                          strerror(errno)));
    }
    if (errno == EACCES) {
      return Status::PermissionDenied(absl::StrFormat(
          "Failed to create cgroup at path %s with permissions %#o. "
          "The current user does not have read, write, execute permissions "
          "for the parent cgroup.\n"
          "Error: %s.",
          cgroup_path,
          S_IRWXU,
          strerror(errno)));
    }
    if (errno == EEXIST) {
      return Status::AlreadyExists(
          absl::StrFormat("Failed to create cgroup at path %s with permissions %#o. "
                          "The cgroup already exists.\n"
                          "Error: %s.",
                          cgroup_path,
                          S_IRWXU,
                          strerror(errno)));
    }
    return Status::InvalidArgument(
        absl::StrFormat("Failed to create cgroup at path %s with permissions %#o.\n"
                        "Error: %s.",
                        cgroup_path,
                        S_IRWXU,
                        strerror(errno)));
  }
  return Status::OK();
}

StatusOr<std::unordered_set<std::string>> SysFsCgroupDriver::GetAvailableControllers(
    const std::string &cgroup_dir) {
  RAY_RETURN_NOT_OK(CheckCgroup(cgroup_dir));

  std::string controller_file_path = cgroup_dir +
                                     std::filesystem::path::preferred_separator +
                                     std::string(kCgroupControllersFilename);
  return ReadControllerFile(controller_file_path);
}

StatusOr<std::unordered_set<std::string>> SysFsCgroupDriver::GetEnabledControllers(
    const std::string &cgroup_dir) {
  RAY_RETURN_NOT_OK(CheckCgroup(cgroup_dir));

  std::string controller_file_path = cgroup_dir +
                                     std::filesystem::path::preferred_separator +
                                     std::string(kCgroupSubtreeControlFilename);
  return ReadControllerFile(controller_file_path);
}

Status SysFsCgroupDriver::MoveAllProcesses(const std::string &from,
                                           const std::string &to) {
  RAY_RETURN_NOT_OK(CheckCgroup(from));
  RAY_RETURN_NOT_OK(CheckCgroup(to));
  std::filesystem::path from_procs_file_path =
      from / std::filesystem::path(kCgroupProcsFilename);
  std::filesystem::path to_procs_file_path =
      to / std::filesystem::path(kCgroupProcsFilename);
  std::ifstream in_file(from_procs_file_path);
  std::ofstream out_file(to_procs_file_path, std::ios::ate);
  if (!in_file.is_open()) {
    return Status::Invalid(absl::StrFormat("Could not open cgroup procs file at path %s.",
                                           from_procs_file_path));
  }
  if (!out_file.is_open()) {
    return Status::Invalid(
        absl::StrFormat("Could not open cgroup procs file %s", to_procs_file_path));
  }
  pid_t pid = 0;
  while (in_file >> pid) {
    if (in_file.fail()) {
      return Status::Invalid(absl::StrFormat(
          "Could not read PID from cgroup procs file %s", from_procs_file_path));
    }
    out_file << pid;
    out_file.flush();
    if (out_file.fail()) {
      return Status::Invalid(absl::StrFormat(
          "Could not write pid to cgroup procs file %s", to_procs_file_path));
    }
  }
  return Status::OK();
}

Status SysFsCgroupDriver::EnableController(const std::string &cgroup_path,
                                           const std::string &controller) {
  RAY_RETURN_NOT_OK(CheckCgroup(cgroup_path));

  StatusOr<std::unordered_set<std::string>> available_controllers_s =
      GetAvailableControllers(cgroup_path);

  RAY_RETURN_NOT_OK(available_controllers_s.status());
  auto available_controllers = available_controllers_s.value();

  if (available_controllers.find(controller) == available_controllers.end()) {
    std::string enabled_controllers_str =
        absl::StrCat("[", absl::StrJoin(available_controllers, ", "), "]");
    return Status::InvalidArgument(absl::StrFormat(
        "Controller %s is not available for cgroup at path %s.\n"
        "Current available controllers are %s. "
        "To enable a controller in a cgroup X, all cgroups in the path from "
        "the root cgroup to X must have the controller enabled.",
        controller,
        cgroup_path,
        enabled_controllers_str));
  }

  std::filesystem::path enabled_ctrls_file =
      std::filesystem::path(cgroup_path + std::filesystem::path::preferred_separator +
                            std::string(kCgroupSubtreeControlFilename));
  std::ofstream out_file(enabled_ctrls_file, std::ios::ate);
  if (!out_file.is_open()) {
    return Status::Invalid(absl::StrFormat("Could not open cgroup controllers file at %s",
                                           enabled_ctrls_file));
  }
  out_file << ("+" + controller);
  out_file.flush();
  if (out_file.fail()) {
    return Status::Invalid(absl::StrFormat(
        "Could not open write to cgroup controllers file %s", enabled_ctrls_file));
  }
  return Status::OK();
}

Status SysFsCgroupDriver::DisableController(const std::string &cgroup_path,
                                            const std::string &controller) {
  RAY_RETURN_NOT_OK(CheckCgroup(cgroup_path));
  std::string controller_file_path = cgroup_path +
                                     std::filesystem::path::preferred_separator +
                                     std::string(kCgroupSubtreeControlFilename);

  StatusOr<std::unordered_set<std::string>> enabled_controllers_s =
      ReadControllerFile(controller_file_path);

  RAY_RETURN_NOT_OK(enabled_controllers_s.status());

  auto enabled_controllers = enabled_controllers_s.value();

  if (enabled_controllers.find(controller) == enabled_controllers.end()) {
    std::string enabled_controllers_str =
        absl::StrCat("[", absl::StrJoin(enabled_controllers, ", "), "]");
    return Status::InvalidArgument(
        absl::StrFormat("Controller %s is not enabled for cgroup at path %s.\n"
                        "Current enabled controllers are %s. ",
                        controller,
                        cgroup_path,
                        enabled_controllers_str));
  }

  std::ofstream out_file(controller_file_path, std::ios::ate);
  if (!out_file.is_open()) {
    return Status::Invalid(absl::StrFormat("Could not open cgroup controllers file at %s",
                                           controller_file_path));
  }
  out_file << ("-" + controller);
  out_file.flush();
  if (!out_file.good()) {
    return Status::Invalid(absl::StrFormat(
        "Could not open write to cgroup controllers file %s", controller_file_path));
  }
  return Status::OK();
}

Status SysFsCgroupDriver::AddConstraint(const std::string &cgroup,
                                        const std::string &constraint,
                                        const std::string &constraint_value) {
  RAY_RETURN_NOT_OK(CheckCgroup(cgroup));
  auto constraint_it = supported_constraints_.find(constraint);
  if (constraint_it == supported_constraints_.end()) {
    std::string supported_constraint_names("[");
    for (auto it = supported_constraints_.begin(); it != supported_constraints_.end();
         ++it) {
      supported_constraint_names.append(it->first);
      if (std::next(it) != supported_constraints_.end()) {
        supported_constraint_names.append(", ");
      }
    }
    supported_constraint_names.append("]");
    return Status::InvalidArgument(absl::StrFormat(
        "Failed to apply constraint %s to cgroup %s. Ray only supports %s",
        constraint,
        cgroup,
        supported_constraint_names));
  }

  // Check if the constraint value is out of range and therefore invalid.
  auto [low, high] = constraint_it->second.range;
  size_t value = static_cast<size_t>(std::stoi(constraint_value));
  if (value < low || value > high) {
    return Status::InvalidArgument(absl::StrFormat(
        "Failed to apply constraint %s=%s to cgroup %s. %s can only have values "
        "in the range[%i, %i].",
        constraint,
        constraint_value,
        cgroup,
        constraint,
        low,
        high));
  }

  // Check if the required controller for the constraint is enabled.
  const std::string &controller = constraint_it->second.controller;
  StatusOr<std::unordered_set<std::string>> available_controllers_s =
      GetEnabledControllers(cgroup);
  RAY_RETURN_NOT_OK(available_controllers_s.status());
  const auto &controllers = available_controllers_s.value();
  if (controllers.find(controller) == controllers.end()) {
    return Status::InvalidArgument(absl::StrFormat(
        "Failed to apply %s to cgroup %s. To use %s, enable the %s controller.",
        constraint,
        cgroup,
        constraint,
        controller));
  }

  // Try to apply the constraint and propagate the appropriate failure error.
  std::string file_path =
      cgroup + std::filesystem::path::preferred_separator + constraint;

  int fd = open(file_path.c_str(), O_RDWR);

  if (fd == -1) {
    return Status::InvalidArgument(
        absl::StrFormat("Failed to apply %s=%s to cgroup %s.\n"
                        "Error: %s",
                        constraint,
                        constraint_value,
                        cgroup,
                        strerror(errno)));
  }

  ssize_t bytes_written = write(fd, constraint_value.c_str(), constraint_value.size());

  if (bytes_written != static_cast<ssize_t>(constraint_value.size())) {
    close(fd);
    return Status::InvalidArgument(
        absl::StrFormat("Failed to apply %s=%s to cgroup %s.\n"
                        "Error: %s",
                        constraint,
                        constraint_value,
                        cgroup,
                        strerror(errno)));
  }
  close(fd);
  return Status::OK();
}

StatusOr<std::unordered_set<std::string>> SysFsCgroupDriver::ReadControllerFile(
    const std::string &controller_file_path) {
  std::ifstream controllers_file(controller_file_path);

  if (!controllers_file.is_open()) {
    return Status::InvalidArgument(absl::StrFormat(
        "Failed to open controllers file at path %s.", controller_file_path));
  }

  std::unordered_set<std::string> controllers;

  if (controllers_file.peek() == EOF) {
    return StatusOr<std::unordered_set<std::string>>(controllers);
  }

  std::string line;
  std::getline(controllers_file, line);

  if (!controllers_file.good()) {
    return Status::InvalidArgument(
        absl::StrFormat("Failed to parse controllers file %s.", controller_file_path));
  }

  std::istringstream input_ss(line);
  std::string controller;

  while (input_ss >> controller) {
    controllers.emplace(std::move(controller));
  }

  std::getline(controllers_file, line);

  // A well-formed controllers file should have just one line.
  if (!controllers_file.eof()) {
    return Status::InvalidArgument(
        absl::StrFormat("Failed to parse controllers file %s.", controller_file_path));
  }

  return StatusOr<std::unordered_set<std::string>>(controllers);
}

}  // namespace ray
