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
#pragma once

#include <linux/magic.h>
#include <mntent.h>

#include <string>
#include <unordered_set>
#include <utility>

#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

// Used to identify if a filesystem is mounted using cgroupv2.
// See: https://docs.kernel.org/admin-guide/cgroup-v2.html#mounting
#ifndef CGROUP2_SUPER_MAGIC
#define CGROUP2_SUPER_MAGIC 0x63677270
#endif

namespace ray {

/**
 * Peforms cgroupv2 operations using the pseudo filesystem documented
 * here https://docs.kernel.org/admin-guide/cgroup-v2.html#interface-files.
 *
 * Usage:
 *    std::unique_ptr<CgroupDriverInterface> driver =
 *    std::make_unique<SysFsCgroupDriver>();
 *    if (driver->CheckCgroupv2Enabled.ok()) {
 *      // perform operations
 *    }
 */
class SysFsCgroupDriver : public CgroupDriverInterface {
 public:
  /**
   * MOUNTED is defined in mntent.h (and typically refers to /etc/mtab)
   * @see https://www.gnu.org/software/libc/manual/2.24/html_node/Mount-Information.html
   *
   * @param mount_file_path only used for testing.
   */
  explicit SysFsCgroupDriver(std::string mount_file_path = MOUNTED)
      : mount_file_path_(std::move(mount_file_path)) {}

  ~SysFsCgroupDriver() override = default;
  SysFsCgroupDriver(const SysFsCgroupDriver &other) = delete;
  SysFsCgroupDriver(const SysFsCgroupDriver &&other) = delete;
  SysFsCgroupDriver &operator=(const SysFsCgroupDriver &other) = delete;
  SysFsCgroupDriver &operator=(const SysFsCgroupDriver &&other) = delete;

  /**
    The recommended way to mount cgroupv2 is with cgroupv1 disabled. This prevents
    cgroup controllers from being migrated between the two modes. This follows
    the recommendation from systemd and K8S.

    Parses the mount file at /etc/mstab and returns Ok if only cgroupv2 is
    mounted.

    Example Mountfile that is correct:
      /dev/root / ext4 rw,relatime,discard
      /dev/nvme2n1 /home/ubuntu ext4 rw,noatime,discard
      cgroup2 /sys/fs/cgroup cgroup2 rw,nosuid,nodev,noexec,relatime,nsdelegate

    Example Mountfile that is incorrect (both v2 and v1 are mounted):
      /dev/root / ext4 rw,relatime,discard
      /dev/nvme2n1 /home/ubuntu ext4 rw,noatime,discard
      cgroup /sys/fs/cgroup cgroup rw,nosuid,nodev,noexec,relatime,nsdelegate
      cgroup2 /sys/fs/cgroup/unified/ cgroup2 rw,nosuid,nodev,noexec,relatime,nsdelegate

    @return OK if no errors
    @return Status::Invalid if cgroupv2 is not enabled correctly.
  */
  Status CheckCgroupv2Enabled() override;

  /**
    Checks to see if the cgroup_path is mounted in the cgroupv2 filesystem
    and that the current process has read, write, and execute permissions for
    the directory. Uses the CGROUP_SUPER_MAGIC to detect that the filesystem
    is mounted as cgroupv2.

    @param cgroup_path the path of a cgroup directory.

    @see The kernel documentation for CGROUP2_SUPER_MAGIC
    https://www.kernel.org/doc/html/v5.4/admin-guide/cgroup-v2.html#mounting

    @return Status::OK if no errors are encounted.
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2.
   */
  Status CheckCgroup(const std::string &cgroup_path) override;

  /**
    To create a cgroup using the cgroupv2 vfs, the current user needs to read, write, and
    execute permissions for the parent cgroup. This can be achieved through cgroup
    delegation.

    @see The relevant manpage section on delegation for more details
    https://docs.kernel.org/admin-guide/cgroup-v2.html#delegation

    @param cgroup_path the absolute path of the cgroup directory to create.

    @return Status::OK if no errors are encounted.
    @return Status::NotFound if an ancestor cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::AlreadyExists if the cgroup already exists.
    */
  Status CreateCgroup(const std::string &cgroup_path) override;

  /**
    Parses the cgroup.controllers file which has a space separated list of all controllers
    available to the cgroup.

    @see For details of the cgroup.controllers file
      https://docs.kernel.org/admin-guide/cgroup-v2.html#enabling-and-disabling.

    @param cgroup_path absolute path of the cgroup.
    @return Status::OK with a set of controllers if successful.
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2 or malformed
    controllers file.
   */
  StatusOr<std::unordered_set<std::string>> GetAvailableControllers(
      const std::string &cgroup_dir) override;

  /**
    Parses the cgroup.subtree_control file which has a space separated list of all
    controllers enabled in the cgroup.

    @see For details of the cgroup.subtree_control file
      https://docs.kernel.org/admin-guide/cgroup-v2.html#enabling-and-disabling.

    @param cgroup_path absolute path of the cgroup.
    @return Status::OK with a set of controllers if successful.
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2 or if the
    cgroup.subtree_control is malformed.
   */
  StatusOr<std::unordered_set<std::string>> GetEnabledControllers(
      const std::string &cgroup_dir) override;

  /**
    Reads the cgroup.procs of "from" and writes them out to the given file.
    The cgroup.procs file is newline seperated. The current user must have
    read-write permissions to both cgroup.procs file as well as the common ancestor
    of the source and destination cgroups.

    @see The cgroup.procs section for more information
      https://docs.kernel.org/admin-guide/cgroup-v2.html#core-interface-files

    @return Status::OK with if successful.
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2.
    @return Status::Invalid if files could not be opened, read from, or written to
    correctly.
    */
  Status MoveAllProcesses(const std::string &from, const std::string &to) override;

  /**
    Enables a controller by writing to the cgroup.subtree_control file. This can
    only happen if

    1. The controller is not enabled in the parent see cgroup.
    2. The cgroup is not a leaf node i.e. it has children. This is called the no internal
    process constraint

    @see the cgroup documentation for the cgroup.subtree_control file
    https://docs.kernel.org/admin-guide/cgroup-v2.html#controlling-controllers

    @param cgroup_path absolute path of the cgroup.
    @param controller name of the controller i.e. "cpu" or "memory" from
    @ref CgroupDriverInterface::supported_controllers_ "supported controllers".

    @return Status::OK if successful
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2, if the controller
    is not available i.e not enabled on the parent.
    @return Status::Invalid if cannot open or write to cgroup.subtree_control.
    */
  Status EnableController(const std::string &cgroup_path,
                          const std::string &controller) override;

  /**
    Disables a controller by writing to the cgroup.subtree_control file. This can
    only happen if the controller is not enabled in child cgroups.

    @see the cgroup documentation for the cgroup.subtree_control file
    https://docs.kernel.org/admin-guide/cgroup-v2.html#controlling-controllers

    @param cgroup_path absolute path of the cgroup.
    @param controller name of the controller i.e. "cpu" or "memory" from
    @ref CgroupDriverInterface::supported_controllers_ "supported controllers".

    @return Status::OK if successful.
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2, if the controller
    is not available i.e not enabled on the parent.
    @return Status::Invalid if cannot open or write to cgroup.subtree_control.
    */
  Status DisableController(const std::string &cgroup_path,
                           const std::string &controller) override;

  /**
    Adds a constraint to the respective cgroup file. See
    @ref CgroupDriverInterface::supported_constraints_ "supported constraints" and valid
    values.

    @return Status::OK if no errors are encounted.
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2, the constraint
    is not supported in ray, the constraint value is out of range, or if cannot write
    to the relevant constraint file.
   */
  Status AddConstraint(const std::string &cgroup,
                       const std::string &constraint,
                       const std::string &constraint_value) override;

 private:
  /**
    @param controller_file_path the absolute path of the controller file to read which is
    one of cgroup.subtree_control or cgroup.controllers.

    @return Status::OK with a list of controllers in the file.
    @return Status::InvalidArgument if failed to read file or file was malformed.
   */
  StatusOr<std::unordered_set<std::string>> ReadControllerFile(
      const std::string &controller_file_path);

  // Used for unit testing through the constructor.
  std::string mount_file_path_;

  static constexpr std::string_view kCgroupProcsFilename = "cgroup.procs";
  static constexpr std::string_view kCgroupSubtreeControlFilename =
      "cgroup.subtree_control";
  static constexpr std::string_view kCgroupControllersFilename = "cgroup.controllers";
};
}  // namespace ray
