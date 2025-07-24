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

#include <linux/magic.h>
#include <mntent.h>

#include <string>
#include <unordered_set>

#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

// Used to identify if a filesystem is mounted using cgroupv2.
// See: https://docs.kernel.org/admin-guide/cgroup-v2.html#mounting
#ifndef CGROUP2_SUPER_MAGIC
#define CGROUP2_SUPER_MAGIC = 0x63677270
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

  // The recommended way to mount cgroupv2 only and disable cgroupv1. This will
  // prevent controllers from being migrated between the two. This is what systemd
  // does and recommends. See the following for more information:
  // https://github.com/systemd/systemd/blob/main/docs/CGROUP_DELEGATION.md#hierarchy-and-controller-support
  // https://kubernetes.io/docs/concepts/architecture/cgroups/#linux-distribution-cgroup-v2-support

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

    @return OK if no errors are encounted, otherwise Invalid.
   */
  Status CheckCgroupv2Enabled() override;

  /**
    Checks to see if the cgroup_path is mounted in the cgroupv2 filesystem
    and that the current process has read, write, and execute permissions for
    the directory.

    @param cgroup_path the path of a cgroup directory (e.g. /sys/fs/cgroup/ray)

    @return OK if no errors are encounted. Otherwise, one of the following errors
    NotFound if the cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute permissions.
    InvalidArgument if the cgroup is not using cgroupv2.

    @see The kernel documentation for CGROUP2_SUPER_MAGIC
    https://www.kernel.org/doc/html/v5.4/admin-guide/cgroup-v2.html#mounting
   */
  Status CheckCgroup(const std::string &cgroup_path) override;

  /**
    To create a cgroup using the cgroupv2 vfs, the current user needs to read, write, and
    execute permissions for the parent cgroup. This can be achieved through cgroup
    delegation.

    @see The relevant manpage section on delegation for more details
    https://docs.kernel.org/admin-guide/cgroup-v2.html#delegation

    @param cgroup_path the absolute path of the cgroup directory to create.

    @return OK if no errors are encounted. Otherwise, one of the following errors
    NotFound if an ancestor cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute permissions.
    AlreadyExists if the cgroup already exists.
    */
  Status CreateCgroup(const std::string &cgroup_path) override;

  /**

    Checks to see if cgroup_dir is a valid cgroup, @see SysFsCgroupDriver::CheckCgroup,
    and returns an appropriate error if not.

    Parses a cgroup.controllers file which has a space separated list of all controllers
    available to the cgroup.

    @see For details of the cgroup.controllers file
      https://docs.kernel.org/admin-guide/cgroup-v2.html#enabling-and-disabling.

    @param cgroup_path absolute path of the cgroup.
    @returns OK with a set of controllers if successful, otherwise one of following
    NotFound if the cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute permissions.
    InvalidArgument if the cgroup is not using cgroupv2 or malformed controllers file.
   */
  StatusOr<std::unordered_set<std::string>> GetAvailableControllers(
      const std::string &cgroup_dir) override;

  //
  StatusOr<std::unordered_set<std::string>> GetEnabledControllers(
      const std::string &cgroup_dir) override;

  // to   -
  // from -
  // Fails if to doesn't exist and from doesn't exist
  /**
    Reads the cgroup.procs of from and writes them out to the given file.
    The cgroup.procs file is newline seperated. The current user must have
    read-write permissions to both cgroup.procs file as well as the common ancestor
    of the source and destination cgroups.

    @see The cgroup.procs section for more information
      https://docs.kernel.org/admin-guide/cgroup-v2.html#core-interface-files
    @return InvalidArgument if process files could not be opened, read from, or written to
    correctly, ok otherwise.
    */
  Status MoveProcesses(const std::string &from, const std::string &to) override;

  /**
      The cgroup.subtree_control file has the list of all currently enabled
      controllers for the cgroup. There are two important caveats:

        1. The no internal process constraint
        2. The controller needs to be enabled through the entiree path to thee cgroup
           constraint.

      @param cgroup_path
      @param controller
    */
  Status EnableController(const std::string &cgroup_path,
                          const std::string &controller) override;

  Status DisableController(const std::string &cgroup_path,
                           const std::string &controller) override;

  /**
    @returns
      InvalidArgument if the cgroup is not valid, or if the constraint file doesn't
      exist.
      InvalidArgument if the constraint is not supported.

   */
  Status AddConstraint(const std::string &cgroup,
                       const std::string &constraint,
                       const std::string &constraint_value) override;

 private:
  // cgroup.subtree_control and cgroup.controllers both have the same format.
  // assumes caller checks the validity of the cgroup
  StatusOr<std::unordered_set<std::string>> ReadControllerFile(
      const std::string &controller_file_path);

  std::string mount_file_path_;
  static constexpr std::string_view kCgroupProcsFilename = "cgroup.procs";
  static constexpr std::string_view kCgroupSubtreeControlFilename =
      "cgroup.subtree_control";
  static constexpr std::string_view kCgroupControllersFilename = "cgroup.controllers";
};
}  // namespace ray
