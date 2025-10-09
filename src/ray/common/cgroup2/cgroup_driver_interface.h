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

#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {

/**
  A utility that can be used to check if cgroupv2 is mounted correctly
  and perform cgroup operations on the system. It supports the memory and cpu controllers
  with the memory.min and cpu.weight constraints respectively.

  @see The cgroupv2 documentation for more details:
  https://docs.kernel.org/admin-guide/cgroup-v2.html
 */
class CgroupDriverInterface {
 public:
  virtual ~CgroupDriverInterface() = default;

  /**
    Checks to see if only cgroupv2 is enabled (known as unified mode) on the system.
    If cgroupv2 is not enabled, or enabled along with cgroupv1, returns Invalid
    with the appropriate error message.

    @see systemd's documentation for more information about unified mode:
    https://github.com/systemd/systemd/blob/main/docs/CGROUP_DELEGATION.md#hierarchy-and-controller-support

    @see K8S documentation on how to enable cgroupv2 and check if it's enabled correctly:
    https://kubernetes.io/docs/concepts/architecture/cgroups/#linux-distribution-cgroup-v2-support

    @return Status::OK if successful,
    @return Status::Invalid if cgroupv2 is not enabled correctly.
    */
  virtual Status CheckCgroupv2Enabled() = 0;

  /**
    Checks that the cgroup is valid. See return values for details of which
    invariants are checked.

    @param cgroup the absolute path of the cgroup.

    @return Status::OK if no errors are encounted. Otherwise, one of the following errors
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2.
   */
  virtual Status CheckCgroup(const std::string &cgroup) = 0;

  /**
    Creates a new cgroup at the specified path.

    Expects all cgroups on the path from root -> the new cgroup to already exist.
    Expects the user to have read, write, and execute privileges to parent cgroup.

    @param cgroup is an absolute path to the cgroup

    @return Status::OK if no errors are encounted.
    @return Status::NotFound if an ancestor cgroup does not exist.
    @return Status::PermissionDenied if the process doesn't have sufficient permissions.
    @return Status::AlreadyExists if the cgroup already exists.
   */
  virtual Status CreateCgroup(const std::string &cgroup) = 0;

  /**
    Deletes the specified cgroup.

    Expects all cgroups from the root -> the specified cgroup to exist.
    Expects the cgroup to have no children.
    Expects the process to have adequate permissions for the parent cgroup.

    @param cgroup is an absolute path to the cgroup

    @return Status::OK if no errors are encounted.
    @return Status::NotFound if an ancestor cgroup does not exist.
    @return Status::PermissionDenied if the process doesn't have sufficient permissions.
   */
  virtual Status DeleteCgroup(const std::string &cgroup) = 0;

  /**
    Move all processes from one cgroup to another. The process must have read, write, and
    execute permissions for both cgroups and their lowest common ancestor.

    @see The relevant section of the cgroup documentation for more details:
    https://docs.kernel.org/admin-guide/cgroup-v2.html#delegation-containment

    @param from the absolute path of the cgroup to migrate processes out of.
    @param to the absolute path of the cgroup to migrate processes into.

    @return Status::OK if no errors are encounted. Otherwise, one of the following errors
    @return Status::NotFound if to or from don't exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::Invalid if any errors occur while reading from and writing to
    cgroups.
   */
  virtual Status MoveAllProcesses(const std::string &from, const std::string &to) = 0;

  /**
    Enables an available controller on a cgroup. A controller can be enabled if the
    1) controller is enabled in the parent of the cgroup.
    2) cgroup has no children i.e. it's a leaf node.

    @param cgroup is an absolute path to the cgroup.
    @param controller is the name of the controller (e.g. "cpu" and not "+cpu")

    @see No Internal Process Constraint for more details:
    https://docs.kernel.org/admin-guide/cgroup-v2.html#no-internal-process-constraint

    @return Status::OK if successful, otherwise one of the following
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions for the cgroup.
    @return Status::InvalidArgument if the controller is not available or if cgroup is not
    a cgroupv2.
    @return Status::Invalid for all other failures.
   */
  virtual Status EnableController(const std::string &cgroup,
                                  const std::string &controller) = 0;

  /**
    Disables an enabled controller in a cgroup. A controller can be disabled if the
    controller is not enabled on a child cgroup.

    @param cgroup is an absolute path to the cgroup.
    @param controller is the name of the controller (e.g. "cpu" and not "-cpu")

    @return Status::OK if successful, otherwise one of the following
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions for the cgroup.
    @return Status::InvalidArgument if the controller is not enabled
    or if cgroup is not a cgroupv2. Status::Invalid for all other failures.
   */
  virtual Status DisableController(const std::string &cgroup,
                                   const std::string &controller) = 0;
  /**
    Adds a resource constraint to the cgroup. To add a constraint
    1) the cgroup must have the relevant controller enabled e.g. memory.min cannot be
    enabled if the memory controller is not enabled.
    2) the constraint must be supported in Ray (@see supported_constraints_).
    3) the constraint value must be in the correct range (@see supported_constraints_).

    @param cgroup is an absolute path to the cgroup.
    @param constraint the name of the constraint.
    @param value the value of the constraint.

    @return Status::OK if successful, otherwise one of the following
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions for the cgroup.
    @return Status::InvalidArgument if the cgroup is not valid or constraint is not
    supported or the value not correct.
   */
  virtual Status AddConstraint(const std::string &cgroup,
                               const std::string &controller,
                               const std::string &constraint,
                               const std::string &value) = 0;
  /**
    Returns a list of controllers that can be enabled on the given cgroup based on
    what is enabled on the parent cgroup.

    @param cgroup absolute path of the cgroup.

    @return Status::OK with a set of controllers if successful, otherwise one of
    following
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2 or malformed
    controllers file.
    */
  virtual StatusOr<std::unordered_set<std::string>> GetAvailableControllers(
      const std::string &cgroup) = 0;

  /**
    Returns a list of controllers enabled on the cgroup.

    @param cgroup absolute path of the cgroup.

    @return Status::OK with a set of controllers if successful, otherwise one of following
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    @return Status::InvalidArgument if the cgroup is not using cgroupv2 or malformed
    controllers file.
    */
  virtual StatusOr<std::unordered_set<std::string>> GetEnabledControllers(
      const std::string &cgroup) = 0;

  /**
    Adds the process to the specified cgroup.

    To move the pid, the process must have read, write, and execute permissions for the
      1) the cgroup the pid is currently in i.e. the source cgroup.
      2) the destination cgroup.
      3) the lowest common ancestor of the source and destination cgroups.

    @param cgroup to move the process into.
    @param pid of the process that will be moved.

    @return Status::OK if the process was moved successfully into the cgroup.
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if process doesn't have read, write, and execute
    permissions for the cgroup.
    @return Status::InvalidArgument if the pid is invalid, does not exist, or any other
    error.
   */
  virtual Status AddProcessToCgroup(const std::string &cgroup,
                                    const std::string &pid) = 0;
};

}  // namespace ray
