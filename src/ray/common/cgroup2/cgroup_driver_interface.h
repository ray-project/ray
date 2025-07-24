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

#include <string>
#include <unordered_set>

#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {

/**
    A utility interface that allows the caller to perform cgroup operations.
    TODO(irabbani): I need to figure out the following
        1. What is the appropriate level of documentation here? This header file is what's
        important by consumers of the API and it needs to provide the correct docstrings
        and API.
        2. Revisiting this API to make it sure it integrates with a possible systemd
        driver implementation (meaning it cannot have any directory based info or any dbus
        implementation).
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

    @return OK if successful, otherwise Invalid.
    */
  virtual Status CheckCgroupv2Enabled() = 0;

  /**
    Checks that the cgroup is valid. See return values for details of which
    invariants are checked.

    @param cgroup the absolute path of the cgroup.

    @return OK if no errors are encounted. Otherwise, one of the following errors
    NotFound if the cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute permissions.
    InvalidArgument if the cgroup is not using cgroupv2.
   */
  virtual Status CheckCgroup(const std::string &cgroup) = 0;

  /**
    Creates a new cgroup at the specified path.
    Expects all cgroups on the path from root -> the new cgroup to already exist.
    Expects the user to have read, write, and execute privileges to parent cgroup.

    @param cgroup is an absolute path to the cgroup

    @return OK if no errors are encounted. Otherwise, one of the following errors
    NotFound if an ancestor cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute permissions.
    AlreadyExists if the cgroup already exists.
   */
  virtual Status CreateCgroup(const std::string &cgroup) = 0;

  /**
    Move all processes from one cgroup to another. The process must have read, write, and
    execute permissions for both cgroups and their lowest common ancestor.

    @see The relevant section of the cgroup documentation for more details:
    https://docs.kernel.org/admin-guide/cgroup-v2.html#delegation-containment

    @param from the absolute path of the cgroup to migrate processes out of.
    @param to the absolute path of the cgroup to migrate processes into.

    @return OK if no errors are encounted. Otherwise, one of the following errors
    NotFound if to or from don't exist.
    PermissionDenied if current user doesn't have read, write, and execute permissions.
    Invalid if any errors occur while reading from and writing to cgroups.
   */
  virtual Status MoveProcesses(const std::string &from, const std::string &to) = 0;

  /**
    Enables an available controller in a cgroup. A controller can be enabled if the
    1) controller is enabled in the parent of the cgroup.
    2) cgroup has no children i.e. it's a leaf node.

    @param cgroup is an absolute path to the cgroup.
    @param controller is the name of the controller (e.g. "cpu" and not "+cpu")

    @see No Internal Process Constraint for more details:
    https://docs.kernel.org/admin-guide/cgroup-v2.html#no-internal-process-constraint

    @return OK if successful, otherwise one of the following
    NotFound if the cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute permissions for
    the cgroup.
    InvalidArgument if the controller is not available or if cgroup is not a cgroupv2.
    Invalid for all other failures.
   */
  virtual Status EnableController(const std::string &cgroup,
                                  const std::string &controller) = 0;

  /**
    Disables an enabled controller in a cgroup. A controller can be disabled if the
    controller is not enabled on a child cgroup.

    @param cgroup is an absolute path to the cgroup.
    @param controller is the name of the controller (e.g. "cpu" and not "-cpu")

    @return OK if successful, otherwise one of the following
    NotFound if the cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute permissions for
    the cgroup.
    InvalidArgument if the controller is not enabled or if cgroup is not a cgroupv2.
    Invalid for all other failures.
   */
  virtual Status DisableController(const std::string &cgroup,
                                   const std::string &controller) = 0;
  /**
    Adds a resource constraint to the cgroup. To add a constraint
    1) the cgroup must have the relevant controller enabled e.g. memory.min cannot be
    enabled if the memory controller is not enabled.
    2) the constraint must be supported in Ray (@see supported_constraints_).
    3) the constraint value must be in the correct range (@see supported_constriants_).

    @param cgroup is an absolute path to the cgroup.
    @param constraint the name of the constraint.
    @param value the value of the constraint.

    @return OK if successful, otherwise one of the following
    NotFound if the cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute permissions for
    the cgroup.
    InvalidArgument if the cgroup is not valid or constraint is not supported or the value
    not correct.
   */
  virtual Status AddConstraint(const std::string &cgroup,
                               const std::string &constraint,
                               const std::string &value) = 0;
  /**
    Returns a list of controllers that can be enabled on the given cgroup based on
    what is enabled on the parent cgroup.

    @param cgroup absolute path of the cgroup.

    @returns OK with a set of controllers if successful, otherwise one of following
    NotFound if the cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    InvalidArgument if the cgroup is not using cgroupv2 or malformed controllers file.
    */
  virtual StatusOr<std::unordered_set<std::string>> GetAvailableControllers(
      const std::string &cgroup) = 0;

  /**
    Returns a list of controllers enabled on the cgroup.

    @param cgroup absolute path of the cgroup.

    @returns OK with a set of controllers if successful, otherwise one of following
    NotFound if the cgroup does not exist.
    PermissionDenied if current user doesn't have read, write, and execute
    permissions.
    InvalidArgument if the cgroup is not using cgroupv2 or malformed controllers file.
    */
  virtual StatusOr<std::unordered_set<std::string>> GetEnabledControllers(
      const std::string &cgroup) = 0;

  struct Constraint {
    std::pair<int, int> range;
    std::string controller;
  };

 protected:
  const std::unordered_map<std::string, Constraint> supported_constraints_ = {
      {"cpu.weight", {{1, 10000}, "cpu"}},
      {"memory.min", {{0, std::numeric_limits<int>::max()}, "memory"}},
  };
  const std::unordered_set<std::string> supported_controllers_ = {"cpu", "memory"};
};
}  // namespace ray
