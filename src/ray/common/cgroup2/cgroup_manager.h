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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/cgroup2/scoped_cgroup_operation.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {
class CgroupManager : public CgroupManagerInterface {
 public:
  /**
    Creates a CgroupManager after checking for the following invariants:

    1. cgroupv2 is mounted correctly in unified mode. For more details (@see
    CgroupDriverInterface::CheckCgroupv2Enabled).
    2. the current process has permissions to read and write to the base_cgroup.
    3. supported cgroup controllers are available (@see supported_controllers_).

    The CgroupManager will be used to
    1. construct the cgroup hierarchy.
    2. move processes into the appropriate cgroups.
    3. enable controllers and resource constraints.

    @param base_cgroup the cgroup that the process will take ownership of.
    @param node_id used to create a ray node cgroup.
    @param system_reserved_cpu_weight a value between [1,10000] to assign to the cgroup
    for system processes. The cgroup for application processes gets 10000 -
    system_reserved_cpu_weight.
    @param system_reserved_memory_bytes used to reserve memory for the system cgroup.
    @param cgroup_driver used to perform cgroup operations.

    @return Status::OK with an instance of CgroupManager if everything succeeds.
    @return Status::Invalid if cgroupv2 is not enabled correctly.
    @return Status::InvalidArgument if base_cgroup is not a cgroup.
    @return Status::NotFound if the base_cgroupd does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and
    execute permissions.
   */
  static StatusOr<std::unique_ptr<CgroupManager>> Create(
      std::string base_cgroup_path,
      const std::string &node_id,
      const int64_t system_reserved_cpu_weight,
      const int64_t system_reserved_memory_bytes,
      std::unique_ptr<CgroupDriverInterface> cgroup_driver);

  // Unmovable and uncopyable type.
  CgroupManager(const CgroupManager &) = delete;
  CgroupManager &operator=(const CgroupManager &) = delete;
  CgroupManager(CgroupManager &&) = default;
  CgroupManager &operator=(CgroupManager &&) = default;

  /**
    Performs cleanup in reverse order from the Initialize function:
      1. remove resource constraints to the system and application cgroups.
      2. disable controllers on the base, system, and application cgroups respectively.
      3. move all processes from the system cgroup into the base cgroup.
      4. delete the node, system, and application cgroups respectively.

    Cleanup is best-effort. If any step fails, it will log a warning.
  */
  ~CgroupManager() override;

 private:
  CgroupManager(std::string base_cgroup_path,
                const std::string &node_id,
                std::unique_ptr<CgroupDriverInterface> cgroup_driver);

  /**
    Performs the following operations:

      1. create the node, system, and application cgroups respectively.
      2. move all processes from the base_cgroup into the system cgroup.
      3. enable controllers the base, node, system, and application cgroups respectively.
      4. add resource constraints to the system and application cgroups.

    @param system_reserved_cpu_weight a value between [1,10000] to assign to the cgroup
    for system processes. The cgroup for application processes gets 10000 -
    system_reserved_cpu_weight.
    @param system_reserved_memory_bytes used to reserve memory for the system cgroup.

    @return Status::OK if no errors encountered.
    @return Status::NotFound if base_cgroup does not exist.
    @return Status::PermissionDenied if the process does not have enough permissions
    to create a cgroup or write to it.
    @return Status::Invalid if processes could not be moved between cgroups.
    @return Status::InvalidArgument if base_cgroup_path_ is not a valid cgroup,
    supported_controllers_ cannot be enabled, or a constraint is not supported.
    @return Status::AlreadyExists if the the node, application, or system cgroup already
    exists.

  */
  Status Initialize(const int64_t system_reserved_cpu_weight,
                    const int64_t system_reserved_memory_bytes);

  // TODO(#54703): This is a placeholder for cleanup. This will be implemented in the a
  // future PR.
  void RegisterDeleteCgroup(const std::string &cgroup_path);
  void RegisterMoveAllProcesses(const std::string &from, const std::string &to);
  template <typename T>
  void RegisterRemoveConstraint(const std::string &cgroup,
                                const Constraint<T> &constraint);
  void RegisterDisableController(const std::string &cgroup,
                                 const std::string &controller);

  std::string base_cgroup_path_;
  std::string node_cgroup_path_;
  std::string system_cgroup_path_;
  std::string application_cgroup_path_;

  // This will be popped in reverse order to clean up all side-effects performed
  // during setup.
  std::vector<ScopedCgroupOperation> cleanup_operations_;

  std::unique_ptr<CgroupDriverInterface> cgroup_driver_;
};
}  // namespace ray
