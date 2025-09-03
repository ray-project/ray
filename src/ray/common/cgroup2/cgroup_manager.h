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

    Factory function that checks the following invariants before constructing
    an instance of CgroupManager:
      1. cgroupv2 is mounted correctly in unified mode. For more details (@see
      CgroupDriverInterface::CheckCgroupv2Enabled).
      2. the current process has permissions to read and write to the base_cgroup.
      3. supported cgroup controllers are available (@see supported_controllers_).

    @param base_cgroup_path the absolute path of the cgroup.
    @param node_id used to construct the ray_node cgroup.
    @param system_reserved_cpu_weight a value between [1,10000] to distribute cpu
    resources between the system and application cgroups. See the cpu.weight section
    in https://docs.kernel.org/admin-guide/cgroup-v2.html#cpu-interface-files
    @param system_reserved_memory_bytes used to reserve memory for the system cgroup.
    See the memory.min section in
    https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files
    @param cgroup_driver used to perform cgroup operations.

    @return Status::OK if all conditions are met.
    @return Status::Invalid if cgroupv2 is not enabled correctly.
    @return Status::NotFound if the cgroup does not exist.
    @return Status::PermissionDenied if current user doesn't have read, write, and
    execute permissions.
    @return Status::InvalidArgument if the base_cgroup_path is not a cgroup path,
    system_reserved_cpu_weight or system_reserved_memory_bytes are invalid.
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
    This will call all clean up functions in the reverse order.
  */
  ~CgroupManager() override;

 private:
  CgroupManager(std::string base_cgroup_path,
                const std::string &node_id,
                std::unique_ptr<CgroupDriverInterface> cgroup_driver);

  /**
    If all checks pass, then it setups up the cgroup hierarchy:
      1. create the base, system, and application cgroups respectively.
      2. move all processes from the base_cgroup into the system cgroup.
      3. enable controllers the base, system, and application cgroups respectively.
      4. add resource constraints to the system and application cgroups.

    If any cgroup operations fail, all of them will be attempted to be rolled back
    in reverse order.
  */
  Status Initialize(const int64_t system_reserved_cpu_weight,
                    const int64_t system_reserved_memory_bytes);

  // TODO(#54703): This is a placeholder for cleanup. This will be implemented in the a
  // future PR.
  void RegisterDeleteCgroup(const std::string &cgroup_path);
  void RegisterMoveAllProcesses(const std::string &from, const std::string &to);
  void RegisterRemoveConstraint(const std::string &cgroup, const std::string &constraint);
  void RegisterDisableController(const std::string &cgroup,
                                 const std::string &controller);

  std::string base_cgroup_path_;
  std::string node_cgroup_path_;
  std::string system_cgroup_path_;
  std::string application_cgroup_path_;
  std::vector<ScopedCgroupOperation> cleanup_operations_;
  std::unique_ptr<CgroupDriverInterface> cgroup_driver_;
};
}  // namespace ray
