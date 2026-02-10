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

#include <memory>
#include <string>

#include "ray/common/cgroup2/cgroup_manager_interface.h"

namespace ray {

// TODO(54703): Refactor the configs into a struct called CgroupManagerConfig
// and delegate input validation and error messages to it.
class CgroupManagerFactory {
 public:
  /**

    This feature is only enabled in Linux. If using Linux, validates inputs, creates the
    ray's cgroup hierarchy, enables constraints, and moves all system processes into the
    system cgroup.

    On non-Linux platforms, this will return a noop implementation.

    @param enable_resource_isolation if true, will create process isolation with using
    cgroups (@see CgroupManager::Create for more information).
    @param cgroup_path the cgroup that the process will take ownership of.
    @param node_id used to create a unique cgroup subtree per running ray node.
    @param system_reserved_cpu_weight a value between [1,10000] to assign to the cgroup
    for system processes. The cgroup for all other processes (including workers) gets
    10000 - system_reserved_cpu_weight.
    @param system_reserved_memory_bytes used to reserve memory for the system cgroup.
    @param system_pids a comma-separated list of pids of ray system processes to move into
    the system cgroup.

    For more information about the parameters, see @ref CgroupManager::Create.

    @note any of the following is undefined behavior and will cause a RAY_CHECK to fail
      1. enable_resource_isolation is true and either
        a. cgroup_path is empty.
        b. system_reserved_cpu_weight or system_reserved_memory_bytes are -1.
      2. The CgroupManager's precondition checks fail
        a. cgroupv2 is not mounted correctly in unified mode (see @ref
          CgroupDriverInterface::CheckCgroupv2Enabled).
        b. the current process does not adequate permissions (see @ref
          CgroupManager::Create).
        c. supported cgroup controllers are not available (see @ref
          CgroupManager::supported_controllers_).
      3. if a process in system_pids cannot be moved into the system cgroup.
  */
  static std::unique_ptr<CgroupManagerInterface> Create(
      bool enable_resource_isolation,
      std::string cgroup_path,
      const std::string &node_id,
      const int64_t system_reserved_cpu_weight,
      const int64_t system_reserved_memory_bytes,
      const std::string &system_pids);
};
}  // namespace ray
