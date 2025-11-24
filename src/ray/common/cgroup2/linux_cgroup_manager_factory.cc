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
#include <sys/types.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/cgroup2/cgroup_manager.h"
#include "ray/common/cgroup2/cgroup_manager_factory.h"
#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/cgroup2/noop_cgroup_manager.h"
#include "ray/common/cgroup2/sysfs_cgroup_driver.h"

namespace ray {

std::unique_ptr<CgroupManagerInterface> CgroupManagerFactory::Create(
    bool enable_resource_isolation,
    std::string cgroup_path,
    const std::string &node_id,
    const int64_t system_reserved_cpu_weight,
    const int64_t system_reserved_memory_bytes,
    const std::string &system_pids) {
  if (!enable_resource_isolation) {
    return std::make_unique<NoopCgroupManager>();
  }

  RAY_CHECK(!cgroup_path.empty())
      << "Failed to start CgroupManager. If enable_resource_isolation is set to true, "
         "cgroup_path cannot be empty.";

  RAY_CHECK_NE(system_reserved_cpu_weight, -1)
      << "Failed to start CgroupManager. If enable_resource_isolation is set to true, "
         "system_reserved_cpu_weight must be set to a value between [1,10000]";

  RAY_CHECK_NE(system_reserved_memory_bytes, -1)
      << "Failed to start CgroupManager. If enable_resource_isolation is set to true, "
         "system_reserved_memory_bytes must be set to a value > 0";

  StatusOr<std::unique_ptr<CgroupManagerInterface>> cgroup_manager_s =
      CgroupManager::Create(cgroup_path,
                            node_id,
                            system_reserved_cpu_weight,
                            system_reserved_memory_bytes,
                            std::make_unique<SysFsCgroupDriver>());

  RAY_CHECK(cgroup_manager_s.ok()) << absl::StrFormat(
      "Failed to start CgroupManager due to %s.", cgroup_manager_s.ToString());

  std::unique_ptr<CgroupManagerInterface> cgroup_manager =
      std::move(cgroup_manager_s.value());

  std::vector<std::string> system_pids_to_move;
  if (!system_pids.empty()) {
    system_pids_to_move = std::move(absl::StrSplit(system_pids, ","));
  }

  system_pids_to_move.emplace_back(std::to_string(getpid()));

  for (const auto &pid : system_pids_to_move) {
    RAY_CHECK_OK(cgroup_manager->AddProcessToSystemCgroup(pid))
        << absl::StrFormat("Failed to move process with pid %s into system cgroup.", pid);
  }

  return cgroup_manager;
}
}  // namespace ray
