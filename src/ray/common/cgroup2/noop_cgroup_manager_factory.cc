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
#include <memory>
#include <string>

#include "ray/common/cgroup2/cgroup_manager_factory.h"
#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/cgroup2/noop_cgroup_manager.h"

namespace ray {

std::unique_ptr<CgroupManagerInterface> CgroupManagerFactory::Create(
    bool enable_resource_isolation,
    std::string cgroup_path,
    const std::string &node_id,
    const int64_t system_reserved_cpu_weight,
    const int64_t system_reserved_memory_bytes,
    const std::string &system_pids) {
  if (enable_resource_isolation) {
    // TODO(54703): Add link to OSS documentation when ready.
    RAY_LOG(WARNING)
        << "Raylet started with --enable_resource_isolation. Resource isolation is only "
           "supported on Linux. This is likey a misconfiguration.";
  }
  return std::make_unique<NoopCgroupManager>();
}
}  // namespace ray
