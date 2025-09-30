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

#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/cgroup2/cgroup_manager.h"
#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/cgroup2/sysfs_cgroup_driver.h"

namespace ray {

class CgroupManagerFactory {
 public:
  static StatusOr<std::unique_ptr<CgroupManagerInterface>> Create(
      std::string base_cgroup,
      const std::string &node_id,
      const int64_t system_reserved_cpu_weight,
      const int64_t system_reserved_memory_bytes);
};
}  // namespace ray
