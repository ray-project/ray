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
#include <utility>

#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/cgroup2/cgroup_manager.h"
#include "ray/common/status_or.h"

namespace ray {

CgroupManager::CgroupManager(std::string base_cgroup_path,
                             const std::string &node_id,
                             std::unique_ptr<CgroupDriverInterface> cgroup_driver) {}

CgroupManager::~CgroupManager() {}

StatusOr<std::unique_ptr<CgroupManager>> CgroupManager::Create(
    std::string base_cgroup_path,
    const std::string &node_id,
    const int64_t system_reserved_cpu_weight,
    const int64_t system_reserved_memory_bytes,
    std::unique_ptr<CgroupDriverInterface> cgroup_driver) {
  return std::unique_ptr<CgroupManager>(
      new CgroupManager(base_cgroup_path, node_id, std::move(cgroup_driver)));
}

Status CgroupManager::AddProcessToSystemCgroup(const std::string &pid) {
  return Status::OK();
}

Status CgroupManager::AddProcessToApplicationCgroup(const std::string &pid) {
  return Status::OK();
}

}  // namespace ray
