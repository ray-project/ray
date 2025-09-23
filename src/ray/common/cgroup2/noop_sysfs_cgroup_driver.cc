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

#include <string>
#include <unordered_set>

#include "ray/common/cgroup2/sysfs_cgroup_driver.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {
Status SysFsCgroupDriver::CheckCgroupv2Enabled() { return Status::OK(); }

Status SysFsCgroupDriver::CheckCgroup(const std::string &cgroup_path) {
  return Status::OK();
}

Status SysFsCgroupDriver::CreateCgroup(const std::string &cgroup_path) {
  return Status::OK();
}

Status SysFsCgroupDriver::DeleteCgroup(const std::string &cgroup_path) {
  return Status::OK();
}

StatusOr<std::unordered_set<std::string>> SysFsCgroupDriver::GetAvailableControllers(
    const std::string &cgroup_dir) {
  return std::unordered_set<std::string>{};
}

StatusOr<std::unordered_set<std::string>> SysFsCgroupDriver::GetEnabledControllers(
    const std::string &cgroup_dir) {
  return std::unordered_set<std::string>{};
}

Status SysFsCgroupDriver::MoveAllProcesses(const std::string &from,
                                           const std::string &to) {
  return Status::OK();
}

Status SysFsCgroupDriver::EnableController(const std::string &cgroup_path,
                                           const std::string &controller) {
  return Status::OK();
}

Status SysFsCgroupDriver::DisableController(const std::string &cgroup_path,
                                            const std::string &controller) {
  return Status::OK();
}

Status SysFsCgroupDriver::AddConstraint(const std::string &cgroup_path,
                                        const std::string &controller,
                                        const std::string &constraint,
                                        const std::string &constraint_value) {
  return Status::OK();
}

StatusOr<std::unordered_set<std::string>> SysFsCgroupDriver::ReadControllerFile(
    const std::string &controller_file_path) {
  return std::unordered_set<std::string>{};
}

Status SysFsCgroupDriver::AddProcessToCgroup(const std::string &cgroup,
                                             const std::string &process) {
  return Status::OK();
}

}  // namespace ray
