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

#include "ray/common/cgroup2/cgroup_manager.h"

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/cgroup2/scoped_cgroup_operation.h"
#include "ray/common/status_or.h"

namespace ray {

CgroupManager::CgroupManager(std::string base_cgroup_path,
                             const std::string &node_id,
                             std::unique_ptr<CgroupDriverInterface> cgroup_driver)
    : base_cgroup_path_(std::move(base_cgroup_path)),
      cgroup_driver_(std::move(cgroup_driver)) {
  node_cgroup_path_ = base_cgroup_path_ + std::filesystem::path::preferred_separator +
                      absl::StrFormat("%s_%s", kNodeCgroupName, node_id);
  system_cgroup_path_ =
      node_cgroup_path_ + std::filesystem::path::preferred_separator + kSystemCgroupName;

  application_cgroup_path_ = node_cgroup_path_ +
                             std::filesystem::path::preferred_separator +
                             kApplicationCgroupName;
}

CgroupManager::~CgroupManager() {
  while (!cleanup_operations_.empty()) {
    cleanup_operations_.pop_back();
  }
}

StatusOr<std::unique_ptr<CgroupManager>> CgroupManager::Create(
    std::string base_cgroup_path,
    const std::string &node_id,
    const int64_t system_reserved_cpu_weight,
    const int64_t system_reserved_memory_bytes,
    std::unique_ptr<CgroupDriverInterface> cgroup_driver) {
  if (!cpu_weight_constraint_.IsValid(system_reserved_cpu_weight)) {
    return Status::InvalidArgument(
        absl::StrFormat("Invalid constraint %s=%d. %s must be in the range [%d, %d].",
                        cpu_weight_constraint_.name_,
                        system_reserved_cpu_weight,
                        cpu_weight_constraint_.name_,
                        cpu_weight_constraint_.Min(),
                        cpu_weight_constraint_.Max()));
  }
  if (!memory_min_constraint_.IsValid(system_reserved_memory_bytes)) {
    return Status::InvalidArgument(
        absl::StrFormat("Invalid constraint %s=%d. %s must be in the range [%d, %d].",
                        memory_min_constraint_.name_,
                        system_reserved_memory_bytes,
                        memory_min_constraint_.name_,
                        memory_min_constraint_.Min(),
                        memory_min_constraint_.Max()));
  }
  RAY_RETURN_NOT_OK(cgroup_driver->CheckCgroupv2Enabled());
  RAY_RETURN_NOT_OK(cgroup_driver->CheckCgroup(base_cgroup_path));
  StatusOr<std::unordered_set<std::string>> available_controllers =
      cgroup_driver->GetAvailableControllers(base_cgroup_path);

  if (!available_controllers.ok()) {
    return available_controllers.status();
  }

  std::string supported_controllers_str =
      absl::StrCat("[", absl::StrJoin(supported_controllers_, ", "), "]");

  for (const auto &ctrl : supported_controllers_) {
    if (available_controllers->find(ctrl) == available_controllers->end()) {
      std::string available_controllers_str =
          absl::StrCat("[", absl::StrJoin(*available_controllers, ", "), "]");
      return Status::Invalid(absl::StrFormat(
          "Failed to initialize resource isolation "
          "because required controllers are not available in the cgroup %s. "
          "To make controllers available in %s, you need to enable controllers for its "
          "ancestor cgroups. See "
          "https://docs.kernel.org/admin-guide/cgroup-v2.html#controlling-controllers "
          "for more details. Available controllers: %s. Required controllers: "
          "%s.",
          base_cgroup_path,
          base_cgroup_path,
          available_controllers_str,
          supported_controllers_str));
    }
  }

  std::unique_ptr<CgroupManager> cgroup_manager = std::unique_ptr<CgroupManager>(
      new CgroupManager(std::move(base_cgroup_path), node_id, std::move(cgroup_driver)));

  RAY_RETURN_NOT_OK(cgroup_manager->Initialize(system_reserved_cpu_weight,
                                               system_reserved_memory_bytes));

  return cgroup_manager;
}

// TODO(#54703): This is a placeholder for cleanup. This will call
// CgroupDriver::DeleteCgroup.
void CgroupManager::RegisterDeleteCgroup(const std::string &cgroup_path) {
  cleanup_operations_.emplace_back([cgroup = cgroup_path]() {
    RAY_LOG(INFO) << absl::StrFormat("Deleting all cgroup %s.", cgroup);
  });
}

// TODO(#54703): This is a placeholder for cleanup. This will call
// CgroupDriver::MoveAllProcesses.
void CgroupManager::RegisterMoveAllProcesses(const std::string &from,
                                             const std::string &to) {
  cleanup_operations_.emplace_back([from_cgroup = from, to_cgroup = to]() {
    RAY_LOG(INFO) << absl::StrFormat(
        "Moved All Processes from %s to %s.", from_cgroup, to_cgroup);
  });
}

// TODO(#54703): This is a placeholder for cleanup. This will call
// CgroupDriver::AddConstraint(cgroup, constraint, default_value).
template <typename T>
void CgroupManager::RegisterRemoveConstraint(const std::string &cgroup,
                                             const Constraint<T> &constraint) {
  cleanup_operations_.emplace_back(
      [constrained_cgroup = cgroup, constraint_to_remove = constraint]() {
        RAY_LOG(INFO) << absl::StrFormat(
            "Setting constraint %s to default value %lld for cgroup %s",
            constraint_to_remove.name_,
            constraint_to_remove.default_value_,
            constrained_cgroup);
      });
}

// TODO(#54703): This is a placeholder for cleanup. This will call
// CgroupDriver::DisableController.
void CgroupManager::RegisterDisableController(const std::string &cgroup,
                                              const std::string &controller) {
  cleanup_operations_.emplace_back([cgroup_to_clean = cgroup,
                                    controller_to_disable = controller]() {
    RAY_LOG(INFO) << absl::StrFormat(
        "Disabling controller %s for cgroup %s.", controller_to_disable, cgroup_to_clean);
  });
}

Status CgroupManager::Initialize(int64_t system_reserved_cpu_weight,
                                 int64_t system_reserved_memory_bytes) {
  std::string supported_controllers =
      absl::StrCat("[", absl::StrJoin(supported_controllers_, ", "), "]");

  // The cpu.weight is distributed between the system and application cgroups.
  // The application cgroup gets whatever is leftover from the system cgroup.
  int64_t application_cgroup_cpu_weight =
      cpu_weight_constraint_.Max() - system_reserved_cpu_weight;

  RAY_LOG(INFO) << absl::StrFormat(
      "Initializing CgroupManager at base cgroup path at %s. Ray's cgroup "
      "hierarchy will under the node cgroup %s. The %s controllers will be "
      "enabled. "
      "System cgroup %s will have constraints [%s=%lld, %s=%lld]. "
      "Application cgroup %s will have constraints [%s=%lld].",
      base_cgroup_path_,
      node_cgroup_path_,
      supported_controllers,
      system_cgroup_path_,
      cpu_weight_constraint_.name_,
      system_reserved_cpu_weight,
      memory_min_constraint_.name_,
      system_reserved_memory_bytes,
      application_cgroup_path_,
      cpu_weight_constraint_.name_,
      application_cgroup_cpu_weight);

  // Create the cgroup heirarchy:
  //      base_cgroup_path (e.g. /sys/fs/cgroup)
  //             |
  //     ray_node_<node_id>
  //       |           |
  //     system     application
  RAY_RETURN_NOT_OK(cgroup_driver_->CreateCgroup(node_cgroup_path_));
  RegisterDeleteCgroup(node_cgroup_path_);

  RAY_RETURN_NOT_OK(cgroup_driver_->CreateCgroup(system_cgroup_path_));
  RegisterDeleteCgroup(system_cgroup_path_);

  RAY_RETURN_NOT_OK(cgroup_driver_->CreateCgroup(application_cgroup_path_));
  RegisterDeleteCgroup(application_cgroup_path_);

  // Move all processes from the base_cgroup into the system_cgroup to make sure
  // that the no internal process constraint is not violated. This is relevant
  // when the base_cgroup_path is not a root cgroup for the system. This is likely
  // the case if Ray is running inside a container.
  RAY_RETURN_NOT_OK(
      cgroup_driver_->MoveAllProcesses(base_cgroup_path_, system_cgroup_path_));
  RegisterMoveAllProcesses(system_cgroup_path_, base_cgroup_path_);

  for (const auto &ctrl : supported_controllers_) {
    RAY_RETURN_NOT_OK(cgroup_driver_->EnableController(base_cgroup_path_, ctrl));
    RegisterDisableController(base_cgroup_path_, ctrl);
    RAY_RETURN_NOT_OK(cgroup_driver_->EnableController(node_cgroup_path_, ctrl));
    RegisterDisableController(node_cgroup_path_, ctrl);
    RAY_RETURN_NOT_OK(cgroup_driver_->EnableController(system_cgroup_path_, ctrl));
    RegisterDisableController(system_cgroup_path_, ctrl);
    RAY_RETURN_NOT_OK(cgroup_driver_->EnableController(application_cgroup_path_, ctrl));
    RegisterDisableController(application_cgroup_path_, ctrl);
  }

  RAY_RETURN_NOT_OK(
      cgroup_driver_->AddConstraint(system_cgroup_path_,
                                    cpu_weight_constraint_.controller_,
                                    cpu_weight_constraint_.name_,
                                    std::to_string(system_reserved_cpu_weight)));
  RegisterRemoveConstraint(system_cgroup_path_, cpu_weight_constraint_);

  RAY_RETURN_NOT_OK(
      cgroup_driver_->AddConstraint(system_cgroup_path_,
                                    memory_min_constraint_.controller_,
                                    memory_min_constraint_.name_,
                                    std::to_string(system_reserved_memory_bytes)));
  RegisterRemoveConstraint(system_cgroup_path_, memory_min_constraint_);

  RAY_RETURN_NOT_OK(
      cgroup_driver_->AddConstraint(application_cgroup_path_,
                                    cpu_weight_constraint_.controller_,
                                    cpu_weight_constraint_.name_,
                                    std::to_string(application_cgroup_cpu_weight)));
  RegisterRemoveConstraint(application_cgroup_path_, cpu_weight_constraint_);

  return Status::OK();
}
}  // namespace ray
