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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/cgroup2/scoped_cgroup_operation.h"
#include "ray/common/status_or.h"
#include "ray/util/logging.h"

namespace ray {

CgroupManager::CgroupManager(std::string base_cgroup,
                             const std::string &node_id,
                             std::unique_ptr<CgroupDriverInterface> cgroup_driver)
    : base_cgroup_(std::move(base_cgroup)), cgroup_driver_(std::move(cgroup_driver)) {
  node_cgroup_ = base_cgroup_ + std::filesystem::path::preferred_separator +
                 absl::StrFormat("%s_%s", kNodeCgroupName, node_id);
  system_cgroup_ =
      node_cgroup_ + std::filesystem::path::preferred_separator + kSystemCgroupName;
  system_leaf_cgroup_ =
      system_cgroup_ + std::filesystem::path::preferred_separator + kLeafCgroupName;
  user_cgroup_ =
      node_cgroup_ + std::filesystem::path::preferred_separator + kUserCgroupName;
  workers_cgroup_ =
      user_cgroup_ + std::filesystem::path::preferred_separator + kWorkersCgroupName;
  non_ray_cgroup_ =
      user_cgroup_ + std::filesystem::path::preferred_separator + kNonRayCgroupName;
}

CgroupManager::~CgroupManager() {
  while (!cleanup_operations_.empty()) {
    cleanup_operations_.pop_back();
  }
}

CgroupManager::CgroupManager(CgroupManager &&other)
    : node_cgroup_(std::move(other.node_cgroup_)),
      system_cgroup_(std::move(other.system_cgroup_)),
      system_leaf_cgroup_(std::move(other.system_leaf_cgroup_)),
      user_cgroup_(std::move(other.user_cgroup_)),
      workers_cgroup_(std::move(other.workers_cgroup_)),
      non_ray_cgroup_(std::move(other.non_ray_cgroup_)),
      cleanup_operations_(std::move(other.cleanup_operations_)),
      cgroup_driver_(std::move(other.cgroup_driver_)) {}

CgroupManager &CgroupManager::operator=(CgroupManager &&other) {
  node_cgroup_ = std::move(other.node_cgroup_);
  system_cgroup_ = std::move(other.system_cgroup_);
  system_leaf_cgroup_ = std::move(other.system_leaf_cgroup_);
  user_cgroup_ = std::move(other.user_cgroup_);
  workers_cgroup_ = std::move(other.workers_cgroup_);
  non_ray_cgroup_ = std::move(other.non_ray_cgroup_);
  cleanup_operations_ = std::move(other.cleanup_operations_);
  cgroup_driver_ = std::move(other.cgroup_driver_);
  return *this;
}

StatusOr<std::unique_ptr<CgroupManager>> CgroupManager::Create(
    std::string base_cgroup,
    const std::string &node_id,
    const int64_t system_reserved_cpu_weight,
    const int64_t system_reserved_memory_bytes,
    std::unique_ptr<CgroupDriverInterface> cgroup_driver) {
  if (!cpu_weight_constraint_.IsValid(system_reserved_cpu_weight)) {
    return Status::InvalidArgument(
        absl::StrFormat(" Invalid constraint %s=%d. %s must be in the range [%d, %d].",
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
  RAY_RETURN_NOT_OK(cgroup_driver->CheckCgroup(base_cgroup));
  StatusOr<std::unordered_set<std::string>> available_controllers =
      cgroup_driver->GetAvailableControllers(base_cgroup);

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
          base_cgroup,
          base_cgroup,
          available_controllers_str,
          supported_controllers_str));
    }
  }

  std::unique_ptr<CgroupManager> cgroup_manager = std::unique_ptr<CgroupManager>(
      new CgroupManager(std::move(base_cgroup), node_id, std::move(cgroup_driver)));

  RAY_RETURN_NOT_OK(cgroup_manager->Initialize(system_reserved_cpu_weight,
                                               system_reserved_memory_bytes));

  return cgroup_manager;
}

void CgroupManager::RegisterDeleteCgroup(const std::string &cgroup_path) {
  cleanup_operations_.emplace_back([this, cgroup = cgroup_path]() {
    Status s = this->cgroup_driver_->DeleteCgroup(cgroup);
    if (!s.ok()) {
      RAY_LOG(WARNING) << absl::StrFormat(
          "Failed to delete cgroup %s with error %s.", cgroup, s.ToString());
    }
  });
}

void CgroupManager::RegisterMoveAllProcesses(const std::string &from,
                                             const std::string &to) {
  cleanup_operations_.emplace_back([this, from_cgroup = from, to_cgroup = to]() {
    Status s = this->cgroup_driver_->MoveAllProcesses(from_cgroup, to_cgroup);
    if (!s.ok()) {
      RAY_LOG(WARNING) << absl::StrFormat(
          "Failed to move all processes from %s to %s with error %s",
          from_cgroup,
          to_cgroup,
          s.ToString());
    }
  });
}

template <typename T>
void CgroupManager::RegisterRemoveConstraint(const std::string &cgroup,
                                             const Constraint<T> &constraint) {
  cleanup_operations_.emplace_back(
      [this, constrained_cgroup = cgroup, constraint_to_remove = constraint]() {
        std::string default_value = std::to_string(constraint_to_remove.default_value_);
        Status s = this->cgroup_driver_->AddConstraint(
            constrained_cgroup, constraint_to_remove.name_, default_value);
        if (!s.ok()) {
          RAY_LOG(WARNING) << absl::StrFormat(
              "Failed to set constraint %s=%s to default value for cgroup %s with error "
              "%s.",
              constraint_to_remove.name_,
              default_value,
              constrained_cgroup,
              s.ToString());
        }
      });
}

void CgroupManager::RegisterDisableController(const std::string &cgroup_path,
                                              const std::string &controller) {
  cleanup_operations_.emplace_back(
      [this, cgroup = cgroup_path, controller_to_disable = controller]() {
        Status s = this->cgroup_driver_->DisableController(cgroup, controller_to_disable);
        if (!s.ok()) {
          RAY_LOG(WARNING) << absl::StrFormat(
              "Failed to disable controller %s for cgroup %s with error %s",
              controller_to_disable,
              cgroup,
              s.ToString());
        }
      });
}

Status CgroupManager::Initialize(int64_t system_reserved_cpu_weight,
                                 int64_t system_reserved_memory_bytes) {
  std::string supported_controllers =
      absl::StrCat("[", absl::StrJoin(supported_controllers_, ", "), "]");

  int64_t user_cpu_weight = cpu_weight_constraint_.Max() - system_reserved_cpu_weight;

  RAY_LOG(INFO) << absl::StrFormat(
      "Initializing CgroupManager at base cgroup at '%s'. Ray's cgroup "
      "hierarchy will under the node cgroup at '%s' with %s controllers enabled. "
      "The system cgroup at '%s' will have [memory] controllers enabled with "
      "[%s=%lld, %s=%lld] constraints. "
      "The user cgroup '%s' will have no controllers enabled with [%s=%lld] "
      "constraints. "
      "The user cgroup will contain the [%s, %s] cgroups.",
      base_cgroup_,
      node_cgroup_,
      supported_controllers,
      system_cgroup_,
      cpu_weight_constraint_.name_,
      system_reserved_cpu_weight,
      memory_min_constraint_.name_,
      system_reserved_memory_bytes,
      user_cgroup_,
      cpu_weight_constraint_.name_,
      user_cpu_weight,
      workers_cgroup_,
      non_ray_cgroup_);

  // Create the cgroup hierarchy:
  //     base_cgroup_path (e.g. /sys/fs/cgroup)
  //            |
  //    ray-node_<node_id>
  //   |                 |
  // system             user
  //   |               |    |
  //  leaf        workers  non-ray

  // There need to be leaf cgroups because of the no the internal processes
  // constraint.
  RAY_RETURN_NOT_OK(cgroup_driver_->CreateCgroup(node_cgroup_));
  RegisterDeleteCgroup(node_cgroup_);

  RAY_RETURN_NOT_OK(cgroup_driver_->CreateCgroup(system_cgroup_));
  RegisterDeleteCgroup(system_cgroup_);

  RAY_RETURN_NOT_OK(cgroup_driver_->CreateCgroup(system_leaf_cgroup_));
  RegisterDeleteCgroup(system_leaf_cgroup_);

  RAY_RETURN_NOT_OK(cgroup_driver_->CreateCgroup(user_cgroup_));
  RegisterDeleteCgroup(user_cgroup_);

  RAY_RETURN_NOT_OK(cgroup_driver_->CreateCgroup(workers_cgroup_));
  RegisterDeleteCgroup(workers_cgroup_);

  // Move all processes from the base_cgroup into the system_leaf_cgroup to make sure
  RAY_RETURN_NOT_OK(cgroup_driver_->CreateCgroup(non_ray_cgroup_));
  RegisterDeleteCgroup(non_ray_cgroup_);

  // Move all processes from the base_cgroup into the non-ray cgroup to make sure
  // that the no internal process constraint is not violated. This is relevant
  // when the base_cgroup is not the OS's root cgroup. This is the case when
  // Ray is running inside a container.
  RAY_RETURN_NOT_OK(cgroup_driver_->MoveAllProcesses(base_cgroup_, non_ray_cgroup_));
  RegisterMoveAllProcesses(non_ray_cgroup_, base_cgroup_);

  // NOTE: Since the raylet does not own the lifecycle of all system or worker processes,
  // there's no guarantee that there are no pids in the system leaf or the workers cgroup.
  // Therefore, pids need to be migrated out of the system cgroup to delete it.
  RegisterMoveAllProcesses(system_leaf_cgroup_, base_cgroup_);
  RegisterMoveAllProcesses(workers_cgroup_, base_cgroup_);

  std::array<const std::string *, 2> cpu_controlled_cgroups{&base_cgroup_, &node_cgroup_};
  std::array<const std::string *, 3> memory_controlled_cgroups{
      &base_cgroup_, &node_cgroup_, &system_cgroup_};

  for (const std::string *cpu_controlled_cgroup : cpu_controlled_cgroups) {
    RAY_RETURN_NOT_OK(cgroup_driver_->EnableController(*cpu_controlled_cgroup, "cpu"));
    RegisterDisableController(*cpu_controlled_cgroup, "cpu");
  }

  for (const std::string *memory_controlled_cgroup : memory_controlled_cgroups) {
    RAY_RETURN_NOT_OK(
        cgroup_driver_->EnableController(*memory_controlled_cgroup, "memory"));
    RegisterDisableController(*memory_controlled_cgroup, "memory");
  }

  RAY_RETURN_NOT_OK(
      cgroup_driver_->AddConstraint(system_cgroup_,
                                    cpu_weight_constraint_.name_,
                                    std::to_string(system_reserved_cpu_weight)));
  RegisterRemoveConstraint(system_cgroup_, cpu_weight_constraint_);

  RAY_RETURN_NOT_OK(
      cgroup_driver_->AddConstraint(system_cgroup_,
                                    memory_min_constraint_.name_,
                                    std::to_string(system_reserved_memory_bytes)));
  RegisterRemoveConstraint(system_cgroup_, memory_min_constraint_);

  RAY_RETURN_NOT_OK(cgroup_driver_->AddConstraint(
      user_cgroup_, cpu_weight_constraint_.name_, std::to_string(user_cpu_weight)));
  RegisterRemoveConstraint(user_cgroup_, cpu_weight_constraint_);

  return Status::OK();
}

Status CgroupManager::AddProcessToCgroup(const std::string &cgroup,
                                         const std::string &pid) {
  Status s = cgroup_driver_->AddProcessToCgroup(cgroup, pid);
  // TODO(#54703): Add link to OSS documentation once available.
  RAY_CHECK(!s.IsNotFound())
      << "Failed to move process " << pid << " into cgroup " << cgroup
      << " because the cgroup was not found. If resource isolation is enabled, Ray's "
         "cgroup hierarchy must not be modified while Ray is running.";
  RAY_CHECK(!s.IsPermissionDenied())
      << "Failed to move process " << pid << " into cgroup " << cgroup
      << " because Ray does not have read, write, and execute "
         "permissions for the cgroup. If resource isolation is enabled, Ray's cgroup "
         "hierarchy must not be modified while Ray is running.";
  return s;
}

Status CgroupManager::AddProcessToWorkersCgroup(const std::string &pid) {
  return AddProcessToCgroup(workers_cgroup_, pid);
}

Status CgroupManager::AddProcessToSystemCgroup(const std::string &pid) {
  return AddProcessToCgroup(system_leaf_cgroup_, pid);
}

}  // namespace ray
