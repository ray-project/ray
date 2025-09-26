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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/cgroup2/cgroup_manager.h"
#include "ray/common/status.h"

namespace ray {

struct FakeCgroup {
  std::string path_;
  std::vector<int> processes_;
  std::unordered_map<std::string, std::string> constraints_;
  std::unordered_set<std::string> available_controllers_;
  std::unordered_set<std::string> enabled_controllers_;
  bool operator==(const FakeCgroup &other) const {
    return path_ == other.path_ && processes_ == other.processes_ &&
           constraints_ == other.constraints_ &&
           available_controllers_ == other.available_controllers_ &&
           enabled_controllers_ == other.enabled_controllers_;
  }
};

struct FakeConstraint {
  std::string cgroup_;
  std::string name_;
};

struct FakeController {
  std::string cgroup_;
  std::string name_;
};

struct FakeMoveProcesses {
  std::string from_;
  std::string to_;
};

// Intended to be used only in unit tests. This class is not thread-safe.
class FakeCgroupDriver : public CgroupDriverInterface {
 public:
  static std::unique_ptr<FakeCgroupDriver> Create(
      std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups = nullptr,
      std::shared_ptr<std::vector<std::pair<int, std::string>>> deleted_cgroups = nullptr,
      std::shared_ptr<std::vector<std::pair<int, FakeConstraint>>> constraints_disabled =
          nullptr,
      std::shared_ptr<std::vector<std::pair<int, FakeController>>> controllers_disabled =
          nullptr,
      std::shared_ptr<std::vector<std::pair<int, FakeMoveProcesses>>> processes_moved =
          nullptr) {
    if (!cgroups) {
      cgroups = std::make_shared<std::unordered_map<std::string, FakeCgroup>>();
    }
    if (!deleted_cgroups) {
      deleted_cgroups = std::make_shared<std::vector<std::pair<int, std::string>>>();
    }
    if (!constraints_disabled) {
      constraints_disabled =
          std::make_shared<std::vector<std::pair<int, FakeConstraint>>>();
    }
    if (!controllers_disabled) {
      controllers_disabled =
          std::make_shared<std::vector<std::pair<int, FakeController>>>();
    }
    if (!processes_moved) {
      processes_moved =
          std::make_shared<std::vector<std::pair<int, FakeMoveProcesses>>>();
    }
    return std::unique_ptr<FakeCgroupDriver>(new FakeCgroupDriver(cgroups,
                                                                  deleted_cgroups,
                                                                  constraints_disabled,
                                                                  controllers_disabled,
                                                                  processes_moved));
  }

  FakeCgroupDriver(
      std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups,
      std::shared_ptr<std::vector<std::pair<int, std::string>>> deleted_cgroups,
      std::shared_ptr<std::vector<std::pair<int, FakeConstraint>>> constraints_disabled,
      std::shared_ptr<std::vector<std::pair<int, FakeController>>> controllers_disabled,
      std::shared_ptr<std::vector<std::pair<int, FakeMoveProcesses>>> processes_moved)
      : cgroups_(cgroups),
        deleted_cgroups_(deleted_cgroups),
        constraints_disabled_(constraints_disabled),
        controllers_disabled_(controllers_disabled),
        processes_moved_(processes_moved) {}

  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups_;

  // Cgroup cleanup order can be recorded by setting cleanup_mode_ to true.
  bool cleanup_mode_ = false;
  // cleanup_counter_ is incremented with each cleanup operation to capture
  // the order of operations.
  int cleanup_counter_ = 0;
  std::shared_ptr<std::vector<std::pair<int, std::string>>> deleted_cgroups_;
  std::shared_ptr<std::vector<std::pair<int, FakeConstraint>>> constraints_disabled_;
  std::shared_ptr<std::vector<std::pair<int, FakeController>>> controllers_disabled_;
  std::shared_ptr<std::vector<std::pair<int, FakeMoveProcesses>>> processes_moved_;

  Status check_cgroup_enabled_s_ = Status::OK();
  Status check_cgroup_s_ = Status::OK();
  Status create_cgroup_s_ = Status::OK();
  Status delete_cgroup_s_ = Status::OK();
  Status move_all_processes_s_ = Status::OK();
  Status enable_controller_s_ = Status::OK();
  Status disable_controller_s_ = Status::OK();
  Status add_constraint_s_ = Status::OK();
  Status available_controllers_s_ = Status::OK();
  Status enabled_controllers_s_ = Status::OK();
  Status add_process_to_cgroup_s_ = Status::OK();

  // These have no side-effects.
  Status CheckCgroupv2Enabled() override { return check_cgroup_enabled_s_; }
  Status CheckCgroup(const std::string &cgroup) override { return check_cgroup_s_; }

  // These have side-effects made visible through the cgroups_ map.
  // All of them can be short-circuited by setting the corresponding
  // status to not ok.
  Status CreateCgroup(const std::string &cgroup) override {
    if (!create_cgroup_s_.ok()) {
      return create_cgroup_s_;
    }
    cgroups_->emplace(cgroup, FakeCgroup{cgroup});
    return create_cgroup_s_;
  }

  Status DeleteCgroup(const std::string &cgroup) override {
    if (!delete_cgroup_s_.ok()) {
      return delete_cgroup_s_;
    }
    cgroups_->erase(cgroup);
    if (cleanup_mode_) {
      deleted_cgroups_->emplace_back(std::make_pair(++cleanup_counter_, cgroup));
    }
    return delete_cgroup_s_;
  }

  Status MoveAllProcesses(const std::string &from, const std::string &to) override {
    if (!move_all_processes_s_.ok()) {
      return move_all_processes_s_;
    }
    FakeCgroup &from_cgroup = (*cgroups_)[from];
    FakeCgroup &to_cgroup = (*cgroups_)[to];
    while (!from_cgroup.processes_.empty()) {
      to_cgroup.processes_.emplace_back(from_cgroup.processes_.back());
      from_cgroup.processes_.pop_back();
    }
    if (cleanup_mode_) {
      processes_moved_->emplace_back(
          std::make_pair(++cleanup_counter_, FakeMoveProcesses{from, to}));
    }
    return move_all_processes_s_;
  }

  Status EnableController(const std::string &cgroup,
                          const std::string &controller) override {
    if (!enable_controller_s_.ok()) {
      return enable_controller_s_;
    }
    (*cgroups_)[cgroup].enabled_controllers_.emplace(controller);
    return enable_controller_s_;
  }

  Status DisableController(const std::string &cgroup,
                           const std::string &controller) override {
    if (!disable_controller_s_.ok()) {
      return disable_controller_s_;
    }
    if (cleanup_mode_) {
      controllers_disabled_->emplace_back(
          std::make_pair(++cleanup_counter_, FakeController{cgroup, controller}));
    }
    (*cgroups_)[cgroup].enabled_controllers_.erase(controller);
    return disable_controller_s_;
  }

  Status AddConstraint(const std::string &cgroup,
                       const std::string &controller,
                       const std::string &constraint,
                       const std::string &value) override {
    if (!add_constraint_s_.ok()) {
      return add_constraint_s_;
    }
    (*cgroups_)[cgroup].constraints_.emplace(constraint, value);
    if (cleanup_mode_) {
      constraints_disabled_->emplace_back(
          std::make_pair(++cleanup_counter_, FakeConstraint{cgroup, constraint}));
    }
    return add_constraint_s_;
  }

  StatusOr<std::unordered_set<std::string>> GetAvailableControllers(
      const std::string &cgroup) override {
    if (!available_controllers_s_.ok()) {
      return available_controllers_s_;
    }
    return (*cgroups_)[cgroup].available_controllers_;
  }

  StatusOr<std::unordered_set<std::string>> GetEnabledControllers(
      const std::string &cgroup) override {
    if (!enabled_controllers_s_.ok()) {
      return enabled_controllers_s_;
    }
    return (*cgroups_)[cgroup].enabled_controllers_;
  }

  Status AddProcessToCgroup(const std::string &cgroup, const std::string &pid) override {
    return add_process_to_cgroup_s_;
  }
};

}  // namespace ray
