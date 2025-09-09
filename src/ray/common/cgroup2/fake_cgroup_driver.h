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
  std::vector<std::pair<std::string, std::string>> constraints_;
  std::unordered_set<std::string> available_controllers_;
  std::unordered_set<std::string> enabled_controllers_;
  bool operator==(const FakeCgroup &other) const {
    return path_ == other.path_ && processes_ == other.processes_ &&
           constraints_ == other.constraints_ &&
           available_controllers_ == other.available_controllers_ &&
           enabled_controllers_ == other.enabled_controllers_;
  }
};
class FakeCgroupDriver : public CgroupDriverInterface {
 public:
  explicit FakeCgroupDriver(
      std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups)
      : cgroups_(cgroups) {}

  explicit FakeCgroupDriver(std::string base_cgroup)
      : cgroups_(std::make_shared<std::unordered_map<std::string, FakeCgroup>>()) {
    RAY_LOG(INFO) << "FakeCgroupDriver(std::string base_cgroup)";
    cgroups_->emplace(base_cgroup, FakeCgroup{base_cgroup});
  }
  FakeCgroupDriver(std::string base_cgroup,
                   std::vector<int> processes_in_base_cgroup,
                   std::unordered_set<std::string> available_controllers)
      : cgroups_(std::make_shared<std::unordered_map<std::string, FakeCgroup>>()) {
    cgroups_->emplace(base_cgroup,
                      FakeCgroup{base_cgroup,
                                 std::move(processes_in_base_cgroup),
                                 {},
                                 std::move(available_controllers),
                                 {}});
  }

  std::shared_ptr<std::unordered_map<std::string, FakeCgroup>> cgroups_;

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
    (*cgroups_)[cgroup].constraints_.emplace_back(constraint, value);
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
};

}  // namespace ray
