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

#include <sys/types.h>

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "ray/common/cgroup2/cgroup_driver_interface.h"
#include "ray/common/status_or.h"

namespace ray {

/**
  Sets up resource isolation for a Ray node using cgroup2 using the following
  cgroup hierachy:

    base_cgroup_path (e.g. /sys/fs/cgroup)
            |
    ray_node_<node_id>
      |           |
    system     application
*/
class CgroupManagerInterface {
  // TODO(#54703): It makes more sense for bounds checking to be a function
  // inside the CgroupManager interface.
  // There are two separate concerns here:
  //  1) Which controllers/constraints are supported inside Ray? This should be inside the
  //  CgroupManager.
  //  2) What are allowed values for constraints? This should be inside the CgroupDriver.
  struct Constraint {
    std::pair<int64_t, int64_t> range;
    std::string controller;
    int64_t default_value;

    int64_t Max() const { return range.second; }
    int64_t Min() const { return range.first; }
  };

 public:
  // TODO(#54703): These will be implemented in a later PR to move processes
  // into a cgroup.
  // virtual Status AddApplicationProcess(int) = 0;
  // virtual Status AddSystemProcess(int) = 0;
  virtual ~CgroupManagerInterface() = default;

 protected:
  inline static const std::string kNodeCgroupName = "ray_node";
  inline static const std::string kSystemCgroupName = "system";
  inline static const std::string kApplicationCgroupName = "application";
  inline static const std::string kCPUWeightConstraint = "cpu.weight";
  inline static const std::string kMemoryMinConstraint = "memory.min";

  inline static const std::unordered_map<std::string, Constraint> supported_constraints_ =
      {{kCPUWeightConstraint, {{1, 10000}, "cpu", 100}},
       {
           kMemoryMinConstraint,
           {{0, std::numeric_limits<size_t>::max()}, "memory", 0},
       }};
  inline static const std::unordered_set<std::string> supported_controllers_ = {"cpu",
                                                                                "memory"};
};
}  // namespace ray
