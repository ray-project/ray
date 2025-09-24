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
      |           |
     leaf       leaf
*/
class CgroupManagerInterface {
 public:
  /*
    Moves the process into the application leaf cgroup (@see kLeafCgroupName).

    To move the pid, the process must have read, write, and execute permissions for the
      1) the cgroup the pid is currently in i.e. the source cgroup.
      2) the system leaf cgroup i.e. the destination cgroup.
      3) the lowest common ancestor of the source and destination cgroups.

    TODO(#54703): There currently is not a good way to signal to the caller that
    the method can cause a FATAL error. Revisit this once we've settled on a pattern.

    NOTE: If the process does not have adequate cgroup permissions or the application leaf
    cgroup does not exist, this will fail a RAY_CHECK.

    @param pid of the process to move into the system leaf cgroup.

    @return Status::OK if pid moved successfully.
    @return Status::NotFound if the application cgroup does not exist.
  */
  virtual Status AddProcessToApplicationCgroup(const std::string &pid) = 0;

  /**
    Moves the process into the system leaf cgroup (@see kLeafCgroupName).

    To move the pid, the process must have read, write, and execute permissions for the
      1) the cgroup the pid is currently in i.e. the source cgroup.
      2) the system leaf cgroup i.e. the destination cgroup.
      3) the lowest common ancestor of the source and destination cgroups.

    TODO(#54703): There currently is not a good way to signal to the caller that
    the method can cause a FATAL error. Revisit this once we've settled on a pattern.

    NOTE: If the process does not have adequate cgroup permissions or the system leaf
    cgroup does not exist, this will fail a RAY_CHECK.

    @param pid of the process to move into the system leaf cgroup.

    @return Status::OK if pid moved successfully.
    @return Status::NotFound if the system cgroup does not exist.
  */
  virtual Status AddProcessToSystemCgroup(const std::string &pid) = 0;

  /**
    Cleans up the cgroup hierarchy, disables all controllers and removes all
    constraints.
  */
  virtual ~CgroupManagerInterface() = default;

 protected:
  inline static const std::string kNodeCgroupName = "ray_node";
  inline static const std::string kSystemCgroupName = "system";
  inline static const std::string kApplicationCgroupName = "application";
  inline static const std::string kLeafCgroupName = "leaf";

  // Controllers that can be enabled in Ray.
  inline static const std::unordered_set<std::string> supported_controllers_ = {"cpu",
                                                                                "memory"};
  /**
    Metadata about constraints that can be used.
    @tparam the type of value that the constraint can take.
  */
  template <typename T>
  struct Constraint {
    std::string name_;
    std::string controller_;
    std::pair<T, T> range_;
    T default_value_;
    T Max() const { return range_.second; }
    T Min() const { return range_.first; }
    bool IsValid(T value) const { return value <= Max() && value >= Min(); }
  };

  // cpu.weight distributes a cgroup's cpu cycles between it's children.
  // See https://docs.kernel.org/admin-guide/cgroup-v2.html#cpu-interface-files
  inline static const Constraint<int64_t> cpu_weight_constraint_{
      "cpu.weight", "cpu", {1, 10000}, 100};

  // memory.min guarantees hard memory protection. If the memory usage of a cgroup
  // is within its effective min boundary, the cgroup’s memory won’t be reclaimed under
  // any conditions.
  // See https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files
  inline static const Constraint<int64_t> memory_min_constraint_{
      "memory.min", "memory", {0, std::numeric_limits<int64_t>::max()}, 0};
};
}  // namespace ray
