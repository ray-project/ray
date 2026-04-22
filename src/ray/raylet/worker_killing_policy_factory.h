// Copyright 2026 The Ray Authors.
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

#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/raylet/worker_killing_policy_interface.h"

namespace ray {

namespace raylet {

class WorkerKillingPolicyFactory {
 public:
  /**
   * Create a worker killing policy instance.
   *
   * @param resource_isolation_enabled Whether resource isolation is enabled.
   *        Used to determine if the threshold the killing policy will kill to will
   *        be based on the cgroup memory constraints.
   * @param cgroup_manager The cgroup manager to use to get the memory constraints.
   * @return a unique pointer to the worker killing policy instance.
   */
  static std::unique_ptr<WorkerKillingPolicyInterface> Create(
      bool resource_isolation_enabled, const CgroupManagerInterface &cgroup_manager);
};

}  // namespace raylet

}  // namespace ray
