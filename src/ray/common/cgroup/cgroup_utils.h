// Copyright 2024 The Ray Authors.
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

// Util functions to setup cgroup.

#pragma once

#include <memory>
#include <string_view>
#include <utility>

#include "ray/common/cgroup/cgroup_context.h"

namespace ray {

// A util class which sets up cgroup at construction, and cleans up at destruction.
// On ctor, creates a cgroup v2 if necessary based on the context. Then puts `ctx.pid`
// into this cgroup. On dtor, puts `ctx.pid` into the default cgroup, and remove this
// cgroup v2 if any.
class CgroupV2Setup {
 public:
  // A failed construction returns nullptr.
  static std::unique_ptr<CgroupV2Setup> New(PhysicalModeExecutionContext ctx);

  ~CgroupV2Setup();

  CgroupV2Setup(const CgroupV2Setup &) = delete;
  CgroupV2Setup &operator=(const CgroupV2Setup &) = delete;
  CgroupV2Setup(CgroupV2Setup &&) = delete;
  CgroupV2Setup &operator=(CgroupV2Setup &&) = delete;

 private:
  CgroupV2Setup(PhysicalModeExecutionContext ctx) : ctx_(std::move(ctx)) {}

  // Setup cgroup based on the given [ctx]. Return whether the setup succeeds or not.
  static bool SetupCgroupV2ForContext(const PhysicalModeExecutionContext &ctx);

  // Cleanup cgroup based on the given [ctx]. Return whether the cleanup succeds or not.
  static bool CleanupCgroupV2ForContext(const PhysicalModeExecutionContext &ctx);

  // Execution context for current cgroup v2 setup.
  PhysicalModeExecutionContext ctx_;
};

}  // namespace ray
