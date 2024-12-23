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

#include <string_view>

#include "ray/common/cgroup/cgroup_context.h"

namespace ray {

// There're two types of memory cgroup constraints:
// 1. For those with limit capped, they will be created a dedicated cgroup;
// 2. For those without limit specified, they will be added to the default cgroup.
inline constexpr std::string_view kDefaultCgroupUuid = "default_cgroup_uuid";

// Setup cgroup based on the given [ctx]. Return whether the setup succeeds or not.
bool SetupCgroupForContext(const PhysicalModeExecutionContext &ctx);

// Cleanup cgroup based on the given [ctx]. Return whether the cleanup succeds or not.
bool CleanupCgroupForContext(const PhysicalModeExecutionContext &ctx);

}  // namespace ray
