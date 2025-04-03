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

#include <array>
#include <cstdint>
#include <string_view>

namespace ray {

// A constant made for cgroup usage, which indicates no memory constraint.
inline constexpr uint64_t kUnlimitedMemory = 0;
// Subtree controller filename within a cgroup, which contains enabled controllers for
// children cgroups.
inline constexpr std::string_view kSubtreeControlFilename = "cgroup.subtree_control";
// Required cgroupv2 controllers for ray resource isolation.
inline constexpr std::array<std::string_view, 2> kRequiredControllers = {"memory", "cpu"};

}  // namespace ray
