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

// This file defines a few common constants for cgroup usage.

#pragma once

#include <cstdint>
#include <string_view>

#ifdef __linux__
#include <sys/stat.h>
#endif  // __linux__

namespace ray {

// A constant made for cgroup usage, which indicates no memory constraint.
inline constexpr uint64_t kUnlimitedCgroupMemory = 0;
// Default cgroup directory.
inline constexpr std::string_view kCgroupDirectory = "/sys/fs/cgroup";
// Process filename within a cgroup.
inline constexpr std::string_view kProcFilename = "cgroup.procs";
// Filename within cgroup, writing to which is used to kill all processes inside.
inline constexpr std::string_view kProcKillFilename = "cgroup.kill";
// Subtree filename within a cgroup.
inline constexpr std::string_view kSubtreeControlFilename = "cgroup.subtree_control";
// Cgroup type filename.
inline constexpr std::string_view kCgroupTypeFilename = "cgroup.type";
// Owner can read and write.
#ifdef __linux__
inline constexpr mode_t kReadWritePerm = S_IRUSR | S_IWUSR;
#else
// Not used in non-linux platform, so randomly assign a meaningless value.
inline constexpr mode_t kReadWritePerm = static_cast<mode_t>(-1);
#endif  // __linux__

}  // namespace ray
