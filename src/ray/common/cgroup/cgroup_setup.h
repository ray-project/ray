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

#pragma once

#include <string>

namespace ray {

namespace internal {

// Return whether cgroupv2 is properly setup for read-write operations in the given
// [directory]. For example, whether the it's mounted as read-write mode, etc.
//
// This function is exposed in header file for unit test purpose.
//
// \param directory: user provided mounted cgroupv2 directory.
bool IsCgroupV2Prepared(const std::string &directory);

}  // namespace internal

// Util function to setup cgroups preparation for resource constraints.
// It's expected to call from raylet to setup node level cgroup configurations.
//
// If error happens, error will be logged and return false.
// Cgroup is not supported on non-linux platforms.
//
// NOTICE: This function is expected to be called once for eacy raylet instance.
bool SetupCgroupsPreparation(const std::string &node_id);

// Get folder name for application cgroup v2 for current raylet instance.
const std::string &GetCgroupV2AppFolder();

// Get folder name for system cgroup v2 for current raylet instance.
const std::string &GetCgroupV2SystemFolder();

}  // namespace ray
