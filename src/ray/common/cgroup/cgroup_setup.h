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

#include "ray/common/status.h"

namespace ray {

namespace internal {

// Checks whether cgroupv2 is properly mounted for read-write operations in the given
// [directory]. Also checks that cgroupv1 is not mounted.
// If not, InvalidArgument status is returned.
//
// This function is exposed in header file for unit test purpose.
//
// \param directory: user provided mounted cgroupv2 directory.
Status CheckCgroupV2MountedRW(const std::string &directory);

}  // namespace internal

// Util function to initialize cgroupv2 directory for the given [node_id].
// It's expected to call from raylet to setup node level cgroup configurations.
//
// If error happens, error will be logged and return false.
// Cgroup is not supported on non-linux platforms.
//
// \param node_id: node indicator, used to deduce cgroupv2 directory.
//
// NOTE: This function is expected to be called once for each raylet instance.
Status InitializeCgroupv2Directory(const std::string &node_id);

// Get folder name for application cgroup v2 for current raylet instance.
const std::string &GetCgroupV2AppFolder();

// Get folder name for system cgroup v2 for current raylet instance.
const std::string &GetCgroupV2SystemFolder();

}  // namespace ray
