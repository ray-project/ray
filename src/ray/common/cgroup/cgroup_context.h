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

#include <cstdint>
#include <string>

#include "ray/util/compat.h"

namespace ray {

// Context used to setup cgroupv2 for a task / actor.
struct AppProcCgroupMetadata {
  // Directory for cgroup, which is applied to application process.
  //
  // TODO(hjiang): Revisit if we could save some CPU/mem with string view.
  std::string cgroup_directory;
  // A unique id to uniquely identity a certain task / actor attempt.
  std::string id;
  // PID for the process.
  pid_t pid;

  // Memory-related spec.
  //
  // Unit: bytes. Corresponds to cgroup V2 `memory.max`, which enforces hard cap on max
  // memory consumption. 0 means no limit.
  uint64_t max_memory = 0;
};

}  // namespace ray
