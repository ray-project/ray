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

// Util functions for cgroup related operations.

#pragma once

#include <string>

#include "ray/common/status.h"

namespace ray {

// Kill all processes under the given [cgroup_folder] and wait for all processes
// termination synchronously.
//
// \param cgroup_folder: cgroup folder which contains processes to kill.
Status KillAllProcAndWait(const std::string &cgroup_folder);

}  // namespace ray
