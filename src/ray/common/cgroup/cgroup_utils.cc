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

#include "ray/common/cgroup/cgroup_utils.h"

#include <fstream>

#include "absl/strings/str_format.h"

namespace ray {

Status KillAllProc(const std::string &cgroup_folder) {
  const std::string kill_proc_file = absl::StrFormat("%s/cgroup.kill", cgroup_folder);
  std::ofstream f{kill_proc_file, std::ios::app | std::ios::out};
  f << "1";
  f.flush();
  if (!f.good()) {
    return Status(StatusCode::Invalid, /*msg=*/"", RAY_LOC())
           << "Fails to kill all processes under the cgroup " << cgroup_folder;
  }
  return Status::OK();
}

}  // namespace ray
