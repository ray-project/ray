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

#ifndef __linux__
namespace ray {
Status KillAllProc(const std::string &cgroup_folder) { return Status::OK(); }
}  // namespace ray
#else

#include <sys/wait.h>

#include <fstream>
#include <string>
#include <vector>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"

namespace ray {

namespace {

void GetAllPidsForCgroup(const std::string &cgroup_directory, std::vector<pid_t> *pids) {
  std::ifstream cgroup_proc_file(ray::JoinPaths(cgroup_directory, "cgroup.procs"));
  RAY_CHECK(cgroup_proc_file.good());  // Sanity check.

  std::string pid_str;
  while (std::getline(cgroup_proc_file, pid_str)) {
    pid_t cur_pid = 0;
    RAY_CHECK(absl::SimpleAtoi(pid_str, &cur_pid));  // Sanity check.
    pids->emplace_back(cur_pid);
  }
}

std::vector<pid_t> GetAllPidsForCgroup(const std::string &cgroup_directory) {
  std::vector<pid_t> pids;
  for (const auto &entry :
       std::filesystem::recursive_directory_iterator(cgroup_directory)) {
    if (std::filesystem::is_directory(entry)) {
      GetAllPidsForCgroup(entry.path(), &pids);
    }
  }
  return pids;
}

// Block wait until all provided processes exit.
void BlockWaitProcExit(const std::vector<pid_t> &pids) {
  for (pid_t cur_pid : pids) {
    // Intentionally ignore return value.
    waitpid(cur_pid, /*status=*/nullptr, /*options=*/0);
  }
}

}  // namespace

Status KillAllProc(const std::string &cgroup_folder) {
  const auto existing_pids = GetAllPidsForCgroup(cgroup_folder);

  const std::string kill_proc_file = absl::StrFormat("%s/cgroup.kill", cgroup_folder);
  std::ofstream f{kill_proc_file, std::ios::app | std::ios::out};
  f << "1";
  f.flush();
  if (!f.good()) {
    return Status(StatusCode::Invalid, /*msg=*/"", RAY_LOC())
           << "Fails to kill all processes under the cgroup " << cgroup_folder;
  }

  BlockWaitProcExit(existing_pids);
  return Status::OK();
}

}  // namespace ray

#endif  // __linux__
