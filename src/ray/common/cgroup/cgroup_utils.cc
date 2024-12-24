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

#include "ray/common/cgroup/cgroup_utils.h"

#include <sys/stat.h>

#include <fstream>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "ray/util/logging.h"

namespace ray {

namespace {

// Owner can read and write.
constexpr int kCgroupFilePerm = 0600;

// Open a cgroup path and append write [content] into the file.
void OpenCgroupFileAndAppend(std::string_view path, std::string_view content) {
  std::ofstream out_file{path.data(), std::ios::out | std::ios::app};
  out_file << content;
}

bool CreateNewCgroup(const PhysicalModeExecutionContext &ctx) {
  // Sanity check.
  RAY_CHECK(!ctx.uuid.empty());
  RAY_CHECK_NE(ctx.uuid, kDefaultCgroupUuid);
  RAY_CHECK_GT(ctx.max_memory, 0);

  const std::string cgroup_folder =
      absl::StrFormat("%s/%s", ctx.cgroup_directory, ctx.uuid);
  int ret_code = mkdir(cgroup_folder.data(), kCgroupFilePerm);
  if (ret_code != 0) {
    return false;
  }

  if (ctx.max_memory > 0) {
    const std::string procs_path = absl::StrFormat("%s/cgroup.procs", cgroup_folder);
    OpenCgroupFileAndAppend(procs_path, absl::StrFormat("%d", ctx.pid));

    const std::string max_memory_path = absl::StrFormat("%s/memory.max", cgroup_folder);
    OpenCgroupFileAndAppend(max_memory_path, absl::StrFormat("%d", ctx.max_memory));
  }

  return true;
}

bool UpdateDefaultCgroup(const PhysicalModeExecutionContext &ctx) {
  // Sanity check.
  RAY_CHECK(!ctx.uuid.empty());
  RAY_CHECK_EQ(ctx.uuid, kDefaultCgroupUuid);
  RAY_CHECK_EQ(ctx.max_memory, 0);

  const std::string cgroup_folder =
      absl::StrFormat("%s/%s", ctx.cgroup_directory, ctx.uuid);
  int ret_code = mkdir(cgroup_folder.data(), kCgroupFilePerm);
  if (ret_code != 0) {
    return false;
  }

  const std::string procs_path = absl::StrFormat("%s/cgroup.procs", cgroup_folder);
  OpenCgroupFileAndAppend(procs_path, absl::StrFormat("%d", ctx.pid));

  return true;
}

bool DeleteCgroup(const PhysicalModeExecutionContext &ctx) {
  // Sanity check.
  RAY_CHECK(!ctx.uuid.empty());
  RAY_CHECK_NE(ctx.uuid, kDefaultCgroupUuid);
  RAY_CHECK_GT(ctx.max_memory, 0);

  const std::string cgroup_folder =
      absl::StrFormat("%s/%s", ctx.cgroup_directory, ctx.uuid);
  return rmdir(cgroup_folder.data()) == 0;
}

bool RemoveCtxFromDefaultCgroup(const PhysicalModeExecutionContext &ctx) {
  // Sanity check.
  RAY_CHECK(!ctx.uuid.empty());
  RAY_CHECK_EQ(ctx.uuid, kDefaultCgroupUuid);
  RAY_CHECK_EQ(ctx.max_memory, 0);

  const std::string cgroup_folder =
      absl::StrFormat("%s/%s", ctx.cgroup_directory, ctx.uuid);
  int ret_code = mkdir(cgroup_folder.data(), kCgroupFilePerm);
  if (ret_code != 0) {
    return false;
  }

  const std::string procs_path = absl::StrFormat("%s/cgroup.procs", cgroup_folder);
  std::ostringstream buffer;
  {
    std::ifstream file(procs_path.data(), std::ios::in);
    buffer << file.rdbuf();
  }
  std::string content = buffer.str();  // contains all PIDs, separated by space

  std::vector<std::string_view> old_pid_strings = absl::StrSplit(content, ' ');
  std::vector<std::string_view> new_pid_strings;
  new_pid_strings.reserve(old_pid_strings.size() - 1);
  for (const auto &cur_pid : old_pid_strings) {
    if (cur_pid == absl::StrFormat("%d", ctx.pid)) {
      continue;
    }
    new_pid_strings.emplace_back(cur_pid);
  }

  const std::string new_pids = absl::StrJoin(new_pid_strings, " ");
  {
    std::ofstream out_file{procs_path.data(), std::ios::out};
    out_file << new_pids;
  }

  return true;
}

}  // namespace

bool SetupCgroupForContext(const PhysicalModeExecutionContext &ctx) {
#ifndef __linux__
  return false;
#endif

  // Create a new cgroup if max memory specified.
  if (ctx.max_memory > 0) {
    return CreateNewCgroup(ctx);
  }

  // Update default cgroup if no max resource specified.
  return UpdateDefaultCgroup(ctx);
}

bool CleanupCgroupForContext(const PhysicalModeExecutionContext &ctx) {
#ifndef __linux__
  return false;
#endif

  // Delete the dedicated cgroup if max memory specified.
  if (ctx.max_memory > 0) {
    return DeleteCgroup(ctx);
  }

  // Update default cgroup if no max resource specified.
  return RemoveCtxFromDefaultCgroup(ctx);
}

}  // namespace ray
