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

#ifndef __linux__
namespace ray {
bool CgroupV2Setup::SetupCgroupV2ForContext(const AppProcCgroupMetadata &ctx) {
  return false;
}
/*static*/ bool CgroupV2Setup::CleanupCgroupV2ForContext(
    const AppProcCgroupMetadata &ctx) {
  return false;
}
}  // namespace ray
#else  // __linux__

#include <sys/stat.h>

#include <fstream>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "ray/util/logging.h"

namespace ray {

namespace {

// Owner can read and write.
constexpr int kCgroupV2FilePerm = 0600;

// There're two types of memory cgroup constraints:
// 1. For those with limit capped, they will be created a dedicated cgroup;
// 2. For those without limit specified, they will be added to the default cgroup.
static constexpr std::string_view kDefaultCgroupV2Id = "default_cgroup_id";

// Open a cgroup path and append write [content] into the file.
// Return whether the append operation succeeds.
bool OpenCgroupV2FileAndAppend(std::string_view path, std::string_view content) {
  std::ofstream out_file{path.data(), std::ios::out | std::ios::app};
  out_file << content;
  return out.good();
}

bool CreateNewCgroupV2(const AppProcCgroupMetadata &ctx) {
  // Sanity check.
  RAY_CHECK(!ctx.id.empty());
  RAY_CHECK_NE(ctx.id, kDefaultCgroupV2Id);
  RAY_CHECK_GT(ctx.max_memory, 0);

  const std::string cgroup_folder =
      absl::StrFormat("%s/%s", ctx.cgroup_directory, ctx.id);
  int ret_code = mkdir(cgroup_folder.data(), kCgroupV2FilePerm);
  if (ret_code != 0) {
    return false;
  }

  const std::string procs_path = absl::StrFormat("%s/cgroup.procs", cgroup_folder);
  if (!OpenCgroupV2FileAndAppend(procs_path, absl::StrFormat("%d", ctx.pid))) {
    return false;
  }

  // Add max memory into cgroup.
  const std::string max_memory_path = absl::StrFormat("%s/memory.max", cgroup_folder);
  if (!OpenCgroupV2FileAndAppend(max_memory_path,
                                 absl::StrFormat("%d", ctx.max_memory))) {
    return false;
  }

  return true;
}

bool UpdateDefaultCgroupV2(const AppProcCgroupMetadata &ctx) {
  // Sanity check.
  RAY_CHECK(!ctx.id.empty());
  RAY_CHECK_EQ(ctx.id, kDefaultCgroupV2Id);
  RAY_CHECK_EQ(ctx.max_memory, 0);

  const std::string cgroup_folder =
      absl::StrFormat("%s/%s", ctx.cgroup_directory, ctx.id);
  int ret_code = mkdir(cgroup_folder.data(), kCgroupV2FilePerm);
  if (ret_code != 0) {
    // Special handle "already exists" error, our cgroup implementation makes sure no
    // duplicate cgroup folder will be created.
    RAY_CHECK_NE(errno, EEXIST) << "Ray is not expected to create two same folder "
                                << cgroup_folder << " for cgroup.";
    return false;
  }

  const std::string procs_path = absl::StrFormat("%s/cgroup.procs", cgroup_folder);
  if (!OpenCgroupV2FileAndAppend(procs_path, absl::StrFormat("%d", ctx.pid))) {
    return false;
  }

  return true;
}

bool DeleteCgroupV2(const AppProcCgroupMetadata &ctx) {
  // Sanity check.
  RAY_CHECK(!ctx.id.empty());
  RAY_CHECK_NE(ctx.id, kDefaultCgroupV2Id);
  RAY_CHECK_GT(ctx.max_memory, 0);

  const std::string cgroup_folder =
      absl::StrFormat("%s/%s", ctx.cgroup_directory, ctx.id);
  return rmdir(cgroup_folder.data()) == 0;
}

void PlaceProcessIntoDefaultCgroup(const AppProcCgroupMetadata &ctx) {
  const std::string procs_path =
      absl::StrFormat("%s/%s/cgroup.procs", ctx.cgroup_directory, kDefaultCgroupV2Id);
  {
    std::ofstream out_file{procs_path.data(), std::ios::out};
    out_file << ctx.pid;
  }

  return;
}

}  // namespace

/*static*/ std::unique_ptr<CgroupV2Setup> CgroupV2Setup::New(AppProcCgroupMetadata ctx) {
  if (!CgroupV2Setup::SetupCgroupV2ForContext(ctx)) {
    return nullptr;
  }
  return std::make_unique<CgroupV2Setup>(std::move(ctx));
}

CgroupV2Setup::~CgroupV2Setup() {
  if (!CleanupCgroupV2ForContext(ctx_)) {
    RAY_LOG(ERROR) << "Fails to cleanup cgroup for execution context with id " << ctx_.id;
  }
}

/*static*/ bool CgroupV2Setup::SetupCgroupV2ForContext(const AppProcCgroupMetadata &ctx) {
  // Create a new cgroup if max memory specified.
  if (ctx.max_memory > 0) {
    return CreateNewCgroupV2(ctx);
  }

  // Update default cgroup if no max resource specified.
  return UpdateDefaultCgroupV2(ctx);
}

/*static*/ bool CgroupV2Setup::CleanupCgroupV2ForContext(
    const AppProcCgroupMetadata &ctx) {
  // Delete the dedicated cgroup if max memory specified.
  if (ctx.max_memory > 0) {
    PlaceProcessIntoDefaultCgroup(ctx);
    return DeleteCgroupV2(ctx);
  }

  // If pid already in default cgroup, no action needed.
  return true;
}

}  // namespace ray

#endif  // __linux__
