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

#include "ray/common/cgroup/fake_cgroup_setup.h"

#include "ray/util/logging.h"

namespace ray {

ScopedCgroupHandler FakeCgroupSetup::AddSystemProcess(pid_t pid) {
  absl::MutexLock lock(&mtx_);
  const bool is_new = system_cgroup_.emplace(pid).second;
  RAY_CHECK(is_new);
  return ScopedCgroupHandler{[this, pid]() { CleanupSystemProcess(pid); }};
}

ScopedCgroupHandler FakeCgroupSetup::ApplyCgroupContext(
    const AppProcCgroupMetadata &ctx) {
  absl::MutexLock lock(&mtx_);
  CgroupFolder cgroup_folder;
  cgroup_folder.max_memory_bytes = ctx.max_memory;
  const auto [_, is_new] = cgroup_to_pids_[std::move(cgroup_folder)].emplace(ctx.pid);
  RAY_CHECK(is_new);
  return ScopedCgroupHandler{[this, ctx = ctx]() { CleanupCgroupContext(ctx); }};
}

void FakeCgroupSetup::CleanupSystemProcess(pid_t pid) {
  absl::MutexLock lock(&mtx_);
  auto iter = system_cgroup_.find(pid);
  RAY_CHECK(iter != system_cgroup_.end())
      << "PID " << pid << " hasn't be added into system cgroup.";
  system_cgroup_.erase(iter);
}

void FakeCgroupSetup::CleanupCgroupContext(const AppProcCgroupMetadata &ctx) {
  absl::MutexLock lock(&mtx_);
  CgroupFolder cgroup_folder;
  cgroup_folder.max_memory_bytes = ctx.max_memory;
  auto ctx_iter = cgroup_to_pids_.find(cgroup_folder);
  RAY_CHECK(ctx_iter != cgroup_to_pids_.end());

  auto &pids = ctx_iter->second;
  auto pid_iter = pids.find(ctx.pid);
  RAY_CHECK(pid_iter != pids.end());

  if (pids.size() == 1) {
    cgroup_to_pids_.erase(ctx_iter);
  } else {
    pids.erase(pid_iter);
  }
}

FakeCgroupSetup::~FakeCgroupSetup() {
  RAY_CHECK(system_cgroup_.empty());
  RAY_CHECK(cgroup_to_pids_.empty());
}

}  // namespace ray
