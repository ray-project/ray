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

// Fake implementation for node-wise cgroup setup, which mimic folder structure in-memory.

#pragma once

#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/cgroup/base_cgroup_setup.h"
#include "ray/common/cgroup/cgroup_context.h"
#include "ray/util/process.h"

namespace ray {

// Use in-memory data structure to mimic filesystem behavior, which is used for unit
// testing.
class FakeCgroupSetup : public BaseCgroupSetup {
 public:
  explicit FakeCgroupSetup(const std::string &node_id /*unused*/) {}
  // Verify system cgroup and application cgroup has been cleaned up.
  ~FakeCgroupSetup() override;

  ScopedCgroupHandler AddSystemProcess(pid_t pid) override;

  ScopedCgroupHandler ApplyCgroupContext(const AppProcCgroupMetadata &ctx) override;

 protected:
  void CleanupSystemProcess(pid_t pid) override;

  void CleanupCgroupContext(const AppProcCgroupMetadata &ctx) override;

 private:
  // TODO(hjiang): For physical mode, as of now we only support max memory, more resource
  // types will be supported in the future.
  struct CgroupFolder {
    // Number of bytes for max memory.
    uint64_t max_memory_bytes = 0;

    template <typename H>
    friend H AbslHashValue(H h, const CgroupFolder &ctx) {
      return H::combine(std::move(h), ctx.max_memory_bytes);
    }
    bool operator==(const CgroupFolder &rhs) const {
      return max_memory_bytes == rhs.max_memory_bytes;
    }
  };

  absl::Mutex mtx_;
  // Stores process id of ray system (i.e. raylet, GCS, etc).
  absl::flat_hash_set<pid_t> system_cgroup_ ABSL_GUARDED_BY(mtx_);
  // Stores process id of application process (aka. user applications).
  // Maps from cgroup folder to its pids.
  absl::flat_hash_map<CgroupFolder, absl::flat_hash_set<pid_t>> cgroup_to_pids_
      ABSL_GUARDED_BY(mtx_);
};

}  // namespace ray
