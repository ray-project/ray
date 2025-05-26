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

// Interface to setup and cleanup node-wise cgroup folder, which is managed by raylet.
//
// It defines a few interfaces to manage cgroup:
// 1. Setup node-wise cgroup folder, and ray system cgroup and application to hold
// ray system components and user application processes.
// 2. Configure cgroup to enable new processes added into cgroup and control on resource
// (i.e. memory).
// 2. Remove ray system component and user application processes out of cgroup managed
// processes.
// 3. Take a cgroup context and add the it into the corresponding cgroup, and return a
// scoped cgroup resource handled for later cleanup.

#pragma once

#include <string>

#include "ray/common/cgroup/cgroup_context.h"
#include "ray/common/cgroup/scoped_cgroup_handle.h"

namespace ray {

class BaseCgroupSetup {
 public:
  BaseCgroupSetup() = default;
  virtual ~BaseCgroupSetup() = default;

  BaseCgroupSetup(const BaseCgroupSetup &) = delete;
  BaseCgroupSetup &operator=(const BaseCgroupSetup &) = delete;

  // Add system process into system cgroup.
  virtual Status AddSystemProcess(pid_t pid) = 0;

  // Apply cgroup context, which adds the process id into the corresponding cgroup.
  virtual ScopedCgroupHandler ApplyCgroupContext(const AppProcCgroupMetadata &ctx) = 0;
};

// A noop cgroup setup class, which does nothing. Used when physical mode is not enabled,
// or fails to enable due to insufficient permission.
class NoopCgroupSetup : public BaseCgroupSetup {
 public:
  NoopCgroupSetup() = default;
  ~NoopCgroupSetup() override = default;

  Status AddSystemProcess(pid_t pid) override { return Status::OK(); }

  ScopedCgroupHandler ApplyCgroupContext(const AppProcCgroupMetadata &ctx) override {
    return {};
  }
};

}  // namespace ray
