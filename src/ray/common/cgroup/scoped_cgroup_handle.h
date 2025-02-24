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

// A scoped cgroup handler, which indicates a successful cgroup operation, and
// automatically cleans up the resources at handler's destruction.

#pragma once

#include <functional>

namespace ray {

class ScopedCgroupHandler {
 public:
  ScopedCgroupHandler() = default;
  explicit ScopedCgroupHandler(std::function<void()> cgroup_cleanup)
      : cgroup_cleanup_(std::move(cgroup_cleanup)) {}
  ScopedCgroupHandler(const ScopedCgroupHandler &) = delete;
  ScopedCgroupHandler &operator=(const ScopedCgroupHandler &) = delete;
  ScopedCgroupHandler(ScopedCgroupHandler &&) = default;
  ScopedCgroupHandler &operator=(ScopedCgroupHandler &&) = default;

  ~ScopedCgroupHandler() {
    if (cgroup_cleanup_) {
      cgroup_cleanup_();
    }
  }

 private:
  std::function<void()> cgroup_cleanup_;
};

}  // namespace ray
