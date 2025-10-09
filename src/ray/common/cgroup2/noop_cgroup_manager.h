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
#pragma once

#include <memory>
#include <string>

#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {
class NoopCgroupManager : public CgroupManagerInterface {
 public:
  // Uncopyable type.
  NoopCgroupManager() = default;
  explicit NoopCgroupManager(const NoopCgroupManager &) = delete;
  NoopCgroupManager &operator=(const NoopCgroupManager &) = delete;
  explicit NoopCgroupManager(NoopCgroupManager &&) {}
  NoopCgroupManager &operator=(NoopCgroupManager &&) { return *this; }
  ~NoopCgroupManager() = default;

  Status AddProcessToWorkersCgroup(const std::string &pid) override {
    return Status::OK();
  }

  Status AddProcessToSystemCgroup(const std::string &pid) override {
    return Status::OK();
  }
};  // namespace ray
}  // namespace ray
