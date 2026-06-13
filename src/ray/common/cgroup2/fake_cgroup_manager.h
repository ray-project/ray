// Copyright 2026 The Ray Authors.
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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/cgroup2/cgroup_manager_interface.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/common/status.h"
#include "ray/common/status_or.h"
#include "ray/util/logging.h"

namespace ray {

/**
 * @brief A test-only fake CgroupManager that stores all cgroup state in a temporary
 * directory on tmpfs. Mocks memory.events and memory.stat
 */
class FakeCgroupManager : public CgroupManagerInterface {
 public:
  explicit FakeCgroupManager(int64_t user_memory_max_bytes,
                             int64_t user_memory_high_bytes)
      : user_memory_max_bytes_(user_memory_max_bytes),
        user_memory_high_bytes_(user_memory_high_bytes) {
    StatusOr<std::unique_ptr<TempDirectory>> temp_dir_or = TempDirectory::Create();
    RAY_CHECK(temp_dir_or.ok()) << temp_dir_or.status().ToString();
    temp_dir_ = std::move(temp_dir_or.value());

    memory_events_file_ =
        std::make_unique<TempFile>(temp_dir_->GetPath() + "/memory.events");
    memory_events_file_->AppendLine("low 0");
    memory_events_file_->AppendLine("high 0");
    memory_events_file_->AppendLine("max 0");
    memory_events_file_->AppendLine("oom 0");
    memory_events_file_->AppendLine("oom_kill 0");

    memory_stat_file_ = std::make_unique<TempFile>(temp_dir_->GetPath() + "/memory.stat");
    memory_stat_file_->AppendLine("anon 0");
    memory_stat_file_->AppendLine("shmem 0");
  }

  Status AddProcessToWorkersCgroup(const std::string &) override { return Status::OK(); }
  Status AddProcessToSystemCgroup(const std::string &) override { return Status::OK(); }

  std::string GetUserCgroupPath() const override { return temp_dir_->GetPath(); }
  std::string GetSystemCgroupPath() const override { return temp_dir_->GetPath(); }

  StatusOr<std::string> GetSystemCgroupConstraintValue(
      const std::string &) const override {
    return Status::IOError("not implemented");
  }

  StatusOr<std::string> GetUserCgroupConstraintValue(
      const std::string &constraint_name) const override {
    if (constraint_name == "memory.max") {
      return std::to_string(user_memory_max_bytes_);
    }
    if (constraint_name == "memory.high") {
      return std::to_string(user_memory_high_bytes_);
    }
    return Status::IOError("constraint not found: " + constraint_name);
  }

  const std::string &GetPath() const { return temp_dir_->GetPath(); }

 private:
  std::unique_ptr<TempDirectory> temp_dir_;
  std::unique_ptr<TempFile> memory_events_file_;
  std::unique_ptr<TempFile> memory_stat_file_;
  int64_t user_memory_max_bytes_;
  int64_t user_memory_high_bytes_;
};

}  // namespace ray
