// Copyright 2017 The Ray Authors.
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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/fiber.h"

namespace ray {
namespace core {

/// The class that manages fiber states for Python asyncio actors.
///
/// We'll create one fiber state for every concurrency group. And
/// create one default fiber state for default concurrency group if
/// necessary.
class FiberStateManager final {
 public:
  explicit FiberStateManager(const std::vector<ConcurrencyGroup> &concurrency_groups = {},
                             const int32_t default_group_max_concurrency = 1000) {
    for (auto &group : concurrency_groups) {
      const auto name = group.name;
      const auto max_concurrency = group.max_concurrency;
      auto fiber = std::make_shared<FiberState>(max_concurrency);
      auto &fds = group.function_descriptors;
      for (auto fd : fds) {
        functions_to_fiber_index_[fd->ToString()] = fiber;
      }
      name_to_fiber_index_[name] = fiber;
    }
    /// Create default fiber state for default concurrency group.
    if (default_group_max_concurrency >= 1) {
      default_fiber_ = std::make_shared<FiberState>(default_group_max_concurrency);
    }
  }

  /// Get the corresponding fiber state by the give concurrency group or function
  /// descriptor.
  ///
  /// Return the corresponding fiber state of the concurrency group
  /// if concurrency_group_name is given.
  /// Otherwise return the corresponding fiber state by the given function descriptor.
  std::shared_ptr<FiberState> GetFiber(const std::string &concurrency_group_name,
                                       ray::FunctionDescriptor fd) {
    if (!concurrency_group_name.empty()) {
      auto it = name_to_fiber_index_.find(concurrency_group_name);
      RAY_CHECK(it != name_to_fiber_index_.end())
          << "Failed to look up the fiber state of the given concurrency group "
          << concurrency_group_name << " . It might be that you didn't define "
          << "the concurrency group " << concurrency_group_name;
      return it->second;
    }

    /// Code path of that this task wasn't specified in a concurrency group addtionally.
    /// Use the predefined concurrency group.
    if (functions_to_fiber_index_.find(fd->ToString()) !=
        functions_to_fiber_index_.end()) {
      return functions_to_fiber_index_[fd->ToString()];
    }
    return default_fiber_;
  }

 private:
  // Map from the name to their corresponding fibers.
  absl::flat_hash_map<std::string, std::shared_ptr<FiberState>> name_to_fiber_index_;

  // Map from the FunctionDescriptors to their corresponding fibers.
  absl::flat_hash_map<std::string, std::shared_ptr<FiberState>> functions_to_fiber_index_;

  // The fiber for default concurrency group. It's nullptr if its max concurrency
  // is 1.
  std::shared_ptr<FiberState> default_fiber_ = nullptr;
};

}  // namespace core
}  // namespace ray
