// Copyright 2020-2021 The Ray Authors.
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

#include "ray/common/task/task_spec.h"

namespace ray {
namespace core {

/// A manager that manages a set of concurrency group executors, which will perform
/// the methods defined in one concurrency group.
///
/// We will create an executor for every concurrency group and create an executor
/// for the default concurrency group if it's necessary.
/// Note that threaded actor and ascycio actor are the actors with a default concurrency
/// group.
///
/// \param ExecutorType The type of executor to execute tasks in concurrency groups.
template <typename ExecutorType>
class ConcurrencyGroupManager final {
 public:
  explicit ConcurrencyGroupManager(
      const std::vector<ConcurrencyGroup> &concurrency_groups = {},
      const int32_t max_concurrency_for_default_concurrency_group = 1,
      std::function<std::function<void()>()> initialize_thread_callback = nullptr);

  /// Get the corresponding concurrency group executor by the give concurrency group or
  /// function descriptor.
  ///
  /// \param concurrency_group_name The concurrency group name of the executor that to
  /// get.
  /// \param fd The function descriptor that's used to get the corresponding exeutor
  /// if the first parameter concurrency_group_name is not gaven.
  ///
  /// \return Return the corresponding executor of the concurrency group
  /// if concurrency_group_name is given.
  /// Otherwise return the corresponding executor by the given function descriptor.
  std::shared_ptr<ExecutorType> GetExecutor(const std::string &concurrency_group_name,
                                            const ray::FunctionDescriptor &fd);

  /// Initialize the executor for specific language runtime.
  ///
  /// \param executor The executor to be initialized.

  /// \return A function that will be called when destructing the executor.
  std::optional<std::function<void()>> InitializeExecutor(
      std::shared_ptr<ExecutorType> executor);

  /// Get the default executor.
  std::shared_ptr<ExecutorType> GetDefaultExecutor() const;

  /// Stop and join the executors that the this manager owns.
  void Stop();

 private:
  // Map from the name to their corresponding concurrency group executor.
  absl::flat_hash_map<std::string, std::shared_ptr<ExecutorType>> name_to_executor_index_;

  // Map from the FunctionDescriptors to their corresponding concurrency group executor.
  absl::flat_hash_map<std::string, std::shared_ptr<ExecutorType>>
      functions_to_executor_index_;

  // The default concurrency group executor. It's nullptr if its max concurrency is 1.
  std::shared_ptr<ExecutorType> default_executor_ = nullptr;

  // The language-specific callback function that initializes threads.
  std::function<std::function<void()>()> initialize_thread_callback_;

  // A vector of language-specific functions used to release the executors.
  std::vector<std::optional<std::function<void()>>> executor_releasers_;

  friend class ConcurrencyGroupManagerTest;
};

}  // namespace core
}  // namespace ray
