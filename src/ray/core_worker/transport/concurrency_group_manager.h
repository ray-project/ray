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

namespace ray {

namespace core {

/// A manager that manages a set of concurrency group executors, which will perform
/// the methods defined in one concurrency group.
template <typename ConcurrencyGroupExecutorType>
class ConcurrencyGroupManager final {
 public:
  explicit ConcurrencyGroupManager(
      const std::vector<ConcurrencyGroup> &concurrency_groups = {},
      const int32_t max_concurrency_for_default_concurrency_group = 1) {
    for (auto &group : concurrency_groups) {
      const auto name = group.name;
      const auto max_concurrency = group.max_concurrency;
      auto executor = std::make_shared<ConcurrencyGroupExecutorType>(max_concurrency);
      auto &fds = group.function_descriptors;
      for (auto fd : fds) {
        functions_to_executor_index_[fd->ToString()] = executor;
      }
      name_to_executor_index_[name] = executor;
    }

    // If max concurrency of default group is 1 and there is no other concurrency group of
    // this actor, the tasks of default group will be performed in main thread instead of
    // any executor pool, otherwise tasks in any concurrency group should be performed in
    // the thread pools instead of main thread.
    if (ConcurrencyGroupExecutorType::NeedDefaultExecutor(
            max_concurrency_for_default_concurrency_group) ||
        !concurrency_groups.empty()) {
      defatult_executor_ = std::make_shared<ConcurrencyGroupExecutorType>(
          max_concurrency_for_default_concurrency_group);
    }
  }

  /// Get the corresponding concurrency group executor by the give concurrency group or
  /// function descriptor.
  ///
  /// Return the corresponding executor of the concurrency group
  /// if concurrency_group_name is given.
  /// Otherwise return the corresponding executor by the given function descriptor.
  std::shared_ptr<ConcurrencyGroupExecutorType> GetExecutor(
      const std::string &concurrency_group_name, ray::FunctionDescriptor fd) {
    if (!concurrency_group_name.empty()) {
      auto it = name_to_executor_index_.find(concurrency_group_name);
      /// TODO(qwang): Fail the user task.
      RAY_CHECK(it != name_to_executor_index_.end())
          << "Failed to look up the executor of the given concurrency group "
          << concurrency_group_name << " . It might be that you didn't define "
          << "the concurrency group " << concurrency_group_name;
      return it->second;
    }
    /// Code path of that this task wasn't specified in a concurrency group addtionally.
    /// Use the predefined concurrency group.
    if (functions_to_executor_index_.find(fd->ToString()) !=
        functions_to_executor_index_.end()) {
      return functions_to_executor_index_[fd->ToString()];
    }
    return defatult_executor_;
  }

  /// Stop and join the executors that the this manager owns.
  void Stop() {
    if (defatult_executor_) {
      RAY_LOG(DEBUG) << "Default executor is stopping.";
      defatult_executor_->Stop();
      RAY_LOG(INFO)
          << "Default executor is joining. If the 'Default executor is joined.' "
             "message is not printed after this, the worker is probably "
             "hanging because the actor task is running an infinite loop.";
      defatult_executor_->Join();
      RAY_LOG(INFO) << "Default executor is joined.";
    }

    for (const auto &it : name_to_executor_index_) {
      it.second->Stop();
    }
    for (const auto &it : name_to_executor_index_) {
      it.second->Join();
    }
  }

 private:
  // Map from the name to their corresponding concurrency group executor.
  absl::flat_hash_map<std::string, std::shared_ptr<ConcurrencyGroupExecutorType>>
      name_to_executor_index_;

  // Map from the FunctionDescriptors to their corresponding concurrency group executor.
  absl::flat_hash_map<std::string, std::shared_ptr<ConcurrencyGroupExecutorType>>
      functions_to_executor_index_;

  // The default concurrency group executor. It's nullptr if its max concurrency is 1.
  std::shared_ptr<ConcurrencyGroupExecutorType> defatult_executor_ = nullptr;
};

}  // namespace core
}  // namespace ray
