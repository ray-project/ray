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

#include "ray/core_worker/transport/concurrency_group_manager.h"

#include <optional>

#include "ray/core_worker/fiber.h"
#include "ray/core_worker/transport/thread_pool.h"

namespace ray {
namespace core {

template <typename ExecutorType>
ConcurrencyGroupManager<ExecutorType>::ConcurrencyGroupManager(
    const std::vector<ConcurrencyGroup> &concurrency_groups,
    const int32_t max_concurrency_for_default_concurrency_group,
    std::function<std::function<void()>()> initialize_thread_callback)
    : initialize_thread_callback_(std::move(initialize_thread_callback)) {
  for (auto &group : concurrency_groups) {
    const auto name = group.name;
    const auto max_concurrency = group.max_concurrency;
    auto executor = std::make_shared<ExecutorType>(max_concurrency);
    executor_releasers_.push_back(InitializeExecutor(executor));
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
  if (ExecutorType::NeedDefaultExecutor(max_concurrency_for_default_concurrency_group,
                                        !concurrency_groups.empty())) {
    default_executor_ =
        std::make_shared<ExecutorType>(max_concurrency_for_default_concurrency_group);
    executor_releasers_.push_back(InitializeExecutor(default_executor_));
  }
}

template <typename ExecutorType>
std::shared_ptr<ExecutorType> ConcurrencyGroupManager<ExecutorType>::GetExecutor(
    const std::string &concurrency_group_name, const ray::FunctionDescriptor &fd) {
  if (concurrency_group_name == RayConfig::instance().system_concurrency_group_name() &&
      name_to_executor_index_.find(concurrency_group_name) ==
          name_to_executor_index_.end()) {
    auto executor = std::make_shared<ExecutorType>(1);
    name_to_executor_index_[concurrency_group_name] = executor;
  }

  if (!concurrency_group_name.empty()) {
    auto it = name_to_executor_index_.find(concurrency_group_name);
    /// TODO(qwang): Fail the user task.
    RAY_CHECK(it != name_to_executor_index_.end())
        << "Failed to look up the executor of the given concurrency group "
        << concurrency_group_name << " . It might be that you didn't define "
        << "the concurrency group " << concurrency_group_name;
    return it->second;
  }
  /// Code path of that this task wasn't specified in a concurrency group additionally.
  /// Use the predefined concurrency group.
  if (functions_to_executor_index_.find(fd->ToString()) !=
      functions_to_executor_index_.end()) {
    return functions_to_executor_index_[fd->ToString()];
  }
  return default_executor_;
}

/// Get the default executor.
template <typename ExecutorType>
std::shared_ptr<ExecutorType> ConcurrencyGroupManager<ExecutorType>::GetDefaultExecutor()
    const {
  return default_executor_;
}

template <typename ExecutorType>
std::optional<std::function<void()>>
ConcurrencyGroupManager<ExecutorType>::InitializeExecutor(
    std::shared_ptr<ExecutorType> executor) {
  if (!initialize_thread_callback_) {
    return std::nullopt;
  }

  if constexpr (std::is_same<ExecutorType, BoundedExecutor>::value) {
    std::promise<void> init_promise;
    auto init_future = init_promise.get_future();
    auto initializer = initialize_thread_callback_;
    std::function<void()> releaser;

    executor->Post([&initializer, &init_promise, &releaser]() {
      releaser = initializer();
      init_promise.set_value();
    });

    // Wait for thread initialization to complete before executing any tasks in the
    // executor.
    init_future.wait();

    return [executor, releaser]() {
      std::promise<void> release_promise;
      auto release_future = release_promise.get_future();
      executor->Post([releaser, &release_promise]() {
        releaser();
        release_promise.set_value();
      });
      release_future.wait();
    };
  }
  return std::nullopt;
}

/// Stop and join the executors that the this manager owns.
template <typename ExecutorType>
void ConcurrencyGroupManager<ExecutorType>::Stop() {
  for (const auto &releaser : executor_releasers_) {
    if (releaser.has_value()) {
      releaser.value()();
    }
  }
  if (default_executor_) {
    RAY_LOG(DEBUG) << "Default executor is stopping.";
    default_executor_->Stop();
    RAY_LOG(INFO) << "Default executor is joining. If the 'Default executor is joined.' "
                     "message is not printed after this, the worker is probably "
                     "hanging because the actor task is running an infinite loop.";
    default_executor_->Join();
    RAY_LOG(INFO) << "Default executor is joined.";
  }

  for (const auto &it : name_to_executor_index_) {
    it.second->Stop();
  }
  for (const auto &it : name_to_executor_index_) {
    it.second->Join();
  }
}

template class ConcurrencyGroupManager<FiberState>;
template class ConcurrencyGroupManager<BoundedExecutor>;

}  // namespace core
}  // namespace ray
