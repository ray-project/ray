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

#include "ray/core_worker/transport/thread_pool_manager.h"

#include <boost/asio/post.hpp>

namespace ray {
namespace core {

/// Wraps a thread-pool to block posts until the pool has free slots. This is used
/// by the SchedulingQueue to provide backpressure to clients.
BoundedExecutor::BoundedExecutor(int max_concurrency)
    : num_running_(0), max_concurrency_(max_concurrency), pool_(max_concurrency){};

/// Posts work to the pool, blocking if no free threads are available.
void BoundedExecutor::PostBlocking(std::function<void()> fn) {
  mu_.LockWhen(absl::Condition(this, &BoundedExecutor::ThreadsAvailable));
  num_running_ += 1;
  mu_.Unlock();
  boost::asio::post(pool_, [this, fn]() {
    fn();
    absl::MutexLock lock(&mu_);
    num_running_ -= 1;
  });
}

/// Stop the thread pool.
void BoundedExecutor::Stop() { pool_.stop(); }

/// Join the thread pool.
void BoundedExecutor::Join() { pool_.join(); }

bool BoundedExecutor::ThreadsAvailable() { return num_running_ < max_concurrency_; }

PoolManager::PoolManager(const std::vector<ConcurrencyGroup> &concurrency_groups,
                         const int32_t default_group_max_concurrency) {
  for (auto &group : concurrency_groups) {
    const auto name = group.name;
    const auto max_concurrency = group.max_concurrency;
    auto pool = std::make_shared<BoundedExecutor>(max_concurrency);
    auto &fds = group.function_descriptors;
    for (auto fd : fds) {
      functions_to_thread_pool_index_[fd->ToString()] = pool;
    }
    name_to_thread_pool_index_[name] = pool;
  }
  // If max concurrency of default group is 1, the tasks of default group
  // will be performed in main thread instead of any executor pool.
  if (default_group_max_concurrency > 1) {
    default_thread_pool_ =
        std::make_shared<BoundedExecutor>(default_group_max_concurrency);
  }
}

std::shared_ptr<BoundedExecutor> PoolManager::GetPool(
    const std::string &concurrency_group_name, ray::FunctionDescriptor fd) {
  if (!concurrency_group_name.empty()) {
    auto it = name_to_thread_pool_index_.find(concurrency_group_name);
    /// TODO(qwang): Fail the user task.
    RAY_CHECK(it != name_to_thread_pool_index_.end());
    return it->second;
  }

  /// Code path of that this task wasn't specified in a concurrency group addtionally.
  /// Use the predefined concurrency group.
  if (functions_to_thread_pool_index_.find(fd->ToString()) !=
      functions_to_thread_pool_index_.end()) {
    return functions_to_thread_pool_index_[fd->ToString()];
  }
  return default_thread_pool_;
}

/// Stop and join the thread pools that the pool manager owns.
void PoolManager::Stop() {
  if (default_thread_pool_) {
    RAY_LOG(DEBUG) << "Default pool is stopping.";
    default_thread_pool_->Stop();
    RAY_LOG(INFO) << "Default pool is joining. If the 'Default pool is joined.' "
                     "message is not printed after this, the worker is probably "
                     "hanging because the actor task is running an infinite loop.";
    default_thread_pool_->Join();
    RAY_LOG(INFO) << "Default pool is joined.";
  }

  for (const auto &it : name_to_thread_pool_index_) {
    it.second->Stop();
  }
  for (const auto &it : name_to_thread_pool_index_) {
    it.second->Join();
  }
}

}  // namespace core
}  // namespace ray
