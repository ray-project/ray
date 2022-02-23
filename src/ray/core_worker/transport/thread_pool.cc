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

#include "ray/core_worker/transport/thread_pool.h"

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

int32_t BoundedExecutor::GetMaxConcurrency() const { return max_concurrency_; }

/// Stop the thread pool.
void BoundedExecutor::Stop() { pool_.stop(); }

/// Join the thread pool.
void BoundedExecutor::Join() { pool_.join(); }

bool BoundedExecutor::ThreadsAvailable() { return num_running_ < max_concurrency_; }

}  // namespace core
}  // namespace ray
