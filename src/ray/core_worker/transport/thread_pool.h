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

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/thread.hpp>
#include <list>
#include <queue>
#include <set>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/task/task_spec.h"

namespace ray {
namespace core {

/// Wraps a thread-pool to block posts until the pool has free slots. This is used
/// by the SchedulingQueue to provide backpressure to clients.
class BoundedExecutor {
 public:
  static bool NeedDefaultExecutor(int32_t max_concurrency_in_default_group) {
    //  Threaded actor mode only need a default executor when max_concurrency > 1.
    return max_concurrency_in_default_group > 1;
  }

  explicit BoundedExecutor(int max_concurrency);

  /// Posts work to the pool
  void Post(std::function<void()> fn) { boost::asio::post(pool_, std::move(fn)); }

  /// Stop the thread pool.
  void Stop();

  /// Join the thread pool.
  void Join();

 private:
  /// The underlying thread pool for running tasks.
  boost::asio::thread_pool pool_;
};

}  // namespace core
}  // namespace ray
