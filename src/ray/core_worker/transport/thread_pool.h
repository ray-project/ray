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

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/chrono.hpp>
#include <functional>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "ray/util/logging.h"

namespace ray {
namespace core {

/// Wraps a thread-pool to block posts until the pool has free slots. This is used
/// by the SchedulingQueue to provide backpressure to clients.
class BoundedExecutor {
 public:
  static bool NeedDefaultExecutor(int32_t max_concurrency_in_default_group,
                                  bool has_other_concurrency_groups) {
    if (max_concurrency_in_default_group == 0) {
      return false;
    }
    return max_concurrency_in_default_group > 1 || has_other_concurrency_groups;
  }

  /// Create a thread pool with `max_concurrency` threads, and execute
  /// `initialize_thread_callback` on each thread. The releaser function returned by the
  /// callback function will be called before the thread is joined.
  ///
  /// \param max_concurrency The maximum number of threads in the pool.
  /// \param initialize_thread_callback The callback function that initializes threads.
  /// \param timeout_ms The timeout for the thread pool to initialize.
  explicit BoundedExecutor(
      int max_concurrency,
      std::function<std::function<void()>()> initialize_thread_callback = nullptr,
      boost::chrono::milliseconds timeout_ms = boost::chrono::milliseconds(10000));

  /// Posts work to the pool. This is a non-blocking call. In addition, the execution
  /// order of the tasks is not guaranteed if there are multiple threads in the pool.
  void Post(std::function<void()> fn);

  /// Stop the thread pool.
  void Stop();

  /// Join the thread pool.
  void Join();

 private:
  boost::asio::io_context io_context_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
  std::vector<std::thread> threads_;
  std::function<std::function<void()>()> initialize_thread_callback_;

  /// A wrapper function that calls the `initialize_thread_callback_` and returns
  /// the releaser function. This should be called on each thread.
  std::function<void()> InitializeThread();
};

}  // namespace core
}  // namespace ray
