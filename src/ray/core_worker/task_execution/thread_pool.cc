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

#include "ray/core_worker/task_execution/thread_pool.h"

#include <boost/asio/post.hpp>
#include <boost/thread/latch.hpp>
#include <future>
#include <memory>
#include <utility>

namespace ray {
namespace core {

BoundedExecutor::BoundedExecutor(
    int max_concurrency,
    std::function<std::function<void()>()> initialize_thread_callback,
    boost::chrono::milliseconds timeout_ms)
    : work_guard_(boost::asio::make_work_guard(io_context_)),
      initialize_thread_callback_(initialize_thread_callback) {
  RAY_CHECK(max_concurrency > 0) << "max_concurrency must be greater than 0";

  boost::latch init_latch(max_concurrency);

  threads_.reserve(max_concurrency);
  for (int i = 0; i < max_concurrency; i++) {
    threads_.emplace_back([this, &init_latch]() {
      std::function<void()> releaser = InitializeThread();

      init_latch.count_down();
      // `io_context_.run()` will block until `work_guard_.reset()` is called.
      io_context_.run();

      if (releaser) {
        releaser();
      }
    });
  }

  // Wait for all threads to initialize with a timeout to prevent hanging.
  auto status = init_latch.wait_for(timeout_ms);
  bool timed_out = status == boost::cv_status::timeout;
  RAY_CHECK(!timed_out) << "Failed to initialize threads in " +
                               std::to_string(timeout_ms.count()) + " milliseconds";
}

void BoundedExecutor::Post(std::function<void()> fn) {
  boost::asio::post(io_context_, std::move(fn));
}

/// Stop the thread pool.
void BoundedExecutor::Stop() {
  work_guard_.reset();
  io_context_.stop();
}

/// Join the thread pool.
void BoundedExecutor::Join() {
  work_guard_.reset();
  for (auto &thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

std::function<void()> BoundedExecutor::InitializeThread() {
  if (initialize_thread_callback_) {
    return initialize_thread_callback_();
  }
  return nullptr;
}

}  // namespace core
}  // namespace ray
