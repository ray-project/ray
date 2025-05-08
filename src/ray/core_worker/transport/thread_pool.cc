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
#include <future>
#include <latch>
#include <memory>
#include <utility>

namespace ray {
namespace core {

BoundedExecutor::BoundedExecutor(
    int max_concurrency,
    std::function<std::function<void()>()> initialize_thread_callback)
    : work_guard_(boost::asio::make_work_guard(io_context_)) {
  RAY_CHECK(max_concurrency > 0) << "max_concurrency must be greater than 0";

  std::latch init_latch(max_concurrency);

  threads_.reserve(max_concurrency);
  for (int i = 0; i < max_concurrency; i++) {
    threads_.emplace_back([this, initialize_thread_callback, &init_latch]() {
      std::function<void()> releaser;
      if (initialize_thread_callback) {
        releaser = initialize_thread_callback();
      }

      init_latch.count_down();
      // `io_context_.run()` will block until `work_guard_.reset()` is called.
      io_context_.run();

      if (releaser) {
        releaser();
      }
    });
  }

  init_latch.wait();
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

}  // namespace core
}  // namespace ray
