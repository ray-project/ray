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

#include <gtest/gtest.h>

#include <atomic>
#include <boost/thread/latch.hpp>
#include <future>

namespace ray {
namespace core {

TEST(BoundedExecutorTest, InitializeThreadCallbackAndReleaserAreCalled) {
  constexpr int kNumThreads = 3;
  std::atomic<int> init_count{0};
  std::atomic<int> release_count{0};

  // The callback increments init_count and returns a releaser that increments
  // release_count.
  auto initialize_thread_callback = [&]() {
    init_count++;
    return [&]() { release_count++; };
  };

  {
    BoundedExecutor executor(kNumThreads, initialize_thread_callback);
    // At this point, all threads should have called the initializer.
    ASSERT_EQ(init_count.load(), kNumThreads);
    ASSERT_EQ(release_count.load(), 0);

    std::atomic<int> task_count{0};
    auto callback = [&]() {
      task_count++;
      while (task_count.load() < kNumThreads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
    };

    // Make sure all threads can run tasks.
    for (int i = 0; i < kNumThreads; i++) {
      executor.Post(callback);
    }

    // Join the pool, which should call the releasers.
    executor.Join();
    ASSERT_EQ(task_count.load(), kNumThreads);
  }
  // After join, all releasers should have been called.
  ASSERT_EQ(release_count.load(), kNumThreads);
}

TEST(BoundedExecutorTest, InitializationTimeout) {
  constexpr int kNumThreads = 3;

  // Create a callback that will hang indefinitely to trigger the timeout
  auto initialize_thread_callback = [&]() {
    while (true) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return nullptr;
  };

  // Verify that the constructor fails with the expected error message
  EXPECT_DEATH(
      BoundedExecutor executor(
          kNumThreads, initialize_thread_callback, boost::chrono::milliseconds(10)),
      "Failed to initialize threads in 10 milliseconds");
}

TEST(BoundedExecutorTest, PostBlockingIfFull) {
  constexpr int kNumThreads = 3;
  BoundedExecutor executor(kNumThreads);

  boost::latch latch(kNumThreads);
  std::atomic<bool> block{true};
  auto callback = [&]() {
    latch.count_down();
    while (block.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  };

  for (int i = 0; i < kNumThreads; i++) {
    executor.Post(callback);
  }
  latch.wait();

  // Submit a new task. It should not run immediately
  // because the thread pool is full.
  std::atomic<bool> running{false};
  std::promise<void> promise;
  std::future<void> future = promise.get_future();
  executor.Post([&]() {
    running = true;
    promise.set_value();
  });

  // Make sure the task is not running yet after 50 ms.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_FALSE(running.load());

  // Unblock the threads. The task should run immediately.
  block.store(false);

  // Wait for the task with a timeout
  auto status = future.wait_for(std::chrono::milliseconds(500));
  ASSERT_EQ(status, std::future_status::ready) << "Task did not complete within timeout";
  ASSERT_TRUE(running.load());

  executor.Join();
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
