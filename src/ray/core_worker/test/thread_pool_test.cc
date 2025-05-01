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

#include <future>
#include "ray/core_worker/transport/thread_pool.h"
#include <gtest/gtest.h>
#include <atomic>

namespace ray {
namespace core {

TEST(BoundedExecutorTest, InitializeThreadCallbackAndReleaserAreCalled) {
  constexpr int kNumThreads = 3;
  std::atomic<int> init_count{0};
  std::atomic<int> release_count{0};

  // The callback increments init_count and returns a releaser that increments release_count.
  auto initialize_thread_callback = [&]() {
    init_count++;
    return [&]() { release_count++; };
  };

  {
    BoundedExecutor executor(kNumThreads, initialize_thread_callback);
    // At this point, all threads should have called the initializer.
    ASSERT_EQ(init_count.load(), kNumThreads);
    ASSERT_EQ(release_count.load(), 0);

    // Post a dummy task to ensure threads are running.
    std::promise<void> p;
    executor.Post([&] { p.set_value(); });
    p.get_future().wait();

    // Join the pool, which should call the releasers.
    executor.Join();
  }
  // After join, all releasers should have been called.
  ASSERT_EQ(release_count.load(), kNumThreads);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
