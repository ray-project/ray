// Copyright 2023 The Ray Authors.
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

#include <algorithm>
#include <atomic>

#include "gtest/gtest.h"
#include "ray/core_worker/task_execution/fiber.h"
#include "ray/util/logging.h"

namespace ray {
namespace core {

TEST(FiberStateTest, Empty) {
  FiberState fiber_state(2);
  fiber_state.Stop();
  fiber_state.Join();
}

class ConcurrencyCounter {
 public:
  std::atomic<int> concurrency_{0};
  std::atomic<int> max_concurrency_{0};

  void inc_yield_dec() {
    concurrency_++;
    max_concurrency_.store(std::max(concurrency_, max_concurrency_));
    boost::this_fiber::sleep_for(std::chrono::milliseconds(10));
    concurrency_--;
  }
};

class TotalCounter {
  boost::fibers::condition_variable cond_;
  boost::fibers::mutex mutex_;
  std::atomic<int> total_{0};

 public:
  void increment() {
    std::unique_lock<boost::fibers::mutex> lock(mutex_);
    total_++;
    cond_.notify_one();
  }

  void wait_for(int total) {
    std::unique_lock<boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this, total]() { return this->total_ >= total; });
  }
};

TEST(FiberStateTest, DrainsInFlightFibersBeforeStopping) {
  // Stop()/Join() drains in-flight fibers: it keeps the scheduler running and
  // waits for them to finish before returning, instead of abandoning them. This
  // is what lets async actors deliver in-flight task results during graceful
  // shutdown. (A fiber that never finishes therefore blocks Join() until the
  // process is force killed, matching threaded actors.)
  FiberState fiber_state(2);

  std::atomic<bool> task_completed{false};
  fiber_state.EnqueueFiber([&]() {
    // Still running when Stop() is called below: Join() must wait for it.
    boost::this_fiber::sleep_for(std::chrono::milliseconds(200));
    task_completed.store(true);
  });

  // EnqueueFiber blocks until the runner accepts the fiber, so it is in flight
  // (started, not yet finished) at this point.
  EXPECT_FALSE(task_completed.load());

  fiber_state.Stop();
  fiber_state.Join();

  // Join() returned only after the in-flight fiber ran to completion.
  EXPECT_TRUE(task_completed.load());
}

TEST(FiberStateTest, RespectsConcurrencyLimit) {
  FiberState fiber_state(2);
  TotalCounter total_counter;

  ConcurrencyCounter counter;

  for (int i = 0; i < 100; ++i) {
    fiber_state.EnqueueFiber([&]() {
      counter.inc_yield_dec();
      total_counter.increment();
    });
  }

  total_counter.wait_for(100);
  EXPECT_EQ(counter.max_concurrency_, 2);

  fiber_state.Stop();
  fiber_state.Join();
}

TEST(FiberStateTest, DoubleStopJoin) {
  FiberState fiber_state(2);
  fiber_state.Stop();
  fiber_state.Join();
  fiber_state.Stop();
  fiber_state.Join();
}

TEST(FiberStateTest, DestructorStopsAndJoins) {
  // Destroying a FiberState without calling Stop()/Join() stops the runner
  // thread, drains in-flight fibers, and joins the thread.
  std::atomic<bool> task_completed{false};
  {
    FiberState fiber_state(2);
    fiber_state.EnqueueFiber([&]() {
      boost::this_fiber::sleep_for(std::chrono::milliseconds(100));
      task_completed.store(true);
    });
  }
  EXPECT_TRUE(task_completed.load());
}

}  // namespace core
}  // namespace ray
