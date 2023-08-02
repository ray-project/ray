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

#include <atomic>
#include <boost/fiber/all.hpp>

#include "gtest/gtest.h"
#include "ray/core_worker/fiber.h"
#include "ray/util/logging.h"

namespace ray {
namespace core {

TEST(FiberThreadTest, Empty) {
  std::vector<ConcurrencyGroup> concurrency_groups{
      {"cg1",
       1,
       {FunctionDescriptorBuilder::BuildPython(
           "module_name", "class_name", "function_name", "function_hash")}},
      {"cg5", 5, {}}};
  FiberThread fiber_thread(concurrency_groups, 2);
  fiber_thread.Stop();
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

TEST(FiberThreadTest, CanStopInfiniteTasks) {
  FiberThread fiber_thread({}, 2);

  FunctionDescriptor fd = FunctionDescriptorBuilder::BuildPython(
      "module_name", "class_name", "function_name", "function_hash");

  fiber_thread.EnqueueFiber("", fd, [&]() {
    while (true) {
      boost::this_fiber::sleep_for(std::chrono::milliseconds(10));
      boost::this_fiber::yield();
    }
  });

  boost::this_fiber::sleep_for(std::chrono::seconds(1));
  fiber_thread.Stop();
  // Can exit normally even if the fiber did not stop.
}

TEST(FiberThreadTest, RespectsDefaultLimit) {
  FiberThread fiber_thread({}, 2);
  TotalCounter total_counter;

  FunctionDescriptor fd = FunctionDescriptorBuilder::BuildPython(
      "module_name", "class_name", "function_name", "function_hash");
  ConcurrencyCounter counter;

  for (int i = 0; i < 100; ++i) {
    fiber_thread.EnqueueFiber("", fd, [&]() {
      counter.inc_yield_dec();
      total_counter.increment();
    });
  }

  total_counter.wait_for(100);
  EXPECT_EQ(counter.max_concurrency_, 2);

  fiber_thread.Stop();
}

TEST(FiberThreadTest, RespectsConcurrencyGroups) {
  std::vector<ConcurrencyGroup> concurrency_groups{{"cg1", 1, {}}, {"cg5", 5, {}}};
  FiberThread fiber_thread(concurrency_groups, 2);
  TotalCounter total_counter;

  FunctionDescriptor fd = FunctionDescriptorBuilder::BuildPython(
      "module_name", "class_name", "function_name", "function_hash");
  ConcurrencyCounter counter1, counter2, counter5;

  for (int i = 0; i < 100; ++i) {
    fiber_thread.EnqueueFiber("cg1", fd, [&]() {
      counter1.inc_yield_dec();
      total_counter.increment();
    });
  }
  for (int i = 0; i < 500; ++i) {
    fiber_thread.EnqueueFiber("cg5", fd, [&]() {
      counter5.inc_yield_dec();
      total_counter.increment();
    });
  }
  for (int i = 0; i < 1000; ++i) {
    fiber_thread.EnqueueFiber("", fd, [&]() {
      counter2.inc_yield_dec();
      total_counter.increment();
    });
  }
  total_counter.wait_for(1600);
  EXPECT_EQ(counter1.max_concurrency_, 1);
  EXPECT_EQ(counter2.max_concurrency_, 2);
  EXPECT_EQ(counter5.max_concurrency_, 5);

  fiber_thread.Stop();
}

// A function descriptor shares rate limiter with its concurrency group, even if in
// scheduling time the concurreny group is not set.
TEST(FiberThreadTest, FunctionSharesRateLimiterWithConcurrencyGroup) {
  FunctionDescriptor fd = FunctionDescriptorBuilder::BuildPython(
      "module_name", "class_name", "function_name", "function_hash");
  FunctionDescriptor fd2 = FunctionDescriptorBuilder::BuildPython(
      "module_name", "class_name", "function_name2", "function_hash2");
  std::vector<ConcurrencyGroup> concurrency_groups{{"cg3", 3, {fd}}, {"cg5", 5, {}}};
  FiberThread fiber_thread(concurrency_groups, 2);
  TotalCounter total_counter;

  ConcurrencyCounter counter3;

  for (int i = 0; i < 200; ++i) {
    fiber_thread.EnqueueFiber("cg3", fd2, [&]() {
      counter3.inc_yield_dec();
      total_counter.increment();
    });

    fiber_thread.EnqueueFiber("", fd, [&]() {
      counter3.inc_yield_dec();
      total_counter.increment();
    });
  }

  total_counter.wait_for(400);
  EXPECT_EQ(counter3.max_concurrency_, 3);

  fiber_thread.Stop();
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
