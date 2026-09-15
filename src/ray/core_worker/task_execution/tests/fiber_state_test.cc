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
#include <array>
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

TEST(FiberStateTest, ReportsExactCurrentFiberStackBounds) {
  FiberState fiber_state(1);
  TotalCounter total_counter;
  std::atomic<bool> found_bounds{false};
  std::atomic<bool> contains_current_stack_pointer{false};
  std::atomic<size_t> stack_size{0};

  fiber_state.EnqueueFiber([&]() {
    void *stack_start_addr = nullptr;
    size_t size = 0;
    found_bounds.store(FiberState::GetCurrentFiberStackBounds(&stack_start_addr, &size));
    char stack_anchor;
    const auto stack_start = reinterpret_cast<uintptr_t>(stack_start_addr);
    const auto stack_end = stack_start + size;
    contains_current_stack_pointer.store(
        stack_start <= reinterpret_cast<uintptr_t>(&stack_anchor) &&
        reinterpret_cast<uintptr_t>(&stack_anchor) < stack_end);
    stack_size.store(size);
    total_counter.increment();
  });

  total_counter.wait_for(1);
  EXPECT_TRUE(found_bounds.load());
  EXPECT_TRUE(contains_current_stack_pointer.load());
  EXPECT_EQ(stack_size.load(), 1024U * 256U);

  fiber_state.Stop();
  fiber_state.Join();
}

TEST(FiberStateTest, ReportsDistinctBoundsForConcurrentFibers) {
  // With several fibers alive at once the registry holds multiple ranges, so
  // this exercises FindContaining's actual lookup (the single-fiber test above
  // passes even if the lookup ignored the address entirely).
  constexpr int kNumFibers = 8;
  FiberState fiber_state(kNumFibers);
  TotalCounter done;

  boost::fibers::mutex mutex;
  boost::fibers::condition_variable barrier;
  int arrived = 0;
  std::array<uintptr_t, kNumFibers> starts{};
  std::array<size_t, kNumFibers> sizes{};
  std::array<uintptr_t, kNumFibers> anchors{};
  std::array<bool, kNumFibers> found{};

  for (int i = 0; i < kNumFibers; i++) {
    fiber_state.EnqueueFiber([&, i]() {
      void *start = nullptr;
      size_t size = 0;
      found[i] = FiberState::GetCurrentFiberStackBounds(&start, &size);
      char anchor;
      starts[i] = reinterpret_cast<uintptr_t>(start);
      sizes[i] = size;
      anchors[i] = reinterpret_cast<uintptr_t>(&anchor);
      // Hold every fiber here until all of them have reported, so all
      // kNumFibers stacks are registered simultaneously.
      {
        std::unique_lock<boost::fibers::mutex> lock(mutex);
        arrived++;
        barrier.notify_all();
        barrier.wait(lock, [&]() { return arrived == kNumFibers; });
      }
      done.increment();
    });
  }
  done.wait_for(kNumFibers);

  for (int i = 0; i < kNumFibers; i++) {
    SCOPED_TRACE(i);
    EXPECT_TRUE(found[i]);
    EXPECT_EQ(sizes[i], 1024U * 256U);
    // Each fiber must get the range containing its own stack pointer.
    EXPECT_GE(anchors[i], starts[i]);
    EXPECT_LT(anchors[i], starts[i] + sizes[i]);
  }
  for (int i = 0; i < kNumFibers; i++) {
    for (int j = i + 1; j < kNumFibers; j++) {
      SCOPED_TRACE(i * 100 + j);
      EXPECT_TRUE(starts[i] + sizes[i] <= starts[j] || starts[j] + sizes[j] <= starts[i])
          << "concurrent fiber stacks overlap";
    }
  }
  // How much of the reported allocation sits above the running stack pointer:
  // Boost reserves the top for its control record, so the reported range is a
  // superset of the usable stack.
  RAY_LOG(INFO) << "unused bytes above fiber SP: " << (starts[0] + sizes[0] - anchors[0]);

  fiber_state.Stop();
  fiber_state.Join();
}

TEST(FiberStateTest, ReportsCorrectBoundsAcrossFiberStackReuse) {
  // Sequential fibers churn the registry (insert/erase) and malloc will hand
  // back previously freed stack addresses, so this covers the reuse case.
  constexpr int kIterations = 100;
  FiberState fiber_state(1);
  TotalCounter done;
  std::atomic<int> failures{0};

  for (int i = 0; i < kIterations; i++) {
    fiber_state.EnqueueFiber([&]() {
      void *start = nullptr;
      size_t size = 0;
      const bool ok = FiberState::GetCurrentFiberStackBounds(&start, &size);
      char anchor;
      const auto base = reinterpret_cast<uintptr_t>(start);
      const auto addr = reinterpret_cast<uintptr_t>(&anchor);
      if (!ok || addr < base || addr >= base + size || size != 1024U * 256U) {
        failures++;
      }
      done.increment();
    });
  }
  done.wait_for(kIterations);
  EXPECT_EQ(failures.load(), 0);

  fiber_state.Stop();
  fiber_state.Join();
}

TEST(FiberStateTest, ReportsNoBoundsOutsideAnyFiber) {
  // Off the fiber runner thread there is no registry, so callers must get a
  // clean "no bounds" answer rather than a bogus range.
  void *start = reinterpret_cast<void *>(0xdeadbeef);
  size_t size = 12345;
  EXPECT_FALSE(FiberState::GetCurrentFiberStackBounds(&start, &size));
  // Out-params must be left untouched when lookup fails.
  EXPECT_EQ(start, reinterpret_cast<void *>(0xdeadbeef));
  EXPECT_EQ(size, 12345U);
}

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
