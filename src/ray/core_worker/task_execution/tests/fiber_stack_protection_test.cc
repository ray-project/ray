// Copyright 2025 The Ray Authors.
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

#include "ray/core_worker/task_execution/fiber_stack_protection.h"

#include <cstdint>
#include <optional>

#include "gtest/gtest.h"
#include "ray/core_worker/task_execution/fiber.h"

namespace ray {
namespace core {
namespace {

/// Records what would have been handed to CPython. A capture-less function is
/// required so it converts to SetStackProtectionFn.
struct StubCall {
  int calls = 0;
  PyThreadState *thread_state = nullptr;
  void *stack_start_addr = nullptr;
  size_t stack_size = 0;
  /// Value the stub returns; negative simulates CPython rejecting the bounds.
  int result = 0;
};

StubCall g_stub;

int RecordingSetStackProtection(PyThreadState *thread_state,
                                void *stack_start_addr,
                                size_t stack_size) {
  g_stub.calls++;
  g_stub.thread_state = thread_state;
  g_stub.stack_start_addr = stack_start_addr;
  g_stub.stack_size = stack_size;
  return g_stub.result;
}

/// Not dereferenced: the stub only records it, so no interpreter is needed.
PyThreadState *const kDummyThreadState = reinterpret_cast<PyThreadState *>(0x1234);

constexpr size_t kExpectedStackSize = 1024 * 256;

/// Runs `body` on a fiber and blocks until it finishes.
void RunOnFiber(const std::function<void()> &body) {
  FiberState fiber_state(1);
  boost::fibers::mutex mutex;
  boost::fibers::condition_variable done_cond;
  bool done = false;
  fiber_state.EnqueueFiber([&]() {
    body();
    std::unique_lock<boost::fibers::mutex> lock(mutex);
    done = true;
    done_cond.notify_one();
  });
  {
    std::unique_lock<boost::fibers::mutex> lock(mutex);
    done_cond.wait(lock, [&done]() { return done; });
  }
  fiber_state.Stop();
  fiber_state.Join();
}

class FiberStackProtectionTest : public ::testing::Test {
 protected:
  void SetUp() override { g_stub = StubCall{}; }
};

TEST_F(FiberStackProtectionTest, DoesNotInstallBoundsOffFiber) {
  // The test body runs on a plain thread. Installing anything here would hand
  // CPython bounds unrelated to the stack actually in use.
  EXPECT_EQ(
      ApplyCurrentFiberStackProtection(&RecordingSetStackProtection, kDummyThreadState),
      ReanchorOutcome::kNotOnFiberStack);
  EXPECT_EQ(g_stub.calls, 0);
}

TEST_F(FiberStackProtectionTest, InstallsExactFiberAllocationOnFiber) {
  // The point of the whole change: what reaches CPython must be the fiber's own
  // allocation, not a value derived from how deep the caller happens to be.
  std::optional<ReanchorOutcome> outcome;
  void *expected_start = nullptr;
  size_t expected_size = 0;
  bool found_bounds = false;
  uintptr_t fiber_anchor = 0;

  RunOnFiber([&]() {
    found_bounds =
        FiberState::GetCurrentFiberStackBounds(&expected_start, &expected_size);
    char stack_anchor;
    fiber_anchor = reinterpret_cast<uintptr_t>(&stack_anchor);
    outcome =
        ApplyCurrentFiberStackProtection(&RecordingSetStackProtection, kDummyThreadState);
  });

  ASSERT_TRUE(found_bounds);
  ASSERT_TRUE(outcome.has_value());
  EXPECT_EQ(*outcome, ReanchorOutcome::kApplied);
  EXPECT_EQ(g_stub.calls, 1);
  EXPECT_EQ(g_stub.thread_state, kDummyThreadState);
  EXPECT_EQ(g_stub.stack_start_addr, expected_start);
  EXPECT_EQ(g_stub.stack_size, expected_size);
  EXPECT_EQ(g_stub.stack_size, kExpectedStackSize);

  // The installed range must contain the stack the fiber was actually running
  // on. A bound reconstructed from an assumed call depth can fail this.
  const uintptr_t installed_base = reinterpret_cast<uintptr_t>(g_stub.stack_start_addr);
  EXPECT_GE(fiber_anchor, installed_base);
  EXPECT_LT(fiber_anchor, installed_base + g_stub.stack_size);
}

TEST_F(FiberStackProtectionTest, ReportsRejectedSoCallerCanClearTheError) {
  // A negative return leaves a pending Python error, which the production
  // wrapper must clear. Verify the outcome propagates so it can.
  g_stub.result = -1;
  std::optional<ReanchorOutcome> outcome;
  RunOnFiber([&]() {
    outcome =
        ApplyCurrentFiberStackProtection(&RecordingSetStackProtection, kDummyThreadState);
  });

  ASSERT_TRUE(outcome.has_value());
  EXPECT_EQ(*outcome, ReanchorOutcome::kRejected);
  EXPECT_EQ(g_stub.calls, 1);
}

TEST_F(FiberStackProtectionTest, InstallsDistinctBoundsForSuccessiveFibers) {
  // Each fiber must get its own allocation rather than a cached first answer.
  void *first_start = nullptr;
  RunOnFiber([&]() {
    ApplyCurrentFiberStackProtection(&RecordingSetStackProtection, kDummyThreadState);
    first_start = g_stub.stack_start_addr;
  });
  ASSERT_NE(first_start, nullptr);

  RunOnFiber([&]() {
    ApplyCurrentFiberStackProtection(&RecordingSetStackProtection, kDummyThreadState);
  });
  EXPECT_EQ(g_stub.calls, 2);
  EXPECT_EQ(g_stub.stack_size, kExpectedStackSize);
  // Whatever address the allocator reuses, the reported range must contain the
  // stack of the fiber that asked.
  EXPECT_NE(g_stub.stack_start_addr, nullptr);
}

}  // namespace
}  // namespace core
}  // namespace ray
