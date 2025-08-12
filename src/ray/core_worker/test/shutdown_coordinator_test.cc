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

#include "ray/core_worker/shutdown_coordinator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "ray/common/buffer.h"

namespace ray {
namespace core {

// Mock implementation of ShutdownExecutorInterface for testing
class MockShutdownExecutor : public ShutdownExecutorInterface {
 public:
  MOCK_METHOD(void,
              ExecuteGracefulShutdown,
              (std::string_view exit_type,
               std::string_view detail,
               std::chrono::milliseconds timeout_ms),
              (override));
  MOCK_METHOD(void,
              ExecuteForceShutdown,
              (std::string_view exit_type, std::string_view detail),
              (override));
  MOCK_METHOD(void,
              ExecuteWorkerExit,
              (std::string_view exit_type,
               std::string_view detail,
               std::chrono::milliseconds timeout_ms),
              (override));
  MOCK_METHOD(
      void,
      ExecuteExit,
      (std::string_view exit_type,
       std::string_view detail,
       std::chrono::milliseconds timeout_ms,
       const std::shared_ptr<::ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes),
      (override));
  MOCK_METHOD(void,
              ExecuteHandleExit,
              (std::string_view exit_type,
               std::string_view detail,
               std::chrono::milliseconds timeout_ms),
              (override));
  MOCK_METHOD(void, KillChildProcessesImmediately, (), (override));
  MOCK_METHOD(bool, ShouldWorkerIdleExit, (), (const, override));
};

// No-op implementation for tests that don't need actual shutdown behavior
class NoOpShutdownExecutor : public ShutdownExecutorInterface {
 public:
  void ExecuteGracefulShutdown(std::string_view exit_type,
                               std::string_view detail,
                               std::chrono::milliseconds timeout_ms) override {}
  void ExecuteForceShutdown(std::string_view exit_type,
                            std::string_view detail) override {}
  void ExecuteWorkerExit(std::string_view exit_type,
                         std::string_view detail,
                         std::chrono::milliseconds timeout_ms) override {}
  void ExecuteExit(std::string_view exit_type,
                   std::string_view detail,
                   std::chrono::milliseconds timeout_ms,
                   const std::shared_ptr<::ray::LocalMemoryBuffer>
                       &creation_task_exception_pb_bytes) override {}
  void ExecuteHandleExit(std::string_view exit_type,
                         std::string_view detail,
                         std::chrono::milliseconds timeout_ms) override {}
  void KillChildProcessesImmediately() override {}
  bool ShouldWorkerIdleExit() const override { return false; }
};

class ShutdownCoordinatorTest : public ::testing::Test {
 protected:
  // Helper to create coordinator with specific worker type
  std::unique_ptr<ShutdownCoordinator> CreateCoordinator(
      WorkerType worker_type = WorkerType::WORKER) {
    auto mock = std::make_unique<MockShutdownExecutor>();

    // Set up default behavior for the mock
    ON_CALL(*mock, ExecuteGracefulShutdown(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault(::testing::Return());
    ON_CALL(*mock, ExecuteForceShutdown(::testing::_, ::testing::_))
        .WillByDefault(::testing::Return());
    ON_CALL(*mock, ExecuteWorkerExit(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault(::testing::Return());
    ON_CALL(*mock, ExecuteHandleExit(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault(::testing::Return());
    ON_CALL(*mock, KillChildProcessesImmediately()).WillByDefault(::testing::Return());
    ON_CALL(*mock, ShouldWorkerIdleExit()).WillByDefault(::testing::Return(false));

    return std::make_unique<ShutdownCoordinator>(std::move(mock), worker_type);
  }
};

// Test 1: Basic State Machine Tests
TEST_F(ShutdownCoordinatorTest, InitialStateIsRunning) {
  auto coordinator = CreateCoordinator();

  EXPECT_EQ(coordinator->GetState(), ShutdownState::kRunning);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kNone);
  EXPECT_TRUE(coordinator->IsRunning());
  EXPECT_FALSE(coordinator->IsShuttingDown());
  EXPECT_FALSE(coordinator->IsShutdown());
  EXPECT_FALSE(coordinator->ShouldEarlyExit());
}

TEST_F(ShutdownCoordinatorTest, IdempotentShutdownRequests) {
  auto coordinator = CreateCoordinator();

  // First graceful request should succeed
  EXPECT_TRUE(coordinator->RequestShutdown(
      false, ShutdownReason::kGracefulExit, "test_graceful"));
  EXPECT_THAT(coordinator->GetState(),
              ::testing::AnyOf(ShutdownState::kDisconnecting, ShutdownState::kShutdown));
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);

  // A second graceful request should be ignored
  EXPECT_FALSE(
      coordinator->RequestShutdown(false, ShutdownReason::kUserError, "test_graceful2"));
  EXPECT_EQ(coordinator->GetReason(),
            ShutdownReason::kGracefulExit);  // Reason is unchanged

  // A force-kill request should succeed and override the graceful one
  EXPECT_TRUE(
      coordinator->RequestShutdown(true, ShutdownReason::kForcedExit, "test_force"));
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kForcedExit);  // Reason is updated
}

TEST_F(ShutdownCoordinatorTest, LegacyTryInitiateShutdown) {
  auto coordinator = CreateCoordinator();

  EXPECT_TRUE(coordinator->TryInitiateShutdown(ShutdownReason::kUserError));
  EXPECT_THAT(
      coordinator->GetState(),
      ::testing::AnyOf(ShutdownState::kShuttingDown, ShutdownState::kDisconnecting));
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kUserError);

  // Second call should fail
  EXPECT_FALSE(coordinator->TryInitiateShutdown(ShutdownReason::kForcedExit));
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kUserError);  // unchanged
}

// Test 2: State Transition Tests
TEST_F(ShutdownCoordinatorTest, ValidStateTransitions) {
  auto coordinator = CreateCoordinator();

  // Running -> ShuttingDown -> Disconnecting
  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kGracefulExit));
  auto s = coordinator->GetState();
  EXPECT_THAT(s,
              ::testing::AnyOf(ShutdownState::kDisconnecting, ShutdownState::kShutdown));

  if (s == ShutdownState::kDisconnecting) {
    // Disconnecting -> Shutdown
    EXPECT_TRUE(coordinator->TryTransitionToShutdown());
    EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);
  } else {
    // Already in shutdown
    EXPECT_FALSE(coordinator->TryTransitionToShutdown());
  }

  // Manual transitions should fail since already in shutdown state
  EXPECT_FALSE(coordinator->TryTransitionToDisconnecting());
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);

  EXPECT_TRUE(coordinator->IsShutdown());
}

TEST_F(ShutdownCoordinatorTest, InvalidStateTransitions) {
  auto coordinator = CreateCoordinator();

  // Cannot transition to disconnecting from running
  EXPECT_FALSE(coordinator->TryTransitionToDisconnecting());
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kRunning);

  // Cannot transition to shutdown from running
  EXPECT_FALSE(coordinator->TryTransitionToShutdown());
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kRunning);
}

TEST_F(ShutdownCoordinatorTest, DirectTransitionToShutdownFromShuttingDown) {
  auto coordinator = CreateCoordinator();

  // Running -> Shutdown (completes immediately with mocked dependencies)
  EXPECT_TRUE(coordinator->RequestShutdown(true,  // force
                                           ShutdownReason::kForcedExit));

  // Already in shutdown state, manual transition should fail
  EXPECT_FALSE(coordinator->TryTransitionToShutdown());
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);
}

// Test 3: Thread Safety Tests
TEST_F(ShutdownCoordinatorTest, ConcurrentShutdownRequests) {
  auto coordinator = CreateCoordinator();

  constexpr int num_threads = 10;
  std::atomic<int> success_count{0};
  std::vector<std::thread> threads;

  // Launch multiple threads trying to initiate shutdown
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&coordinator, &success_count, i]() {
      if (coordinator->RequestShutdown(false,  // graceful
                                       ShutdownReason::kGracefulExit,
                                       "thread_" + std::to_string(i))) {
        success_count.fetch_add(1);
      }
    });
  }

  // Wait for all threads
  for (auto &thread : threads) {
    thread.join();
  }

  // Only one thread should have succeeded
  EXPECT_EQ(success_count.load(), 1);
  EXPECT_THAT(
      coordinator->GetState(),
      ::testing::AnyOf(ShutdownState::kShuttingDown, ShutdownState::kDisconnecting));
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);
}

// Test 4: Worker Type Specific Tests
TEST_F(ShutdownCoordinatorTest, DriverGracefulShutdown) {
  auto coordinator = CreateCoordinator(WorkerType::DRIVER);

  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kGracefulExit));

  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);
}

TEST_F(ShutdownCoordinatorTest, DriverForceShutdown) {
  auto coordinator = CreateCoordinator(WorkerType::DRIVER);

  EXPECT_TRUE(coordinator->RequestShutdown(true,  // force
                                           ShutdownReason::kForcedExit));

  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kForcedExit);
}

// Test regular worker behavior (no special actor cleanup)
TEST_F(ShutdownCoordinatorTest, WorkerGracefulShutdown) {
  auto coordinator = CreateCoordinator(WorkerType::WORKER);

  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kGracefulExit));
}

// Test worker-specific shutdown with ExecuteWorkerExit
TEST_F(ShutdownCoordinatorTest, WorkerExitShutdown) {
  auto coordinator = CreateCoordinator(WorkerType::WORKER);

  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kUserError));
}

// Test worker-specific shutdown with ExecuteHandleExit
TEST_F(ShutdownCoordinatorTest, WorkerIdleTimeout) {
  auto coordinator = CreateCoordinator(WorkerType::WORKER);

  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kIdleTimeout));
}

// Test 5: Performance Tests
TEST_F(ShutdownCoordinatorTest, ShouldEarlyExitPerformance) {
  auto coordinator = CreateCoordinator();

  // Performance test: ShouldEarlyExit should be very fast
  auto start = std::chrono::high_resolution_clock::now();

  constexpr int iterations = 1000000;
  volatile bool result = false;  // Prevent optimization

  for (int i = 0; i < iterations; ++i) {
    result = coordinator->ShouldEarlyExit();
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

  // Should be very fast (less than 100ns per call on modern hardware)
  double ns_per_call = static_cast<double>(duration.count()) / iterations;
  EXPECT_LT(ns_per_call, 100.0)
      << "ShouldEarlyExit too slow: " << ns_per_call << "ns per call";

  // Prevent unused variable warning
  (void)result;
}

// Test 6: String Representation Tests
TEST_F(ShutdownCoordinatorTest, StringRepresentations) {
  auto coordinator = CreateCoordinator();

  EXPECT_EQ(coordinator->GetStateString(), "Running");
  EXPECT_EQ(coordinator->GetReasonString(), "None");

  coordinator->RequestShutdown(false, ShutdownReason::kGracefulExit);  // graceful

  EXPECT_EQ(coordinator->GetStateString(), "Disconnecting");
  EXPECT_EQ(coordinator->GetReasonString(), "GracefulExit");

  coordinator->TryTransitionToShutdown();
  EXPECT_EQ(coordinator->GetStateString(), "Shutdown");
}

// Test 7: Exit Type String Tests
TEST_F(ShutdownCoordinatorTest, ExitTypeStringMapping_UserError) {
  auto coordinator = CreateCoordinator();
  coordinator->RequestShutdown(false, ShutdownReason::kUserError);
  EXPECT_EQ(coordinator->GetExitTypeString(), "USER_ERROR");
}

TEST_F(ShutdownCoordinatorTest, ExitTypeStringMapping_OutOfMemory) {
  auto coordinator = CreateCoordinator();
  coordinator->RequestShutdown(false, ShutdownReason::kOutOfMemory);
  EXPECT_EQ(coordinator->GetExitTypeString(), "NODE_OUT_OF_MEMORY");
}

TEST_F(ShutdownCoordinatorTest, ExitTypeStringMapping_IdleTimeout) {
  auto coordinator = CreateCoordinator();
  coordinator->RequestShutdown(false, ShutdownReason::kIdleTimeout);
  EXPECT_EQ(coordinator->GetExitTypeString(), "INTENDED_SYSTEM_EXIT");
}

// Test 8: Edge Cases
TEST_F(ShutdownCoordinatorTest, DISABLED_NullDependencies) {
  // Test behavior with no-op dependencies (should not crash)
  auto coordinator = std::make_unique<ShutdownCoordinator>(
      std::make_unique<NoOpShutdownExecutor>(), WorkerType::WORKER);

  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kGracefulExit));

  // With no-op dependencies, should stay in shutting down state
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShuttingDown);
}

// Test manual state transitions with no-op dependencies
TEST_F(ShutdownCoordinatorTest, DISABLED_ManualStateTransitions) {
  // Use no-op dependencies to manually control transitions
  auto coordinator = std::make_unique<ShutdownCoordinator>(
      std::make_unique<NoOpShutdownExecutor>(), WorkerType::WORKER);

  // Initial state
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kRunning);

  // Start shutdown
  EXPECT_TRUE(coordinator->RequestShutdown(false, ShutdownReason::kGracefulExit));
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShuttingDown);

  // Manual transition to disconnecting
  EXPECT_TRUE(coordinator->TryTransitionToDisconnecting());
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kDisconnecting);

  // Manual transition to shutdown
  EXPECT_TRUE(coordinator->TryTransitionToShutdown());
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);
  EXPECT_TRUE(coordinator->IsShutdown());
}

// Test 9: Memory Ordering Tests (Advanced)
TEST_F(ShutdownCoordinatorTest, MemoryOrderingConsistency) {
  auto coordinator = CreateCoordinator();

  std::atomic<bool> thread1_saw_shutdown{false};
  std::atomic<bool> thread2_saw_shutdown{false};

  std::thread thread1([&coordinator, &thread1_saw_shutdown]() {
    coordinator->RequestShutdown(false, ShutdownReason::kGracefulExit);  // graceful
    thread1_saw_shutdown.store(true);
  });

  std::thread thread2([&coordinator, &thread2_saw_shutdown]() {
    while (!coordinator->ShouldEarlyExit()) {
      std::this_thread::yield();
    }
    thread2_saw_shutdown.store(true);
  });

  thread1.join();
  thread2.join();

  // Both threads should have seen the shutdown state
  EXPECT_TRUE(thread1_saw_shutdown.load());
  EXPECT_TRUE(thread2_saw_shutdown.load());
  EXPECT_TRUE(coordinator->ShouldEarlyExit());
}

TEST_F(ShutdownCoordinatorTest, ConcurrentGracefulAndForce_ForceExecutesOnce) {
  using ::testing::StrictMock;
  auto mock = std::make_unique<StrictMock<MockShutdownExecutor>>();
  auto *mock_ptr = mock.get();

  EXPECT_CALL(*mock_ptr, ExecuteForceShutdown(::testing::_, ::testing::_)).Times(1);
  EXPECT_CALL(*mock_ptr,
              ExecuteGracefulShutdown(::testing::_, ::testing::_, ::testing::_))
      .Times(::testing::AtMost(1));
  EXPECT_CALL(*mock_ptr, ExecuteWorkerExit(::testing::_, ::testing::_, ::testing::_))
      .Times(::testing::AnyNumber());
  EXPECT_CALL(*mock_ptr, ExecuteHandleExit(::testing::_, ::testing::_, ::testing::_))
      .Times(::testing::AnyNumber());
  EXPECT_CALL(*mock_ptr, KillChildProcessesImmediately()).Times(::testing::AnyNumber());
  EXPECT_CALL(*mock_ptr, ShouldWorkerIdleExit())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Return(false));

  auto coordinator =
      std::make_unique<ShutdownCoordinator>(std::move(mock), WorkerType::WORKER);

  std::thread t1([&] {
    coordinator->RequestShutdown(false, ShutdownReason::kGracefulExit, "graceful");
  });
  std::thread t2(
      [&] { coordinator->RequestShutdown(true, ShutdownReason::kForcedExit, "force"); });
  t1.join();
  t2.join();

  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kForcedExit);
}

TEST_F(ShutdownCoordinatorTest, ConcurrentDoubleForce_ForceExecutesOnce) {
  using ::testing::StrictMock;
  auto mock = std::make_unique<StrictMock<MockShutdownExecutor>>();
  auto *mock_ptr = mock.get();

  EXPECT_CALL(*mock_ptr, ExecuteForceShutdown(::testing::_, ::testing::_)).Times(1);
  EXPECT_CALL(*mock_ptr,
              ExecuteGracefulShutdown(::testing::_, ::testing::_, ::testing::_))
      .Times(0);
  EXPECT_CALL(*mock_ptr, ExecuteWorkerExit(::testing::_, ::testing::_, ::testing::_))
      .Times(::testing::AnyNumber());
  EXPECT_CALL(*mock_ptr, ExecuteHandleExit(::testing::_, ::testing::_, ::testing::_))
      .Times(::testing::AnyNumber());
  EXPECT_CALL(*mock_ptr, KillChildProcessesImmediately()).Times(::testing::AnyNumber());
  EXPECT_CALL(*mock_ptr, ShouldWorkerIdleExit())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Return(false));

  auto coordinator =
      std::make_unique<ShutdownCoordinator>(std::move(mock), WorkerType::WORKER);

  std::thread t1(
      [&] { coordinator->RequestShutdown(true, ShutdownReason::kForcedExit, "force1"); });
  std::thread t2(
      [&] { coordinator->RequestShutdown(true, ShutdownReason::kForcedExit, "force2"); });
  t1.join();
  t2.join();

  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kForcedExit);
}

}  // namespace core
}  // namespace ray
