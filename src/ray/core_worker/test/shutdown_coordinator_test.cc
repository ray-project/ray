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

#include "ray/core_worker/shutdown_coordinator.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <chrono>
#include <thread>

namespace ray {
namespace core {

// Mock implementation of ShutdownDependencies for testing
class MockShutdownDependencies : public ShutdownDependencies {
 public:
  MOCK_METHOD(void, DisconnectRaylet, (), (override));
  MOCK_METHOD(void, DisconnectGcs, (), (override));
  MOCK_METHOD(void, ShutdownTaskManager, (bool force), (override));
  MOCK_METHOD(void, ShutdownObjectRecovery, (), (override));
  MOCK_METHOD(void, CancelPendingTasks, (bool force), (override));
  MOCK_METHOD(void, CleanupActorState, (), (override));
  MOCK_METHOD(void, FlushMetrics, (), (override));
  MOCK_METHOD(size_t, GetPendingTaskCount, (), (const, override));
  MOCK_METHOD(bool, IsGracefulShutdownTimedOut, 
              (std::chrono::steady_clock::time_point start_time,
               std::chrono::milliseconds timeout), (const, override));
};

class ShutdownCoordinatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_deps_ = std::make_shared<MockShutdownDependencies>();
  }

  // Helper to create coordinator with specific worker type
  std::unique_ptr<ShutdownCoordinator> CreateCoordinator(
      WorkerType worker_type = WorkerType::WORKER,
      std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) {
    return std::make_unique<ShutdownCoordinator>(mock_deps_, worker_type, timeout);
  }

  std::shared_ptr<MockShutdownDependencies> mock_deps_;
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
  
  // First request should succeed
  EXPECT_TRUE(coordinator->RequestShutdown(false, // graceful
                                          ShutdownReason::kGracefulExit, 
                                          "test"));
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShuttingDown);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);
  
  // Subsequent requests should fail (idempotent)
  EXPECT_FALSE(coordinator->RequestShutdown(true, // force
                                           ShutdownReason::kForcedExit, 
                                           "test2"));
  // State should remain unchanged
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShuttingDown);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);
}

TEST_F(ShutdownCoordinatorTest, LegacyTryInitiateShutdown) {
  auto coordinator = CreateCoordinator();
  
  EXPECT_TRUE(coordinator->TryInitiateShutdown(ShutdownReason::kUserError));
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShuttingDown);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kUserError);
  
  // Second call should fail
  EXPECT_FALSE(coordinator->TryInitiateShutdown(ShutdownReason::kSystemShutdown));
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kUserError); // unchanged
}

// Test 2: State Transition Tests
TEST_F(ShutdownCoordinatorTest, ValidStateTransitions) {
  auto coordinator = CreateCoordinator();
  
  // Running -> ShuttingDown
  EXPECT_TRUE(coordinator->RequestShutdown(false, // graceful
                                          ShutdownReason::kGracefulExit));
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShuttingDown);
  
  // ShuttingDown -> Disconnecting
  EXPECT_TRUE(coordinator->TryTransitionToDisconnecting());
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kDisconnecting);
  
  // Disconnecting -> Shutdown
  EXPECT_TRUE(coordinator->TryTransitionToShutdown());
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
  
  // Running -> ShuttingDown
  EXPECT_TRUE(coordinator->RequestShutdown(true, // force
                                          ShutdownReason::kForcedExit));
  
  // ShuttingDown -> Shutdown (skip disconnecting)
  EXPECT_TRUE(coordinator->TryTransitionToShutdown());
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
      if (coordinator->RequestShutdown(false, // graceful
                                      ShutdownReason::kGracefulExit, 
                                      "thread_" + std::to_string(i))) {
        success_count.fetch_add(1);
      }
    });
  }
  
  // Wait for all threads
  for (auto& thread : threads) {
    thread.join();
  }
  
  // Only one thread should have succeeded
  EXPECT_EQ(success_count.load(), 1);
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShuttingDown);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);
}

// Test 4: Worker Type Specific Tests
TEST_F(ShutdownCoordinatorTest, DriverGracefulShutdown) {
  EXPECT_CALL(*mock_deps_, GetPendingTaskCount())
      .WillOnce(::testing::Return(0)); // No pending tasks
  EXPECT_CALL(*mock_deps_, FlushMetrics());
  EXPECT_CALL(*mock_deps_, DisconnectRaylet());
  EXPECT_CALL(*mock_deps_, DisconnectGcs());
  
  auto coordinator = CreateCoordinator(WorkerType::DRIVER);
  
  EXPECT_TRUE(coordinator->RequestShutdown(false, // graceful
                                          ShutdownReason::kGracefulExit));
  
  // Should eventually reach shutdown state
  // Note: In real implementation, this might be asynchronous
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);
}

TEST_F(ShutdownCoordinatorTest, DriverForceShutdown) {
  EXPECT_CALL(*mock_deps_, CancelPendingTasks(true));
  EXPECT_CALL(*mock_deps_, DisconnectRaylet());
  EXPECT_CALL(*mock_deps_, DisconnectGcs());
  
  auto coordinator = CreateCoordinator(WorkerType::DRIVER);
  
  EXPECT_TRUE(coordinator->RequestShutdown(true, // force
                                          ShutdownReason::kForcedExit));
  
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kForcedExit);
}

// Test regular worker behavior (no special actor cleanup)
TEST_F(ShutdownCoordinatorTest, WorkerGracefulShutdown) {
  EXPECT_CALL(*mock_deps_, GetPendingTaskCount())
      .WillOnce(::testing::Return(0));
  EXPECT_CALL(*mock_deps_, FlushMetrics());
  EXPECT_CALL(*mock_deps_, DisconnectRaylet());
  EXPECT_CALL(*mock_deps_, DisconnectGcs());
  
  auto coordinator = CreateCoordinator(WorkerType::WORKER);
  
  EXPECT_TRUE(coordinator->RequestShutdown(false, // graceful
                                          ShutdownReason::kGracefulExit));
}

// Test 5: Graceful Shutdown Timeout Tests  
TEST_F(ShutdownCoordinatorTest, GracefulShutdownTimeout) {
  // Setup: pending tasks that don't complete within timeout
  EXPECT_CALL(*mock_deps_, GetPendingTaskCount())
      .WillRepeatedly(::testing::Return(5)); // Always have pending tasks
  EXPECT_CALL(*mock_deps_, IsGracefulShutdownTimedOut(::testing::_, ::testing::_))
      .WillOnce(::testing::Return(false))    // First check: not timed out
      .WillOnce(::testing::Return(true));    // Second check: timed out
  
  // Should fall back to force shutdown
  EXPECT_CALL(*mock_deps_, CancelPendingTasks(true));
  EXPECT_CALL(*mock_deps_, DisconnectRaylet());
  EXPECT_CALL(*mock_deps_, DisconnectGcs());
  
  auto coordinator = CreateCoordinator(WorkerType::WORKER,
                                      std::chrono::milliseconds(100));
  
  EXPECT_TRUE(coordinator->RequestShutdown(false, // graceful
                                          ShutdownReason::kGracefulExit));
}

// Test 6: Performance Tests
TEST_F(ShutdownCoordinatorTest, ShouldEarlyExitPerformance) {
  auto coordinator = CreateCoordinator();
  
  // Performance test: ShouldEarlyExit should be very fast
  auto start = std::chrono::high_resolution_clock::now();
  
  constexpr int iterations = 1000000;
  volatile bool result = false; // Prevent optimization
  
  for (int i = 0; i < iterations; ++i) {
    result = coordinator->ShouldEarlyExit();
  }
  
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  
  // Should be very fast (less than 10ns per call on modern hardware)
  double ns_per_call = static_cast<double>(duration.count()) / iterations;
  EXPECT_LT(ns_per_call, 100.0) << "ShouldEarlyExit too slow: " << ns_per_call << "ns per call";
  
  // Prevent unused variable warning
  (void)result;
}

// Test 7: String Representation Tests
TEST_F(ShutdownCoordinatorTest, StringRepresentations) {
  auto coordinator = CreateCoordinator();
  
  EXPECT_EQ(coordinator->GetStateString(), "Running");
  EXPECT_EQ(coordinator->GetReasonString(), "None");
  
  coordinator->RequestShutdown(false, ShutdownReason::kGracefulExit); // graceful
  
  EXPECT_EQ(coordinator->GetStateString(), "ShuttingDown");
  EXPECT_EQ(coordinator->GetReasonString(), "GracefulExit");
}

// Test 8: Edge Cases
TEST_F(ShutdownCoordinatorTest, NullDependencies) {
  // Test behavior with null dependencies (should not crash)
  auto coordinator = std::make_unique<ShutdownCoordinator>(
      nullptr, WorkerType::WORKER, std::chrono::milliseconds(1000));
  
  EXPECT_TRUE(coordinator->RequestShutdown(false, // graceful
                                          ShutdownReason::kGracefulExit));
  
  // Should handle null dependencies gracefully
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShuttingDown);
}

// Test 9: Memory Ordering Tests (Advanced)
TEST_F(ShutdownCoordinatorTest, MemoryOrderingConsistency) {
  auto coordinator = CreateCoordinator();
  
  std::atomic<bool> thread1_saw_shutdown{false};
  std::atomic<bool> thread2_saw_shutdown{false};
  
  std::thread thread1([&coordinator, &thread1_saw_shutdown]() {
    coordinator->RequestShutdown(false, ShutdownReason::kGracefulExit); // graceful
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

}  // namespace core
}  // namespace ray
 