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

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/buffer.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

// Simple fake executor for tests without gmock.
class FakeShutdownExecutor : public ShutdownExecutorInterface {
 public:
  std::atomic<int> graceful_calls{0};
  std::atomic<int> force_calls{0};
  std::atomic<int> worker_exit_calls{0};
  std::atomic<int> handle_exit_calls{0};
  std::atomic<bool> idle_exit_allowed{false};

  std::string last_exit_type;
  std::string last_detail;
  mutable absl::Mutex mu_;

  std::string GetLastExitType() const {
    absl::MutexLock lk(&mu_);
    return last_exit_type;
  }

  std::string GetLastDetail() const {
    absl::MutexLock lk(&mu_);
    return last_detail;
  }

  void ExecuteGracefulShutdown(std::string_view exit_type,
                               std::string_view detail,
                               std::chrono::milliseconds timeout_ms) override {
    graceful_calls++;
    {
      absl::MutexLock lk(&mu_);
      last_exit_type = std::string(exit_type);
      last_detail = std::string(detail);
    }
  }
  void ExecuteForceShutdown(std::string_view exit_type,
                            std::string_view detail) override {
    force_calls++;
    {
      absl::MutexLock lk(&mu_);
      last_exit_type = std::string(exit_type);
      last_detail = std::string(detail);
    }
  }
  void ExecuteExit(std::string_view exit_type,
                   std::string_view detail,
                   std::chrono::milliseconds timeout_ms,
                   const std::shared_ptr<::ray::LocalMemoryBuffer>
                       &creation_task_exception_pb_bytes) override {
    worker_exit_calls++;
    {
      absl::MutexLock lk(&mu_);
      last_exit_type = std::string(exit_type);
      last_detail = std::string(detail);
    }
  }
  void ExecuteExitIfIdle(std::string_view exit_type,
                         std::string_view detail,
                         std::chrono::milliseconds timeout_ms) override {
    handle_exit_calls++;
    {
      absl::MutexLock lk(&mu_);
      last_exit_type = std::string(exit_type);
      last_detail = std::string(detail);
    }
  }
  void KillChildProcessesImmediately() override {}
  bool ShouldWorkerIdleExit() const override { return idle_exit_allowed.load(); }
};

// No-op executor used in disabled/manual-transition tests.
class NoOpShutdownExecutor : public ShutdownExecutorInterface {
 public:
  void ExecuteGracefulShutdown(std::string_view,
                               std::string_view,
                               std::chrono::milliseconds) override {}
  void ExecuteForceShutdown(std::string_view, std::string_view) override {}
  void ExecuteExit(std::string_view,
                   std::string_view,
                   std::chrono::milliseconds,
                   const std::shared_ptr<::ray::LocalMemoryBuffer> &) override {}
  void ExecuteExitIfIdle(std::string_view,
                         std::string_view,
                         std::chrono::milliseconds) override {}
  void KillChildProcessesImmediately() override {}
  bool ShouldWorkerIdleExit() const override { return false; }
};

class ShutdownCoordinatorTest : public ::testing::Test {
 protected:
  // Helper to create coordinator with specific worker type
  std::unique_ptr<ShutdownCoordinator> CreateCoordinator(
      rpc::WorkerType worker_type = rpc::WorkerType::WORKER) {
    auto fake = std::make_unique<FakeShutdownExecutor>();
    return std::make_unique<ShutdownCoordinator>(std::move(fake), worker_type);
  }
};

TEST_F(ShutdownCoordinatorTest, InitialStateWithNoTransitions_IsRunning) {
  auto coordinator = CreateCoordinator();

  EXPECT_EQ(coordinator->GetState(), ShutdownState::kRunning);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kNone);
  EXPECT_TRUE(coordinator->IsRunning());
  EXPECT_FALSE(coordinator->IsShuttingDown());
  EXPECT_FALSE(coordinator->IsShutdown());
  EXPECT_FALSE(coordinator->ShouldEarlyExit());
}

TEST_F(ShutdownCoordinatorTest, RequestShutdown_IdempotentBehavior) {
  auto coordinator = CreateCoordinator();

  // First graceful request should succeed
  EXPECT_TRUE(coordinator->RequestShutdown(
      false, ShutdownReason::kGracefulExit, "test_graceful"));
  const auto state = coordinator->GetState();
  EXPECT_TRUE(state == ShutdownState::kDisconnecting ||
              state == ShutdownState::kShutdown);
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

TEST_F(ShutdownCoordinatorTest, RequestShutdown_DelegatesToGraceful_OnlyFirstSucceeds) {
  auto coordinator = CreateCoordinator();

  EXPECT_TRUE(coordinator->RequestShutdown(false, ShutdownReason::kUserError));
  const auto state = coordinator->GetState();
  EXPECT_TRUE(state == ShutdownState::kShuttingDown ||
              state == ShutdownState::kDisconnecting);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kUserError);

  // Second call should fail
  EXPECT_FALSE(coordinator->RequestShutdown(false, ShutdownReason::kForcedExit));
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kUserError);  // unchanged
}

TEST_F(ShutdownCoordinatorTest,
       RequestShutdown_Graceful_SetsDisconnecting_ThenTryTransitionToShutdown_Succeeds) {
  auto coordinator = std::make_unique<ShutdownCoordinator>(
      std::make_unique<NoOpShutdownExecutor>(), rpc::WorkerType::WORKER);

  // Running -> ShuttingDown -> Disconnecting
  EXPECT_TRUE(
      coordinator->RequestShutdown(false /*graceful*/, ShutdownReason::kGracefulExit));

  // worker path enters Disconnecting and requires explicit final step.
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kDisconnecting);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);

  // Disconnecting -> Shutdown
  EXPECT_TRUE(coordinator->RequestShutdown(true, ShutdownReason::kForcedExit));
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);

  // Further transitions are no-ops.
  EXPECT_FALSE(coordinator->RequestShutdown(false, ShutdownReason::kGracefulExit));
  EXPECT_FALSE(coordinator->RequestShutdown(true, ShutdownReason::kForcedExit));
}

TEST_F(ShutdownCoordinatorTest, ForceShutdown_TransitionsDirectlyToShutdown) {
  auto coordinator = CreateCoordinator();

  // Running -> Shutdown (completes immediately with mocked dependencies)
  EXPECT_TRUE(coordinator->RequestShutdown(true,  // force
                                           ShutdownReason::kForcedExit));

  // Already in shutdown state, manual transition should fail
  EXPECT_FALSE(coordinator->RequestShutdown(true, ShutdownReason::kForcedExit));
  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);
}

TEST_F(ShutdownCoordinatorTest,
       RequestShutdown_Graceful_OnlyOneInitiatorUnderConcurrency) {
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
  const auto state = coordinator->GetState();
  EXPECT_TRUE(state == ShutdownState::kShuttingDown ||
              state == ShutdownState::kDisconnecting);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);
}

TEST_F(ShutdownCoordinatorTest, Driver_GracefulReasonRecorded) {
  auto coordinator = CreateCoordinator(rpc::WorkerType::DRIVER);

  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kGracefulExit));

  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kGracefulExit);
}

TEST_F(ShutdownCoordinatorTest, Driver_ForceReasonRecorded) {
  auto coordinator = CreateCoordinator(rpc::WorkerType::DRIVER);

  EXPECT_TRUE(coordinator->RequestShutdown(true,  // force
                                           ShutdownReason::kForcedExit));

  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kForcedExit);
}

TEST_F(ShutdownCoordinatorTest, Worker_GracefulInitiates) {
  auto coordinator = CreateCoordinator(rpc::WorkerType::WORKER);

  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kGracefulExit));
}

TEST_F(ShutdownCoordinatorTest, Worker_ExecuteWorkerExit_OnUserError) {
  auto coordinator = CreateCoordinator(rpc::WorkerType::WORKER);

  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kUserError));
}

TEST_F(ShutdownCoordinatorTest, Worker_HandleExit_OnIdleTimeout) {
  auto coordinator = CreateCoordinator(rpc::WorkerType::WORKER);

  EXPECT_TRUE(coordinator->RequestShutdown(false,  // graceful
                                           ShutdownReason::kIdleTimeout));
}

TEST_F(ShutdownCoordinatorTest, StringRepresentations_StateAndReason_AreReadable) {
  auto coordinator = CreateCoordinator();

  EXPECT_EQ(coordinator->GetStateString(), "Running");
  EXPECT_EQ(coordinator->GetReasonString(), "None");

  coordinator->RequestShutdown(false, ShutdownReason::kGracefulExit);  // graceful

  EXPECT_EQ(coordinator->GetStateString(), "Disconnecting");
  EXPECT_EQ(coordinator->GetReasonString(), "GracefulExit");

  coordinator->RequestShutdown(true, ShutdownReason::kForcedExit);
  EXPECT_EQ(coordinator->GetStateString(), "Shutdown");
}

TEST_F(ShutdownCoordinatorTest, ExitTypeStringMapping_UserError_IsUSER_ERROR) {
  auto coordinator = CreateCoordinator();
  coordinator->RequestShutdown(false, ShutdownReason::kUserError);
  EXPECT_EQ(coordinator->GetExitTypeString(), "USER_ERROR");
}

TEST_F(ShutdownCoordinatorTest, ExitTypeStringMapping_OOM_IsNODE_OUT_OF_MEMORY) {
  auto coordinator = CreateCoordinator();
  coordinator->RequestShutdown(false, ShutdownReason::kOutOfMemory);
  EXPECT_EQ(coordinator->GetExitTypeString(), "NODE_OUT_OF_MEMORY");
}

TEST_F(ShutdownCoordinatorTest,
       ExitTypeStringMapping_IdleTimeout_IsINTENDED_SYSTEM_EXIT) {
  auto coordinator = CreateCoordinator();
  coordinator->RequestShutdown(false, ShutdownReason::kIdleTimeout);
  EXPECT_EQ(coordinator->GetExitTypeString(), "INTENDED_SYSTEM_EXIT");
}

TEST_F(ShutdownCoordinatorTest, ShouldEarlyExit_MemoryOrdering_ConcurrentVisibility) {
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

TEST_F(ShutdownCoordinatorTest, Concurrent_GracefulVsForce_ForceExecutesOnce) {
  auto fake = std::make_unique<FakeShutdownExecutor>();
  auto *fake_ptr = fake.get();
  auto coordinator =
      std::make_unique<ShutdownCoordinator>(std::move(fake), rpc::WorkerType::WORKER);

  std::thread t1([&] {
    coordinator->RequestShutdown(false, ShutdownReason::kGracefulExit, "graceful");
  });
  std::thread t2(
      [&] { coordinator->RequestShutdown(true, ShutdownReason::kForcedExit, "force"); });
  t1.join();
  t2.join();

  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kForcedExit);
  EXPECT_EQ(fake_ptr->force_calls.load(), 1);
  EXPECT_LE(fake_ptr->graceful_calls.load(), 1);
}

TEST_F(ShutdownCoordinatorTest, Concurrent_DoubleForce_ForceExecutesOnce) {
  auto fake = std::make_unique<FakeShutdownExecutor>();
  auto *fake_ptr = fake.get();
  auto coordinator =
      std::make_unique<ShutdownCoordinator>(std::move(fake), rpc::WorkerType::WORKER);

  std::thread t1(
      [&] { coordinator->RequestShutdown(true, ShutdownReason::kForcedExit, "force1"); });
  std::thread t2(
      [&] { coordinator->RequestShutdown(true, ShutdownReason::kForcedExit, "force2"); });
  t1.join();
  t2.join();

  EXPECT_EQ(coordinator->GetState(), ShutdownState::kShutdown);
  EXPECT_EQ(coordinator->GetReason(), ShutdownReason::kForcedExit);
  // Verify that only one forced shutdown was called
  EXPECT_EQ(fake_ptr->force_calls.load(), 1);
  EXPECT_EQ(fake_ptr->graceful_calls.load(), 0);
  EXPECT_TRUE(fake_ptr->GetLastDetail() == "force1" ||
              fake_ptr->GetLastDetail() == "force2");
}

}  // namespace core
}  // namespace ray
