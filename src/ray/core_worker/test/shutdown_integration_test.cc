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

#include "gtest/gtest.h"

#include "ray/core_worker/shutdown_coordinator.h"

// Mock CoreWorker for testing
class MockCoreWorker {
 public:
  MockCoreWorker() : disconnect_called_(false), shutdown_task_manager_called_(false) {}

  void Disconnect() { disconnect_called_ = true; }
  void ShutdownTaskManagerForTest() { shutdown_task_manager_called_ = true; }
  
  bool disconnect_called_;
  bool shutdown_task_manager_called_;
  
  WorkerType GetWorkerType() const { return WorkerType::WORKER; }
};

// Mock implementation of ShutdownDependencies for testing
class MockCoreWorkerShutdownDependencies : public ShutdownDependencies {
 public:
  explicit MockCoreWorkerShutdownDependencies(MockCoreWorker& core_worker)
      : core_worker_(core_worker) {}

  void DisconnectRaylet() override {
    core_worker_.Disconnect();
  }

  void DisconnectGcs() override {
    // No-op for test
  }

  void ShutdownTaskManager(bool force) override {
    core_worker_.ShutdownTaskManagerForTest();
  }

  void ShutdownObjectRecovery() override {
    // No-op for test
  }

  void CancelPendingTasks(bool force) override {
    // No-op for test
  }

  void CleanupActorState() override {
    // No-op for test
  }

  void FlushMetrics() override {
    // No-op for test
  }

  size_t GetPendingTaskCount() const override {
    return 0;
  }

  bool IsGracefulShutdownTimedOut(
      std::chrono::steady_clock::time_point start_time,
      std::chrono::milliseconds timeout) const override {
    return false;
  }

 private:
  MockCoreWorker& core_worker_;
};

class ShutdownIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_core_worker_ = std::make_unique<MockCoreWorker>();
    shutdown_dependencies_ = std::make_shared<MockCoreWorkerShutdownDependencies>(
        *mock_core_worker_);
    shutdown_coordinator_ = std::make_shared<ShutdownCoordinator>(
        shutdown_dependencies_, WorkerType::WORKER);
  }

  std::unique_ptr<MockCoreWorker> mock_core_worker_;
  std::shared_ptr<MockCoreWorkerShutdownDependencies> shutdown_dependencies_;
  std::shared_ptr<ShutdownCoordinator> shutdown_coordinator_;
};

TEST_F(ShutdownIntegrationTest, BasicIntegration) {
  // Verify initial state
  EXPECT_EQ(shutdown_coordinator_->GetCurrentState(), ShutdownState::Running);
  EXPECT_FALSE(mock_core_worker_->disconnect_called_);
  EXPECT_FALSE(mock_core_worker_->shutdown_task_manager_called_);

  // Request shutdown
  bool shutdown_requested = shutdown_coordinator_->RequestShutdown(
      false, ShutdownReason::IntentionalSystemExit, "test shutdown");
  
  EXPECT_TRUE(shutdown_requested);
  EXPECT_EQ(shutdown_coordinator_->GetCurrentState(), ShutdownState::Shutdown);
  
  // Verify that the shutdown dependencies were called
  EXPECT_TRUE(mock_core_worker_->disconnect_called_);
  EXPECT_TRUE(mock_core_worker_->shutdown_task_manager_called_);
}

TEST_F(ShutdownIntegrationTest, GracefulShutdown) {
  // Request graceful shutdown
  bool shutdown_requested = shutdown_coordinator_->RequestShutdown(
      false, ShutdownReason::IntentionalSystemExit, "graceful test shutdown");
  
  EXPECT_TRUE(shutdown_requested);
  EXPECT_EQ(shutdown_coordinator_->GetCurrentState(), ShutdownState::Shutdown);
}

TEST_F(ShutdownIntegrationTest, ForceShutdown) {
  // Request force shutdown
  bool shutdown_requested = shutdown_coordinator_->RequestShutdown(
      true, ShutdownReason::NodeFailure, "force test shutdown");
  
  EXPECT_TRUE(shutdown_requested);
  EXPECT_EQ(shutdown_coordinator_->GetCurrentState(), ShutdownState::Shutdown);
}

TEST_F(ShutdownIntegrationTest, IdempotentShutdown) {
  // First shutdown request
  bool first_shutdown = shutdown_coordinator_->RequestShutdown(
      false, ShutdownReason::IntentionalSystemExit, "first shutdown");
  EXPECT_TRUE(first_shutdown);
  
  // Second shutdown request should be idempotent
  bool second_shutdown = shutdown_coordinator_->RequestShutdown(
      false, ShutdownReason::IntentionalSystemExit, "second shutdown");
  EXPECT_FALSE(second_shutdown);  // Already shut down
  
  EXPECT_EQ(shutdown_coordinator_->GetCurrentState(), ShutdownState::Shutdown);
} 