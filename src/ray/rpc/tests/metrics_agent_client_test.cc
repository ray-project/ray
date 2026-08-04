// Copyright 2024 The Ray Authors.
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

#include "ray/rpc/metrics_agent_client.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {
namespace rpc {

constexpr int kCountToReturnOk = 3;
constexpr int kRetryIntervalMs = 100;

class TestableMetricsAgentClientImpl : public MetricsAgentClientImpl {
 public:
  TestableMetricsAgentClientImpl(const std::string &address,
                                 const int port,
                                 instrumented_io_context &io_service,
                                 rpc::ClientCallManager &client_call_manager,
                                 int count_to_return_ok)
      : MetricsAgentClientImpl(address, port, io_service, client_call_manager),
        count_to_return_ok_(count_to_return_ok) {}

  void HealthCheck(const HealthCheckRequest &request,
                   const ClientCallback<HealthCheckReply> &callback) override {
    health_check_count_++;
    if (health_check_count_ <= count_to_return_ok_) {
      callback(Status::RpcError("Failed to connect to the metrics agent server.", 14),
               HealthCheckReply());
    } else {
      callback(Status::OK(), HealthCheckReply());
    }
  }

 private:
  int count_to_return_ok_;
  int health_check_count_ = 1;
};

class DeferredCallbackMetricsAgentClientImpl : public MetricsAgentClientImpl {
 public:
  using MetricsAgentClientImpl::MetricsAgentClientImpl;

  void HealthCheck(const HealthCheckRequest &request,
                   const ClientCallback<HealthCheckReply> &callback) override {
    pending_callbacks_.push_back(callback);
  }

  void InvokeAllPendingCallbacksWithOk() {
    for (auto &callback : pending_callbacks_) {
      callback(Status::OK(), HealthCheckReply());
    }
    pending_callbacks_.clear();
  }

  std::vector<ClientCallback<HealthCheckReply>> pending_callbacks_;
};

class MetricsAgentClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_call_manager_ = std::make_unique<ClientCallManager>(
        io_service_, /*record_stats=*/true, /*local_address=*/"");
    client_ = std::make_unique<TestableMetricsAgentClientImpl>(
        "127.0.0.1", 8000, io_service_, *client_call_manager_, kCountToReturnOk);
  }

  instrumented_io_context io_service_;
  std::unique_ptr<MetricsAgentClientImpl> client_;
  std::unique_ptr<ClientCallManager> client_call_manager_;
};

TEST_F(MetricsAgentClientTest, WaitForServerReadyWithRetrySuccess) {
  client_->WaitForServerReadyWithRetry(
      [](const Status &server_status) { ASSERT_TRUE(server_status.ok()); },
      0,
      kCountToReturnOk,
      kRetryIntervalMs);
  io_service_.run_for(std::chrono::milliseconds(kCountToReturnOk * kRetryIntervalMs));
  ASSERT_TRUE(client_->exporter_initialized_.load());
}

TEST_F(MetricsAgentClientTest, WaitForServerReadyWithRetryFailure) {
  client_->WaitForServerReadyWithRetry(
      [](const Status &server_status) { ASSERT_FALSE(server_status.ok()); },
      0,
      kCountToReturnOk - 2,
      kRetryIntervalMs);
  io_service_.run_for(std::chrono::milliseconds(kCountToReturnOk * kRetryIntervalMs));
  ASSERT_FALSE(client_->exporter_initialized_.load());
}

TEST_F(MetricsAgentClientTest, ConcurrentCallbacksCallInitExporterFnOnlyOnce) {
  auto deferred_client = std::make_unique<DeferredCallbackMetricsAgentClientImpl>(
      "127.0.0.1", 8000, io_service_, *client_call_manager_);

  int init_call_count = 0;
  auto init_fn = [&init_call_count](const Status &status) { init_call_count++; };

  deferred_client->WaitForServerReadyWithRetry(init_fn, 0, 10, kRetryIntervalMs);
  deferred_client->WaitForServerReadyWithRetry(init_fn, 0, 10, kRetryIntervalMs);

  deferred_client->InvokeAllPendingCallbacksWithOk();

  ASSERT_EQ(init_call_count, 1);
  ASSERT_TRUE(deferred_client->exporter_initialized_.load());
}

// Test that documents the expected behavior when all retries fail.
// This test validates that the callback receives a failure status when max retries
// are exhausted. In minimal installs, the fix is to check port > 0 BEFORE creating
// the client (in main.cc and core_worker_process.cc), so this code path won't be hit.
TEST_F(MetricsAgentClientTest, ExhaustedRetriesReturnsFailure) {
  // Create a client that always fails health checks (count_to_return_ok = INT_MAX)
  auto always_fail_client = std::make_unique<TestableMetricsAgentClientImpl>(
      "127.0.0.1", 8000, io_service_, *client_call_manager_, INT_MAX);

  bool callback_called = false;
  Status final_status;
  always_fail_client->WaitForServerReadyWithRetry(
      [&callback_called, &final_status](const Status &status) {
        callback_called = true;
        final_status = status;
      },
      0,
      2,  // max_retry = 2, exhaust retries
      kRetryIntervalMs);

  io_service_.run_for(std::chrono::milliseconds(kRetryIntervalMs * 5));

  // After exhausting retries, callback should be called with failure status.
  ASSERT_TRUE(callback_called);
  ASSERT_FALSE(final_status.ok());
  ASSERT_FALSE(always_fail_client->exporter_initialized_.load());
}

}  // namespace rpc
}  // namespace ray
