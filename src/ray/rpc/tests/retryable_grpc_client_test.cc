// Copyright 2026 The Ray Authors.
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

#include "ray/rpc/retryable_grpc_client.h"

#include <chrono>
#include <limits>
#include <memory>
#include <thread>

#include "gtest/gtest.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/tests/grpc_test_common.h"

namespace ray {
namespace rpc {

// Exercises RetryableGrpcClient::RetryOnTimeoutPolicy against a real server
// whose handler can be frozen: while frozen, every attempt fails with
// DEADLINE_EXCEEDED client-side.
class RetryableGrpcClientTimeoutRetryFixture : public ::testing::Test {
 public:
  void SetUp() override {
    handler_io_service_thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
          handler_io_service_work_(handler_io_service_.get_executor());
      handler_io_service_.run();
    });
    grpc_server_.reset(new GrpcServer("test-retry-on-timeout", 0, true));
    grpc_server_->RegisterService(
        std::make_unique<TestGrpcService>(handler_io_service_, test_service_handler_),
        false);
    grpc_server_->Run();
    while (grpc_server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    client_thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
          client_io_service_work_(client_io_service_.get_executor());
      client_io_service_.run();
    });
    client_call_manager_.reset(
        new ClientCallManager(client_io_service_, false, /*local_address=*/""));
    grpc_client_.reset(new GrpcClient<TestService>(
        "127.0.0.1", grpc_server_->GetPort(), *client_call_manager_));
    retryable_grpc_client_ = RetryableGrpcClient::Create(
        grpc_client_->Channel(),
        client_io_service_,
        /*max_pending_requests_bytes=*/
        std::numeric_limits<uint64_t>::max(),
        /*check_channel_status_interval_milliseconds=*/100,
        /*server_reconnect_timeout_base_seconds=*/60,
        /*server_reconnect_timeout_max_seconds=*/60,
        /*server_unavailable_timeout_callback=*/[]() {},
        /*server_name=*/"test-server");
  }

  void TearDown() override {
    test_service_handler_.frozen = false;
    retryable_grpc_client_.reset();
    grpc_client_.reset();
    client_call_manager_.reset();
    client_io_service_.stop();
    if (client_thread_->joinable()) {
      client_thread_->join();
    }
    grpc_server_->Shutdown();
    handler_io_service_.stop();
    if (handler_io_service_thread_->joinable()) {
      handler_io_service_thread_->join();
    }
  }

  // Sends a Ping under the given retry-on-timeout policy and blocks for the
  // final status.
  Status PingWithRetryOnTimeout(int64_t timeout_ms, int64_t per_attempt_timeout_ms) {
    std::promise<Status> promise;
    retryable_grpc_client_->CallMethod<TestService, PingRequest, PingReply>(
        &TestService::Stub::PrepareAsyncPing,
        grpc_client_,
        "TestService.grpc_client.Ping",
        PingRequest(),
        [&promise](const Status &status, PingReply &&reply) {
          promise.set_value(status);
        },
        timeout_ms,
        RetryableGrpcClient::RetryOnTimeoutPolicy{per_attempt_timeout_ms});
    return promise.get_future().get();
  }

 protected:
  // Server.
  TestServiceHandler test_service_handler_;
  instrumented_io_context handler_io_service_;
  std::unique_ptr<std::thread> handler_io_service_thread_;
  std::unique_ptr<GrpcServer> grpc_server_;
  // Client.
  instrumented_io_context client_io_service_;
  std::unique_ptr<std::thread> client_thread_;
  std::unique_ptr<ClientCallManager> client_call_manager_;
  std::shared_ptr<GrpcClient<TestService>> grpc_client_;
  std::shared_ptr<RetryableGrpcClient> retryable_grpc_client_;
};

TEST_F(RetryableGrpcClientTimeoutRetryFixture, RetriesTimedOutAttemptsUntilSuccess) {
  // Freeze the handler so early attempts exceed their 500 ms per-attempt
  // deadline, then unfreeze after ~1.5 s: a later attempt inside the 20 s
  // overall budget must succeed. Without the policy the first DEADLINE_EXCEEDED
  // would be final.
  test_service_handler_.frozen = true;
  std::thread unfreezer([this]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    test_service_handler_.frozen = false;
  });
  const Status status =
      PingWithRetryOnTimeout(/*timeout_ms=*/20000, /*per_attempt_timeout_ms=*/500);
  unfreezer.join();
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_GE(test_service_handler_.request_count, 2);
}

TEST_F(RetryableGrpcClientTimeoutRetryFixture, DeliversTimedOutWhenBudgetExhausted) {
  // Handler stays frozen: every attempt times out and the final status must be
  // TimedOut, delivered no earlier than the overall budget.
  test_service_handler_.frozen = true;
  const auto start = std::chrono::steady_clock::now();
  const Status status =
      PingWithRetryOnTimeout(/*timeout_ms=*/1500, /*per_attempt_timeout_ms=*/400);
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start)
                              .count();
  ASSERT_TRUE(status.IsTimedOut()) << status;
  // Slightly under the 1500 ms budget to tolerate clock-source skew between
  // the test's steady_clock and the client's absl::Now deadline.
  ASSERT_GE(elapsed_ms, 1400);
}

}  // namespace rpc
}  // namespace ray
