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

  // HealthCheck is a macro+template method that supposes to invoke the callback upon
  // the completion of an RPC call. We override it to invoke the callback directly
  // without the RPC call. Ideally we would create a GrpcClientMock that overrides
  // the RPC call. However, currently the RPC call is a template method, which cannot
  // be overridden.
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
  ASSERT_TRUE(client_->exporter_initialized_);
}

TEST_F(MetricsAgentClientTest, WaitForServerReadyWithRetryFailure) {
  client_->WaitForServerReadyWithRetry(
      [](const Status &server_status) { ASSERT_FALSE(server_status.ok()); },
      0,
      kCountToReturnOk - 2,
      kRetryIntervalMs);
  io_service_.run_for(std::chrono::milliseconds(kCountToReturnOk * kRetryIntervalMs));
  ASSERT_FALSE(client_->exporter_initialized_);
}

}  // namespace rpc
}  // namespace ray
