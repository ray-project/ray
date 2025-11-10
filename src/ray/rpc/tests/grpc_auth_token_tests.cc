// Copyright 2021 The Ray Authors.
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

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "ray/rpc/authentication/authentication_token_loader.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/tests/grpc_test_common.h"
#include "ray/util/env.h"
#include "src/ray/protobuf/test_service.grpc.pb.h"

namespace ray {
namespace rpc {

class TestGrpcServerClientTokenAuthFixture : public ::testing::Test {
 public:
  void SetUp() override {
    // Configure token auth via RayConfig
    std::string config_json = R"({"auth_mode": "token"})";
    RayConfig::instance().initialize(config_json);
    AuthenticationTokenLoader::instance().ResetCache();
  }

  void SetUpServerAndClient(const std::string &server_token,
                            const std::string &client_token) {
    // Set client token in environment for ClientCallManager to read from
    // AuthenticationTokenLoader
    if (!client_token.empty()) {
      ray::SetEnv("RAY_AUTH_TOKEN", client_token);
    } else {
      RayConfig::instance().initialize(R"({"auth_mode": "disabled"})");
      AuthenticationTokenLoader::instance().ResetCache();
      ray::UnsetEnv("RAY_AUTH_TOKEN");
    }

    // Start client thread FIRST
    client_thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
          client_io_service_work_(client_io_service_.get_executor());
      client_io_service_.run();
    });

    // Start handler thread for server
    handler_thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
          handler_io_service_work_(handler_io_service_.get_executor());
      handler_io_service_.run();
    });

    // Create and start server
    // Pass server token explicitly for testing scenarios with different tokens
    std::optional<AuthenticationToken> server_auth_token;
    if (!server_token.empty()) {
      server_auth_token = AuthenticationToken(server_token);
    } else {
      // Explicitly set empty token (no auth required)
      server_auth_token = AuthenticationToken("");
    }
    grpc_server_.reset(new GrpcServer("test", 0, true, 1, 7200000, server_auth_token));
    grpc_server_->RegisterService(
        std::make_unique<TestGrpcService>(handler_io_service_, test_service_handler_),
        false);
    grpc_server_->Run();

    while (grpc_server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Create client (will read auth token from AuthenticationTokenLoader which reads the
    // environment)
    client_call_manager_.reset(
        new ClientCallManager(client_io_service_, false, /*local_address=*/""));
    grpc_client_.reset(new GrpcClient<TestService>(
        "127.0.0.1", grpc_server_->GetPort(), *client_call_manager_));
  }

  void TearDown() override {
    if (grpc_client_) {
      grpc_client_.reset();
    }
    if (client_call_manager_) {
      client_call_manager_.reset();
    }
    if (client_thread_) {
      client_io_service_.stop();
      if (client_thread_->joinable()) {
        client_thread_->join();
      }
    }

    if (grpc_server_) {
      grpc_server_->Shutdown();
    }
    if (handler_thread_) {
      handler_io_service_.stop();
      if (handler_thread_->joinable()) {
        handler_thread_->join();
      }
    }

    // Clean up environment variables
    ray::UnsetEnv("RAY_AUTH_TOKEN");
    ray::UnsetEnv("RAY_AUTH_TOKEN_PATH");
    // Reset the token loader for test isolation
    AuthenticationTokenLoader::instance().ResetCache();
  }

  // Helper to execute RPC and wait for result
  struct PingResult {
    bool completed;
    bool success;
    std::string error_msg;
  };

  PingResult ExecutePingAndWait() {
    PingRequest request;
    auto result_promise = std::make_shared<std::promise<PingResult>>();
    std::future<PingResult> result_future = result_promise->get_future();

    Ping(request, [result_promise](const Status &status, const PingReply &reply) {
      RAY_LOG(INFO) << "Token auth test replied, status=" << status;
      bool success = status.ok();
      std::string error_msg = status.ok() ? "" : status.message();
      result_promise->set_value({true, success, error_msg});
    });

    // Wait for response with timeout
    if (result_future.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
      return {false, false, "Request timed out"};
    }

    return result_future.get();
  }

 protected:
  VOID_RPC_CLIENT_METHOD(TestService, Ping, grpc_client_, /*method_timeout_ms*/ -1, )

  TestServiceHandler test_service_handler_;
  instrumented_io_context handler_io_service_;
  std::unique_ptr<std::thread> handler_thread_;
  std::unique_ptr<GrpcServer> grpc_server_;

  instrumented_io_context client_io_service_;
  std::unique_ptr<std::thread> client_thread_;
  std::unique_ptr<ClientCallManager> client_call_manager_;
  std::unique_ptr<GrpcClient<TestService>> grpc_client_;
};

TEST_F(TestGrpcServerClientTokenAuthFixture, TestTokenAuthSuccess) {
  // Both server and client have the same token
  const std::string token = "test_secret_token_123";
  SetUpServerAndClient(token, token);

  auto result = ExecutePingAndWait();

  ASSERT_TRUE(result.completed) << "Request did not complete in time";
  ASSERT_TRUE(result.success) << "Request should succeed with matching token";
}

TEST_F(TestGrpcServerClientTokenAuthFixture, TestTokenAuthFailureWrongToken) {
  // Server and client have different tokens
  SetUpServerAndClient("server_token", "wrong_client_token");

  auto result = ExecutePingAndWait();

  ASSERT_TRUE(result.completed) << "Request did not complete in time";
  ASSERT_FALSE(result.success) << "Request should fail with wrong client token";
  ASSERT_TRUE(result.error_msg.find(
                  "InvalidAuthToken: Authentication token is missing or incorrect") !=
              std::string::npos)
      << "Error message should contain token auth error. Got: " << result.error_msg;
}

TEST_F(TestGrpcServerClientTokenAuthFixture, TestTokenAuthFailureMissingToken) {
  // Server expects token, client doesn't send one (empty token)
  SetUpServerAndClient("server_token", "");

  auto result = ExecutePingAndWait();

  ASSERT_TRUE(result.completed) << "Request did not complete in time";
  // If the server has a token but the client doesn't, auth should fail
  ASSERT_FALSE(result.success)
      << "Request should fail when client doesn't provide required token";
}

TEST_F(TestGrpcServerClientTokenAuthFixture,
       TestClientProvidesTokenServerDoesNotRequire) {
  // Client provides token, but server doesn't require one (should succeed)
  SetUpServerAndClient("", "client_token");

  auto result = ExecutePingAndWait();

  ASSERT_TRUE(result.completed) << "Request did not complete in time";
  // Server should accept request even though client sent unnecessary token
  ASSERT_TRUE(result.success)
      << "Request should succeed when server doesn't require token";
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
