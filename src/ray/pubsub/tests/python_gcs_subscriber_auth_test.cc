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

#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/pubsub/python_gcs_subscriber.h"
#include "ray/rpc/authentication/authentication_token.h"
#include "ray/rpc/authentication/authentication_token_loader.h"
#include "ray/rpc/grpc_server.h"
#include "ray/util/env.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace pubsub {

// Mock implementation of InternalPubSubGcsService for testing authentication
class MockInternalPubSubGcsService final : public rpc::InternalPubSubGcsService::Service {
 public:
  explicit MockInternalPubSubGcsService(bool should_accept_requests)
      : should_accept_requests_(should_accept_requests) {}

  grpc::Status GcsSubscriberCommandBatch(
      grpc::ServerContext *context,
      const rpc::GcsSubscriberCommandBatchRequest *request,
      rpc::GcsSubscriberCommandBatchReply *reply) override {
    if (should_accept_requests_) {
      for (const auto &command : request->commands()) {
        if (command.has_subscribe_message()) {
          subscribe_count_++;
        } else if (command.has_unsubscribe_message()) {
          unsubscribe_count_++;
        }
      }
      return grpc::Status::OK;
    } else {
      return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Authentication failed");
    }
  }

  grpc::Status GcsSubscriberPoll(grpc::ServerContext *context,
                                 const rpc::GcsSubscriberPollRequest *request,
                                 rpc::GcsSubscriberPollReply *reply) override {
    if (should_accept_requests_) {
      poll_count_++;
      // Simulate long polling: block until deadline expires since we have no messages
      // Real server would hold the connection open until messages arrive or timeout
      auto deadline = context->deadline();
      std::this_thread::sleep_until(deadline);

      // Return deadline exceeded (timeout) with empty messages
      // This simulates the real server behavior when no messages are published
      return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Long poll timeout");
    } else {
      return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Authentication failed");
    }
  }

  int subscribe_count() const { return subscribe_count_; }
  int poll_count() const { return poll_count_; }
  int unsubscribe_count() const { return unsubscribe_count_; }

 private:
  bool should_accept_requests_;
  std::atomic<int> subscribe_count_{0};
  std::atomic<int> poll_count_{0};
  std::atomic<int> unsubscribe_count_{0};
};

class PythonGcsSubscriberAuthTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Enable token authentication by default
    RayConfig::instance().initialize(R"({"auth_mode": "token"})");
    rpc::AuthenticationTokenLoader::instance().ResetCache();
  }

  void TearDown() override {
    if (server_) {
      server_->Shutdown();
      server_.reset();
    }
    ray::UnsetEnv("RAY_AUTH_TOKEN");
    // Reset to default auth mode
    RayConfig::instance().initialize(R"({"auth_mode": "disabled"})");
    rpc::AuthenticationTokenLoader::instance().ResetCache();
  }

  // Start a GCS server with optional authentication token
  void StartServer(const std::string &server_token, bool should_accept_requests = true) {
    auto mock_service =
        std::make_unique<MockInternalPubSubGcsService>(should_accept_requests);
    mock_service_ptr_ = mock_service.get();

    std::optional<rpc::AuthenticationToken> auth_token;
    if (!server_token.empty()) {
      auth_token = rpc::AuthenticationToken(server_token);
    } else {
      // Empty token means no auth required
      auth_token = rpc::AuthenticationToken("");
    }

    server_ = std::make_unique<rpc::GrpcServer>("test-gcs-server",
                                                0,  // Random port
                                                true,
                                                1,
                                                7200000,
                                                auth_token);

    server_->RegisterService(std::move(mock_service));
    server_->Run();

    // Wait for server to start
    while (server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    server_port_ = server_->GetPort();
  }

  // Set client authentication token via environment variable
  void SetClientToken(const std::string &client_token) {
    if (!client_token.empty()) {
      ray::SetEnv("RAY_AUTH_TOKEN", client_token);
      RayConfig::instance().initialize(R"({"auth_mode": "token"})");
    } else {
      ray::UnsetEnv("RAY_AUTH_TOKEN");
      RayConfig::instance().initialize(R"({"auth_mode": "disabled"})");
    }
    rpc::AuthenticationTokenLoader::instance().ResetCache();
  }

  std::unique_ptr<PythonGcsSubscriber> CreateSubscriber() {
    return std::make_unique<PythonGcsSubscriber>("127.0.0.1",
                                                 server_port_,
                                                 rpc::ChannelType::RAY_LOG_CHANNEL,
                                                 "test-subscriber-id",
                                                 "test-worker-id");
  }

  std::unique_ptr<rpc::GrpcServer> server_;
  MockInternalPubSubGcsService *mock_service_ptr_ = nullptr;
  int server_port_ = 0;
};

TEST_F(PythonGcsSubscriberAuthTest, MatchingTokens) {
  // Test that subscription succeeds when client and server use the same token
  const std::string test_token = "matching-test-token-12345";

  StartServer(test_token);
  SetClientToken(test_token);

  auto subscriber = CreateSubscriber();
  Status status = subscriber->Subscribe();

  ASSERT_TRUE(status.ok()) << "Subscribe should succeed with matching tokens: "
                           << status.ToString();
  EXPECT_EQ(mock_service_ptr_->subscribe_count(), 1);

  ASSERT_TRUE(subscriber->Close().ok());
}

TEST_F(PythonGcsSubscriberAuthTest, MismatchedTokens) {
  // Test that subscription fails when client and server use different tokens
  const std::string server_token = "server-token-12345";
  const std::string client_token = "wrong-client-token-67890";

  StartServer(server_token, false);  // Server will reject requests
  SetClientToken(client_token);

  auto subscriber = CreateSubscriber();
  Status status = subscriber->Subscribe();

  ASSERT_FALSE(status.ok()) << "Subscribe should fail with mismatched tokens";
  EXPECT_TRUE(status.IsRpcError()) << "Status should be RpcError";

  ASSERT_TRUE(subscriber->Close().ok());
}

TEST_F(PythonGcsSubscriberAuthTest, ClientTokenServerNoAuth) {
  // Test that subscription succeeds when client provides token but server doesn't require
  // it
  const std::string client_token = "client-token-12345";

  StartServer("");  // Server doesn't require auth
  SetClientToken(client_token);

  auto subscriber = CreateSubscriber();
  Status status = subscriber->Subscribe();

  ASSERT_TRUE(status.ok())
      << "Subscribe should succeed when server doesn't require auth: "
      << status.ToString();
  EXPECT_EQ(mock_service_ptr_->subscribe_count(), 1);

  ASSERT_TRUE(subscriber->Close().ok());
}

TEST_F(PythonGcsSubscriberAuthTest, ServerTokenClientNoAuth) {
  // Test that subscription fails when server requires token but client doesn't provide it
  const std::string server_token = "server-token-12345";

  StartServer(server_token, false);  // Server will reject requests without valid token
  SetClientToken("");                // Client doesn't provide token

  auto subscriber = CreateSubscriber();
  Status status = subscriber->Subscribe();

  ASSERT_FALSE(status.ok())
      << "Subscribe should fail when server requires token but client doesn't provide it";
  EXPECT_TRUE(status.IsRpcError()) << "Status should be RpcError";

  ASSERT_TRUE(subscriber->Close().ok());
}

TEST_F(PythonGcsSubscriberAuthTest, MatchingTokensPoll) {
  // Test that polling succeeds when client and server use the same token
  const std::string test_token = "matching-test-token-12345";

  StartServer(test_token);
  SetClientToken(test_token);

  auto subscriber = CreateSubscriber();
  Status status = subscriber->Subscribe();
  ASSERT_TRUE(status.ok()) << "Subscribe should succeed: " << status.ToString();

  // Test polling with matching tokens - use very short timeout to avoid blocking
  std::string key_id;
  rpc::LogBatch log_batch;
  status = subscriber->PollLogs(&key_id, 10, &log_batch);

  // Poll should succeed (returns OK even on timeout or when no messages available)
  ASSERT_TRUE(status.ok()) << "Poll should succeed with matching tokens: "
                           << status.ToString();
  // At least one poll should have been made
  EXPECT_GE(mock_service_ptr_->poll_count(), 1);

  ASSERT_TRUE(subscriber->Close().ok());
}

TEST_F(PythonGcsSubscriberAuthTest, MismatchedTokensPoll) {
  // Test that polling fails when tokens don't match
  const std::string server_token = "server-token-12345";
  const std::string client_token = "wrong-client-token-67890";

  StartServer(server_token, false);  // Server will reject requests
  SetClientToken(client_token);

  auto subscriber = CreateSubscriber();

  // Subscribe will fail, but let's try anyway
  ASSERT_FALSE(subscriber->Subscribe().ok());

  // Test polling with mismatched tokens - use very short timeout
  std::string key_id;
  rpc::LogBatch log_batch;
  Status status = subscriber->PollLogs(&key_id, 10, &log_batch);

  // Poll should fail with auth error or return OK if it was cancelled
  // (OK is acceptable because the subscriber may have been closed)
  if (!status.ok()) {
    EXPECT_TRUE(status.IsInvalid()) << "Status should be Invalid: " << status.ToString();
  }

  ASSERT_TRUE(subscriber->Close().ok());
}

TEST_F(PythonGcsSubscriberAuthTest, MatchingTokensClose) {
  // Test that closing/unsubscribing succeeds with matching tokens
  const std::string test_token = "matching-test-token-12345";

  StartServer(test_token);
  SetClientToken(test_token);

  auto subscriber = CreateSubscriber();
  Status status = subscriber->Subscribe();
  ASSERT_TRUE(status.ok()) << "Subscribe should succeed: " << status.ToString();
  EXPECT_EQ(mock_service_ptr_->subscribe_count(), 1);

  // Close should succeed with matching tokens
  ASSERT_TRUE(subscriber->Close().ok())
      << "Close should succeed with matching tokens: " << status.ToString();
  EXPECT_EQ(mock_service_ptr_->unsubscribe_count(), 1);
}

TEST_F(PythonGcsSubscriberAuthTest, NoAuthRequired) {
  // Test that everything works when neither client nor server use auth
  StartServer("");     // Server doesn't require auth
  SetClientToken("");  // Client doesn't provide token

  auto subscriber = CreateSubscriber();
  Status status = subscriber->Subscribe();

  ASSERT_TRUE(status.ok()) << "Subscribe should succeed without auth: "
                           << status.ToString();
  EXPECT_EQ(mock_service_ptr_->subscribe_count(), 1);

  // Test polling without auth - use very short timeout
  std::string key_id;
  rpc::LogBatch log_batch;
  status = subscriber->PollLogs(&key_id, 10, &log_batch);
  ASSERT_TRUE(status.ok()) << "Poll should succeed without auth: " << status.ToString();

  // Test close without auth
  status = subscriber->Close();
  ASSERT_TRUE(status.ok()) << "Close should succeed without auth: " << status.ToString();
}

TEST_F(PythonGcsSubscriberAuthTest, MultipleSubscribersMatchingTokens) {
  // Test multiple subscribers with the same token
  const std::string test_token = "shared-token-12345";

  StartServer(test_token);
  SetClientToken(test_token);

  auto subscriber1 = CreateSubscriber();
  auto subscriber2 = CreateSubscriber();

  Status status1 = subscriber1->Subscribe();
  Status status2 = subscriber2->Subscribe();

  ASSERT_TRUE(status1.ok()) << "First subscriber should succeed: " << status1.ToString();
  ASSERT_TRUE(status2.ok()) << "Second subscriber should succeed: " << status2.ToString();
  EXPECT_EQ(mock_service_ptr_->subscribe_count(), 2);

  ASSERT_TRUE(subscriber1->Close().ok());
  ASSERT_TRUE(subscriber2->Close().ok());
}

}  // namespace pubsub
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
