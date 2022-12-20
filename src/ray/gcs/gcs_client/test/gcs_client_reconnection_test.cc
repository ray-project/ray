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

#include <boost/asio/ip/tcp.hpp>
#include <chrono>

#include "absl/strings/substitute.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/util/util.h"

using namespace std::chrono_literals;
using namespace ray;
using namespace std::chrono;

class GcsClientReconnectionTest : public ::testing::Test {
 public:
  GcsClientReconnectionTest() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  ~GcsClientReconnectionTest() { TestSetupUtil::ShutDownRedisServers(); }

  void StartGCS() {
    RAY_CHECK(gcs_server_ == nullptr);
    server_io_service_ = std::make_unique<instrumented_io_context>();
    gcs_server_ = std::make_unique<gcs::GcsServer>(config_, *server_io_service_);
    gcs_server_->Start();
    server_io_service_thread_ = std::make_unique<std::thread>([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*server_io_service_));
      server_io_service_->run();
    });

    // Wait until server starts listening.
    while (!gcs_server_->IsStarted() || !CheckHealth()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  void ShutdownGCS() {
    if (!gcs_server_) {
      return;
    }

    server_io_service_->stop();
    server_io_service_thread_->join();
    gcs_server_->Stop();
    gcs_server_.reset();
  }

  bool CheckHealth() {
    auto channel =
        grpc::CreateChannel(absl::StrCat("127.0.0.1:", config_.grpc_server_port),
                            grpc::InsecureChannelCredentials());
    auto stub = grpc::health::v1::Health::NewStub(channel);
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 1s);
    ::grpc::health::v1::HealthCheckRequest request;
    ::grpc::health::v1::HealthCheckResponse reply;
    auto status = stub->Check(&context, request, &reply);
    if (!status.ok() ||
        reply.status() != ::grpc::health::v1::HealthCheckResponse::SERVING) {
      RAY_LOG(WARNING) << "Unable to reach GCS: " << status.error_code() << " "
                       << status.error_message();
      return false;
    }
    return true;
  }

  gcs::GcsClient *CreateGCSClient() {
    RAY_CHECK(gcs_client_ == nullptr);
    client_io_service_ = std::make_unique<instrumented_io_context>();
    client_io_service_thread_ = std::make_unique<std::thread>([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*client_io_service_));
      client_io_service_->run();
    });
    gcs::GcsClientOptions options("127.0.0.1:" +
                                  std::to_string(config_.grpc_server_port));
    gcs_client_ = std::make_unique<gcs::GcsClient>(options);
    RAY_CHECK_OK(gcs_client_->Connect(*client_io_service_));
    return gcs_client_.get();
  }

  void CloseGCSClient() {
    if (!gcs_client_) {
      return;
    }

    client_io_service_->stop();
    client_io_service_thread_->join();
    gcs_client_->Disconnect();
    gcs_client_.reset();
  }

  bool WaitUntil(std::function<bool()> predicate, std::chrono::nanoseconds timeout) {
    RAY_LOG(INFO) << "Waiting for " << timeout.count();
    auto start = steady_clock::now();
    while (steady_clock::now() - start <= timeout) {
      if (predicate()) {
        return true;
      }
      std::this_thread::sleep_for(100ms);
    }
    return false;
  }

 protected:
  unsigned short GetFreePort() {
    using namespace boost::asio;
    io_service service;
    ip::tcp::acceptor acceptor(service, ip::tcp::endpoint(ip::tcp::v4(), 0));
    unsigned short port = acceptor.local_endpoint().port();
    return port;
  }

  void SetUp() override {
    config_.redis_address = "127.0.0.1";
    config_.enable_sharding_conn = false;
    config_.redis_port = TEST_REDIS_SERVER_PORTS.front();
    config_.grpc_server_port = GetFreePort();
    config_.grpc_server_name = "MockedGcsServer";
    config_.grpc_server_thread_num = 1;
    config_.node_ip_address = "127.0.0.1";
    config_.enable_sharding_conn = false;
  }

  void TearDown() override {
    ShutdownGCS();
    CloseGCSClient();
    TestSetupUtil::FlushAllRedisServers();
  }

  // GCS server.
  gcs::GcsServerConfig config_;
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> server_io_service_thread_;
  std::unique_ptr<instrumented_io_context> server_io_service_;

  // GCS client.
  std::unique_ptr<std::thread> client_io_service_thread_;
  std::unique_ptr<instrumented_io_context> client_io_service_;
  std::unique_ptr<gcs::GcsClient> gcs_client_;

  // Timeout waiting for GCS server reply, default is 2s.
  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(GcsClientReconnectionTest, ReconnectionBasic) {
  RayConfig::instance().initialize(
      R"(
{
  "gcs_rpc_server_reconnect_timeout_s": 60,
  "gcs_storage": "redis"
}
  )");

  // Start GCS server
  StartGCS();

  // Create client and send KV request
  auto client = CreateGCSClient();

  std::promise<void> p0;
  auto f0 = p0.get_future();
  RAY_UNUSED(client->InternalKV().AsyncInternalKVPut(
      "", "A", "B", false, [&p0](auto status, auto) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p0.set_value();
      }));
  f0.get();

  // Shutdown GCS server
  ShutdownGCS();

  // Send get request
  std::promise<std::string> p1;
  auto f1 = p1.get_future();
  RAY_UNUSED(client->InternalKV().AsyncInternalKVGet("", "A", [&p1](auto status, auto p) {
    ASSERT_TRUE(status.ok()) << status.ToString();
    p1.set_value(*p);
  }));
  ASSERT_EQ(f1.wait_for(1s), std::future_status::timeout);

  // Make sure io context is not blocked
  std::promise<void> p2;
  client_io_service_->post([&p2]() { p2.set_value(); }, "");
  auto f2 = p2.get_future();
  f2.wait();

  // Resume GCS server
  StartGCS();

  // Make sure the request is executed
  ASSERT_EQ(f1.get(), "B");
}

TEST_F(GcsClientReconnectionTest, ReconnectionBackoff) {
  // This test is to ensure that during reconnection, we got the right status
  // of the channel and also very basic test to verify gRPC's backoff is working.
  RayConfig::instance().initialize(
      R"(
{
  "gcs_rpc_server_reconnect_timeout_s": 60,
  "gcs_storage": "redis",
  "gcs_grpc_initial_reconnect_backoff_ms": 2000,
  "gcs_grpc_max_reconnect_backoff_ms": 2000
}
  )");
  StartGCS();
  auto client = CreateGCSClient();
  std::promise<void> p1;
  auto f1 = p1.get_future();
  RAY_UNUSED(client->InternalKV().AsyncInternalKVPut(
      "", "A", "B", false, [&p1](auto status, auto) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p1.set_value();
      }));
  ASSERT_NE(f1.wait_for(1s), std::future_status::timeout);

  auto channel = client->GetGcsRpcClient().GetChannel();
  ASSERT_EQ(GRPC_CHANNEL_READY, channel->GetState(false));

  ShutdownGCS();

  RAY_UNUSED(
      client->InternalKV().AsyncInternalKVPut("", "A", "B", false, [](auto, auto) {}));

  ASSERT_TRUE(WaitUntil(
      [channel]() {
        auto status = channel->GetState(false);
        return status == GRPC_CHANNEL_TRANSIENT_FAILURE;
      },
      1s));

  auto now = std::chrono::steady_clock::now();
  StartGCS();

  // For 2s, there is no reconnection
  auto remaining = 1s - (std::chrono::steady_clock::now() - now);
  remaining = remaining < 0s ? 0s : remaining;

  ASSERT_FALSE(WaitUntil(
      [channel]() {
        auto status = channel->GetState(false);
        return status != GRPC_CHANNEL_TRANSIENT_FAILURE;
      },
      remaining));

  // Then there is reconnection
  ASSERT_TRUE(WaitUntil(
      [channel]() {
        auto status = channel->GetState(false);
        return status != GRPC_CHANNEL_TRANSIENT_FAILURE;
      },
      4s));

  // Eventually it should be ready.
  ASSERT_FALSE(WaitUntil(
      [channel]() {
        auto status = channel->GetState(false);
        return status != GRPC_CHANNEL_READY;
      },
      1s));
}

TEST_F(GcsClientReconnectionTest, QueueingAndBlocking) {
  RayConfig::instance().initialize(
      R"(
{
  "gcs_rpc_server_reconnect_timeout_s": 60,
  "gcs_storage": "redis",
  "gcs_grpc_max_request_queued_max_bytes": 10
}
  )");
  StartGCS();
  auto client = CreateGCSClient();
  std::promise<void> p1;
  auto f1 = p1.get_future();
  RAY_UNUSED(client->InternalKV().AsyncInternalKVPut(
      "", "A", "B", false, [&p1](auto status, auto) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p1.set_value();
      }));
  f1.get();

  ShutdownGCS();

  // Send one request which should fail
  RAY_UNUSED(client->InternalKV().AsyncInternalKVPut(
      "", "A", "B", false, [](auto status, auto) {}));

  // Make sure it's not blocking
  std::promise<void> p2;
  client_io_service_->post([&p2]() { p2.set_value(); }, "");
  auto f2 = p2.get_future();
  ASSERT_EQ(std::future_status::ready, f2.wait_for(1s));

  // Send the second one and it should block the thread
  RAY_UNUSED(client->InternalKV().AsyncInternalKVPut(
      "", "A", "B", false, [](auto status, auto) {}));
  std::this_thread::sleep_for(1s);
  std::promise<void> p3;
  client_io_service_->post([&p3]() { p3.set_value(); }, "");
  auto f3 = p3.get_future();
  ASSERT_EQ(std::future_status::timeout, f3.wait_for(1s));

  // Resume GCS server and it should unblock
  StartGCS();
  ASSERT_EQ(std::future_status::ready, f3.wait_for(5s));
}

TEST_F(GcsClientReconnectionTest, Timeout) {
  RayConfig::instance().initialize(
      R"(
{
  "gcs_rpc_server_reconnect_timeout_s": 60,
  "gcs_storage": "redis",
  "gcs_grpc_max_request_queued_max_bytes": 10,
  "gcs_server_request_timeout_seconds": 3
}
  )");
  StartGCS();
  auto client = CreateGCSClient();
  bool added = false;
  ASSERT_TRUE(client->InternalKV().Put("", "A", "B", false, added).ok());
  ASSERT_TRUE(added);

  ShutdownGCS();

  std::vector<std::string> values;
  ASSERT_TRUE(client->InternalKV().Keys("", "A", values).IsTimedOut());
  ASSERT_TRUE(values.empty());
  StartGCS();
  ASSERT_TRUE(client->InternalKV().Keys("", "A", values).ok());
  ASSERT_EQ(std::vector<std::string>{"A"}, values);
}

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
