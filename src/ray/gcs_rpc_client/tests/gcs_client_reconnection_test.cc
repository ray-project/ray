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
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "absl/strings/substitute.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_server.h"
#include "ray/gcs_rpc_client/accessor.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/util/network_util.h"
#include "ray/util/path_utils.h"
#include "ray/util/raii.h"

using namespace std::chrono_literals;  // NOLINT
using namespace ray;                   // NOLINT
using namespace std::chrono;           // NOLINT

class GcsClientReconnectionTest : public ::testing::Test {
 public:
  GcsClientReconnectionTest() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  ~GcsClientReconnectionTest() { TestSetupUtil::ShutDownRedisServers(); }

  void StartGCS() {
    RAY_CHECK(gcs_server_ == nullptr);
    server_io_service_ = std::make_unique<instrumented_io_context>();

    // Create the metrics struct
    ray::gcs::GcsServerMetrics gcs_server_metrics{
        /*actor_by_state_gauge=*/actor_by_state_gauge_,
        /*gcs_actor_by_state_gauge=*/gcs_actor_by_state_gauge_,
        /*running_job_gauge=*/running_job_gauge_,
        /*finished_job_counter=*/finished_job_counter_,
        /*job_duration_in_seconds_gauge=*/job_duration_in_seconds_gauge_,
        /*placement_group_gauge=*/placement_group_gauge_,
        /*placement_group_creation_latency_in_ms_histogram=*/
        placement_group_creation_latency_in_ms_histogram_,
        /*placement_group_scheduling_latency_in_ms_histogram=*/
        placement_group_scheduling_latency_in_ms_histogram_,
        /*placement_group_count_gauge=*/placement_group_count_gauge_,
        /*task_events_reported_gauge=*/task_events_reported_gauge_,
        /*task_events_dropped_gauge=*/task_events_dropped_gauge_,
        /*task_events_stored_gauge=*/task_events_stored_gauge_,
        /*event_recorder_dropped_events_counter=*/fake_dropped_events_counter_,
        /*storage_operation_latency_in_ms_histogram=*/
        storage_operation_latency_in_ms_histogram_,
        /*storage_operation_count_counter=*/storage_operation_count_counter_,
        scheduler_placement_time_s_histogram_,
    };

    gcs_server_ = std::make_unique<gcs::GcsServer>(
        config_, gcs_server_metrics, *server_io_service_);
    gcs_server_->Start();
    server_io_service_thread_ = std::make_unique<std::thread>([this] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          server_io_service_->get_executor());
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
        grpc::CreateChannel(BuildAddress("127.0.0.1", config_.grpc_server_port),
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
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          client_io_service_->get_executor());
      client_io_service_->run();
    });
    gcs::GcsClientOptions options("127.0.0.1",
                                  config_.grpc_server_port,
                                  ClusterID::Nil(),
                                  /*allow_cluster_id_nil=*/true,
                                  /*fetch_cluster_id_if_nil=*/false);
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
    using namespace boost::asio;  // NOLINT
    io_service service;
    ip::tcp::acceptor acceptor(service, ip::tcp::endpoint(ip::tcp::v4(), 0));
    unsigned short port = acceptor.local_endpoint().port();
    return port;
  }

  void SetUp() override {
    config_.redis_address = "127.0.0.1";
    config_.redis_port = TEST_REDIS_SERVER_PORTS.front();
    config_.grpc_server_port = GetFreePort();
    config_.grpc_server_name = "MockedGcsServer";
    config_.grpc_server_thread_num = 1;
    config_.node_ip_address = "127.0.0.1";
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
  // Fake metrics for testing
  observability::FakeGauge actor_by_state_gauge_;
  observability::FakeGauge gcs_actor_by_state_gauge_;
  observability::FakeGauge running_job_gauge_;
  observability::FakeCounter finished_job_counter_;
  observability::FakeGauge job_duration_in_seconds_gauge_;
  observability::FakeGauge placement_group_gauge_;
  observability::FakeHistogram placement_group_creation_latency_in_ms_histogram_;
  observability::FakeHistogram placement_group_scheduling_latency_in_ms_histogram_;
  observability::FakeGauge placement_group_count_gauge_;
  observability::FakeGauge task_events_reported_gauge_;
  observability::FakeGauge task_events_dropped_gauge_;
  observability::FakeGauge task_events_stored_gauge_;
  observability::FakeHistogram storage_operation_latency_in_ms_histogram_;
  observability::FakeCounter storage_operation_count_counter_;
  observability::FakeCounter fake_dropped_events_counter_;
  observability::FakeHistogram scheduler_placement_time_s_histogram_;

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
      "", "A", "B", false, gcs::GetGcsTimeoutMs(), [&p0](auto status, auto) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p0.set_value();
      }));
  f0.get();

  // Shutdown GCS server
  ShutdownGCS();

  // Send get request
  std::promise<std::string> p1;
  auto f1 = p1.get_future();
  RAY_UNUSED(client->InternalKV().AsyncInternalKVGet(
      "", "A", gcs::GetGcsTimeoutMs(), [&p1](auto status, auto p) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p1.set_value(*p);
      }));
  ASSERT_EQ(std::future_status::timeout, f1.wait_for(1s));

  // Make sure io context is not blocked
  std::promise<void> p2;
  client_io_service_->post([&p2]() { p2.set_value(); }, "");
  auto f2 = p2.get_future();
  f2.wait();

  // Resume GCS server
  StartGCS();

  // Make sure the request is executed
  ASSERT_EQ("B", f1.get());
}

TEST_F(GcsClientReconnectionTest, ReconnectionBackoff) {
  // This test is to ensure that during reconnection, we got the right status
  // of the channel and also very basic test to verify gRPC's backoff is working.
  RayConfig::instance().initialize(
      R"(
{
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
      "", "A", "B", false, gcs::GetGcsTimeoutMs(), [&p1](auto status, auto) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p1.set_value();
      }));
  ASSERT_NE(f1.wait_for(1s), std::future_status::timeout);

  auto channel = client->GetGcsRpcClient().GetChannel();
  ASSERT_EQ(GRPC_CHANNEL_READY, channel->GetState(false));

  ShutdownGCS();

  std::promise<void> p2;
  auto f2 = p2.get_future();
  RAY_UNUSED(client->InternalKV().AsyncInternalKVPut(
      "", "A", "B", false, gcs::GetGcsTimeoutMs(), [&p2](auto status, auto) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p2.set_value();
      }));
  ASSERT_EQ(std::future_status::timeout, f2.wait_for(1s));

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
  "gcs_storage": "redis",
  "gcs_grpc_max_request_queued_max_bytes": 10
}
  )");
  StartGCS();
  auto client = CreateGCSClient();
  std::promise<void> p1;
  auto f1 = p1.get_future();
  RAY_UNUSED(client->InternalKV().AsyncInternalKVPut(
      "", "A", "B", false, gcs::GetGcsTimeoutMs(), [&p1](auto status, auto) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p1.set_value();
      }));
  f1.get();

  ShutdownGCS();

  // Send one request which should fail
  std::promise<void> p2;
  auto f2 = p2.get_future();
  RAY_UNUSED(client->InternalKV().AsyncInternalKVPut(
      "", "A", "B", false, gcs::GetGcsTimeoutMs(), [&p2](auto status, auto) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p2.set_value();
      }));
  ASSERT_EQ(std::future_status::timeout, f2.wait_for(1s));

  // Make sure it's not blocking
  std::promise<void> p3;
  client_io_service_->post([&p3]() { p3.set_value(); }, "");
  auto f3 = p3.get_future();
  ASSERT_EQ(std::future_status::ready, f3.wait_for(1s));

  // Send the second one and it should block the thread
  std::promise<void> p4;
  auto f4 = p4.get_future();
  RAY_UNUSED(client->InternalKV().AsyncInternalKVPut(
      "", "A", "B", false, gcs::GetGcsTimeoutMs(), [&p4](auto status, auto) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        p4.set_value();
      }));
  ASSERT_EQ(std::future_status::timeout, f4.wait_for(1s));

  std::promise<void> p5;
  client_io_service_->post([&p5]() { p5.set_value(); }, "");
  auto f5 = p5.get_future();
  ASSERT_EQ(std::future_status::timeout, f5.wait_for(1s));

  // Resume GCS server and it should unblock
  StartGCS();
  ASSERT_EQ(std::future_status::ready, f5.wait_for(5s));
  ASSERT_EQ(std::future_status::ready, f2.wait_for(1s));
  ASSERT_EQ(std::future_status::ready, f4.wait_for(1s));
}

TEST_F(GcsClientReconnectionTest, Timeout) {
  RayConfig::instance().initialize(
      R"(
{
  "gcs_storage": "redis",
  "gcs_grpc_max_request_queued_max_bytes": 10,
  "gcs_server_request_timeout_seconds": 10
}
  )");
  StartGCS();
  auto client = CreateGCSClient();
  bool added = false;
  ASSERT_TRUE(
      client->InternalKV().Put("", "A", "B", false, gcs::GetGcsTimeoutMs(), added).ok());
  ASSERT_TRUE(added);

  ShutdownGCS();
  std::vector<std::string> values;
  ASSERT_TRUE(
      client->InternalKV().Keys("", "A", gcs::GetGcsTimeoutMs(), values).IsTimedOut());
  ASSERT_TRUE(values.empty());

  StartGCS();
  ASSERT_TRUE(client->InternalKV().Keys("", "A", gcs::GetGcsTimeoutMs(), values).ok());
  ASSERT_EQ(std::vector<std::string>{"A"}, values);
}

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(
      ray::RayLog::StartRayLog,
      ray::RayLog::ShutDownRayLog,
      argv[0],
      ray::RayLogLevel::INFO,
      ray::GetLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::GetErrLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::RayLog::GetRayLogRotationMaxBytesOrDefault(),
      ray::RayLog::GetRayLogRotationBackupCountOrDefault());
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
