// Copyright 2022 The Ray Authors.
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

#include "ray/gcs/gcs_client/usage_stats_client.h"

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_server.h"

using namespace ray;

class UsageStatsClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.redis_address = "";
    config_.redis_port = 0;
    config_.grpc_server_port = 0;
    config_.grpc_server_name = "MockedGcsServer";
    config_.grpc_server_thread_num = 1;
    config_.node_ip_address = "127.0.0.1";
    config_.enable_sharding_conn = false;

    server_io_service_ = std::make_unique<instrumented_io_context>();
    gcs_server_ = std::make_unique<gcs::GcsServer>(config_, *server_io_service_);
    gcs_server_->Start();
    server_io_service_thread_ = std::make_unique<std::thread>([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*server_io_service_));
      server_io_service_->run();
    });

    // Wait until server starts listening.
    while (!gcs_server_->IsStarted()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    client_io_service_ = std::make_unique<instrumented_io_context>();
    client_io_service_thread_ = std::make_unique<std::thread>([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*client_io_service_));
      client_io_service_->run();
    });
    gcs::GcsClientOptions options("127.0.0.1:" + std::to_string(gcs_server_->GetPort()));
    gcs_client_ = std::make_unique<gcs::GcsClient>(options);
    RAY_CHECK_OK(gcs_client_->Connect(*client_io_service_));
  }

  void TearDown() override {
    client_io_service_->stop();
    client_io_service_thread_->join();
    gcs_client_->Disconnect();
    gcs_client_.reset();

    server_io_service_->stop();
    server_io_service_thread_->join();
    gcs_server_->Stop();
    gcs_server_.reset();
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
};

TEST_F(UsageStatsClientTest, TestRecordExtraUsageTag) {
  gcs::UsageStatsClient usage_stats_client(
      "127.0.0.1:" + std::to_string(gcs_server_->GetPort()), *client_io_service_);
  usage_stats_client.RecordExtraUsageTag(usage::TagKey::_TEST1, "value1");
  ASSERT_TRUE(WaitForCondition(
      [this]() {
        std::string value;
        RAY_CHECK_OK(this->gcs_client_->InternalKV().Get(
            "usage_stats", "extra_usage_tag__test1", value));
        return value == "value1";
      },
      10000));
  // Make sure the value is overriden for the same key.
  usage_stats_client.RecordExtraUsageTag(usage::TagKey::_TEST2, "value2");
  ASSERT_TRUE(WaitForCondition(
      [this]() {
        std::string value;
        RAY_CHECK_OK(this->gcs_client_->InternalKV().Get(
            "usage_stats", "extra_usage_tag__test2", value));
        return value == "value2";
      },
      10000));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
