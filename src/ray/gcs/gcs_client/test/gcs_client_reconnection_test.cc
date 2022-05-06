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

namespace ray {
class GcsClientReconnectionTest : public ::testing::Test {
 public:
  void StartGCS() {
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
  }
  void ShutdownGCS() {
    if (server_io_service_) {
      server_io_service_->stop();
      server_io_service_.reset();
    }

    if (server_io_service_thread_) {
      server_io_service_thread_->join();
      server_io_service_thread_.reset();
    }

    if (gcs_server_) {
      gcs_server_->Stop();
      gcs_server_.reset();
    }
  }

  gcs::GcsClient *CreateGCSClient() {
    client_io_service_ = std::make_unique<instrumented_io_context>();
    client_io_service_thread_ = std::make_unique<std::thread>([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*client_io_service_));
      client_io_service_->run();
    });
    gcs::GcsClientOptions options("127.0.0.1:5397");
    gcs_client_ = std::make_unique<gcs::GcsClient>(options);
    RAY_CHECK_OK(gcs_client_->Connect(*client_io_service_));
    return gcs_client_.get();
  }

  void CloseGCSClient() {
    if (client_io_service_) {
      client_io_service_->stop();
      client_io_service_.reset();
    }
    if (client_io_service_thread_) {
      client_io_service_thread_->join();
      client_io_service_thread_.reset();
    }

    if (gcs_client_) {
      gcs_client_->Disconnect();
      gcs_client_.reset();
    }
  }

 protected:
  void SetUp() override {
    config_.redis_address = "127.0.0.1";
    config_.enable_sharding_conn = false;
    config_.redis_port = TEST_REDIS_SERVER_PORTS.front();
    config_.grpc_server_port = 5397;
    config_.grpc_server_name = "MockedGcsServer";
    config_.grpc_server_thread_num = 1;
    config_.node_ip_address = "127.0.0.1";
    config_.enable_sharding_conn = false;

    server_io_service_ = std::make_unique<instrumented_io_context>();
  }

  void TearDown() override {
    ShutdownGCS();
    CloseGCSClient();
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

TEST_F(GcsClientReconnectionTest, ReconnectionBasic) {}

TEST_F(GcsClientReconnectionTest, QueueingAndBlocking) {}

TEST_F(GcsClientReconnectionTest, QueueingAndBlocking) {}

}  // namespace ray
