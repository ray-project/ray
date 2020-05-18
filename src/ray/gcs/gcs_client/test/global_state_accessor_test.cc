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

#include "ray/gcs/gcs_client/global_state_accessor.h"
#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {

class GlobalStateAccessorTest : public RedisServiceManagerForTest {
 protected:
  void SetUp() override {
    config.grpc_server_port = 0;
    config.grpc_server_name = "MockedGcsServer";
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.is_test = true;
    config.redis_port = REDIS_SERVER_PORTS.front();
    gcs_server_.reset(new gcs::GcsServer(config));
    io_service_.reset(new boost::asio::io_service());

    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*io_service_));
      io_service_->run();
    }));

    thread_gcs_server_.reset(new std::thread([this] { gcs_server_->Start(); }));

    // Wait until server starts listening.
    while (!gcs_server_->IsStarted()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Create GCS client.
    gcs::GcsClientOptions options(config.redis_address, config.redis_port,
                                  config.redis_password, config.is_test);
    gcs_client_.reset(new gcs::ServiceBasedGcsClient(options));
    RAY_CHECK_OK(gcs_client_->Connect(*io_service_));

    // Create global state.
    std::stringstream address;
    address << config.redis_address << ":" << config.redis_port;
    global_state_.reset(new gcs::GlobalStateAccessor(address.str(), "", true));
    RAY_CHECK(global_state_->Connect());
  }

  void TearDown() override {
    gcs_server_->Stop();
    io_service_->stop();
    thread_io_service_->join();
    thread_gcs_server_->join();
    gcs_client_->Disconnect();
    global_state_->Disconnect();
    global_state_.reset();
    FlushAll();
  }

  bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms) {
    auto status = future.wait_for(timeout_ms);
    return status == std::future_status::ready && future.get();
  }

  // GCS server.
  gcs::GcsServerConfig config;
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<std::thread> thread_gcs_server_;
  std::unique_ptr<boost::asio::io_service> io_service_;

  // GCS client.
  std::unique_ptr<gcs::GcsClient> gcs_client_;

  std::unique_ptr<gcs::GlobalStateAccessor> global_state_;

  // Timeout waiting for GCS server reply, default is 2s.
  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(GlobalStateAccessorTest, TestJobTable) {
  int job_count = 100;
  ASSERT_EQ(global_state_->GetAllJobInfo().size(), 0);
  for (int index = 0; index < job_count; ++index) {
    auto job_id = JobID::FromInt(index);
    auto job_table_data = Mocker::GenJobTableData(job_id);
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncAdd(
        job_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    WaitReady(promise.get_future(), timeout_ms_);
  }
  ASSERT_EQ(global_state_->GetAllJobInfo().size(), job_count);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
