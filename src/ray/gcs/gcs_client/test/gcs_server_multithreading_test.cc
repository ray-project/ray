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

#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_client/service_based_accessor.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {

class GcsServerMultithreadingTest : public ::testing::Test {
 public:
  GcsServerMultithreadingTest() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  virtual ~GcsServerMultithreadingTest() { TestSetupUtil::ShutDownRedisServers(); }

 protected:
  void SetUp() override {
    config_.grpc_server_port = 0;
    config_.grpc_server_name = "MockedGcsServer";
    config_.grpc_server_thread_num = 10;
    config_.redis_address = "127.0.0.1";
    config_.is_test = true;
    config_.redis_port = TEST_REDIS_SERVER_PORTS.front();

    gcs_clients_.resize(number_of_gcs_client_, nullptr);
    client_io_service_threads_.resize(number_of_gcs_client_, nullptr);
    client_io_services_.resize(number_of_gcs_client_, nullptr);

    for (int index = 0; index < number_of_gcs_client_; ++index) {
      client_io_services_[index].reset(new boost::asio::io_service());
      client_io_service_threads_[index].reset(new std::thread([this, index] {
        std::unique_ptr<boost::asio::io_service::work> work(
            new boost::asio::io_service::work(*client_io_services_[index]));
        client_io_services_[index]->run();
      }));
    }

    server_io_service_.reset(new boost::asio::io_service());
    gcs_server_.reset(new gcs::GcsServer(config_, *server_io_service_));
    gcs_server_->Start();
    server_io_service_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*server_io_service_));
      server_io_service_->run();
    }));

    // Wait until server starts listening.
    while (!gcs_server_->IsStarted()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Create GCS client.
    gcs::GcsClientOptions options(config_.redis_address, config_.redis_port,
                                  config_.redis_password, config_.is_test);
    for (int index = 0; index < number_of_gcs_client_; ++index) {
      gcs_clients_[index].reset(new gcs::ServiceBasedGcsClient(options));
      RAY_CHECK_OK(gcs_clients_[index]->Connect(*client_io_services_[index]));
    }
  }

  void TearDown() override {
    for (int index = 0; index < number_of_gcs_client_; ++index) {
      client_io_services_[index]->stop();
      gcs_clients_[index]->Disconnect();
      client_io_service_threads_[index]->join();
    }

    gcs_server_->Stop();
    server_io_service_->stop();
    gcs_server_.reset();
    server_io_service_thread_->join();
    TestSetupUtil::FlushAllRedisServers();
  }

  bool SubscribeToFinishedJobs(
      const gcs::SubscribeCallback<JobID, rpc::JobTableData> &subscribe, std::shared_ptr<gcs::GcsClient> &gcs_client) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client->Jobs().AsyncSubscribeToFinishedJobs(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms) {
    auto status = future.wait_for(timeout_ms);
    return status == std::future_status::ready && future.get();
  }

  void WaitPendingDone(std::atomic<int> &current_count, int expected_count) {
    auto condition = [&current_count, expected_count]() {
      return current_count == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  // GCS server.
  gcs::GcsServerConfig config_;
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> server_io_service_thread_;
  std::unique_ptr<boost::asio::io_service> server_io_service_;

  // GCS client.
  int number_of_gcs_client_ = 10;
  std::vector<std::shared_ptr<gcs::GcsClient>> gcs_clients_;
  std::vector<std::shared_ptr<std::thread>> client_io_service_threads_;
  std::vector<std::shared_ptr<boost::asio::io_service>> client_io_services_;

  // Timeout waiting for GCS server reply, default is 2s.
  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(GcsServerMultithreadingTest, TestJobOperation) {
  // Subscribe to finished jobs.
  std::atomic<int> finished_job_count(0);
  auto on_subscribe = [&finished_job_count](const JobID &job_id,
                                            const gcs::JobTableData &data) {
    finished_job_count++;
  };
  ASSERT_TRUE(SubscribeToFinishedJobs(on_subscribe, gcs_clients_[0]));

  int job_count = 10000;

  // Create job table data.
  std::unordered_map<JobID, std::shared_ptr<rpc::JobTableData>> jobs;
  for (int index = 1; index <= job_count; ++index) {
    auto job_id = JobID::FromInt(index);
    auto job_table_data = Mocker::GenJobTableData(job_id);
    jobs[job_id] = job_table_data;
  }

  int index = 0;
  for (auto &job : jobs) {
    auto client_index = index++ % number_of_gcs_client_;
    RAY_LOG(INFO) << "My xxxxxxxxxx client_index is " << client_index;
    RAY_CHECK_OK(gcs_clients_[client_index]->Jobs().AsyncAdd(
        job.second, nullptr));
    RAY_CHECK_OK(gcs_clients_[client_index]->Jobs().AsyncMarkFinished(
        job.first, nullptr));
  }

  WaitPendingDone(finished_job_count, job_count);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::TEST_REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
