// Copyright 2020-2021 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_job_manager.h"

#include <memory>

// clang-format off
#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/pubsub/publisher.h"
// clang-format on

namespace ray {

class MockInMemoryStoreClient : public gcs::InMemoryStoreClient {
 public:
  explicit MockInMemoryStoreClient(instrumented_io_context &main_io_service)
      : gcs::InMemoryStoreClient(main_io_service) {}
};

class GcsJobManagerTest : public ::testing::Test {
 public:
  GcsJobManagerTest() : runtime_env_manager_(nullptr) {
    std::promise<bool> promise;
    thread_io_service_ = std::make_unique<std::thread>([this, &promise] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      promise.set_value(true);
      io_service_.run();
    });
    promise.get_future().get();

    gcs_publisher_ = std::make_shared<gcs::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    store_client_ = std::make_shared<MockInMemoryStoreClient>(io_service_);
    gcs_table_storage_ = std::make_shared<gcs::GcsTableStorage>(store_client_);
    kv_ = std::make_unique<gcs::MockInternalKVInterface>();
    function_manager_ = std::make_unique<gcs::GcsFunctionManager>(*kv_);
  }

  ~GcsJobManagerTest() {
    io_service_.stop();
    thread_io_service_->join();
  }

 protected:
  instrumented_io_context io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::unique_ptr<gcs::GcsFunctionManager> function_manager_;
  std::unique_ptr<gcs::MockInternalKVInterface> kv_;
  RuntimeEnvManager runtime_env_manager_;
  const std::chrono::milliseconds timeout_ms_{5000};
};

TEST_F(GcsJobManagerTest, TestGetJobConfig) {
  gcs::GcsJobManager gcs_job_manager(
      gcs_table_storage_, gcs_publisher_, runtime_env_manager_, *function_manager_);

  auto job_id1 = JobID::FromInt(1);
  auto job_id2 = JobID::FromInt(2);
  gcs::GcsInitData gcs_init_data(gcs_table_storage_);
  gcs_job_manager.Initialize(/*init_data=*/gcs_init_data);

  rpc::AddJobReply empty_reply;
  std::promise<bool> promise1;
  std::promise<bool> promise2;

  auto add_job_request1 = Mocker::GenAddJobRequest(job_id1, "namespace_1");
  gcs_job_manager.HandleAddJob(
      *add_job_request1,
      &empty_reply,
      [&promise1](Status, std::function<void()>, std::function<void()>) {
        promise1.set_value(true);
      });
  promise1.get_future().get();

  auto add_job_request2 = Mocker::GenAddJobRequest(job_id2, "namespace_2");
  gcs_job_manager.HandleAddJob(
      *add_job_request2,
      &empty_reply,
      [&promise2](Status, std::function<void()>, std::function<void()>) {
        promise2.set_value(true);
      });
  promise2.get_future().get();

  auto job_config1 = gcs_job_manager.GetJobConfig(job_id1);
  ASSERT_EQ("namespace_1", job_config1->ray_namespace());

  auto job_config2 = gcs_job_manager.GetJobConfig(job_id2);
  ASSERT_EQ("namespace_2", job_config2->ray_namespace());
}

TEST_F(GcsJobManagerTest, TestPreserveDriverInfo) {
  gcs::GcsJobManager gcs_job_manager(
      gcs_table_storage_, gcs_publisher_, runtime_env_manager_, *function_manager_);

  auto job_id = JobID::FromInt(1);
  gcs::GcsInitData gcs_init_data(gcs_table_storage_);
  gcs_job_manager.Initialize(/*init_data=*/gcs_init_data);
  auto add_job_request = Mocker::GenAddJobRequest(job_id, "namespace");
  add_job_request->mutable_data()->set_driver_ip_address("10.0.0.1");
  add_job_request->mutable_data()->set_driver_pid(8264);

  rpc::AddJobReply empty_reply;
  std::promise<bool> promise;

  gcs_job_manager.HandleAddJob(
      *add_job_request,
      &empty_reply,
      [&promise](Status, std::function<void()>, std::function<void()>) {
        promise.set_value(true);
      });
  promise.get_future().get();

  rpc::MarkJobFinishedRequest job_finished_request;
  rpc::MarkJobFinishedReply job_finished_reply;
  std::promise<bool> job_finished_promise;

  job_finished_request.set_job_id(JobID::FromInt(1).Binary());

  gcs_job_manager.HandleMarkJobFinished(
      job_finished_request,
      &job_finished_reply,
      [&job_finished_promise](Status, std::function<void()>, std::function<void()>) {
        job_finished_promise.set_value(true);
      });
  job_finished_promise.get_future().get();

  rpc::GetAllJobInfoRequest all_job_info_request;
  rpc::GetAllJobInfoReply all_job_info_reply;
  std::promise<bool> all_job_info_promise;

  gcs_job_manager.HandleGetAllJobInfo(
      all_job_info_request,
      &all_job_info_reply,
      [&all_job_info_promise](Status, std::function<void()>, std::function<void()>) {
        all_job_info_promise.set_value(true);
      });
  all_job_info_promise.get_future().get();

  ASSERT_EQ(all_job_info_reply.job_info_list().size(), 1);
  rpc::JobTableData data = all_job_info_reply.job_info_list().Get(0);
  ASSERT_EQ(data.driver_ip_address(), "10.0.0.1");
  ASSERT_EQ(data.driver_pid(), 8264);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
}  // namespace ray
