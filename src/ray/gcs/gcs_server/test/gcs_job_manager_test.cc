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

  Status AsyncPut(const std::string &table_name,
                  const std::string &key,
                  const std::string &data,
                  const gcs::StatusCallback &callback) override {
    callback(Status::OK());
    return Status::OK();
  }
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
  auto add_job_request1 = Mocker::GenAddJobRequest(job_id1, "namespace_1", 4);

  rpc::AddJobReply empty_reply;

  gcs_job_manager.HandleAddJob(
      *add_job_request1,
      &empty_reply,
      [](Status, std::function<void()>, std::function<void()>) {});
  auto add_job_request2 = Mocker::GenAddJobRequest(job_id2, "namespace_2", 8);
  gcs_job_manager.HandleAddJob(
      *add_job_request2,
      &empty_reply,
      [](Status, std::function<void()>, std::function<void()>) {});

  auto job_config1 = gcs_job_manager.GetJobConfig(job_id1);
  ASSERT_EQ("namespace_1", job_config1->ray_namespace());
  ASSERT_EQ(4, job_config1->num_java_workers_per_process());

  auto job_config2 = gcs_job_manager.GetJobConfig(job_id2);
  ASSERT_EQ("namespace_2", job_config2->ray_namespace());
  ASSERT_EQ(8, job_config2->num_java_workers_per_process());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
}  // namespace ray
