// Copyright 2024 The Ray Authors.
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

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "mock/ray/gcs/gcs_kv_manager.h"
#include "mock/ray/pubsub/publisher.h"
#include "mock/ray/rpc/worker/core_worker_client.h"
#include "ray/common/test_utils.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/gcs/gcs_job_manager.h"
#include "ray/gcs/gcs_kv_manager.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/observability/fake_ray_event_recorder.h"

using json = nlohmann::json;

namespace ray {

class GcsJobManagerTest : public ::testing::Test {
 public:
  GcsJobManagerTest() : runtime_env_manager_(nullptr) {
    std::promise<bool> promise;
    thread_io_service_ = std::make_unique<std::thread>([this, &promise] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_.get_executor());
      promise.set_value(true);
      io_service_.run();
    });
    promise.get_future().get();

    gcs_publisher_ = std::make_shared<pubsub::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>();
    gcs_table_storage_ = std::make_shared<gcs::GcsTableStorage>(store_client_);
    kv_ = std::make_unique<gcs::MockInternalKVInterface>();
    fake_kv_ = std::make_unique<gcs::FakeInternalKVInterface>();
    function_manager_ = std::make_unique<gcs::GCSFunctionManager>(*kv_, io_service_);

    // Mock client factory which abuses the "address" argument to return a
    // CoreWorkerClient whose number of running tasks equal to the address port. This is
    // just for testing purposes.
    worker_client_pool_ =
        std::make_unique<rpc::CoreWorkerClientPool>([](const rpc::Address &address) {
          return std::make_shared<rpc::MockCoreWorkerClientConfigurableRunningTasks>(
              address.port());
        });
    fake_ray_event_recorder_ = std::make_unique<observability::FakeRayEventRecorder>();
    log_dir_ = "event_12345";
  }

  ~GcsJobManagerTest() {
    io_service_.stop();
    thread_io_service_->join();
    std::filesystem::remove_all(log_dir_.c_str());
  }

 protected:
  instrumented_io_context io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<pubsub::GcsPublisher> gcs_publisher_;
  std::unique_ptr<gcs::GCSFunctionManager> function_manager_;
  std::unique_ptr<gcs::MockInternalKVInterface> kv_;
  std::unique_ptr<gcs::FakeInternalKVInterface> fake_kv_;
  std::unique_ptr<rpc::CoreWorkerClientPool> worker_client_pool_;
  std::unique_ptr<observability::FakeRayEventRecorder> fake_ray_event_recorder_;
  RuntimeEnvManager runtime_env_manager_;
  const std::chrono::milliseconds timeout_ms_{5000};
  std::string log_dir_;
};

TEST_F(GcsJobManagerTest, TestRayEventDriverJobEvents) {
  RayConfig::instance().initialize(
      R"(
{
  "enable_ray_event": true
}
  )");
  gcs::GcsJobManager gcs_job_manager(*gcs_table_storage_,
                                     *gcs_publisher_,
                                     runtime_env_manager_,
                                     *function_manager_,
                                     *fake_kv_,
                                     io_service_,
                                     *worker_client_pool_,
                                     *fake_ray_event_recorder_,
                                     "test_session_name");
  gcs::GcsInitData gcs_init_data(*gcs_table_storage_);
  gcs_job_manager.Initialize(gcs_init_data);
  auto job_api_job_id = JobID::FromInt(100);
  std::string submission_id = "submission_id_100";
  auto add_job_request = GenAddJobRequest(job_api_job_id, "namespace_100", submission_id);
  rpc::AddJobReply empty_reply;
  std::promise<bool> promise;
  gcs_job_manager.HandleAddJob(
      *add_job_request,
      &empty_reply,
      [&promise](Status, std::function<void()>, std::function<void()>) {
        promise.set_value(true);
      });
  promise.get_future().get();
  auto buffer = fake_ray_event_recorder_->FlushBuffer();

  ASSERT_EQ(buffer.size(), 2);
  ASSERT_EQ(buffer[0]->GetEventType(),
            rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT);
  ASSERT_EQ(buffer[1]->GetEventType(), rpc::events::RayEvent::DRIVER_JOB_LIFECYCLE_EVENT);
}

TEST_F(GcsJobManagerTest, TestExportDriverJobEvents) {
  // Test adding and marking a driver job as finished, and that corresponding
  // export events are written.
  RayConfig::instance().initialize(
      R"(
{
  "enable_export_api_write": true
}
  )");
  const std::vector<ray::SourceTypeVariant> source_types = {
      rpc::ExportEvent_SourceType::ExportEvent_SourceType_EXPORT_DRIVER_JOB};
  RayEventInit_(source_types,
                absl::flat_hash_map<std::string, std::string>(),
                log_dir_,
                "warning",
                false);
  gcs::GcsJobManager gcs_job_manager(*gcs_table_storage_,
                                     *gcs_publisher_,
                                     runtime_env_manager_,
                                     *function_manager_,
                                     *fake_kv_,
                                     io_service_,
                                     *worker_client_pool_,
                                     *fake_ray_event_recorder_,
                                     "test_session_name");

  gcs::GcsInitData gcs_init_data(*gcs_table_storage_);
  gcs_job_manager.Initialize(gcs_init_data);

  auto job_api_job_id = JobID::FromInt(100);
  std::string submission_id = "submission_id_100";
  auto add_job_request = GenAddJobRequest(job_api_job_id, "namespace_100", submission_id);
  rpc::AddJobReply empty_reply;
  std::promise<bool> promise;
  gcs_job_manager.HandleAddJob(
      *add_job_request,
      &empty_reply,
      [&promise](Status, std::function<void()>, std::function<void()>) {
        promise.set_value(true);
      });
  promise.get_future().get();

  std::vector<std::string> vc;
  ReadContentFromFile(vc, log_dir_ + "/export_events/event_EXPORT_DRIVER_JOB.log");
  ASSERT_EQ((int)vc.size(), 1);
  json event_data = json::parse(vc[0])["event_data"].get<json>();
  ASSERT_EQ(event_data["is_dead"], false);

  rpc::MarkJobFinishedRequest job_finished_request;
  rpc::MarkJobFinishedReply job_finished_reply;
  std::promise<bool> job_finished_promise;
  job_finished_request.set_job_id(JobID::FromInt(100).Binary());

  gcs_job_manager.HandleMarkJobFinished(
      job_finished_request,
      &job_finished_reply,
      [&job_finished_promise](Status, std::function<void()>, std::function<void()>) {
        job_finished_promise.set_value(true);
      });
  job_finished_promise.get_future().get();

  vc.clear();
  ReadContentFromFile(vc, log_dir_ + "/export_events/event_EXPORT_DRIVER_JOB.log");
  ASSERT_EQ((int)vc.size(), 2);
  event_data = json::parse(vc[1])["event_data"].get<json>();
  ASSERT_EQ(event_data["is_dead"], true);
}
}  // namespace ray
