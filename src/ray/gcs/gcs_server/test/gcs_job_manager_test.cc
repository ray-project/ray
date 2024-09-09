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
#include "mock/ray/pubsub/subscriber.h"
#include "mock/ray/rpc/worker/core_worker_client.h"

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
    fake_kv_ = std::make_unique<gcs::FakeInternalKVInterface>();
    function_manager_ = std::make_unique<gcs::GcsFunctionManager>(*kv_);

    // Mock client factory which abuses the "address" argument to return a
    // CoreWorkerClient whose number of running tasks equal to the address port. This is
    // just for testing purposes.
    client_factory_ = [](const rpc::Address &address) {
      return std::make_shared<rpc::MockCoreWorkerClientConfigurableRunningTasks>(
          address.port());
    };
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
  std::unique_ptr<gcs::FakeInternalKVInterface> fake_kv_;
  rpc::ClientFactoryFn client_factory_;
  RuntimeEnvManager runtime_env_manager_;
  const std::chrono::milliseconds timeout_ms_{5000};
};

TEST_F(GcsJobManagerTest, TestFakeInternalKV) {
  fake_kv_->Put("ns", "key", "value", /*overwrite=*/true, /*callback=*/[](auto) {});
  fake_kv_->Get(
      "ns", "key", [](std::optional<std::string> v) { ASSERT_EQ(v.value(), "value"); });
  fake_kv_->Put("ns", "key2", "value2", /*overwrite=*/true, /*callback=*/[](auto) {});

  fake_kv_->MultiGet("ns",
                     {"key", "key2"},
                     [](const std::unordered_map<std::string, std::string> &result) {
                       ASSERT_EQ(result.size(), 2);
                       ASSERT_EQ(result.at("key"), "value");
                       ASSERT_EQ(result.at("key2"), "value2");
                     });
}

TEST_F(GcsJobManagerTest, TestIsRunningTasks) {
  gcs::GcsJobManager gcs_job_manager(gcs_table_storage_,
                                     gcs_publisher_,
                                     runtime_env_manager_,
                                     *function_manager_,
                                     *fake_kv_,
                                     client_factory_);

  gcs::GcsInitData gcs_init_data(gcs_table_storage_);
  gcs_job_manager.Initialize(/*init_data=*/gcs_init_data);

  // Add 100 jobs. Job i should have i running tasks.
  int num_jobs = 100;
  for (int i = 0; i < num_jobs; ++i) {
    auto job_id = JobID::FromInt(i);
    // Create an address with port equal to the number of running tasks. We use the mock
    // client factory to create a core worker client with number of running tasks
    // equal to the address port.
    rpc::Address address;
    // Set the number of running tasks to 0 for even jobs and i for odd jobs.
    int num_running_tasks = i % 2 == 0 ? 0 : i;
    address.set_port(num_running_tasks);

    // Populate other fields, the value is not important.
    address.set_raylet_id(NodeID::FromRandom().Binary());
    address.set_ip_address("123.456.7.8");
    address.set_worker_id(WorkerID::FromRandom().Binary());

    auto add_job_request =
        Mocker::GenAddJobRequest(job_id, std::to_string(i), std::to_string(i), address);
    rpc::AddJobReply empty_reply;
    std::promise<bool> promise;
    gcs_job_manager.HandleAddJob(
        *add_job_request,
        &empty_reply,
        [&promise](Status, std::function<void()>, std::function<void()>) {
          promise.set_value(true);
        });
    promise.get_future().get();
  }

  // Get all jobs.
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

  ASSERT_EQ(all_job_info_reply.job_info_list().size(), num_jobs);

  // Check that the is_running_tasks field is correct for each job.
  for (int i = 0; i < num_jobs; ++i) {
    auto job_info = all_job_info_reply.job_info_list(i);
    int job_id = JobID::FromBinary(job_info.job_id()).ToInt();
    ASSERT_EQ(job_info.is_running_tasks(), job_id % 2 != 0);
  }
}

TEST_F(GcsJobManagerTest, TestGetAllJobInfo) {
  gcs::GcsJobManager gcs_job_manager(gcs_table_storage_,
                                     gcs_publisher_,
                                     runtime_env_manager_,
                                     *function_manager_,
                                     *fake_kv_,
                                     client_factory_);

  gcs::GcsInitData gcs_init_data(gcs_table_storage_);
  gcs_job_manager.Initialize(/*init_data=*/gcs_init_data);

  // Add 100 jobs.
  for (int i = 0; i < 100; ++i) {
    auto job_id = JobID::FromInt(i);
    auto add_job_request =
        Mocker::GenAddJobRequest(job_id, "namespace_" + std::to_string(i));
    rpc::AddJobReply empty_reply;
    std::promise<bool> promise;
    gcs_job_manager.HandleAddJob(
        *add_job_request,
        &empty_reply,
        [&promise](Status, std::function<void()>, std::function<void()>) {
          promise.set_value(true);
        });
    promise.get_future().get();
  }

  // Get all jobs.
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

  ASSERT_EQ(all_job_info_reply.job_info_list().size(), 100);

  // Add a job with a submission id (simulate a job being "submitted via the Ray Job
  // API.")
  auto job_api_job_id = JobID::FromInt(100);
  std::string submission_id = "submission_id_100";
  auto add_job_request =
      Mocker::GenAddJobRequest(job_api_job_id, "namespace_100", submission_id);
  rpc::AddJobReply empty_reply;
  std::promise<bool> promise;
  gcs_job_manager.HandleAddJob(
      *add_job_request,
      &empty_reply,
      [&promise](Status, std::function<void()>, std::function<void()>) {
        promise.set_value(true);
      });
  promise.get_future().get();

  // Manually put sample JobInfo for that job into the internal kv.
  // This is ordinarily done in Python by the Ray Job API.
  std::string job_info_json = R"(
    {
      "status": "PENDING",
      "entrypoint": "echo hi",
      "entrypoint_num_cpus": 1,
      "entrypoint_num_gpus": 1,
      "entrypoint_resources": {
        "Custom": 1
      },
      "runtime_env_json": "{\"pip\": [\"pkg\"]}"
    }
  )";

  std::promise<bool> kv_promise;
  fake_kv_->Put("job",
                gcs::JobDataKey(submission_id),
                job_info_json,
                /*overwrite=*/true,
                [&kv_promise](auto) { kv_promise.set_value(true); });
  kv_promise.get_future().get();

  // Get all job info again.
  rpc::GetAllJobInfoRequest all_job_info_request2;
  rpc::GetAllJobInfoReply all_job_info_reply2;
  std::promise<bool> all_job_info_promise2;

  gcs_job_manager.HandleGetAllJobInfo(
      all_job_info_request2,
      &all_job_info_reply2,
      [&all_job_info_promise2](Status, std::function<void()>, std::function<void()>) {
        all_job_info_promise2.set_value(true);
      });
  all_job_info_promise2.get_future().get();

  ASSERT_EQ(all_job_info_reply2.job_info_list().size(), 101);

  // From the reply, get the job info for the job "submitted via the Ray Job API."
  rpc::JobTableData job_table_data_for_api_job;
  for (auto job_info : all_job_info_reply2.job_info_list()) {
    if (job_info.job_id() == job_api_job_id.Binary()) {
      job_table_data_for_api_job = job_info;
      break;
    }
  }

  // Verify the contents of the job info proto from the reply.
  auto job_info = job_table_data_for_api_job.job_info();
  ASSERT_EQ(job_info.status(), "PENDING");
  ASSERT_EQ(job_info.entrypoint(), "echo hi");
  ASSERT_EQ(job_info.entrypoint_num_cpus(), 1);
  ASSERT_EQ(job_info.entrypoint_num_gpus(), 1);
  ASSERT_EQ(job_info.entrypoint_resources().size(), 1);
  ASSERT_EQ(job_info.entrypoint_resources().at("Custom"), 1);
  ASSERT_EQ(job_info.runtime_env_json(), "{\"pip\": [\"pkg\"]}");

  // Manually overwrite with bad JobInfo JSON to test error handling on parse.
  job_info_json = R"(
    {
      "status": "PENDING",
      "entrypoint": "echo hi",
      "not_a_real_field": 1
    }
  )";

  std::promise<bool> kv_promise2;
  fake_kv_->Put("job",
                gcs::JobDataKey(submission_id),
                job_info_json,
                /*overwrite=*/true,
                [&kv_promise2](auto) { kv_promise2.set_value(true); });
  kv_promise2.get_future().get();

  // Get all job info again.
  rpc::GetAllJobInfoRequest all_job_info_request3;
  rpc::GetAllJobInfoReply all_job_info_reply3;
  std::promise<bool> all_job_info_promise3;

  gcs_job_manager.HandleGetAllJobInfo(
      all_job_info_request3,
      &all_job_info_reply3,
      [&all_job_info_promise3](Status, std::function<void()>, std::function<void()>) {
        all_job_info_promise3.set_value(true);
      });

  // Make sure the GCS didn't hang or crash.
  all_job_info_promise3.get_future().get();

  // Add another job with the *same* submission ID. This can happen if the entrypoint
  // script calls ray.init() multiple times.
  auto job_id2 = JobID::FromInt(2);
  auto add_job_request2 =
      Mocker::GenAddJobRequest(job_id2, "namespace_100", submission_id);
  std::promise<bool> promise4;
  gcs_job_manager.HandleAddJob(
      *add_job_request2,
      &empty_reply,
      [&promise4](Status, std::function<void()>, std::function<void()>) {
        promise4.set_value(true);
      });
  promise4.get_future().get();

  // Get all job info again.
  rpc::GetAllJobInfoRequest all_job_info_request4;
  rpc::GetAllJobInfoReply all_job_info_reply4;
  std::promise<bool> all_job_info_promise4;

  gcs_job_manager.HandleGetAllJobInfo(
      all_job_info_request4,
      &all_job_info_reply4,
      [&all_job_info_promise4](Status, std::function<void()>, std::function<void()>) {
        all_job_info_promise4.set_value(true);
      });
  all_job_info_promise4.get_future().get();

  ASSERT_EQ(all_job_info_reply4.job_info_list().size(), 101);
}

TEST_F(GcsJobManagerTest, TestGetAllJobInfoWithLimit) {
  gcs::GcsJobManager gcs_job_manager(gcs_table_storage_,
                                     gcs_publisher_,
                                     runtime_env_manager_,
                                     *function_manager_,
                                     *fake_kv_,
                                     client_factory_);

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

  // Get all jobs with limit.
  rpc::GetAllJobInfoRequest all_job_info_request;
  rpc::GetAllJobInfoReply all_job_info_reply;
  std::promise<bool> all_job_info_promise;

  all_job_info_request.set_limit(1);
  gcs_job_manager.HandleGetAllJobInfo(
      all_job_info_request,
      &all_job_info_reply,
      [&all_job_info_promise](Status, std::function<void()>, std::function<void()>) {
        all_job_info_promise.set_value(true);
      });
  all_job_info_promise.get_future().get();

  ASSERT_EQ(all_job_info_reply.job_info_list().size(), 1);

  // Test edge case of limit=0.
  rpc::GetAllJobInfoRequest all_job_info_request2;
  rpc::GetAllJobInfoReply all_job_info_reply2;
  std::promise<bool> all_job_info_promise2;

  all_job_info_request2.set_limit(0);
  gcs_job_manager.HandleGetAllJobInfo(
      all_job_info_request2,
      &all_job_info_reply2,
      [&all_job_info_promise2](Status, std::function<void()>, std::function<void()>) {
        all_job_info_promise2.set_value(true);
      });
  all_job_info_promise2.get_future().get();

  ASSERT_EQ(all_job_info_reply2.job_info_list().size(), 0);

  // Test get all jobs with limit larger than the number of jobs.
  rpc::GetAllJobInfoRequest all_job_info_request3;
  rpc::GetAllJobInfoReply all_job_info_reply3;
  std::promise<bool> all_job_info_promise3;

  all_job_info_request3.set_limit(100);
  gcs_job_manager.HandleGetAllJobInfo(
      all_job_info_request3,
      &all_job_info_reply3,
      [&all_job_info_promise3](Status, std::function<void()>, std::function<void()>) {
        all_job_info_promise3.set_value(true);
      });
  all_job_info_promise3.get_future().get();

  ASSERT_EQ(all_job_info_reply3.job_info_list().size(), 2);

  // Test get all jobs with limit -1. Should fail validation.
  rpc::GetAllJobInfoRequest all_job_info_request4;
  rpc::GetAllJobInfoReply all_job_info_reply4;
  std::promise<bool> all_job_info_promise4;

  all_job_info_request4.set_limit(-1);
  gcs_job_manager.HandleGetAllJobInfo(
      all_job_info_request4,
      &all_job_info_reply4,
      [&all_job_info_promise4](Status, std::function<void()>, std::function<void()>) {
        all_job_info_promise4.set_value(true);
      });
  all_job_info_promise4.get_future().get();

  // Check that the reply has the invalid status.
  ASSERT_EQ(all_job_info_reply4.status().code(), (int)StatusCode::Invalid);
  // Check that the reply has the correct error message.
  ASSERT_EQ(all_job_info_reply4.status().message(), "Invalid limit");
}
TEST_F(GcsJobManagerTest, TestGetJobConfig) {
  gcs::GcsJobManager gcs_job_manager(gcs_table_storage_,
                                     gcs_publisher_,
                                     runtime_env_manager_,
                                     *function_manager_,
                                     *kv_,
                                     client_factory_);

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
  gcs::GcsJobManager gcs_job_manager(gcs_table_storage_,
                                     gcs_publisher_,
                                     runtime_env_manager_,
                                     *function_manager_,
                                     *fake_kv_,
                                     client_factory_);

  auto job_id = JobID::FromInt(1);
  gcs::GcsInitData gcs_init_data(gcs_table_storage_);
  gcs_job_manager.Initialize(/*init_data=*/gcs_init_data);
  auto add_job_request = Mocker::GenAddJobRequest(job_id, "namespace");

  rpc::Address address;
  address.set_ip_address("10.0.0.1");
  address.set_port(8264);
  address.set_raylet_id(NodeID::FromRandom().Binary());
  address.set_worker_id(WorkerID::FromRandom().Binary());
  add_job_request->mutable_data()->set_driver_ip_address("10.0.0.1");
  add_job_request->mutable_data()->mutable_driver_address()->CopyFrom(address);

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
  ASSERT_EQ(data.driver_address().ip_address(), "10.0.0.1");
  ASSERT_EQ(data.driver_ip_address(), "10.0.0.1");
  ASSERT_EQ(data.driver_pid(), 8264);
}

TEST_F(GcsJobManagerTest, TestNodeFailure) {
  gcs::GcsJobManager gcs_job_manager(gcs_table_storage_,
                                     gcs_publisher_,
                                     runtime_env_manager_,
                                     *function_manager_,
                                     *fake_kv_,
                                     client_factory_);

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

  rpc::GetAllJobInfoRequest all_job_info_request;
  rpc::GetAllJobInfoReply all_job_info_reply;
  std::promise<bool> all_job_info_promise;

  // Check if all job are not dead
  gcs_job_manager.HandleGetAllJobInfo(
      all_job_info_request,
      &all_job_info_reply,
      [&all_job_info_promise](Status, std::function<void()>, std::function<void()>) {
        all_job_info_promise.set_value(true);
      });
  all_job_info_promise.get_future().get();
  for (auto job_info : all_job_info_reply.job_info_list()) {
    ASSERT_TRUE(!job_info.is_dead());
  }

  // Remove node and then check that the job is dead.
  auto address = all_job_info_reply.job_info_list().Get(0).driver_address();
  auto node_id = NodeID::FromBinary(address.raylet_id());
  gcs_job_manager.OnNodeDead(node_id);

  // Test get all jobs and check if killed node jobs marked as finished
  auto condition = [&gcs_job_manager, node_id]() -> bool {
    rpc::GetAllJobInfoRequest all_job_info_request2;
    rpc::GetAllJobInfoReply all_job_info_reply2;
    std::promise<bool> all_job_info_promise2;
    gcs_job_manager.HandleGetAllJobInfo(
        all_job_info_request2,
        &all_job_info_reply2,
        [&all_job_info_promise2](Status, std::function<void()>, std::function<void()>) {
          all_job_info_promise2.set_value(true);
        });
    all_job_info_promise2.get_future().get();

    bool job_condition = true;
    // job1 from the current node should dead, while job2 is still alive
    for (auto job_info : all_job_info_reply2.job_info_list()) {
      auto job_node_id = NodeID::FromBinary(job_info.driver_address().raylet_id());
      job_condition = job_condition && (job_info.is_dead() == (job_node_id == node_id));
    }
    return job_condition;
  };

  EXPECT_TRUE(WaitForCondition(condition, 2000));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
}  // namespace ray
