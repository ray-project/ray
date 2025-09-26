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

#include "ray/gcs/gcs_worker_manager.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "mock/ray/pubsub/publisher.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/store_client_kv.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

using namespace ::testing;    // NOLINT
using namespace ray::gcs;     // NOLINT
using namespace ray::pubsub;  // NOLINT
using namespace ray;          // NOLINT

class GcsWorkerManagerTest : public Test {
 public:
  GcsWorkerManagerTest() {
    gcs_publisher_ = std::make_shared<pubsub::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    gcs_table_storage_ =
        std::make_unique<gcs::GcsTableStorage>(std::make_unique<InMemoryStoreClient>());
  }

  void SetUp() override {
    // Needs a separate thread to run io service.
    // Alternatively, we can manually run io service. In this test, we chose to
    // start a new thread as other GCS tests do.
    thread_io_service_ = std::make_unique<std::thread>([this] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_.get_executor());
      io_service_.run();
    });
    worker_manager_ = std::make_shared<gcs::GcsWorkerManager>(
        *gcs_table_storage_, io_service_, *gcs_publisher_);
  }

  void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  rpc::WorkerTableData GenWorkerTableData(pid_t pid) {
    rpc::WorkerTableData worker_data;
    worker_data.mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
    worker_data.set_worker_type(rpc::WorkerType::DRIVER);
    worker_data.set_is_alive(true);
    worker_data.set_pid(pid);
    return worker_data;
  }

  std::shared_ptr<gcs::GcsWorkerManager> GetWorkerManager() { return worker_manager_; }

 private:
  std::unique_ptr<std::thread> thread_io_service_;
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<pubsub::GcsPublisher> gcs_publisher_;
  std::shared_ptr<gcs::GcsWorkerManager> worker_manager_;
};

TEST_F(GcsWorkerManagerTest, TestGetAllWorkersLimit) {
  auto num_workers = 3;
  auto worker_manager = GetWorkerManager();
  std::vector<rpc::WorkerTableData> workers;

  for (int i = 0; i < num_workers; i++) {
    workers.push_back(GenWorkerTableData(i));
  }

  for (const auto &worker : workers) {
    rpc::AddWorkerInfoRequest request;
    request.mutable_worker_data()->CopyFrom(worker);
    rpc::AddWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleAddWorkerInfo(request, &reply, callback);
    promise.get_future().get();
  }

  {
    /// Test normal case without limit.
    rpc::GetAllWorkerInfoRequest request;
    rpc::GetAllWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleGetAllWorkerInfo(request, &reply, callback);
    promise.get_future().get();

    ASSERT_EQ(reply.worker_table_data().size(), 3);
    ASSERT_EQ(reply.total(), 3);
  }

  {
    /// Test the case where limit is specified.
    rpc::GetAllWorkerInfoRequest request;
    request.set_limit(2);
    rpc::GetAllWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleGetAllWorkerInfo(request, &reply, callback);
    promise.get_future().get();

    ASSERT_EQ(reply.worker_table_data().size(), 2);
    ASSERT_EQ(reply.total(), 3);
  }
}

TEST_F(GcsWorkerManagerTest, TestGetAllWorkersFilters) {
  auto worker_manager = GetWorkerManager();
  std::vector<rpc::WorkerTableData> workers;

  auto worker_paused_threads = GenWorkerTableData(1);
  worker_paused_threads.set_num_paused_threads(1);

  auto worker_normal = GenWorkerTableData(2);

  auto worker_non_alive = GenWorkerTableData(3);
  worker_non_alive.set_is_alive(false);

  for (const auto &worker : {worker_paused_threads, worker_normal, worker_non_alive}) {
    rpc::AddWorkerInfoRequest request;
    request.mutable_worker_data()->CopyFrom(worker);
    rpc::AddWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleAddWorkerInfo(request, &reply, callback);
    promise.get_future().get();
  }

  {
    /// Filter: exist_paused_threads
    rpc::GetAllWorkerInfoRequest request;
    request.mutable_filters()->set_exist_paused_threads(true);
    rpc::GetAllWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleGetAllWorkerInfo(request, &reply, callback);
    promise.get_future().get();

    ASSERT_EQ(reply.worker_table_data().size(), 1);
    ASSERT_EQ(reply.total(), 3);
    ASSERT_EQ(reply.num_filtered(), 2);
  }

  {
    /// Filter: is_alive
    rpc::GetAllWorkerInfoRequest request;
    request.mutable_filters()->set_is_alive(true);
    rpc::GetAllWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleGetAllWorkerInfo(request, &reply, callback);
    promise.get_future().get();

    ASSERT_EQ(reply.worker_table_data().size(), 2);
    ASSERT_EQ(reply.total(), 3);
    ASSERT_EQ(reply.num_filtered(), 1);
  }
  {
    /// Filter: is_alive + limits
    rpc::GetAllWorkerInfoRequest request;
    request.mutable_filters()->set_is_alive(true);
    request.set_limit(1);
    rpc::GetAllWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleGetAllWorkerInfo(request, &reply, callback);
    promise.get_future().get();

    ASSERT_EQ(reply.worker_table_data().size(), 1);
    ASSERT_EQ(reply.total(), 3);
    ASSERT_LE(reply.num_filtered(), 1);
  }
}

TEST_F(GcsWorkerManagerTest, TestUpdateWorkerDebuggerPort) {
  auto worker_manager = GetWorkerManager();
  auto worker = GenWorkerTableData(0);
  auto debugger_port = 1000;
  {
    // add worker
    rpc::AddWorkerInfoRequest request;
    request.mutable_worker_data()->CopyFrom(worker);
    rpc::AddWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleAddWorkerInfo(request, &reply, callback);
    promise.get_future().get();
  }

  {
    // update the worker debugger port
    rpc::UpdateWorkerDebuggerPortRequest request;
    request.set_worker_id(worker.worker_address().worker_id());
    request.set_debugger_port(debugger_port);
    rpc::UpdateWorkerDebuggerPortReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleUpdateWorkerDebuggerPort(request, &reply, callback);
    promise.get_future().get();
  }

  {
    // Get the worker and verify the debugger port
    rpc::GetAllWorkerInfoRequest request;
    rpc::GetAllWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleGetAllWorkerInfo(request, &reply, callback);
    promise.get_future().get();

    ASSERT_EQ(reply.worker_table_data().size(), 1);
    ASSERT_EQ(reply.total(), 1);
    ASSERT_EQ(reply.worker_table_data(0).debugger_port(), debugger_port);
  }
}

TEST_F(GcsWorkerManagerTest, TestUpdateWorkerNumPausedThreads) {
  auto worker_manager = GetWorkerManager();
  auto worker = GenWorkerTableData(0);
  auto num_paused_threads_delta = 2;
  {
    // add worker
    rpc::AddWorkerInfoRequest request;
    request.mutable_worker_data()->CopyFrom(worker);
    rpc::AddWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleAddWorkerInfo(request, &reply, callback);
    promise.get_future().get();
  }

  {
    // update the worker num paused threads
    rpc::UpdateWorkerNumPausedThreadsRequest request;
    request.set_worker_id(worker.worker_address().worker_id());
    request.set_num_paused_threads_delta(num_paused_threads_delta);
    rpc::UpdateWorkerNumPausedThreadsReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleUpdateWorkerNumPausedThreads(request, &reply, callback);
    promise.get_future().get();
  }

  {
    // Get the worker and verify the num paused threads
    rpc::GetAllWorkerInfoRequest request;
    rpc::GetAllWorkerInfoReply reply;
    std::promise<void> promise;
    auto callback = [&promise](Status status,
                               std::function<void()> success,
                               std::function<void()> failure) { promise.set_value(); };
    worker_manager->HandleGetAllWorkerInfo(request, &reply, callback);
    promise.get_future().get();

    ASSERT_EQ(reply.worker_table_data().size(), 1);
    ASSERT_EQ(reply.total(), 1);
    ASSERT_EQ(reply.worker_table_data(0).num_paused_threads(), num_paused_threads_delta);
  }
}
