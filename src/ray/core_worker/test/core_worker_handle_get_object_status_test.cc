// Copyright 2025 The Ray Authors.
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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fakes/ray/common/asio/fake_periodical_runner.h"
#include "fakes/ray/core_worker/actor_creator.h"
#include "fakes/ray/core_worker/actor_task_submitter.h"
#include "fakes/ray/core_worker/experimental_mutable_object_provider.h"
#include "fakes/ray/core_worker/lease_policy.h"
#include "fakes/ray/core_worker/task_event_buffer.h"
#include "fakes/ray/core_worker/task_manager.h"
#include "fakes/ray/pubsub/publisher.h"
#include "fakes/ray/pubsub/subscriber.h"
#include "fakes/ray/rpc/raylet/raylet_client.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_rpc_proxy.h"
#include "ray/core_worker/future_resolver.h"
#include "ray/core_worker/object_recovery_manager.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/transport/actor_task_submitter.h"
#include "ray/core_worker/transport/normal_task_submitter.h"
#include "ray/rpc/worker/core_worker_client_pool.h"

namespace ray {
namespace core {

using ::testing::_;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;

class CoreWorkerHandleGetObjectStatusTest : public ::testing::Test {
 public:
  CoreWorkerHandleGetObjectStatusTest()
      : io_work_(io_service_.get_executor()),
        task_execution_service_work_(task_execution_service_.get_executor()) {
    CoreWorkerOptions options;
    options.worker_type = WorkerType::WORKER;
    options.language = Language::PYTHON;
    options.node_ip_address = "127.0.0.1";
    options.raylet_ip_address = "127.0.0.1";
    options.task_execution_callback =
        [](const rpc::Address &caller_address,
           TaskType task_type,
           const std::string task_name,
           const RayFunction &ray_function,
           const std::unordered_map<std::string, double> &required_resources,
           const std::vector<std::shared_ptr<RayObject>> &args,
           const std::vector<rpc::ObjectReference> &arg_refs,
           const std::string &debugger_breakpoint,
           const std::string &serialized_retry_exception_allowlist,
           std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *returns,
           std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *dynamic_returns,
           std::vector<std::pair<ObjectID, bool>> *streaming_generator_returns,
           std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes,
           bool *is_retryable_error,
           std::string *application_error,
           const std::vector<ConcurrencyGroup> &defined_concurrency_groups,
           const std::string name_of_concurrency_group_to_execute,
           bool is_reattempt,
           bool is_streaming_generator,
           bool retry_exception,
           int64_t generator_backpressure_num_objects,
           const rpc::TensorTransport &tensor_transport) -> Status {
      return Status::OK();
    };

    auto core_worker_client_pool =
        std::make_shared<rpc::CoreWorkerClientPool>([](const rpc::Address &) {
          return std::make_shared<rpc::CoreWorkerClientInterface>();
        });

    auto raylet_client_pool = std::make_shared<rpc::RayletClientPool>(
        [](const rpc::Address &) { return std::make_shared<FakeRayletClient>(); });

    auto mock_gcs_client = std::make_shared<gcs::MockGcsClient>();

    auto fake_local_raylet_client = std::make_shared<FakeRayletClient>();

    auto service_handler = std::make_unique<CoreWorkerServiceHandlerProxy>();
    auto worker_context = std::make_unique<WorkerContext>(
        WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(1));
    auto core_worker_server =
        std::make_unique<rpc::GrpcServer>(WorkerTypeString(options.worker_type), 0, true);
    core_worker_server->RegisterService(
        std::make_unique<rpc::CoreWorkerGrpcService>(io_service_, *service_handler),
        false /* token_auth */);
    core_worker_server->Run();

    rpc::Address rpc_address;
    rpc_address.set_ip_address(options.node_ip_address);
    rpc_address.set_port(core_worker_server->GetPort());
    rpc_address.set_raylet_id(NodeID::FromRandom().Binary());
    rpc_address.set_worker_id(worker_context->GetWorkerID().Binary());

    auto fake_object_info_publisher = std::make_unique<pubsub::FakePublisher>();
    auto fake_object_info_subscriber = std::make_unique<pubsub::FakeSubscriber>();

    reference_counter_ = std::make_shared<ReferenceCounter>(
        rpc_address,
        fake_object_info_publisher.get(),
        fake_object_info_subscriber.get(),
        [](const NodeID &) { return false; },
        false);

    memory_store_ = std::make_shared<CoreWorkerMemoryStore>(
        io_service_, reference_counter_.get(), nullptr);

    auto future_resolver = std::make_unique<FutureResolver>(
        memory_store_,
        reference_counter_,
        [](const ObjectID &object_id,
           const absl::flat_hash_set<NodeID> &locations,
           uint64_t object_size) {},
        core_worker_client_pool,
        rpc_address);

    auto fake_task_manager = std::make_shared<FakeTaskManager>();

    auto object_recovery_manager = std::make_unique<ObjectRecoveryManager>(
        rpc_address,
        raylet_client_pool,
        fake_local_raylet_client,
        [](const ObjectID &object_id, const ObjectLookupCallback &callback) {
          return Status::OK();
        },
        *fake_task_manager,
        *reference_counter_,
        *memory_store_,
        [](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {});

    auto lease_policy = std::make_unique<FakeLeasePolicy>();

    auto lease_request_rate_limiter = std::make_shared<StaticLeaseRequestRateLimiter>(10);

    auto fake_actor_creator = std::make_shared<FakeActorCreator>();

    auto normal_task_submitter = std::make_unique<NormalTaskSubmitter>(
        rpc_address,
        fake_local_raylet_client,
        core_worker_client_pool,
        raylet_client_pool,
        std::move(lease_policy),
        memory_store_,
        *fake_task_manager,
        NodeID::Nil(),
        WorkerType::WORKER,
        10000,
        fake_actor_creator,
        JobID::Nil(),
        lease_request_rate_limiter,
        [](const ObjectID &object_id) { return rpc::TensorTransport::OBJECT_STORE; },
        boost::asio::steady_timer(io_service_));

    auto fake_actor_task_submitter = std::make_unique<FakeActorTaskSubmitter>();

    auto actor_manager = std::make_unique<ActorManager>(
        mock_gcs_client, *fake_actor_task_submitter, *reference_counter_);

    auto fake_task_event_buffer = std::make_unique<FakeTaskEventBuffer>();

    io_thread_ = boost::thread([this]() { io_service_.run(); });

    auto client_call_manager =
        std::make_unique<rpc::ClientCallManager>(io_service_, /*record_stats=*/false);

    auto fake_experimental_mutable_object_provider =
        std::make_shared<experimental::FakeMutableObjectProvider>();

    auto periodical_runner = std::make_shared<FakePeriodicalRunner>();

    EXPECT_CALL(*mock_gcs_client->mock_worker_accessor, AsyncAdd(_, _)).Times(1);
    EXPECT_CALL(*mock_gcs_client->mock_node_accessor, AsyncSubscribeToNodeChange(_, _))
        .Times(1);
    // Create CoreWorker
    core_worker_ =
        std::make_shared<CoreWorker>(std::move(options),
                                     std::move(worker_context),
                                     io_service_,
                                     std::move(client_call_manager),
                                     std::move(core_worker_client_pool),
                                     std::move(raylet_client_pool),
                                     std::move(periodical_runner),
                                     std::move(core_worker_server),
                                     std::move(rpc_address),
                                     std::move(mock_gcs_client),
                                     std::move(fake_local_raylet_client),
                                     io_thread_,
                                     reference_counter_,
                                     memory_store_,
                                     nullptr,  // plasma_store_provider_
                                     std::move(fake_experimental_mutable_object_provider),
                                     std::move(future_resolver),
                                     std::move(fake_task_manager),
                                     std::move(fake_actor_creator),
                                     std::move(fake_actor_task_submitter),
                                     std::move(fake_object_info_publisher),
                                     std::move(fake_object_info_subscriber),
                                     std::move(lease_request_rate_limiter),
                                     std::move(normal_task_submitter),
                                     std::move(object_recovery_manager),
                                     std::move(actor_manager),
                                     task_execution_service_,
                                     std::move(fake_task_event_buffer),
                                     getpid());
  }

  void TearDown() override {
    io_service_.stop();
    if (io_thread_.joinable()) {
      io_thread_.join();
    }
  }

 protected:
  // Core io contexts (must be first)
  instrumented_io_context io_service_{/*enable_lag_probe=*/false,
                                      /*running_on_single_thread=*/true};
  instrumented_io_context task_execution_service_{/*enable_lag_probe=*/false,
                                                  /*running_on_single_thread=*/true};
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      task_execution_service_work_;
  boost::thread io_thread_;

  std::shared_ptr<ReferenceCounter> reference_counter_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;
  std::shared_ptr<CoreWorker> core_worker_;
};

TEST_F(CoreWorkerHandleGetObjectStatusTest, IdempotencyTest) {
  auto object_id = ObjectID::FromRandom();
  std::string data_str = "test_data";
  std::string metadata_str = "meta";
  auto data = std::make_shared<LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(data_str.data()), data_str.size(), true);
  auto metadata = std::make_shared<LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(metadata_str.data()), metadata_str.size(), true);
  auto ray_object =
      std::make_shared<RayObject>(data, metadata, std::vector<rpc::ObjectReference>());

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(object_id, {}, owner_address, "", 0, false, true);
  ASSERT_TRUE(memory_store_->Put(*ray_object, object_id));
  rpc::GetObjectStatusRequest request1;
  request1.set_object_id(object_id.Binary());
  request1.set_owner_worker_id(core_worker_->GetWorkerID().Binary());

  rpc::GetObjectStatusReply reply1;
  std::mutex callback_mutex1;
  std::condition_variable callback_cv1;
  bool callback_called1 = false;
  core_worker_->HandleGetObjectStatus(
      request1,
      &reply1,
      [&](Status s, std::function<void()> success, std::function<void()> failure) {
        std::lock_guard<std::mutex> lock(callback_mutex1);
        callback_called1 = true;
        callback_cv1.notify_one();
      });

  rpc::GetObjectStatusReply reply2;
  std::mutex callback_mutex2;
  std::condition_variable callback_cv2;
  bool callback_called2 = false;
  core_worker_->HandleGetObjectStatus(
      request1,
      &reply2,
      [&](Status s, std::function<void()> success, std::function<void()> failure) {
        std::lock_guard<std::mutex> lock(callback_mutex2);
        callback_called2 = true;
        callback_cv2.notify_one();
      });

  std::unique_lock<std::mutex> lock1(callback_mutex1);
  callback_cv1.wait(lock1, [&] { return callback_called1; });
  std::unique_lock<std::mutex> lock2(callback_mutex2);
  callback_cv2.wait(lock2, [&] { return callback_called2; });

  // Verify both replies are identical (idempotency)
  EXPECT_EQ(reply1.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_EQ(reply2.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_EQ("test_data", reply1.object().data());
  EXPECT_EQ("test_data", reply2.object().data());
  EXPECT_EQ("meta", reply1.object().metadata());
  EXPECT_EQ("meta", reply2.object().metadata());
}

}  // namespace core
}  // namespace ray
