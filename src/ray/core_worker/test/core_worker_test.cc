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

#include "ray/core_worker/core_worker.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fakes/ray/common/asio/fake_periodical_runner.h"
#include "fakes/ray/ipc/raylet_ipc_client.h"
#include "fakes/ray/pubsub/publisher.h"
#include "fakes/ray/pubsub/subscriber.h"
#include "fakes/ray/rpc/raylet/raylet_client.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker_rpc_proxy.h"
#include "ray/core_worker/future_resolver.h"
#include "ray/core_worker/object_recovery_manager.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_submission/actor_task_submitter.h"
#include "ray/core_worker/task_submission/normal_task_submitter.h"
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

    auto client_call_manager =
        std::make_unique<rpc::ClientCallManager>(io_service_, /*record_stats=*/false);

    auto core_worker_client_pool =
        std::make_shared<rpc::CoreWorkerClientPool>([](const rpc::Address &) {
          return std::make_shared<rpc::CoreWorkerClientInterface>();
        });

    auto raylet_client_pool = std::make_shared<rpc::RayletClientPool>(
        [](const rpc::Address &) { return std::make_shared<FakeRayletClient>(); });

    auto mock_gcs_client = std::make_shared<gcs::MockGcsClient>();

    auto fake_local_raylet_rpc_client = std::make_shared<FakeRayletClient>();

    auto fake_raylet_ipc_client = std::make_shared<ipc::FakeRayletIpcClient>();

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
    rpc_address.set_node_id(NodeID::FromRandom().Binary());
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

    auto task_event_buffer = std::make_unique<worker::TaskEventBufferImpl>(
        std::make_unique<gcs::MockGcsClient>(),
        std::make_unique<rpc::EventAggregatorClientImpl>(0, *client_call_manager),
        "test_session");

    auto task_manager = std::make_shared<TaskManager>(
        *memory_store_,
        *reference_counter_,
        [](const RayObject &object, const ObjectID &object_id) { return Status::OK(); },
        [](TaskSpecification &spec, bool object_recovery, uint32_t delay_ms) {},
        [](const TaskSpecification &spec) { return false; },
        [](const JobID &job_id,
           const std::string &type,
           const std::string &error_message,
           double timestamp) { return Status::OK(); },
        RayConfig::instance().max_lineage_bytes(),
        *task_event_buffer,
        [](const ActorID &actor_id) {
          return std::make_shared<rpc::CoreWorkerClientInterface>();
        },
        mock_gcs_client);

    auto object_recovery_manager = std::make_unique<ObjectRecoveryManager>(
        rpc_address,
        raylet_client_pool,
        [](const ObjectID &object_id, const ObjectLookupCallback &callback) {
          return Status::OK();
        },
        *task_manager,
        *reference_counter_,
        *memory_store_,
        [](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {});

    auto lease_policy = std::unique_ptr<LeasePolicyInterface>(
        std::make_unique<LocalLeasePolicy>(rpc_address));

    auto lease_request_rate_limiter = std::make_shared<StaticLeaseRequestRateLimiter>(10);

    auto actor_creator = std::make_shared<DefaultActorCreator>(mock_gcs_client);

    auto normal_task_submitter = std::make_unique<NormalTaskSubmitter>(
        rpc_address,
        fake_local_raylet_rpc_client,
        core_worker_client_pool,
        raylet_client_pool,
        std::move(lease_policy),
        memory_store_,
        *task_manager,
        NodeID::Nil(),
        WorkerType::WORKER,
        10000,
        actor_creator,
        JobID::Nil(),
        lease_request_rate_limiter,
        [](const ObjectID &object_id) { return rpc::TensorTransport::OBJECT_STORE; },
        boost::asio::steady_timer(io_service_));

    auto actor_task_submitter = std::make_unique<ActorTaskSubmitter>(
        *core_worker_client_pool,
        *memory_store_,
        *task_manager,
        *actor_creator,
        /*tensor_transport_getter=*/
        [](const ObjectID &object_id) { return rpc::TensorTransport::OBJECT_STORE; },
        [](const ActorID &actor_id, uint64_t num_queued) { return Status::OK(); },
        io_service_,
        reference_counter_);

    auto actor_manager = std::make_unique<ActorManager>(
        mock_gcs_client, *actor_task_submitter, *reference_counter_);

    auto periodical_runner = std::make_unique<FakePeriodicalRunner>();

    // TODO(joshlee): Dependency inject socket into plasma_store_provider_ so we can
    // create a real plasma_store_provider_ and mutable_object_provider_
    core_worker_ = std::make_shared<CoreWorker>(std::move(options),
                                                std::move(worker_context),
                                                io_service_,
                                                std::move(client_call_manager),
                                                std::move(core_worker_client_pool),
                                                std::move(raylet_client_pool),
                                                std::move(periodical_runner),
                                                std::move(core_worker_server),
                                                std::move(rpc_address),
                                                std::move(mock_gcs_client),
                                                std::move(fake_raylet_ipc_client),
                                                std::move(fake_local_raylet_rpc_client),
                                                io_thread_,
                                                reference_counter_,
                                                memory_store_,
                                                nullptr,  // plasma_store_provider_
                                                nullptr,  // mutable_object_provider_
                                                std::move(future_resolver),
                                                std::move(task_manager),
                                                std::move(actor_creator),
                                                std::move(actor_task_submitter),
                                                std::move(fake_object_info_publisher),
                                                std::move(fake_object_info_subscriber),
                                                std::move(lease_request_rate_limiter),
                                                std::move(normal_task_submitter),
                                                std::move(object_recovery_manager),
                                                std::move(actor_manager),
                                                task_execution_service_,
                                                std::move(task_event_buffer),
                                                getpid());
  }

 protected:
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

std::shared_ptr<RayObject> MakeRayObject(const std::string &data_str,
                                         const std::string &metadata_str) {
  auto data = std::make_shared<LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(const_cast<char *>(data_str.data())),
      data_str.size(),
      true);
  auto metadata = std::make_shared<LocalMemoryBuffer>(
      reinterpret_cast<uint8_t *>(const_cast<char *>(metadata_str.data())),
      metadata_str.size(),
      true);
  return std::make_shared<RayObject>(data, metadata, std::vector<rpc::ObjectReference>());
}

TEST_F(CoreWorkerHandleGetObjectStatusTest, IdempotencyTest) {
  auto object_id = ObjectID::FromRandom();
  auto ray_object = MakeRayObject("test_data", "meta");

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(object_id, {}, owner_address, "", 0, false, true);

  memory_store_->Put(*ray_object, object_id);

  rpc::GetObjectStatusRequest request;
  request.set_object_id(object_id.Binary());
  request.set_owner_worker_id(core_worker_->GetWorkerID().Binary());

  std::promise<Status> promise1;
  auto future1 = promise1.get_future();
  rpc::GetObjectStatusReply reply1;

  std::promise<Status> promise2;
  auto future2 = promise2.get_future();
  rpc::GetObjectStatusReply reply2;

  // Make both requests with the same parameters to test idempotency
  core_worker_->HandleGetObjectStatus(
      request,
      &reply1,
      [&promise1](Status s,
                  std::function<void()> success,
                  std::function<void()> failure) { promise1.set_value(s); });

  core_worker_->HandleGetObjectStatus(
      request,
      &reply2,
      [&promise2](Status s,
                  std::function<void()> success,
                  std::function<void()> failure) { promise2.set_value(s); });

  io_service_.run_one();
  io_service_.run_one();

  ASSERT_TRUE(future1.get().ok());
  ASSERT_TRUE(future2.get().ok());
  EXPECT_EQ(reply1.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_EQ(reply2.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_EQ("test_data", reply1.object().data());
  EXPECT_EQ("test_data", reply2.object().data());
  EXPECT_EQ("meta", reply1.object().metadata());
  EXPECT_EQ("meta", reply2.object().metadata());
}

TEST_F(CoreWorkerHandleGetObjectStatusTest, ObjectPutAfterFirstRequest) {
  auto object_id = ObjectID::FromRandom();
  auto ray_object = MakeRayObject("test_data", "meta");

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(object_id, {}, owner_address, "", 0, false, true);

  rpc::GetObjectStatusRequest request;
  request.set_object_id(object_id.Binary());
  request.set_owner_worker_id(core_worker_->GetWorkerID().Binary());

  std::promise<Status> promise1;
  auto future1 = promise1.get_future();
  rpc::GetObjectStatusReply reply1;

  core_worker_->HandleGetObjectStatus(
      request,
      &reply1,
      [&promise1](Status s,
                  std::function<void()> success,
                  std::function<void()> failure) { promise1.set_value(s); });

  // Verify that the callback hasn't been called yet since the object doesn't exist
  ASSERT_FALSE(io_service_.poll_one());

  memory_store_->Put(*ray_object, object_id);

  io_service_.run_one();

  ASSERT_TRUE(future1.get().ok());
  EXPECT_EQ(reply1.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_EQ("test_data", reply1.object().data());
  EXPECT_EQ("meta", reply1.object().metadata());

  std::promise<Status> promise2;
  auto future2 = promise2.get_future();
  rpc::GetObjectStatusReply reply2;

  // Make second request after object is already available
  core_worker_->HandleGetObjectStatus(
      request,
      &reply2,
      [&promise2](Status s,
                  std::function<void()> success,
                  std::function<void()> failure) { promise2.set_value(s); });

  io_service_.run_one();

  ASSERT_TRUE(future2.get().ok());
  EXPECT_EQ(reply2.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_EQ("test_data", reply2.object().data());
  EXPECT_EQ("meta", reply2.object().metadata());
}

TEST_F(CoreWorkerHandleGetObjectStatusTest, ObjectFreedBetweenRequests) {
  auto object_id = ObjectID::FromRandom();
  auto ray_object = MakeRayObject("test_data", "meta");

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(object_id, {}, owner_address, "", 0, false, true);

  memory_store_->Put(*ray_object, object_id);

  rpc::GetObjectStatusRequest request;
  request.set_object_id(object_id.Binary());
  request.set_owner_worker_id(core_worker_->GetWorkerID().Binary());

  std::promise<Status> promise1;
  auto future1 = promise1.get_future();
  rpc::GetObjectStatusReply reply1;

  core_worker_->HandleGetObjectStatus(
      request,
      &reply1,
      [&promise1](Status s,
                  std::function<void()> success,
                  std::function<void()> failure) { promise1.set_value(s); });

  io_service_.run_one();

  ASSERT_TRUE(future1.get().ok());
  EXPECT_EQ(reply1.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_EQ("test_data", reply1.object().data());
  EXPECT_EQ("meta", reply1.object().metadata());

  std::vector<ObjectID> objects_to_free = {object_id};
  memory_store_->Delete(objects_to_free);

  std::promise<Status> promise2;
  auto future2 = promise2.get_future();
  rpc::GetObjectStatusReply reply2;

  core_worker_->HandleGetObjectStatus(
      request,
      &reply2,
      [&promise2](Status s,
                  std::function<void()> success,
                  std::function<void()> failure) { promise2.set_value(s); });

  // Object is freed, so the callback is stored until the object is put back in the store
  ASSERT_FALSE(io_service_.poll_one());
}

TEST_F(CoreWorkerHandleGetObjectStatusTest, ObjectOutOfScope) {
  auto object_id = ObjectID::FromRandom();
  auto ray_object = MakeRayObject("test_data", "meta");

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(object_id, {}, owner_address, "", 0, false, true);

  memory_store_->Put(*ray_object, object_id);

  rpc::GetObjectStatusRequest request;
  request.set_object_id(object_id.Binary());
  request.set_owner_worker_id(core_worker_->GetWorkerID().Binary());

  std::promise<Status> promise1;
  auto future1 = promise1.get_future();
  rpc::GetObjectStatusReply reply1;

  core_worker_->HandleGetObjectStatus(
      request,
      &reply1,
      [&promise1](Status s,
                  std::function<void()> success,
                  std::function<void()> failure) { promise1.set_value(s); });

  io_service_.run_one();

  ASSERT_TRUE(future1.get().ok());
  EXPECT_EQ(reply1.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_EQ("test_data", reply1.object().data());
  EXPECT_EQ("meta", reply1.object().metadata());

  // Simulate object going out of scope by removing the local reference
  reference_counter_->RemoveLocalReference(object_id, nullptr);

  std::promise<Status> promise2;
  auto future2 = promise2.get_future();
  rpc::GetObjectStatusReply reply2;

  core_worker_->HandleGetObjectStatus(
      request,
      &reply2,
      [&promise2](Status s,
                  std::function<void()> success,
                  std::function<void()> failure) { promise2.set_value(s); });

  // Not calling io_service_.run_one() because the callback is called on the main thread
  ASSERT_TRUE(future2.get().ok());
  EXPECT_EQ(reply2.status(), rpc::GetObjectStatusReply::OUT_OF_SCOPE);
}

}  // namespace core
}  // namespace ray
