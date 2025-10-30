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

#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "mock/ray/gcs_client/gcs_client.h"
#include "mock/ray/object_manager/plasma/client.h"
#include "ray/common/asio/fake_periodical_runner.h"
#include "ray/common/buffer.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker_rpc_proxy.h"
#include "ray/core_worker/future_resolver.h"
#include "ray/core_worker/grpc_service.h"
#include "ray/core_worker/object_recovery_manager.h"
#include "ray/core_worker/reference_counter.h"
#include "ray/core_worker/reference_counter_interface.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/task_submission/actor_task_submitter.h"
#include "ray/core_worker/task_submission/normal_task_submitter.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/core_worker_rpc_client/fake_core_worker_client.h"
#include "ray/object_manager/plasma/fake_plasma_client.h"
#include "ray/observability/fake_metric.h"
#include "ray/pubsub/fake_subscriber.h"
#include "ray/pubsub/publisher.h"
#include "ray/raylet_ipc_client/fake_raylet_ipc_client.h"
#include "ray/raylet_rpc_client/fake_raylet_client.h"

namespace ray {
namespace core {

using ::testing::_;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;

class CoreWorkerTest : public ::testing::Test {
 public:
  CoreWorkerTest()
      : io_work_(io_service_.get_executor()),
        task_execution_service_work_(task_execution_service_.get_executor()),
        current_time_ms_(0.0) {
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

    auto client_call_manager = std::make_unique<rpc::ClientCallManager>(
        io_service_, /*record_stats=*/false, /*local_address=*/"");

    auto core_worker_client_pool =
        std::make_shared<rpc::CoreWorkerClientPool>([](const rpc::Address &) {
          return std::make_shared<rpc::FakeCoreWorkerClient>();
        });

    auto raylet_client_pool = std::make_shared<rpc::RayletClientPool>(
        [](const rpc::Address &) { return std::make_shared<rpc::FakeRayletClient>(); });

    mock_gcs_client_ = std::make_shared<gcs::MockGcsClient>();

    auto fake_local_raylet_rpc_client = std::make_shared<rpc::FakeRayletClient>();

    auto fake_raylet_ipc_client = std::make_shared<ipc::FakeRayletIpcClient>();

    auto service_handler = std::make_unique<CoreWorkerServiceHandlerProxy>();
    auto worker_context = std::make_unique<WorkerContext>(
        WorkerType::WORKER, WorkerID::FromRandom(), JobID::FromInt(1));
    auto core_worker_server =
        std::make_unique<rpc::GrpcServer>(WorkerTypeString(options.worker_type), 0, true);
    core_worker_server->RegisterService(
        std::make_unique<rpc::CoreWorkerGrpcService>(
            io_service_, *service_handler, /*max_active_rpcs_per_handler_=*/-1),
        false /* token_auth */);
    core_worker_server->Run();

    rpc_address_.set_ip_address(options.node_ip_address);
    rpc_address_.set_port(core_worker_server->GetPort());
    rpc_address_.set_node_id(NodeID::FromRandom().Binary());
    rpc_address_.set_worker_id(worker_context->GetWorkerID().Binary());

    fake_periodical_runner_ = std::make_unique<FakePeriodicalRunner>();

    auto object_info_publisher = std::make_unique<pubsub::Publisher>(
        /*channels=*/
        std::vector<rpc::ChannelType>{rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                      rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL,
                                      rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL},
        /*periodical_runner=*/*fake_periodical_runner_,
        /*get_time_ms=*/[this]() { return current_time_ms_; },
        /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
        /*publish_batch_size_=*/RayConfig::instance().publish_batch_size(),
        worker_context->GetWorkerID());

    object_info_publisher_ = object_info_publisher.get();

    auto fake_object_info_subscriber = std::make_unique<pubsub::FakeSubscriber>();

    reference_counter_ = std::make_shared<ReferenceCounter>(
        rpc_address_,
        object_info_publisher.get(),
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
        rpc_address_);

    auto task_event_buffer = std::make_unique<worker::TaskEventBufferImpl>(
        std::make_unique<gcs::MockGcsClient>(),
        std::make_unique<rpc::EventAggregatorClientImpl>(0, *client_call_manager),
        "test_session");

    task_manager_ = std::make_shared<TaskManager>(
        *memory_store_,
        *reference_counter_,
        [](const RayObject &object, const ObjectID &object_id) { return Status::OK(); },
        [](TaskSpecification &spec, uint32_t delay_ms) {},
        [](const TaskSpecification &spec) { return false; },
        [](const JobID &job_id,
           const std::string &type,
           const std::string &error_message,
           double timestamp) { return Status::OK(); },
        RayConfig::instance().max_lineage_bytes(),
        *task_event_buffer,
        [](const ActorID &actor_id) {
          return std::make_shared<rpc::FakeCoreWorkerClient>();
        },
        mock_gcs_client_,
        fake_task_by_state_gauge_,
        fake_total_lineage_bytes_gauge_,
        /*free_actor_object_callback=*/[](const ObjectID &object_id) {});

    auto object_recovery_manager = std::make_unique<ObjectRecoveryManager>(
        rpc_address_,
        raylet_client_pool,
        [](const ObjectID &object_id, const ObjectLookupCallback &callback) {
          return Status::OK();
        },
        *task_manager_,
        *reference_counter_,
        *memory_store_,
        [](const ObjectID &object_id, rpc::ErrorType reason, bool pin_object) {});

    auto lease_policy = std::unique_ptr<LeasePolicyInterface>(
        std::make_unique<LocalLeasePolicy>(rpc_address_));

    auto lease_request_rate_limiter = std::make_shared<StaticLeaseRequestRateLimiter>(10);

    actor_creator_ = std::make_shared<ActorCreator>(mock_gcs_client_->Actors());

    auto normal_task_submitter = std::make_unique<NormalTaskSubmitter>(
        rpc_address_,
        fake_local_raylet_rpc_client,
        core_worker_client_pool,
        raylet_client_pool,
        std::move(lease_policy),
        memory_store_,
        *task_manager_,
        NodeID::Nil(),
        WorkerType::WORKER,
        10000,
        actor_creator_,
        JobID::Nil(),
        lease_request_rate_limiter,
        [](const ObjectID &object_id) { return rpc::TensorTransport::OBJECT_STORE; },
        boost::asio::steady_timer(io_service_),
        fake_scheduler_placement_time_s_histogram_);

    auto actor_task_submitter = std::make_unique<ActorTaskSubmitter>(
        *core_worker_client_pool,
        *memory_store_,
        *task_manager_,
        *actor_creator_,
        /*tensor_transport_getter=*/
        [](const ObjectID &object_id) { return rpc::TensorTransport::OBJECT_STORE; },
        [](const ActorID &actor_id, const std::string &, uint64_t num_queued) {},
        io_service_,
        reference_counter_);
    actor_task_submitter_ = actor_task_submitter.get();

    auto actor_manager = std::make_unique<ActorManager>(
        mock_gcs_client_, *actor_task_submitter, *reference_counter_);

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
                                                std::move(rpc_address_),
                                                mock_gcs_client_,
                                                std::move(fake_raylet_ipc_client),
                                                std::move(fake_local_raylet_rpc_client),
                                                io_thread_,
                                                reference_counter_,
                                                memory_store_,
                                                nullptr,  // plasma_store_provider_
                                                nullptr,  // mutable_object_provider_
                                                std::move(future_resolver),
                                                task_manager_,
                                                actor_creator_,
                                                std::move(actor_task_submitter),
                                                std::move(object_info_publisher),
                                                std::move(fake_object_info_subscriber),
                                                std::move(lease_request_rate_limiter),
                                                std::move(normal_task_submitter),
                                                std::move(object_recovery_manager),
                                                std::move(actor_manager),
                                                task_execution_service_,
                                                std::move(task_event_buffer),
                                                getpid(),
                                                fake_task_by_state_gauge_,
                                                fake_actor_by_state_gauge_);
  }

 protected:
  instrumented_io_context io_service_;
  instrumented_io_context task_execution_service_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      task_execution_service_work_;

  boost::thread io_thread_;

  rpc::Address rpc_address_;
  std::shared_ptr<ReferenceCounterInterface> reference_counter_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;
  ActorTaskSubmitter *actor_task_submitter_;
  pubsub::Publisher *object_info_publisher_;
  std::shared_ptr<TaskManager> task_manager_;
  std::shared_ptr<gcs::MockGcsClient> mock_gcs_client_;
  std::shared_ptr<ActorCreator> actor_creator_;
  std::shared_ptr<CoreWorker> core_worker_;
  ray::observability::FakeGauge fake_task_by_state_gauge_;
  ray::observability::FakeGauge fake_actor_by_state_gauge_;
  ray::observability::FakeGauge fake_total_lineage_bytes_gauge_;
  ray::observability::FakeHistogram fake_scheduler_placement_time_s_histogram_;
  std::unique_ptr<FakePeriodicalRunner> fake_periodical_runner_;

  // Controllable time for testing publisher timeouts
  double current_time_ms_;
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

TEST_F(CoreWorkerTest, RecordMetrics) {
  std::vector<std::shared_ptr<RayObject>> results;
  auto status = core_worker_->Get({}, -1, results);
  ASSERT_TRUE(status.ok());
  // disconnect to trigger metric recording
  core_worker_->Disconnect(rpc::WorkerExitType::SYSTEM_ERROR, "test", nullptr);
  auto tag_to_value = fake_task_by_state_gauge_.GetTagToValue();
  // 5 states: RUNNING, SUBMITTED_TO_WORKER, RUNNING_IN_RAY_GET, RUNNING_IN_RAY_WAIT, and
  // GETTING_AND_PINNING_ARGS
  ASSERT_EQ(tag_to_value.size(), 5);
  for (auto &[key, value] : tag_to_value) {
    ASSERT_EQ(key.at("Name"), "Unknown task");
    ASSERT_EQ(key.at("Source"), "executor");
    ASSERT_EQ(key.at("IsRetry"), "0");
  }
}

TEST_F(CoreWorkerTest, HandleGetObjectStatusIdempotency) {
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

TEST_F(CoreWorkerTest, HandleGetObjectStatusObjectPutAfterFirstRequest) {
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

TEST_F(CoreWorkerTest, HandleGetObjectStatusObjectFreedBetweenRequests) {
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

TEST_F(CoreWorkerTest, HandleGetObjectStatusObjectOutOfScope) {
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

namespace {

ObjectID CreateInlineObjectInMemoryStoreAndRefCounter(
    CoreWorkerMemoryStore &memory_store,
    ReferenceCounterInterface &reference_counter,
    rpc::Address &rpc_address) {
  auto inlined_dependency_id = ObjectID::FromRandom();
  std::string data = "hello";
  auto data_ptr = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(data.data()));
  auto data_buffer =
      std::make_shared<ray::LocalMemoryBuffer>(data_ptr, data.size(), /*copy_data=*/true);
  RayObject memory_store_object(data_buffer,
                                /*metadata=*/nullptr,
                                std::vector<rpc::ObjectReference>(),
                                /*copy_data=*/true);
  reference_counter.AddOwnedObject(inlined_dependency_id,
                                   /*contained_ids=*/{},
                                   rpc_address,
                                   "call_site",
                                   /*object_size=*/100,
                                   /*is_reconstructable=*/false,
                                   /*add_local_ref=*/true);
  memory_store.Put(memory_store_object, inlined_dependency_id);
  return inlined_dependency_id;
}
}  // namespace
TEST_F(CoreWorkerTest, ActorTaskCancelDuringDepResolution) {
  /*
  See https://github.com/ray-project/ray/pull/56123 for context.
  1. Put an inline object in the memory store + ref counter.
  2. Create an actor (just creating an actor queue in the submitter).
  3. Submit an actor task with the inline objects as dependencies.
  4. Cancel the actor task.
  5. Run the io context to completion to run the actual submission + dependency
     resolution logic.
  */

  auto inlined_dependency_id = CreateInlineObjectInMemoryStoreAndRefCounter(
      *memory_store_, *reference_counter_, rpc_address_);

  auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  actor_task_submitter_->AddActorQueueIfNotExists(actor_id,
                                                  /*max_pending_calls=*/-1,
                                                  /*allow_out_of_order_execution=*/false,
                                                  /*fail_if_actor_unreachable=*/true,
                                                  /*owned=*/false);

  TaskSpecification task;
  auto &task_message = task.GetMutableMessage();
  task_message.set_task_id(TaskID::FromRandom(actor_id.JobId()).Binary());
  task_message.set_type(TaskType::ACTOR_TASK);
  task_message.mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  task_message.add_args()->mutable_object_ref()->set_object_id(
      inlined_dependency_id.Binary());
  task_manager_->AddPendingTask(rpc_address_, task, "call_site");
  actor_task_submitter_->SubmitTask(task);

  actor_task_submitter_->CancelTask(task, /*recursive=*/false);

  while (io_service_.poll_one() > 0) {
  }
}

TEST(BatchingPassesTwoTwoOneIntoPlasmaGet, CallsPlasmaGetInCorrectBatches) {
  auto fake_raylet = std::make_shared<ipc::FakeRayletIpcClient>();
  // Build a ReferenceCounter with minimal dependencies.
  rpc::Address addr;
  addr.set_ip_address("127.0.0.1");
  auto is_node_dead = [](const NodeID &) { return false; };
  ReferenceCounter ref_counter(addr,
                               /*object_info_publisher=*/nullptr,
                               /*object_info_subscriber=*/nullptr,
                               is_node_dead);

  // Fake plasma client that records Get calls.
  std::vector<std::vector<ObjectID>> observed_batches;
  class RecordingPlasmaGetClient : public plasma::FakePlasmaClient {
   public:
    explicit RecordingPlasmaGetClient(std::vector<std::vector<ObjectID>> *observed)
        : observed_(observed) {}
    Status Get(const std::vector<ObjectID> &object_ids,
               int64_t timeout_ms,
               std::vector<plasma::ObjectBuffer> *object_buffers) override {
      if (observed_ != nullptr) {
        observed_->push_back(object_ids);
      }
      object_buffers->resize(object_ids.size());
      for (size_t i = 0; i < object_ids.size(); i++) {
        uint8_t byte = 0;
        auto parent = std::make_shared<LocalMemoryBuffer>(&byte, 1, /*copy_data=*/true);
        (*object_buffers)[i].data = SharedMemoryBuffer::Slice(parent, 0, 1);
        (*object_buffers)[i].metadata = SharedMemoryBuffer::Slice(parent, 0, 1);
      }
      return Status::OK();
    }

   private:
    std::vector<std::vector<ObjectID>> *observed_;
  };

  auto fake_plasma = std::make_shared<RecordingPlasmaGetClient>(&observed_batches);

  CoreWorkerPlasmaStoreProvider provider(
      /*store_socket=*/"",
      fake_raylet,
      ref_counter,
      /*check_signals=*/[] { return Status::OK(); },
      /*warmup=*/false,
      /*store_client=*/fake_plasma,
      /*fetch_batch_size=*/2,
      /*get_current_call_site=*/nullptr);

  // Build a set of 5 object ids.
  std::vector<ObjectID> ids;
  for (int i = 0; i < 5; i++) ids.push_back(ObjectID::FromRandom());
  absl::flat_hash_set<ObjectID> idset(ids.begin(), ids.end());

  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> results;

  ASSERT_TRUE(provider.Get(idset, /*timeout_ms=*/-1, &results).ok());

  // Assert: batches seen by plasma Get are [2,2,1].
  ASSERT_EQ(observed_batches.size(), 3U);
  EXPECT_EQ(observed_batches[0].size(), 2U);
  EXPECT_EQ(observed_batches[1].size(), 2U);
  EXPECT_EQ(observed_batches[2].size(), 1U);
}

class CoreWorkerPubsubWorkerObjectEvictionChannelTest
    : public CoreWorkerTest,
      public ::testing::WithParamInterface<bool> {};

TEST_P(CoreWorkerPubsubWorkerObjectEvictionChannelTest, HandlePubsubCommandBatchRetries) {
  // should_free_object: determines whether the object is freed from plasma. This is used
  // to trigger AddObjectOutOfScopeOrFreedCallback in HandlePubsubCommandBatch which
  // stores the unpin_object callback that publishes the message to the
  // WORKER_OBJECT_EVICTION channel
  // should_free_object == true: the object is freed from plasma and we expect the message
  // to the WORKER_OBJECT_EVICTION channel to be published.
  // should_free_object == false: the object is not freed and we expect the message to the
  // WORKER_OBJECT_EVICTION channel to not be published.
  bool should_free_object = GetParam();

  auto subscriber_id = NodeID::FromRandom();
  auto object_id = ObjectID::FromRandom();

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(object_id, {}, owner_address, "", 0, false, true);

  rpc::PubsubCommandBatchRequest command_batch_request;
  command_batch_request.set_subscriber_id(subscriber_id.Binary());
  auto *command = command_batch_request.add_commands();
  command->set_channel_type(rpc::ChannelType::WORKER_OBJECT_EVICTION);
  command->set_key_id(object_id.Binary());
  auto *sub_message = command->mutable_subscribe_message();
  auto *real_sub_message = sub_message->mutable_worker_object_eviction_message();
  real_sub_message->set_intended_worker_id(core_worker_->GetWorkerID().Binary());
  real_sub_message->set_object_id(object_id.Binary());
  *real_sub_message->mutable_subscriber_address() = rpc_address_;

  rpc::PubsubCommandBatchReply command_reply1;
  rpc::PubsubCommandBatchReply command_reply2;
  // Each call to HandlePubsubCommandBatch causes the reference counter to store the
  // unpin_object callback that publishes the WORKER_OBJECT_EVICTION message
  core_worker_->HandlePubsubCommandBatch(
      command_batch_request,
      &command_reply1,
      [](const Status &status, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(status.ok());
      });
  core_worker_->HandlePubsubCommandBatch(
      command_batch_request,
      &command_reply2,
      [](const Status &status, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(status.ok());
      });

  if (should_free_object) {
    // Triggers the unpin_object callbacks that publish the message to the
    // WORKER_OBJECT_EVICTION channel
    reference_counter_->FreePlasmaObjects({object_id});
  }

  rpc::PubsubLongPollingRequest request;
  request.set_subscriber_id(subscriber_id.Binary());
  request.set_max_processed_sequence_id(0);
  request.set_publisher_id("");

  rpc::PubsubLongPollingReply reply;

  // should_free_object == true: Each call to HandlePubsubCommandBatch adds an
  // unpin_object callback that is triggered via FreePlasmaObjects which publishes the
  // message to the WORKER_OBJECT_EVICTION channel, hence we have 1 publish per callback
  // so 2 in total. The long poll connection is closed
  // should_free_object == false: Since FreePlasmaObjects is not called, the unpin_object
  // callbacks are not triggered and we have 0 publishes. NOTE: The long poll connection
  // is not closed when should_free_object == false since there was no publish.
  core_worker_->HandlePubsubLongPolling(
      request,
      &reply,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });

  int expected_messages = should_free_object ? 2 : 0;
  EXPECT_EQ(reply.pub_messages_size(), expected_messages);

  for (int i = 0; i < expected_messages; i++) {
    const auto &msg = reply.pub_messages(i);
    EXPECT_EQ(msg.channel_type(), rpc::ChannelType::WORKER_OBJECT_EVICTION);
    EXPECT_EQ(msg.key_id(), object_id.Binary());
    EXPECT_EQ(msg.sequence_id(), i + 1);
    EXPECT_EQ(msg.worker_object_eviction_message().object_id(), object_id.Binary());
  }

  if (!should_free_object) {
    // Since the long poll connection is not closed, we need to flush it. Otherwise this
    // can trigger undefined behavior since unlike in prod where grpc arena allocates the
    // reply, here we allocate the reply on the stack. Hence the normal order of
    // destruction is: reply goes out of scope -> publisher is destructed -> flushes the
    // reply which access freed memory
    current_time_ms_ += RayConfig::instance().subscriber_timeout_ms();
    object_info_publisher_->CheckDeadSubscribers();
  }
}

INSTANTIATE_TEST_SUITE_P(WorkerObjectEvictionChannel,
                         CoreWorkerPubsubWorkerObjectEvictionChannelTest,
                         ::testing::Values(true, false));

class CoreWorkerPubsubWorkerRefRemovedChannelTest
    : public CoreWorkerTest,
      public ::testing::WithParamInterface<bool> {};

TEST_P(CoreWorkerPubsubWorkerRefRemovedChannelTest, HandlePubsubCommandBatchRetries) {
  // should_remove_ref: determines whether the object ref is removed from the reference
  // counter. This is used to trigger RemoveLocalReference in HandlePubsubCommandBatch
  // which flips the publish_ref_removed flag to true. Once the ref is removed via
  // RemoveLocalReference, the message to the WORKER_REF_REMOVED channel is published
  // should_remove_ref == true: the object ref is removed from the reference counter and
  // we expect the message to the WORKER_REF_REMOVED channel to be published.
  // should_remove_ref == false: the object ref is not removed from the reference counter
  // and we expect the message to the WORKER_REF_REMOVED channel to not be published.
  bool should_remove_ref = GetParam();

  auto subscriber_id = NodeID::FromRandom();
  auto object_id = ObjectID::FromRandom();

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(object_id, {}, owner_address, "", 0, false, true);

  rpc::PubsubCommandBatchRequest command_batch_request;
  command_batch_request.set_subscriber_id(subscriber_id.Binary());
  auto *command = command_batch_request.add_commands();
  command->set_channel_type(rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL);
  command->set_key_id(object_id.Binary());
  auto *sub_message = command->mutable_subscribe_message();
  auto *real_sub_message = sub_message->mutable_worker_ref_removed_message();
  real_sub_message->set_intended_worker_id(core_worker_->GetWorkerID().Binary());
  real_sub_message->mutable_reference()->set_object_id(object_id.Binary());
  real_sub_message->set_contained_in_id(ObjectID::FromRandom().Binary());
  real_sub_message->set_subscriber_worker_id(core_worker_->GetWorkerID().Binary());

  rpc::PubsubCommandBatchReply command_reply1;
  rpc::PubsubCommandBatchReply command_reply2;
  core_worker_->HandlePubsubCommandBatch(
      command_batch_request,
      &command_reply1,
      [](const Status &status, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(status.ok());
      });
  // NOTE: unlike in the worker object eviction channel test, the second call to
  // HandlePubsubComandBatch does not store a unique callback and just turns on
  // publish_ref_removed which is already true
  core_worker_->HandlePubsubCommandBatch(
      command_batch_request,
      &command_reply2,
      [](const Status &status, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(status.ok());
      });

  if (should_remove_ref) {
    // This will check the publish_ref_removed flag and publish one
    // message to the WORKER_REF_REMOVED channel
    reference_counter_->RemoveLocalReference(object_id, nullptr);
  }

  rpc::PubsubLongPollingRequest request;
  request.set_subscriber_id(subscriber_id.Binary());
  request.set_max_processed_sequence_id(0);
  request.set_publisher_id("");

  rpc::PubsubLongPollingReply reply;

  // should_remove_ref == true: each call to HandlePubsubCommandBatch modifies the
  // publish_ref_removed flag and RemoveLocalReference triggers one single publish
  // should_remove_ref == false: since RemoveLocalReference is not called, the ref remains
  // in scope and no publish is triggered
  core_worker_->HandlePubsubLongPolling(
      request,
      &reply,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });

  int expected_messages = should_remove_ref ? 1 : 0;
  EXPECT_EQ(reply.pub_messages_size(), expected_messages);

  if (should_remove_ref) {
    const auto &msg1 = reply.pub_messages(0);
    EXPECT_EQ(msg1.channel_type(), rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL);
    EXPECT_EQ(msg1.key_id(), object_id.Binary());
    EXPECT_EQ(msg1.sequence_id(), 1);
    EXPECT_EQ(msg1.worker_ref_removed_message().borrowed_refs_size(), 0);
  }
  if (!should_remove_ref) {
    // See the above comment in the worker object eviction channel test
    current_time_ms_ += RayConfig::instance().subscriber_timeout_ms();
    object_info_publisher_->CheckDeadSubscribers();
  }
}

INSTANTIATE_TEST_SUITE_P(WorkerRefRemovedChannel,
                         CoreWorkerPubsubWorkerRefRemovedChannelTest,
                         ::testing::Values(true, false));

TEST_F(CoreWorkerTest, HandlePubsubWorkerObjectLocationsChannelRetries) {
  // Unlike the other pubsub channel tests, this test starts off with a LongPollingRequest
  // to test what happens when a HandlePubsubCommandBatch encounters an open long poll
  // connection
  auto subscriber_id = NodeID::FromRandom();
  auto object_id = ObjectID::FromRandom();
  auto node_id = NodeID::FromRandom();
  const uint64_t object_size = 1024;

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(
      object_id, {}, owner_address, "", object_size, false, true);
  // NOTE: this triggers a publish to no subscribers so its not stored in any mailbox but
  // bumps the sequence id by 1
  reference_counter_->AddObjectLocation(object_id, node_id);

  rpc::PubsubLongPollingRequest request;
  request.set_subscriber_id(subscriber_id.Binary());
  request.set_max_processed_sequence_id(0);
  request.set_publisher_id("");

  rpc::PubsubLongPollingReply long_polling_reply1;
  core_worker_->HandlePubsubLongPolling(
      request,
      &long_polling_reply1,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });

  rpc::PubsubCommandBatchRequest command_batch_request;
  command_batch_request.set_subscriber_id(subscriber_id.Binary());
  auto *command = command_batch_request.add_commands();
  command->set_channel_type(rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL);
  command->set_key_id(object_id.Binary());
  auto *sub_message = command->mutable_subscribe_message();
  auto *real_sub_message = sub_message->mutable_worker_object_locations_message();
  real_sub_message->set_intended_worker_id(core_worker_->GetWorkerID().Binary());
  real_sub_message->set_object_id(object_id.Binary());

  // The first call to HandlePubsubCommandBatch publishes the object location. The
  // publisher stores the first snapshot in the mailbox, sends it to the subscriber, and
  // closes the long poll connection.
  rpc::PubsubCommandBatchReply command_reply1;
  core_worker_->HandlePubsubCommandBatch(
      command_batch_request,
      &command_reply1,
      [](const Status &status, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(status.ok());
      });

  // The second call to HandlePubsubCommandBatch publishes the object location. The
  // publisher stores the second snapshot in the mailbox.
  rpc::PubsubCommandBatchReply command_reply2;
  core_worker_->HandlePubsubCommandBatch(
      command_batch_request,
      &command_reply2,
      [](const Status &status, std::function<void()>, std::function<void()>) {
        ASSERT_TRUE(status.ok());
      });

  // Since the max_processed_sequence_id is 0, the publisher sends the second AND first
  // snapshot of the object location. The first snapshot is not erased until it gets a
  // long poll request with a max_processed_sequence_id greater or equal to the first
  // snapshot's sequence id.
  rpc::PubsubLongPollingReply long_polling_reply2;
  core_worker_->HandlePubsubLongPolling(
      request,
      &long_polling_reply2,
      [](Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
      });

  EXPECT_EQ(long_polling_reply1.pub_messages_size(), 1);
  EXPECT_EQ(long_polling_reply2.pub_messages_size(), 2);

  auto CheckMessage = [&](const rpc::PubMessage &msg, int i) {
    EXPECT_EQ(msg.channel_type(), rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL);
    EXPECT_EQ(msg.key_id(), object_id.Binary());
    EXPECT_EQ(msg.worker_object_locations_message().node_ids_size(), 1);
    EXPECT_EQ(msg.worker_object_locations_message().object_size(), object_size);
    EXPECT_EQ(msg.worker_object_locations_message().node_ids(0), node_id.Binary());
    // AddObjectLocation triggers a publish so the sequence id is bumped by 1
    EXPECT_EQ(msg.sequence_id(), i + 2);
  };
  for (int i = 0; i < 2; i++) {
    if (i == 0) {
      const auto &msg = long_polling_reply1.pub_messages(i);
      CheckMessage(msg, i);
    }
    const auto &msg = long_polling_reply2.pub_messages(i);
    CheckMessage(msg, i);
  }
}

class HandleWaitForActorRefDeletedRetriesTest
    : public CoreWorkerTest,
      public ::testing::WithParamInterface<bool> {};

TEST_P(HandleWaitForActorRefDeletedRetriesTest, ActorRefDeletedForRegisteredActor) {
  // delete_actor_handle: determines whether the actor handle is removed from the
  // reference counter. This is used to trigger the send_reply_callback which is stored in
  // the reference counter via delete_actor_handle == true: the actor handle is removed
  // from the reference counter and we expect the send_reply_callback to be triggered.
  // delete_actor_handle == false: the actor handle is not removed from the reference
  // counter and we expect the send_reply_callback to not be triggered.
  bool delete_actor_handle = GetParam();

  auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 0);
  auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(
      actor_creation_return_id, {}, owner_address, "test", 0, false, true);

  rpc::WaitForActorRefDeletedRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_intended_worker_id(core_worker_->GetWorkerID().Binary());

  size_t callback_count = 0;
  rpc::WaitForActorRefDeletedReply reply1;
  rpc::WaitForActorRefDeletedReply reply2;

  core_worker_->HandleWaitForActorRefDeleted(
      request,
      &reply1,
      [&callback_count](
          Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
        callback_count++;
      });

  if (delete_actor_handle) {
    std::vector<ObjectID> deleted;
    // Triggers the send_reply_callback which is stored in the reference counter
    reference_counter_->RemoveLocalReference(actor_creation_return_id, &deleted);
    ASSERT_EQ(deleted.size(), 1u);
    ASSERT_EQ(callback_count, 1);
  } else {
    ASSERT_EQ(callback_count, 0);
  }

  // The send_reply_callback is immediately triggered since the object has gone out of
  // scope if delete_actor_handle is true. Otherwise, it is not triggered.
  core_worker_->HandleWaitForActorRefDeleted(
      request,
      &reply2,
      [&callback_count](
          Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
        callback_count++;
      });

  if (delete_actor_handle) {
    ASSERT_EQ(callback_count, 2);
  } else {
    ASSERT_EQ(callback_count, 0);
  }
}

INSTANTIATE_TEST_SUITE_P(ActorRefDeletedForRegisteredActor,
                         HandleWaitForActorRefDeletedRetriesTest,
                         ::testing::Values(true, false));

class HandleWaitForActorRefDeletedWhileRegisteringRetriesTest
    : public CoreWorkerTest,
      public ::testing::WithParamInterface<bool> {};

TEST_P(HandleWaitForActorRefDeletedWhileRegisteringRetriesTest,
       ActorRefDeletedForRegisteringActor) {
  // delete_actor_handle: determines whether the actor handle is removed from the
  // reference counter. This is used to trigger the send_reply_callback which is stored in
  // the reference counter via delete_actor_handle == true: the actor handle is removed
  // from the reference counter and we expect the send_reply_callback to be triggered.
  // delete_actor_handle == false: the actor handle is not removed from the reference
  // counter and we expect the send_reply_callback to not be triggered.
  bool delete_actor_handle = GetParam();

  auto actor_id = ActorID::Of(JobID::FromInt(0), TaskID::Nil(), 1);
  auto actor_creation_return_id = ObjectID::ForActorHandle(actor_id);

  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());

  reference_counter_->AddOwnedObject(
      actor_creation_return_id, {}, owner_address, "test", 0, false, true);

  rpc::TaskSpec task_spec_msg;
  task_spec_msg.set_type(rpc::TaskType::ACTOR_CREATION_TASK);
  auto *actor_creation_spec = task_spec_msg.mutable_actor_creation_task_spec();
  actor_creation_spec->set_actor_id(actor_id.Binary());
  actor_creation_spec->set_max_actor_restarts(0);
  actor_creation_spec->set_max_task_retries(0);
  TaskSpecification task_spec(task_spec_msg);

  gcs::StatusCallback register_callback;
  EXPECT_CALL(*mock_gcs_client_->mock_actor_accessor,
              AsyncRegisterActor(::testing::_, ::testing::_, ::testing::_))
      .WillOnce(::testing::SaveArg<1>(&register_callback));

  actor_creator_->AsyncRegisterActor(task_spec, nullptr);

  ASSERT_TRUE(actor_creator_->IsActorInRegistering(actor_id));

  rpc::WaitForActorRefDeletedRequest request;
  request.set_actor_id(actor_id.Binary());
  request.set_intended_worker_id(core_worker_->GetWorkerID().Binary());

  size_t callback_count = 0;
  rpc::WaitForActorRefDeletedReply reply1;
  rpc::WaitForActorRefDeletedReply reply2;

  // Since the actor is in the registering state, we store the callbacks and trigger them
  // when the the actor is done registering.
  core_worker_->HandleWaitForActorRefDeleted(
      request,
      &reply1,
      [&callback_count](
          Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
        callback_count++;
      });

  core_worker_->HandleWaitForActorRefDeleted(
      request,
      &reply2,
      [&callback_count](
          Status s, std::function<void()> success, std::function<void()> failure) {
        ASSERT_TRUE(s.ok());
        callback_count++;
      });

  ASSERT_EQ(callback_count, 0);
  register_callback(Status::OK());
  // Triggers the callbacks passed to AsyncWaitForActorRegisterFinish
  ASSERT_FALSE(actor_creator_->IsActorInRegistering(actor_id));

  if (delete_actor_handle) {
    std::vector<ObjectID> deleted;
    // Triggers the send_reply_callback which is stored in the reference counter
    reference_counter_->RemoveLocalReference(actor_creation_return_id, &deleted);
    ASSERT_EQ(deleted.size(), 1u);
    ASSERT_EQ(callback_count, 2);
  } else {
    ASSERT_EQ(callback_count, 0);
  }
}

INSTANTIATE_TEST_SUITE_P(ActorRefDeletedForRegisteringActor,
                         HandleWaitForActorRefDeletedWhileRegisteringRetriesTest,
                         ::testing::Values(true, false));

}  // namespace core
}  // namespace ray
