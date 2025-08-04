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
#include <vector>
#include <utility>
#include <iostream>
#include <unordered_map>

#include "fakes/ray/common/asio/fake_periodical_runner.h"
#include "mock/ray/core_worker/actor_creator.h"
#include "mock/ray/core_worker/experimental_mutable_object_provider.h"
#include "mock/ray/core_worker/lease_policy.h"
#include "mock/ray/core_worker/task_manager_interface.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "mock/ray/object_manager/plasma/client.h"
#include "mock/ray/pubsub/publisher.h"
#include "mock/ray/pubsub/subscriber.h"
#include "mock/ray/raylet_client/raylet_client.h"
#include "mock/ray/rpc/worker/core_worker_client.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_rpc_proxy.h"
#include "ray/core_worker/future_resolver.h"
#include "ray/core_worker/object_recovery_manager.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/transport/actor_task_submitter.h"
#include "ray/core_worker/transport/normal_task_submitter.h"
#include "ray/rpc/worker/core_worker_client_pool.h"

namespace ray {
namespace core {

using ::testing::_;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;

namespace {

class MockTaskEventBuffer : public worker::TaskEventBuffer {
 public:
  void AddTaskEvent(std::unique_ptr<worker::TaskEvent> task_event) override {
    task_events.emplace_back(std::move(task_event));
  }

  void FlushEvents(bool forced) override {}

  Status Start(bool auto_flush = true) override { return Status::OK(); }

  void Stop() override {}

  bool Enabled() const override { return true; }

  std::string DebugString() override { return ""; }

  std::vector<std::unique_ptr<worker::TaskEvent>> task_events;
};

class MockActorTaskSubmitter : public ActorTaskSubmitterInterface {
 public:
  MockActorTaskSubmitter() : ActorTaskSubmitterInterface() {}
  MOCK_METHOD5(AddActorQueueIfNotExists,
               void(const ActorID &actor_id,
                    int32_t max_pending_calls,
                    bool execute_out_of_order,
                    bool fail_if_actor_unreachable,
                    bool owned));
  MOCK_METHOD3(ConnectActor,
               void(const ActorID &actor_id,
                    const rpc::Address &address,
                    int64_t num_restarts));
  MOCK_METHOD5(DisconnectActor,
               void(const ActorID &actor_id,
                    int64_t num_restarts,
                    bool dead,
                    const rpc::ActorDeathCause &death_cause,
                    bool is_restartable));

  MOCK_METHOD0(CheckTimeoutTasks, void());

  MOCK_METHOD(void, SetPreempted, (const ActorID &actor_id), (override));

  virtual ~MockActorTaskSubmitter() {}
};

}  // namespace

class CoreWorkerHandleGetObjectStatusTest : public ::testing::Test {
 public:
  CoreWorkerHandleGetObjectStatusTest()
      : io_work_(io_service_.get_executor()),
        task_execution_service_work_(task_execution_service_.get_executor()),
        core_worker_client_pool_(
            std::make_shared<rpc::CoreWorkerClientPool>([](const rpc::Address &) {
              return std::make_shared<rpc::MockCoreWorkerClientInterface>();
            })),
        raylet_client_pool_(
            std::make_shared<rpc::RayletClientPool>([](const rpc::Address &) {
              return std::make_shared<MockRayletClientInterface>();
            })) {}

  void SetUp() override {
    CoreWorkerOptions options;
    options.worker_type = WorkerType::WORKER;
    options.language = Language::PYTHON;
    options.store_socket = "/tmp/plasma_socket_test";
    options.raylet_socket = "/tmp/raylet_socket_test";
    options.job_id = job_id_;
    // gcs_options uses default constructor
    options.enable_logging = false;
    options.log_dir = "";
    options.install_failure_signal_handler = false;
    options.interactive = false;
    options.node_ip_address = "127.0.0.1";
    options.node_manager_port = 12345;
    options.raylet_ip_address = "127.0.0.1";
    options.driver_name = "test_driver";
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
      // Dummy task execution callback for testing - just return OK
      return Status::OK();
    };
    options.free_actor_object_callback = nullptr;
    options.check_signals = nullptr;
    options.initialize_thread_callback = nullptr;
    options.gc_collect = nullptr;
    options.spill_objects = nullptr;
    options.restore_spilled_objects = nullptr;
    options.delete_spilled_objects = nullptr;
    options.unhandled_exception_handler = nullptr;
    options.get_lang_stack = nullptr;
    options.kill_main = nullptr;
    options.cancel_async_actor_task = nullptr;
    options.is_local_mode = false;
    options.terminate_asyncio_thread = nullptr;
    options.serialized_job_config = "{}";
    options.metrics_agent_port = -1;
    options.runtime_env_hash = 123456;
    options.startup_token = StartupToken{0};
    options.cluster_id = ClusterID::FromHex("0123456789abcdef0123456789abcdef01234567");
    options.object_allocator = nullptr;
    options.session_name = "test_session";
    options.entrypoint = "test_entrypoint";
    options.worker_launch_time_ms = 1000;
    options.worker_launched_time_ms = 2000;
    options.debug_source = "test_debug_source";
    options.enable_resource_isolation = false;

    worker_id_ = WorkerID::FromRandom();
    job_id_ = JobID::FromInt(1);
    object_id_ = ObjectID::FromRandom();
    service_handler_ = std::make_unique<CoreWorkerServiceHandlerProxy>();
    worker_context_ =
        std::make_unique<WorkerContext>(WorkerType::WORKER, worker_id_, job_id_);
    core_worker_server_ = std::make_unique<rpc::GrpcServer>(
        WorkerTypeString(options.worker_type), 0, options.node_ip_address == "127.0.0.1");
    // Start RPC server after all the task receivers are properly initialized and we have
    // our assigned port from the raylet.
    core_worker_server_->RegisterService(
        std::make_unique<rpc::CoreWorkerGrpcService>(io_service_, *service_handler_),
        false /* token_auth */);
    core_worker_server_->Run();
    // Create other mock dependencies
    rpc_address_.set_ip_address(options.node_ip_address);
    rpc_address_.set_port(core_worker_server_->GetPort());
    rpc_address_.set_raylet_id(NodeID::FromRandom().Binary());
    rpc_address_.set_worker_id(worker_context_->GetWorkerID().Binary());
    mock_gcs_client_ = std::make_shared<gcs::MockGcsClient>();
    // mock_task_manager_ = std::make_shared<MockTaskManagerInterface>();
    mock_actor_creator_ = std::make_shared<MockActorCreatorInterface>();
    mock_lease_policy_ = std::make_shared<MockLeasePolicyInterface>();
    // mock_experimental_mutable_object_provider_ =
    //     std::make_shared<experimental::MockMutableObjectProvider>();
    mock_plasma_client_ = std::make_shared<plasma::MockPlasmaClient>();
    mock_object_info_publisher_ = std::make_unique<pubsub::MockPublisher>();
    mock_object_info_subscriber_ = std::make_unique<pubsub::MockSubscriber>();
    mock_local_raylet_client_ = std::make_shared<MockRayletClientInterface>();

    // Create submitters
    lease_request_rate_limiter_ = std::make_shared<StaticLeaseRequestRateLimiter>(10);

    // mock_actor_task_submitter_ = std::make_unique<MockActorTaskSubmitter>();

    // Create other required objects
    periodical_runner_ = std::make_shared<FakePeriodicalRunner>();

    // plasma_store_provider_ = std::make_shared<CoreWorkerPlasmaStoreProvider>(
    //     mock_plasma_client_, nullptr, io_service_, false);

    auto report_locality_data_callback = [](const ObjectID &object_id,
                                            const absl::flat_hash_set<NodeID> &locations,
                                            uint64_t object_size) {};

    // Create real reference counter and memory store (as per requirements)
    reference_counter_ = std::make_shared<ReferenceCounter>(
        rpc_address_,
        mock_object_info_publisher_.get(),
        mock_object_info_subscriber_.get(),
        [](const NodeID &) { return false; },
        false);
    memory_store_ = std::make_shared<CoreWorkerMemoryStore>(
        io_service_, reference_counter_.get(), nullptr);

    future_resolver_ =
        std::make_unique<FutureResolver>(memory_store_,
                                         reference_counter_,
                                         std::move(report_locality_data_callback),
                                         core_worker_client_pool_,
                                         rpc_address_);

    // object_recovery_manager_ = std::make_shared<ObjectRecoveryManager>(
    //     rpc_address_, raylet_client_pool_, core_worker_client_pool_);

    // actor_manager_ = std::make_unique<ActorManager>(
    //     mock_gcs_client_, *mock_actor_task_submitter_, *reference_counter_);

    task_event_buffer_ = std::make_unique<MockTaskEventBuffer>();

    client_call_manager_ =
        std::make_unique<rpc::ClientCallManager>(io_service_, /*record_stats=*/false);

    // mock_normal_task_submitter_ = std::make_unique<NormalTaskSubmitter>();

    // Start the io_thread to run the io_service
    io_thread_ = boost::thread([this]() { io_service_.run(); });

    // Create CoreWorker with dependency injection
    CreateCoreWorker(options);
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  void TearDown() override {
    // Clean up the CoreWorker first
    core_worker_.reset();

    // Stop the io_service and join the thread
    io_service_.stop();
    if (io_thread_.joinable()) {
      io_thread_.join();
    }
  }

  void CreateCoreWorker(CoreWorkerOptions options) {
    core_worker_ = std::make_shared<CoreWorker>(std::move(options),
                                                std::move(worker_context_),
                                                io_service_,
                                                std::move(client_call_manager_),
                                                core_worker_client_pool_,
                                                raylet_client_pool_,
                                                periodical_runner_,
                                                std::move(core_worker_server_),
                                                rpc_address_,
                                                mock_gcs_client_,
                                                std::move(mock_local_raylet_client_),
                                                io_thread_,
                                                reference_counter_,
                                                memory_store_,
                                                nullptr,
                                                nullptr,
                                                std::move(future_resolver_),
                                                nullptr,
                                                nullptr,
                                                nullptr,
                                                std::move(mock_object_info_publisher_),
                                                std::move(mock_object_info_subscriber_),
                                                lease_request_rate_limiter_,
                                                nullptr,
                                                nullptr,
                                                nullptr,
                                                task_execution_service_,
                                                std::move(task_event_buffer_),
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
  WorkerID worker_id_;
  JobID job_id_;
  ObjectID object_id_;
  rpc::Address rpc_address_;

  // Real dependencies (as per requirements)
  std::shared_ptr<ReferenceCounter> reference_counter_;
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;

  // Mock dependencies
  std::unique_ptr<WorkerContext> worker_context_;
  std::shared_ptr<gcs::MockGcsClient> mock_gcs_client_;
  std::shared_ptr<MockTaskManagerInterface> mock_task_manager_;
  std::shared_ptr<MockActorCreatorInterface> mock_actor_creator_;
  std::shared_ptr<MockLeasePolicyInterface> mock_lease_policy_;
  std::shared_ptr<experimental::MockMutableObjectProvider>
      mock_experimental_mutable_object_provider_;
  std::shared_ptr<plasma::MockPlasmaClient> mock_plasma_client_;
  std::unique_ptr<pubsub::PublisherInterface> mock_object_info_publisher_;
  std::unique_ptr<pubsub::SubscriberInterface> mock_object_info_subscriber_;
  std::shared_ptr<MockRayletClientInterface> mock_local_raylet_client_;

  // Other dependencies
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
  std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool_;
  std::shared_ptr<rpc::RayletClientPool> raylet_client_pool_;
  std::shared_ptr<LeaseRequestRateLimiter> lease_request_rate_limiter_;
  std::unique_ptr<MockActorTaskSubmitter> mock_actor_task_submitter_;
  std::unique_ptr<NormalTaskSubmitter> mock_normal_task_submitter_;
  std::shared_ptr<FakePeriodicalRunner> periodical_runner_;
  std::shared_ptr<CoreWorkerPlasmaStoreProvider> plasma_store_provider_;
  std::unique_ptr<FutureResolver> future_resolver_;
  std::shared_ptr<ObjectRecoveryManager> object_recovery_manager_;
  std::unique_ptr<ActorManager> actor_manager_;
  std::unique_ptr<worker::TaskEventBuffer> task_event_buffer_;
  std::unique_ptr<rpc::GrpcServer> core_worker_server_;
  std::unique_ptr<CoreWorkerServiceHandlerProxy> service_handler_;
  std::shared_ptr<CoreWorker> core_worker_;
};

TEST_F(CoreWorkerHandleGetObjectStatusTest, IdempotencyTest) {
  // Set up object in memory store
  auto data = std::make_shared<LocalMemoryBuffer>(
      const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>("test_data")), 9, true);
  auto metadata = std::make_shared<LocalMemoryBuffer>(
      const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>("meta")), 4, true);
  auto ray_object =
      std::make_shared<RayObject>(data, metadata, std::vector<rpc::ObjectReference>());

  // Add ownership information to reference counter first (required for memory store)
  rpc::Address owner_address;
  owner_address.set_worker_id(core_worker_->GetWorkerID().Binary());
  reference_counter_->AddOwnedObject(object_id_, {}, owner_address, "", 0, false, true);

  // Add object to memory store
  ASSERT_TRUE(memory_store_->Put(*ray_object, object_id_));

  // First call to HandleGetObjectStatus
  rpc::GetObjectStatusRequest request1;
  request1.set_object_id(object_id_.Binary());
  request1.set_owner_worker_id(core_worker_->GetWorkerID().Binary());

  rpc::GetObjectStatusReply reply1;
  bool callback_called1 = false;
  Status status1;
  std::mutex callback_mutex1;
  std::condition_variable callback_cv1;

  std::cout << "Calling HandleGetObjectStatus with object_id: " << object_id_.Hex()
            << " and worker_id: " << core_worker_->GetWorkerID().Hex() << std::endl;

  core_worker_->HandleGetObjectStatus(
      request1,
      &reply1,
      [&](Status s, std::function<void()> success, std::function<void()> failure) {
        std::lock_guard<std::mutex> lock(callback_mutex1);
        std::cout << "Callback invoked with status: " << s.ToString() << std::endl;
        status1 = s;
        callback_called1 = true;
        callback_cv1.notify_one();
      });

  // Wait for async operation to complete
  {
    std::unique_lock<std::mutex> lock(callback_mutex1);
    callback_cv1.wait_for(
        lock, std::chrono::milliseconds(1000), [&] { return callback_called1; });
  }

  std::cout << "Callback called: " << callback_called1 << std::endl;
  if (callback_called1) {
    std::cout << "Reply status: " << reply1.status() << std::endl;
    std::cout << "Reply has object: " << reply1.has_object() << std::endl;
  }

  EXPECT_TRUE(callback_called1);
  EXPECT_TRUE(status1.ok());
  EXPECT_EQ(reply1.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_TRUE(reply1.has_object());
  EXPECT_EQ(reply1.object().data(), "test_data");
  EXPECT_EQ(reply1.object().metadata(), "meta");

  // Second call to HandleGetObjectStatus (idempotency test)
  rpc::GetObjectStatusRequest request2;
  request2.set_object_id(object_id_.Binary());
  request2.set_owner_worker_id(core_worker_->GetWorkerID().Binary());

  rpc::GetObjectStatusReply reply2;
  bool callback_called2 = false;
  Status status2;
  std::mutex callback_mutex2;
  std::condition_variable callback_cv2;

  core_worker_->HandleGetObjectStatus(
      request2,
      &reply2,
      [&](Status s, std::function<void()> success, std::function<void()> failure) {
        std::lock_guard<std::mutex> lock(callback_mutex2);
        status2 = s;
        callback_called2 = true;
        callback_cv2.notify_one();
      });

  // Wait for async operation to complete
  {
    std::unique_lock<std::mutex> lock(callback_mutex2);
    callback_cv2.wait_for(
        lock, std::chrono::milliseconds(1000), [&] { return callback_called2; });
  }

  EXPECT_TRUE(callback_called2);
  EXPECT_TRUE(status2.ok());
  EXPECT_EQ(reply2.status(), rpc::GetObjectStatusReply::CREATED);
  EXPECT_TRUE(reply2.has_object());
  EXPECT_EQ(reply2.object().data(), "test_data");
  EXPECT_EQ(reply2.object().metadata(), "meta");

  // Verify both replies are identical (idempotency)
  EXPECT_EQ(reply1.status(), reply2.status());
  EXPECT_EQ(reply1.object().data(), reply2.object().data());
  EXPECT_EQ(reply1.object().metadata(), reply2.object().metadata());
}

}  // namespace core
}  // namespace ray
