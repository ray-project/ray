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
#include "ray/util/util.h"

namespace ray {

class ServiceBasedGcsClientTest : public ::testing::Test {
 public:
  ServiceBasedGcsClientTest() {
    RayConfig::instance().initialize(
        {{"ping_gcs_rpc_server_max_retries", std::to_string(60)},
         {"maximum_gcs_destroyed_actor_cached_count", std::to_string(10)},
         {"maximum_gcs_dead_node_cached_count", std::to_string(10)}});
    TestSetupUtil::StartUpRedisServers(std::vector<int>());
  }

  virtual ~ServiceBasedGcsClientTest() { TestSetupUtil::ShutDownRedisServers(); }

 protected:
  void SetUp() override {
    config_.grpc_server_port = 0;
    config_.grpc_server_name = "MockedGcsServer";
    config_.grpc_server_thread_num = 1;
    config_.redis_address = "127.0.0.1";
    config_.is_test = true;
    config_.redis_port = TEST_REDIS_SERVER_PORTS.front();

    client_io_service_.reset(new boost::asio::io_service());
    client_io_service_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*client_io_service_));
      client_io_service_->run();
    }));

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
    gcs_client_.reset(new gcs::ServiceBasedGcsClient(options));
    RAY_CHECK_OK(gcs_client_->Connect(*client_io_service_));
  }

  void TearDown() override {
    client_io_service_->stop();
    gcs_client_->Disconnect();

    gcs_server_->Stop();
    server_io_service_->stop();
    server_io_service_thread_->join();
    gcs_server_.reset();
    TestSetupUtil::FlushAllRedisServers();
    client_io_service_thread_->join();
  }

  void RestartGcsServer() {
    RAY_LOG(INFO) << "Stopping GCS service, port = " << gcs_server_->GetPort();
    gcs_server_->Stop();
    server_io_service_->stop();
    server_io_service_thread_->join();
    gcs_server_.reset();
    RAY_LOG(INFO) << "Finished stopping GCS service.";

    server_io_service_.reset(new boost::asio::io_service());
    gcs_server_.reset(new gcs::GcsServer(config_, *server_io_service_));
    gcs_server_->Start();
    server_io_service_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*server_io_service_));
      server_io_service_->run();
    }));

    // Wait until server starts listening.
    while (gcs_server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    RAY_LOG(INFO) << "GCS service restarted, port = " << gcs_server_->GetPort();
  }

  bool SubscribeToAllJobs(
      const gcs::SubscribeCallback<JobID, rpc::JobTableData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncSubscribeAll(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool AddJob(const std::shared_ptr<rpc::JobTableData> &job_table_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncAdd(
        job_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool MarkJobFinished(const JobID &job_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFinished(
        job_id, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool SubscribeActor(
      const ActorID &actor_id,
      const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribe(
        actor_id, subscribe,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  void UnsubscribeActor(const ActorID &actor_id) {
    RAY_CHECK_OK(gcs_client_->Actors().AsyncUnsubscribe(actor_id));
  }

  void WaitForActorUnsubscribed(const ActorID &actor_id) {
    auto condition = [this, actor_id]() {
      return gcs_client_->Actors().IsActorUnsubscribed(actor_id);
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  bool SubscribeAllActors(
      const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribeAll(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterActor(const std::shared_ptr<rpc::ActorTableData> &actor_table_data,
                     bool is_detached = true, bool skip_wait = false) {
    rpc::TaskSpec message;
    auto actor_id = ActorID::FromBinary(actor_table_data->actor_id());
    message.set_job_id(actor_id.JobId().Binary());
    message.set_type(TaskType::ACTOR_CREATION_TASK);
    message.set_task_id(TaskID::ForActorCreationTask(actor_id).Binary());
    message.set_caller_id(actor_id.Binary());
    message.set_max_retries(0);
    message.set_num_returns(1);
    message.set_parent_task_id(TaskID::ForActorCreationTask(actor_id).Binary());
    message.mutable_actor_creation_task_spec()->set_actor_id(actor_id.Binary());
    message.mutable_actor_creation_task_spec()->set_is_detached(is_detached);
    // If the actor is non-detached, the `WaitForActorOutOfScope` function of the core
    // worker client is called during the actor registration process. In order to simulate
    // the scenario of registration failure, we set the address to an illegal value.
    if (!is_detached) {
      rpc::Address address;
      address.set_ip_address("");
      message.mutable_caller_address()->CopyFrom(address);
    }
    TaskSpecification task_spec(message);

    if (skip_wait) {
      return gcs_client_->Actors()
          .AsyncRegisterActor(task_spec, [](Status status) {})
          .ok();
    }

    // NOTE: GCS will not reply when actor registration fails, so when GCS restarts, gcs
    // client will register the actor again and promise may be set twice.
    auto promise = std::make_shared<std::promise<bool>>();
    RAY_CHECK_OK(
        gcs_client_->Actors().AsyncRegisterActor(task_spec, [promise](Status status) {
          try {
            promise->set_value(status.ok());
          } catch (...) {
          }
        }));
    return WaitReady(promise->get_future(), timeout_ms_);
  }

  rpc::ActorTableData GetActor(const ActorID &actor_id) {
    std::promise<bool> promise;
    rpc::ActorTableData actor_table_data;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncGet(
        actor_id, [&actor_table_data, &promise](
                      Status status, const boost::optional<rpc::ActorTableData> &result) {
          assert(result);
          actor_table_data.CopyFrom(*result);
          promise.set_value(true);
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return actor_table_data;
  }

  std::vector<rpc::ActorTableData> GetAllActors(bool filter_non_dead_actor = false) {
    std::promise<bool> promise;
    std::vector<rpc::ActorTableData> actors;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncGetAll(
        [filter_non_dead_actor, &actors, &promise](
            Status status, const std::vector<rpc::ActorTableData> &result) {
          if (!result.empty()) {
            if (filter_non_dead_actor) {
              for (auto &iter : result) {
                if (iter.state() == gcs::ActorTableData::DEAD) {
                  actors.emplace_back(iter);
                }
              }
            } else {
              actors.assign(result.begin(), result.end());
            }
          }
          promise.set_value(true);
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return actors;
  }

  bool SubscribeToNodeChange(
      const gcs::SubscribeCallback<NodeID, rpc::GcsNodeInfo> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeToNodeChange(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterSelf(const rpc::GcsNodeInfo &local_node_info) {
    Status status = gcs_client_->Nodes().RegisterSelf(local_node_info, nullptr);
    return status.ok();
  }

  bool UnregisterSelf() {
    Status status = gcs_client_->Nodes().UnregisterSelf();
    return status.ok();
  }

  bool RegisterNode(const rpc::GcsNodeInfo &node_info) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncRegister(
        node_info, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::vector<rpc::GcsNodeInfo> GetNodeInfoList() {
    std::promise<bool> promise;
    std::vector<rpc::GcsNodeInfo> nodes;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(
        [&nodes, &promise](Status status, const std::vector<rpc::GcsNodeInfo> &result) {
          assert(!result.empty());
          nodes.assign(result.begin(), result.end());
          promise.set_value(status.ok());
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return nodes;
  }

  bool UnregisterNode(const NodeID &node_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncUnregister(
        node_id, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool SubscribeToResources(const gcs::ItemCallback<rpc::NodeResourceChange> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncSubscribeToResources(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  gcs::NodeResourceInfoAccessor::ResourceMap GetResources(const NodeID &node_id) {
    gcs::NodeResourceInfoAccessor::ResourceMap resource_map;
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetResources(
        node_id,
        [&resource_map, &promise](
            Status status,
            const boost::optional<gcs::NodeResourceInfoAccessor::ResourceMap> &result) {
          if (result) {
            resource_map.insert(result->begin(), result->end());
          }
          promise.set_value(true);
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return resource_map;
  }

  bool UpdateResources(const NodeID &node_id, const std::string &key) {
    std::promise<bool> promise;
    gcs::NodeResourceInfoAccessor::ResourceMap resource_map;
    auto resource = std::make_shared<rpc::ResourceTableData>();
    resource->set_resource_capacity(1.0);
    resource_map[key] = resource;
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncUpdateResources(
        node_id, resource_map,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool DeleteResources(const NodeID &node_id,
                       const std::vector<std::string> &resource_names) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncDeleteResources(
        node_id, resource_names,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool SubscribeBatchResourceUsage(
      const gcs::ItemCallback<rpc::ResourceUsageBatchData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncSubscribeBatchedResourceUsage(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool ReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> heartbeat) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncReportHeartbeat(
        heartbeat, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool ReportResourceUsage(const std::shared_ptr<rpc::ResourcesData> resources) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncReportResourceUsage(
        resources, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::vector<rpc::AvailableResources> GetAllAvailableResources() {
    std::promise<bool> promise;
    std::vector<rpc::AvailableResources> resources;
    RAY_CHECK_OK(gcs_client_->NodeResources().AsyncGetAllAvailableResources(
        [&resources, &promise](Status status,
                               const std::vector<rpc::AvailableResources> &result) {
          EXPECT_TRUE(!result.empty());
          resources.assign(result.begin(), result.end());
          promise.set_value(status.ok());
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return resources;
  }

  void WaitForTaskLeaseUnsubscribed(const TaskID &task_id) {
    auto condition = [this, task_id]() {
      return gcs_client_->Tasks().IsTaskLeaseUnsubscribed(task_id);
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  bool AddTask(const std::shared_ptr<rpc::TaskTableData> task) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncAdd(
        task, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  rpc::TaskTableData GetTask(const TaskID &task_id) {
    std::promise<bool> promise;
    rpc::TaskTableData task_table_data;
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncGet(
        task_id, [&task_table_data, &promise](
                     Status status, const boost::optional<rpc::TaskTableData> &result) {
          if (result) {
            task_table_data.CopyFrom(*result);
          }
          promise.set_value(status.ok());
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return task_table_data;
  }

  bool SubscribeTaskLease(
      const TaskID &task_id,
      const gcs::SubscribeCallback<TaskID, boost::optional<rpc::TaskLeaseData>>
          &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncSubscribeTaskLease(
        task_id, subscribe,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  void UnsubscribeTaskLease(const TaskID &task_id) {
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncUnsubscribeTaskLease(task_id));
  }

  bool AddTaskLease(const std::shared_ptr<rpc::TaskLeaseData> task_lease) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncAddTaskLease(
        task_lease, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool AttemptTaskReconstruction(
      const std::shared_ptr<rpc::TaskReconstructionData> task_reconstruction_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Tasks().AttemptTaskReconstruction(
        task_reconstruction_data,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool SubscribeToLocations(
      const ObjectID &object_id,
      const gcs::SubscribeCallback<ObjectID, std::vector<rpc::ObjectLocationChange>>
          &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Objects().AsyncSubscribeToLocations(
        object_id, subscribe,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  void UnsubscribeToLocations(const ObjectID &object_id) {
    RAY_CHECK_OK(gcs_client_->Objects().AsyncUnsubscribeToLocations(object_id));
  }

  void WaitForObjectUnsubscribed(const ObjectID &object_id) {
    auto condition = [this, object_id]() {
      return gcs_client_->Objects().IsObjectUnsubscribed(object_id);
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  bool AddLocation(const ObjectID &object_id, const NodeID &node_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Objects().AsyncAddLocation(
        object_id, node_id, 0,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RemoveLocation(const ObjectID &object_id, const NodeID &node_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Objects().AsyncRemoveLocation(
        object_id, node_id,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::vector<rpc::ObjectTableData> GetLocations(const ObjectID &object_id) {
    std::promise<bool> promise;
    std::vector<rpc::ObjectTableData> locations;
    RAY_CHECK_OK(gcs_client_->Objects().AsyncGetLocations(
        object_id,
        [&locations, &promise](Status status,
                               const boost::optional<rpc::ObjectLocationInfo> &result) {
          for (const auto &loc : result->locations()) {
            locations.push_back(loc);
          }
          promise.set_value(status.ok());
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return locations;
  }

  bool AddProfileData(const std::shared_ptr<rpc::ProfileTableData> &profile_table_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Stats().AsyncAddProfileData(
        profile_table_data,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool ReportJobError(const std::shared_ptr<rpc::ErrorTableData> &error_table_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(
        error_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool SubscribeToWorkerFailures(
      const gcs::ItemCallback<rpc::WorkerDeltaData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Workers().AsyncSubscribeToWorkerFailures(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool ReportWorkerFailure(
      const std::shared_ptr<rpc::WorkerTableData> &worker_failure_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Workers().AsyncReportWorkerFailure(
        worker_failure_data,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool AddWorker(const std::shared_ptr<rpc::WorkerTableData> &worker_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Workers().AsyncAdd(
        worker_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  void CheckActorData(const gcs::ActorTableData &actor,
                      rpc::ActorTableData_ActorState expected_state) {
    ASSERT_TRUE(actor.state() == expected_state);
  }

  absl::flat_hash_set<NodeID> RegisterNodeAndMarkDead(int node_count) {
    absl::flat_hash_set<NodeID> node_ids;
    for (int index = 0; index < node_count; ++index) {
      auto node_info = Mocker::GenNodeInfo();
      auto node_id = NodeID::FromBinary(node_info->node_id());
      EXPECT_TRUE(RegisterNode(*node_info));
      EXPECT_TRUE(UnregisterNode(node_id));
      node_ids.insert(node_id);
    }
    return node_ids;
  }

  // GCS server.
  gcs::GcsServerConfig config_;
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> server_io_service_thread_;
  std::unique_ptr<boost::asio::io_service> server_io_service_;

  // GCS client.
  std::unique_ptr<gcs::GcsClient> gcs_client_;
  std::unique_ptr<std::thread> client_io_service_thread_;
  std::unique_ptr<boost::asio::io_service> client_io_service_;

  // Timeout waiting for GCS server reply, default is 2s.
  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(ServiceBasedGcsClientTest, TestJobInfo) {
  // Create job table data.
  JobID add_job_id = JobID::FromInt(1);
  auto job_table_data = Mocker::GenJobTableData(add_job_id);

  // Subscribe to all jobs.
  std::atomic<int> job_updates(0);
  auto on_subscribe = [&job_updates](const JobID &job_id, const gcs::JobTableData &data) {
    job_updates++;
  };
  ASSERT_TRUE(SubscribeToAllJobs(on_subscribe));

  ASSERT_TRUE(AddJob(job_table_data));
  ASSERT_TRUE(MarkJobFinished(add_job_id));
  WaitForExpectedCount(job_updates, 2);
}

TEST_F(ServiceBasedGcsClientTest, TestActorSubscribeAll) {
  // NOTE: `TestActorSubscribeAll` will subscribe to all actor messages, so we need to
  // execute it before `TestActorInfo`, otherwise `TestActorSubscribeAll` will receive
  // messages from `TestActorInfo`.
  // Create actor table data.
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data1 = Mocker::GenActorTableData(job_id);
  auto actor_table_data2 = Mocker::GenActorTableData(job_id);

  // Subscribe to any register or update operations of actors.
  std::atomic<int> actor_update_count(0);
  auto on_subscribe = [&actor_update_count](const ActorID &actor_id,
                                            const gcs::ActorTableData &data) {
    ++actor_update_count;
  };
  ASSERT_TRUE(SubscribeAllActors(on_subscribe));

  // Register an actor to GCS.
  RegisterActor(actor_table_data1, false);
  RegisterActor(actor_table_data2, false);
  WaitForExpectedCount(actor_update_count, 2);
}

TEST_F(ServiceBasedGcsClientTest, TestActorInfo) {
  // Create actor table data.
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data = Mocker::GenActorTableData(job_id);
  ActorID actor_id = ActorID::FromBinary(actor_table_data->actor_id());

  // Subscribe to any update operations of an actor.
  std::atomic<int> actor_update_count(0);
  auto on_subscribe = [&actor_update_count](const ActorID &actor_id,
                                            const gcs::ActorTableData &data) {
    ++actor_update_count;
  };
  ASSERT_TRUE(SubscribeActor(actor_id, on_subscribe));

  // Register an actor to GCS.
  ASSERT_TRUE(RegisterActor(actor_table_data));
  ASSERT_TRUE(GetActor(actor_id).state() == rpc::ActorTableData::DEPENDENCIES_UNREADY);

  // Cancel subscription to an actor.
  UnsubscribeActor(actor_id);
  WaitForActorUnsubscribed(actor_id);
}

TEST_F(ServiceBasedGcsClientTest, TestNodeInfo) {
  // Create gcs node info.
  auto gcs_node1_info = Mocker::GenNodeInfo();
  NodeID node1_id = NodeID::FromBinary(gcs_node1_info->node_id());

  // Subscribe to node addition and removal events from GCS.
  std::atomic<int> register_count(0);
  std::atomic<int> unregister_count(0);
  auto on_subscribe = [&register_count, &unregister_count](const NodeID &node_id,
                                                           const rpc::GcsNodeInfo &data) {
    if (data.state() == rpc::GcsNodeInfo::ALIVE) {
      ++register_count;
    } else if (data.state() == rpc::GcsNodeInfo::DEAD) {
      ++unregister_count;
    }
  };
  ASSERT_TRUE(SubscribeToNodeChange(on_subscribe));

  // Register local node to GCS.
  ASSERT_TRUE(RegisterSelf(*gcs_node1_info));
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  EXPECT_EQ(gcs_client_->Nodes().GetSelfId(), node1_id);
  EXPECT_EQ(gcs_client_->Nodes().GetSelfInfo().node_id(), gcs_node1_info->node_id());
  EXPECT_EQ(gcs_client_->Nodes().GetSelfInfo().state(), gcs_node1_info->state());

  // Register a node to GCS.
  auto gcs_node2_info = Mocker::GenNodeInfo();
  NodeID node2_id = NodeID::FromBinary(gcs_node2_info->node_id());
  ASSERT_TRUE(RegisterNode(*gcs_node2_info));
  WaitForExpectedCount(register_count, 2);

  // Get information of all nodes from GCS.
  std::vector<rpc::GcsNodeInfo> node_list = GetNodeInfoList();
  EXPECT_EQ(node_list.size(), 2);
  ASSERT_TRUE(gcs_client_->Nodes().Get(node1_id));
  EXPECT_EQ(gcs_client_->Nodes().GetAll().size(), 2);

  // Cancel registration of local node to GCS.
  ASSERT_TRUE(UnregisterSelf());

  // Cancel registration of a node to GCS.
  ASSERT_TRUE(UnregisterNode(node2_id));
  WaitForExpectedCount(unregister_count, 2);

  // Get information of all nodes from GCS.
  node_list = GetNodeInfoList();
  EXPECT_EQ(node_list.size(), 2);
  EXPECT_EQ(node_list[0].state(),
            rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  EXPECT_EQ(node_list[1].state(),
            rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  ASSERT_TRUE(gcs_client_->Nodes().IsRemoved(node2_id));
}

TEST_F(ServiceBasedGcsClientTest, TestNodeResources) {
  // Subscribe to node resource changes.
  std::atomic<int> add_count(0);
  std::atomic<int> remove_count(0);
  auto on_subscribe = [&add_count,
                       &remove_count](const rpc::NodeResourceChange &notification) {
    if (0 == notification.deleted_resources_size()) {
      ++add_count;
    } else {
      ++remove_count;
    }
  };
  ASSERT_TRUE(SubscribeToResources(on_subscribe));

  // Register node.
  auto node_info = Mocker::GenNodeInfo();
  RAY_CHECK(RegisterNode(*node_info));

  // Update resources of node in GCS.
  NodeID node_id = NodeID::FromBinary(node_info->node_id());
  std::string key = "CPU";
  ASSERT_TRUE(UpdateResources(node_id, key));
  WaitForExpectedCount(add_count, 1);
  ASSERT_TRUE(GetResources(node_id).count(key));

  // Delete resources of a node from GCS.
  ASSERT_TRUE(DeleteResources(node_id, {key}));
  WaitForExpectedCount(remove_count, 1);
  ASSERT_TRUE(GetResources(node_id).empty());
}

TEST_F(ServiceBasedGcsClientTest, TestNodeResourceUsage) {
  // Subscribe batched state of all nodes from GCS.
  std::atomic<int> resource_batch_count(0);
  auto on_subscribe = [&resource_batch_count](const gcs::ResourceUsageBatchData &result) {
    ++resource_batch_count;
  };
  ASSERT_TRUE(SubscribeBatchResourceUsage(on_subscribe));

  // Register node.
  auto node_info = Mocker::GenNodeInfo();
  RAY_CHECK(RegisterNode(*node_info));

  // Report resource usage of a node to GCS.
  NodeID node_id = NodeID::FromBinary(node_info->node_id());
  auto resource = std::make_shared<rpc::ResourcesData>();
  resource->set_node_id(node_id.Binary());
  resource->set_should_global_gc(true);
  ASSERT_TRUE(ReportResourceUsage(resource));
  WaitForExpectedCount(resource_batch_count, 1);
}

TEST_F(ServiceBasedGcsClientTest, TestNodeResourceUsageWithLightResourceUsageReport) {
  // Subscribe batched state of all nodes from GCS.
  std::atomic<int> resource_batch_count(0);
  auto on_subscribe = [&resource_batch_count](const gcs::ResourceUsageBatchData &result) {
    ++resource_batch_count;
  };
  ASSERT_TRUE(SubscribeBatchResourceUsage(on_subscribe));

  // Register node.
  auto node_info = Mocker::GenNodeInfo();
  RAY_CHECK(RegisterNode(*node_info));

  // Report unchanged resource usage of a node to GCS.
  NodeID node_id = NodeID::FromBinary(node_info->node_id());
  auto resource = std::make_shared<rpc::ResourcesData>();
  resource->set_node_id(node_id.Binary());
  ASSERT_TRUE(ReportResourceUsage(resource));
  WaitForExpectedCount(resource_batch_count, 0);

  // Report changed resource usage of a node to GCS.
  auto resource1 = std::make_shared<rpc::ResourcesData>();
  resource1->set_node_id(node_id.Binary());
  resource1->set_resources_available_changed(true);
  ASSERT_TRUE(ReportResourceUsage(resource1));
  WaitForExpectedCount(resource_batch_count, 1);
}

TEST_F(ServiceBasedGcsClientTest, TestGetAllAvailableResources) {
  // Subscribe batched state of all nodes from GCS.
  std::atomic<int> resource_batch_count(0);
  auto on_subscribe = [&resource_batch_count](const gcs::ResourceUsageBatchData &result) {
    ++resource_batch_count;
  };
  ASSERT_TRUE(SubscribeBatchResourceUsage(on_subscribe));

  // Register node.
  auto node_info = Mocker::GenNodeInfo();
  RAY_CHECK(RegisterNode(*node_info));

  // Report resource usage of a node to GCS.
  NodeID node_id = NodeID::FromBinary(node_info->node_id());
  auto resource = std::make_shared<rpc::ResourcesData>();
  resource->set_node_id(node_id.Binary());
  // Set this flag to indicate resources has changed.
  resource->set_resources_available_changed(true);
  (*resource->mutable_resources_available())["CPU"] = 1.0;
  (*resource->mutable_resources_available())["GPU"] = 10.0;
  ASSERT_TRUE(ReportResourceUsage(resource));
  WaitForExpectedCount(resource_batch_count, 1);

  // Assert get all available resources right.
  std::vector<rpc::AvailableResources> resources = GetAllAvailableResources();
  EXPECT_EQ(resources.size(), 1);
  EXPECT_EQ(resources[0].resources_available_size(), 2);
  EXPECT_EQ((*resources[0].mutable_resources_available())["CPU"], 1.0);
  EXPECT_EQ((*resources[0].mutable_resources_available())["GPU"], 10.0);
}

TEST_F(ServiceBasedGcsClientTest,
       TestGetAllAvailableResourcesWithLightResourceUsageReport) {
  // Subscribe batched state of all nodes from GCS.
  std::atomic<int> resource_batch_count(0);
  auto on_subscribe = [&resource_batch_count](const gcs::ResourceUsageBatchData &result) {
    ++resource_batch_count;
  };
  ASSERT_TRUE(SubscribeBatchResourceUsage(on_subscribe));

  // Register node.
  auto node_info = Mocker::GenNodeInfo();
  RAY_CHECK(RegisterNode(*node_info));

  // Report resource usage of a node to GCS.
  NodeID node_id = NodeID::FromBinary(node_info->node_id());
  auto resource = std::make_shared<rpc::ResourcesData>();
  resource->set_node_id(node_id.Binary());
  resource->set_resources_available_changed(true);
  (*resource->mutable_resources_available())["CPU"] = 1.0;
  (*resource->mutable_resources_available())["GPU"] = 10.0;
  ASSERT_TRUE(ReportResourceUsage(resource));
  WaitForExpectedCount(resource_batch_count, 1);

  // Assert get all available resources right.
  std::vector<rpc::AvailableResources> resources = GetAllAvailableResources();
  EXPECT_EQ(resources.size(), 1);
  EXPECT_EQ(resources[0].resources_available_size(), 2);
  EXPECT_EQ((*resources[0].mutable_resources_available())["CPU"], 1.0);
  EXPECT_EQ((*resources[0].mutable_resources_available())["GPU"], 10.0);

  // Report unchanged resource usage of a node to GCS.
  auto resource1 = std::make_shared<rpc::ResourcesData>();
  resource1->set_node_id(node_id.Binary());
  (*resource1->mutable_resources_available())["GPU"] = 8.0;
  ASSERT_TRUE(ReportResourceUsage(resource1));
  WaitForExpectedCount(resource_batch_count, 1);

  // The value would remain unchanged.
  std::vector<rpc::AvailableResources> resources1 = GetAllAvailableResources();
  EXPECT_EQ(resources1.size(), 1);
  EXPECT_EQ(resources1[0].resources_available_size(), 2);
  EXPECT_EQ((*resources1[0].mutable_resources_available())["CPU"], 1.0);
  EXPECT_EQ((*resources1[0].mutable_resources_available())["GPU"], 10.0);
}

TEST_F(ServiceBasedGcsClientTest, TestTaskInfo) {
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  auto task_table_data = Mocker::GenTaskTableData(job_id.Binary(), task_id.Binary());

  // Add a task to GCS.
  ASSERT_TRUE(AddTask(task_table_data));
  auto get_task_result = GetTask(task_id);
  ASSERT_TRUE(get_task_result.task().task_spec().task_id() == task_id.Binary());
  ASSERT_TRUE(get_task_result.task().task_spec().job_id() == job_id.Binary());

  // Subscribe to the event that the given task lease is added in GCS.
  std::atomic<int> task_lease_count(0);
  auto task_lease_subscribe = [&task_lease_count](
                                  const TaskID &id,
                                  const boost::optional<rpc::TaskLeaseData> &result) {
    ++task_lease_count;
  };
  ASSERT_TRUE(SubscribeTaskLease(task_id, task_lease_subscribe));

  // Add a task lease to GCS.
  NodeID node_id = NodeID::FromRandom();
  auto task_lease = Mocker::GenTaskLeaseData(task_id.Binary(), node_id.Binary());
  ASSERT_TRUE(AddTaskLease(task_lease));
  WaitForExpectedCount(task_lease_count, 2);

  // Cancel subscription to a task lease.
  UnsubscribeTaskLease(task_id);
  WaitForTaskLeaseUnsubscribed(task_id);

  // Add a task lease to GCS again.
  ASSERT_TRUE(AddTaskLease(task_lease));

  // Assert unsubscribe succeeded.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(task_lease_count, 2);

  // Attempt task reconstruction to GCS.
  auto task_reconstruction_data = std::make_shared<rpc::TaskReconstructionData>();
  task_reconstruction_data->set_task_id(task_id.Binary());
  task_reconstruction_data->set_num_reconstructions(0);
  ASSERT_TRUE(AttemptTaskReconstruction(task_reconstruction_data));
}

TEST_F(ServiceBasedGcsClientTest, TestObjectInfo) {
  ObjectID object_id = ObjectID::FromRandom();
  NodeID node_id = NodeID::FromRandom();

  // Subscribe to any update of an object's location.
  std::atomic<int> object_add_count(0);
  std::atomic<int> object_remove_count(0);
  auto on_subscribe = [&object_add_count, &object_remove_count](
                          const ObjectID &object_id,
                          const std::vector<rpc::ObjectLocationChange> &result) {
    for (const auto &res : result) {
      if (res.is_add()) {
        ++object_add_count;
      } else {
        ++object_remove_count;
      }
    }
  };
  ASSERT_TRUE(SubscribeToLocations(object_id, on_subscribe));

  // Add location of object to GCS.
  ASSERT_TRUE(AddLocation(object_id, node_id));
  WaitForExpectedCount(object_add_count, 1);

  // Get object's locations from GCS.
  auto locations = GetLocations(object_id);
  ASSERT_EQ(locations.size(), 1);
  ASSERT_EQ(locations.back().manager(), node_id.Binary());

  // Remove location of object from GCS.
  ASSERT_TRUE(RemoveLocation(object_id, node_id));
  WaitForExpectedCount(object_remove_count, 1);
  ASSERT_TRUE(GetLocations(object_id).empty());

  // Cancel subscription to any update of an object's location.
  UnsubscribeToLocations(object_id);
  WaitForObjectUnsubscribed(object_id);

  // Add location of object to GCS again.
  ASSERT_TRUE(AddLocation(object_id, node_id));

  // Assert unsubscribe succeeded.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_EQ(object_add_count, 1);
}

TEST_F(ServiceBasedGcsClientTest, TestStats) {
  // Add profile data to GCS.
  NodeID node_id = NodeID::FromRandom();
  auto profile_table_data = Mocker::GenProfileTableData(node_id);
  ASSERT_TRUE(AddProfileData(profile_table_data));
}

TEST_F(ServiceBasedGcsClientTest, TestWorkerInfo) {
  // Subscribe to all unexpected failure of workers from GCS.
  std::atomic<int> worker_failure_count(0);
  auto on_subscribe = [&worker_failure_count](const rpc::WorkerDeltaData &result) {
    ++worker_failure_count;
  };
  ASSERT_TRUE(SubscribeToWorkerFailures(on_subscribe));

  // Report a worker failure to GCS when this worker doesn't exist.
  auto worker_data = Mocker::GenWorkerTableData();
  worker_data->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
  ASSERT_TRUE(ReportWorkerFailure(worker_data));
  WaitForExpectedCount(worker_failure_count, 1);

  // Add a worker to GCS.
  ASSERT_TRUE(AddWorker(worker_data));

  // Report a worker failure to GCS when this worker is actually exist.
  ASSERT_TRUE(ReportWorkerFailure(worker_data));
  WaitForExpectedCount(worker_failure_count, 2);
}

TEST_F(ServiceBasedGcsClientTest, TestErrorInfo) {
  // Report a job error to GCS.
  JobID job_id = JobID::FromInt(1);
  auto error_table_data = Mocker::GenErrorTableData(job_id);
  ASSERT_TRUE(ReportJobError(error_table_data));
}

TEST_F(ServiceBasedGcsClientTest, TestJobTableResubscribe) {
  // Test that subscription of the job table can still work when GCS server restarts.
  JobID job_id = JobID::FromInt(1);
  auto job_table_data = Mocker::GenJobTableData(job_id);

  // Subscribe to all jobs.
  std::atomic<int> job_update_count(0);
  auto subscribe = [&job_update_count](const JobID &id, const rpc::JobTableData &result) {
    ++job_update_count;
  };
  ASSERT_TRUE(SubscribeToAllJobs(subscribe));

  ASSERT_TRUE(AddJob(job_table_data));
  WaitForExpectedCount(job_update_count, 1);
  RestartGcsServer();

  // The GCS client will fetch data from the GCS server after the GCS server is restarted,
  // and the GCS server keeps a job record, so `job_update_count` plus one.
  WaitForExpectedCount(job_update_count, 2);

  ASSERT_TRUE(MarkJobFinished(job_id));
  WaitForExpectedCount(job_update_count, 3);
}

TEST_F(ServiceBasedGcsClientTest, TestActorTableResubscribe) {
  // Test that subscription of the actor table can still work when GCS server restarts.
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data = Mocker::GenActorTableData(job_id);
  auto actor_id = ActorID::FromBinary(actor_table_data->actor_id());

  // Number of notifications for the following `SubscribeAllActors` operation.
  std::atomic<int> num_subscribe_all_notifications(0);
  // All the notifications for the following `SubscribeAllActors` operation.
  std::vector<gcs::ActorTableData> subscribe_all_notifications;
  auto subscribe_all = [&num_subscribe_all_notifications, &subscribe_all_notifications](
                           const ActorID &id, const rpc::ActorTableData &data) {
    subscribe_all_notifications.emplace_back(data);
    ++num_subscribe_all_notifications;
  };
  // Subscribe to updates of all actors.
  ASSERT_TRUE(SubscribeAllActors(subscribe_all));

  // Number of notifications for the following `SubscribeActor` operation.
  std::atomic<int> num_subscribe_one_notifications(0);
  // All the notifications for the following `SubscribeActor` operation.
  std::vector<gcs::ActorTableData> subscribe_one_notifications;
  auto actor_subscribe = [&num_subscribe_one_notifications, &subscribe_one_notifications](
                             const ActorID &actor_id, const gcs::ActorTableData &data) {
    subscribe_one_notifications.emplace_back(data);
    ++num_subscribe_one_notifications;
  };
  // Subscribe to updates for this actor.
  ASSERT_TRUE(SubscribeActor(actor_id, actor_subscribe));

  ASSERT_FALSE(RegisterActor(actor_table_data, false));

  // We should receive a new DEAD notification from the subscribe channel.
  WaitForExpectedCount(num_subscribe_all_notifications, 1);
  WaitForExpectedCount(num_subscribe_one_notifications, 1);
  CheckActorData(subscribe_all_notifications[0], rpc::ActorTableData::DEAD);
  CheckActorData(subscribe_one_notifications[0], rpc::ActorTableData::DEAD);

  // Restart GCS server.
  RestartGcsServer();

  // When GCS client detects that GCS server has restarted, but the pub-sub server
  // didn't restart, it will fetch data again from the GCS server. The GCS will destroy
  // the actor because it finds that the actor is out of scope, so we'll receive another
  // notification of DEAD state.
  WaitForExpectedCount(num_subscribe_one_notifications, 3);

  // NOTE: GCS will not reply when actor registration fails, so when GCS restarts, gcs
  // client will register the actor again. When an actor is registered, the status in GCS
  // is `DEPENDENCIES_UNREADY`. When GCS finds that the owner of an actor is nil, it will
  // destroy the actor and the status of the actor will change to `DEAD`. The GCS client
  // fetch actor info from the GCS server, and the status of the actor may be
  // `DEPENDENCIES_UNREADY` or `DEAD`, so we do not assert the actor status here any
  // more.
  // If the status of the actor is `DEPENDENCIES_UNREADY`, we will fetch two records, so
  // `num_subscribe_all_notifications` will be 4. If the status of the actor is `DEAD`, we
  // will fetch one record, so `num_subscribe_all_notifications` will be 3.
  auto condition = [&num_subscribe_all_notifications]() {
    return num_subscribe_all_notifications == 3 || num_subscribe_all_notifications == 4;
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
}

TEST_F(ServiceBasedGcsClientTest, TestObjectTableResubscribe) {
  ObjectID object1_id = ObjectID::FromRandom();
  ObjectID object2_id = ObjectID::FromRandom();
  NodeID node_id = NodeID::FromRandom();

  // Subscribe to any update of an object's location.
  std::atomic<int> object1_change_count(0);
  std::atomic<int> object2_change_count(0);
  ASSERT_TRUE(SubscribeToLocations(
      object1_id,
      [&object1_change_count](const ObjectID &object_id,
                              const std::vector<rpc::ObjectLocationChange> &result) {
        if (!result.empty()) {
          ++object1_change_count;
        }
      }));
  ASSERT_TRUE(SubscribeToLocations(
      object2_id,
      [&object2_change_count](const ObjectID &object_id,
                              const std::vector<rpc::ObjectLocationChange> &result) {
        if (!result.empty()) {
          ++object2_change_count;
        }
      }));

  ASSERT_TRUE(AddLocation(object1_id, node_id));
  WaitForExpectedCount(object1_change_count, 1);
  ASSERT_TRUE(AddLocation(object2_id, node_id));
  WaitForExpectedCount(object2_change_count, 1);

  // Cancel subscription to any update of an object's location.
  UnsubscribeToLocations(object1_id);
  WaitForObjectUnsubscribed(object1_id);

  // Restart GCS.
  RestartGcsServer();
  // When GCS client detects that GCS server has restarted, but the pub-sub server
  // didn't restart, it will fetch the subscription data again from the GCS server, so
  // `object2_change_count` plus 1.
  WaitForExpectedCount(object2_change_count, 2);

  // Add location of object to GCS again and check if resubscribe works.
  ASSERT_TRUE(AddLocation(object1_id, node_id));
  WaitForExpectedCount(object1_change_count, 1);
  ASSERT_TRUE(AddLocation(object2_id, node_id));
  WaitForExpectedCount(object2_change_count, 3);
}

TEST_F(ServiceBasedGcsClientTest, TestNodeTableResubscribe) {
  // Test that subscription of the node table can still work when GCS server restarts.
  // Subscribe to node addition and removal events from GCS and cache those information.
  std::atomic<int> node_change_count(0);
  auto node_subscribe = [&node_change_count](const NodeID &id,
                                             const rpc::GcsNodeInfo &result) {
    ++node_change_count;
  };
  ASSERT_TRUE(SubscribeToNodeChange(node_subscribe));

  // Subscribe to node resource changes.
  std::atomic<int> resource_change_count(0);
  auto resource_subscribe =
      [&resource_change_count](const rpc::NodeResourceChange &result) {
        ++resource_change_count;
      };
  ASSERT_TRUE(SubscribeToResources(resource_subscribe));

  // Subscribe batched state of all nodes from GCS.
  std::atomic<int> batch_resource_usage_count(0);
  auto batch_resource_usage_subscribe =
      [&batch_resource_usage_count](const rpc::ResourceUsageBatchData &result) {
        ++batch_resource_usage_count;
      };
  ASSERT_TRUE(SubscribeBatchResourceUsage(batch_resource_usage_subscribe));

  auto node_info = Mocker::GenNodeInfo(1);
  ASSERT_TRUE(RegisterNode(*node_info));
  NodeID node_id = NodeID::FromBinary(node_info->node_id());
  std::string key = "CPU";
  ASSERT_TRUE(UpdateResources(node_id, key));
  auto resources = std::make_shared<rpc::ResourcesData>();
  resources->set_node_id(node_info->node_id());
  // Set this flag because GCS won't publish unchanged resources.
  resources->set_should_global_gc(true);
  ASSERT_TRUE(ReportResourceUsage(resources));
  WaitForExpectedCount(batch_resource_usage_count, 1);

  RestartGcsServer();

  node_info = Mocker::GenNodeInfo(1);
  ASSERT_TRUE(RegisterNode(*node_info));
  node_id = NodeID::FromBinary(node_info->node_id());
  ASSERT_TRUE(UpdateResources(node_id, key));
  resources->set_node_id(node_info->node_id());
  ASSERT_TRUE(ReportResourceUsage(resources));

  WaitForExpectedCount(node_change_count, 2);
  WaitForExpectedCount(resource_change_count, 2);
  WaitForExpectedCount(batch_resource_usage_count, 2);
}

TEST_F(ServiceBasedGcsClientTest, TestTaskTableResubscribe) {
  JobID job_id = JobID::FromInt(6);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  auto task_table_data = Mocker::GenTaskTableData(job_id.Binary(), task_id.Binary());

  // Subscribe to the event that the given task lease is added in GCS.
  std::atomic<int> task_lease_count(0);
  auto task_lease_subscribe = [&task_lease_count](
                                  const TaskID &task_id,
                                  const boost::optional<rpc::TaskLeaseData> &data) {
    if (data) {
      ++task_lease_count;
    }
  };
  ASSERT_TRUE(SubscribeTaskLease(task_id, task_lease_subscribe));

  ASSERT_TRUE(AddTask(task_table_data));
  NodeID node_id = NodeID::FromRandom();
  auto task_lease = Mocker::GenTaskLeaseData(task_id.Binary(), node_id.Binary());
  ASSERT_TRUE(AddTaskLease(task_lease));
  WaitForExpectedCount(task_lease_count, 1);

  RestartGcsServer();

  node_id = NodeID::FromRandom();
  task_lease = Mocker::GenTaskLeaseData(task_id.Binary(), node_id.Binary());
  ASSERT_TRUE(AddTaskLease(task_lease));
  WaitForExpectedCount(task_lease_count, 3);
}

TEST_F(ServiceBasedGcsClientTest, TestWorkerTableResubscribe) {
  // Subscribe to all unexpected failure of workers from GCS.
  std::atomic<int> worker_failure_count(0);
  auto on_subscribe = [&worker_failure_count](const rpc::WorkerDeltaData &result) {
    ++worker_failure_count;
  };
  ASSERT_TRUE(SubscribeToWorkerFailures(on_subscribe));

  // Restart GCS
  RestartGcsServer();

  // Add a worker before report worker failure to GCS.
  auto worker_data = Mocker::GenWorkerTableData();
  worker_data->mutable_worker_address()->set_worker_id(WorkerID::FromRandom().Binary());
  ASSERT_TRUE(AddWorker(worker_data));

  // Report a worker failure to GCS and check if resubscribe works.
  ASSERT_TRUE(ReportWorkerFailure(worker_data));
  WaitForExpectedCount(worker_failure_count, 1);
}

TEST_F(ServiceBasedGcsClientTest, TestGcsTableReload) {
  ObjectID object_id = ObjectID::FromRandom();
  NodeID node_id = NodeID::FromRandom();

  // Register node to GCS.
  auto node_info = Mocker::GenNodeInfo();
  ASSERT_TRUE(RegisterNode(*node_info));

  // Add location of object to GCS.
  ASSERT_TRUE(AddLocation(object_id, node_id));

  // Restart GCS.
  RestartGcsServer();

  // Get information of nodes from GCS.
  std::vector<rpc::GcsNodeInfo> node_list = GetNodeInfoList();
  EXPECT_EQ(node_list.size(), 1);

  // Get object's locations from GCS.
  auto locations = GetLocations(object_id);
  ASSERT_EQ(locations.size(), 1);
  ASSERT_EQ(locations.back().manager(), node_id.Binary());
}

TEST_F(ServiceBasedGcsClientTest, TestGcsRedisFailureDetector) {
  // Stop redis.
  TestSetupUtil::ShutDownRedisServers();

  // Sleep 3 times of gcs_redis_heartbeat_interval_milliseconds to make sure gcs_server
  // detects that the redis is failure and then stop itself.
  auto interval_ms = RayConfig::instance().gcs_redis_heartbeat_interval_milliseconds();
  std::this_thread::sleep_for(std::chrono::milliseconds(3 * interval_ms));

  // Check if GCS server has exited.
  RAY_CHECK(gcs_server_->IsStopped());
}

TEST_F(ServiceBasedGcsClientTest, TestMultiThreadSubAndUnsub) {
  auto sub_finished_count = std::make_shared<std::atomic<int>>(0);
  int size = 5;
  std::vector<std::unique_ptr<std::thread>> threads;
  threads.resize(size);

  // The number of times each thread executes subscribe & resubscribe & unsubscribe.
  int sub_and_unsub_loop_count = 20;

  // Multithreading subscribe/resubscribe/unsubscribe actors.
  auto job_id = JobID::FromInt(1);
  for (int index = 0; index < size; ++index) {
    threads[index].reset(new std::thread([this, sub_and_unsub_loop_count, job_id] {
      for (int index = 0; index < sub_and_unsub_loop_count; ++index) {
        auto actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
        ASSERT_TRUE(SubscribeActor(
            actor_id, [](const ActorID &id, const rpc::ActorTableData &result) {}));
        gcs_client_->Actors().AsyncResubscribe(false);
        UnsubscribeActor(actor_id);
      }
    }));
  }
  for (auto &thread : threads) {
    thread->join();
    thread.reset();
  }

  // Multithreading subscribe/resubscribe/unsubscribe objects.
  for (int index = 0; index < size; ++index) {
    threads[index].reset(new std::thread([this, sub_and_unsub_loop_count] {
      for (int index = 0; index < sub_and_unsub_loop_count; ++index) {
        auto object_id = ObjectID::FromRandom();
        ASSERT_TRUE(SubscribeToLocations(
            object_id, [](const ObjectID &id,
                          const std::vector<rpc::ObjectLocationChange> &result) {}));
        gcs_client_->Objects().AsyncResubscribe(false);
        UnsubscribeToLocations(object_id);
      }
    }));
  }
  for (auto &thread : threads) {
    thread->join();
    thread.reset();
  }
}

// This UT is only used to test the query actor info performance.
// We disable it by default.
TEST_F(ServiceBasedGcsClientTest, DISABLED_TestGetActorPerf) {
  // Register actors.
  JobID job_id = JobID::FromInt(1);
  int actor_count = 5000;
  rpc::TaskSpec task_spec;
  rpc::TaskArg task_arg;
  task_arg.set_data("0123456789");
  for (int index = 0; index < 10000; ++index) {
    task_spec.add_args()->CopyFrom(task_arg);
  }
  for (int index = 0; index < actor_count; ++index) {
    auto actor_table_data = Mocker::GenActorTableData(job_id);
    actor_table_data->mutable_task_spec()->CopyFrom(task_spec);
    RegisterActor(actor_table_data, false, true);
  }

  // Get all actors.
  auto condition = [this, actor_count]() {
    return (int)GetAllActors().size() == actor_count;
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));

  int64_t start_time = current_time_ms();
  auto actors = GetAllActors();
  RAY_LOG(INFO) << "It takes " << current_time_ms() - start_time << "ms to query "
                << actor_count << " actors.";
}

TEST_F(ServiceBasedGcsClientTest, TestEvictExpiredDestroyedActors) {
  // Register actors and the actors will be destroyed.
  JobID job_id = JobID::FromInt(1);
  int actor_count = RayConfig::instance().maximum_gcs_destroyed_actor_cached_count();
  for (int index = 0; index < actor_count; ++index) {
    auto actor_table_data = Mocker::GenActorTableData(job_id);
    RegisterActor(actor_table_data, false);
  }

  // Restart GCS.
  RestartGcsServer();

  absl::flat_hash_set<ActorID> actor_ids;
  for (int index = 0; index < actor_count; ++index) {
    auto actor_table_data = Mocker::GenActorTableData(job_id);
    RegisterActor(actor_table_data, false);
    actor_ids.insert(ActorID::FromBinary(actor_table_data->actor_id()));
  }

  // NOTE: GCS will not reply when actor registration fails, so when GCS restarts, gcs
  // client will register the actor again and the status of the actor may be
  // `DEPENDENCIES_UNREADY` or `DEAD`. We should get all dead actors.
  auto condition = [this]() {
    return GetAllActors(true).size() ==
           RayConfig::instance().maximum_gcs_destroyed_actor_cached_count();
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));

  auto actors = GetAllActors(true);
  for (const auto &actor : actors) {
    EXPECT_TRUE(actor_ids.contains(ActorID::FromBinary(actor.actor_id())));
  }
}

TEST_F(ServiceBasedGcsClientTest, TestEvictExpiredDeadNodes) {
  // Simulate the scenario of node dead.
  int node_count = RayConfig::instance().maximum_gcs_dead_node_cached_count();
  RegisterNodeAndMarkDead(node_count);

  // Restart GCS.
  RestartGcsServer();

  const auto &node_ids = RegisterNodeAndMarkDead(node_count);

  // Get all nodes.
  auto condition = [this]() {
    return GetNodeInfoList().size() ==
           RayConfig::instance().maximum_gcs_dead_node_cached_count();
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));

  auto nodes = GetNodeInfoList();
  for (const auto &node : nodes) {
    EXPECT_TRUE(node_ids.contains(NodeID::FromBinary(node.node_id())));
  }
}

// TODO(sang): Add tests after adding asyncAdd

}  // namespace ray

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::TEST_REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
