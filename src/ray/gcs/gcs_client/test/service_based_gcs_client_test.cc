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

namespace ray {

class ServiceBasedGcsClientTest : public RedisServiceManagerForTest {
 protected:
  void SetUp() override {
    config.grpc_server_port = 0;
    config.grpc_server_name = "MockedGcsServer";
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.is_test = true;
    config.redis_port = REDIS_SERVER_PORT;
    gcs_server_.reset(new gcs::GcsServer(config));
    io_service_.reset(new boost::asio::io_service());

    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*io_service_));
      io_service_->run();
    }));

    thread_gcs_server_.reset(new std::thread([this] { gcs_server_->Start(); }));

    // Wait until server starts listening.
    while (!gcs_server_->IsStarted()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Create GCS client.
    gcs::GcsClientOptions options(config.redis_address, config.redis_port,
                                  config.redis_password, config.is_test);
    gcs_client_.reset(new gcs::ServiceBasedGcsClient(options));
    RAY_CHECK_OK(gcs_client_->Connect(*io_service_));
  }

  void TearDown() override {
    gcs_server_->Stop();
    io_service_->stop();
    thread_io_service_->join();
    thread_gcs_server_->join();
    gcs_client_->Disconnect();
    FlushAll();
  }

  bool SubscribeToFinishedJobs(
      const gcs::SubscribeCallback<JobID, rpc::JobTableData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncSubscribeToFinishedJobs(
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

  bool UnsubscribeActor(const ActorID &actor_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncUnsubscribe(
        actor_id, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool SubscribeAllActors(
      const gcs::SubscribeCallback<ActorID, rpc::ActorTableData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribeAll(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterActor(const std::shared_ptr<rpc::ActorTableData> &actor_table_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncRegister(
        actor_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UpdateActor(const ActorID &actor_id,
                   const std::shared_ptr<rpc::ActorTableData> &actor_table_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncUpdate(
        actor_id, actor_table_data,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
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

  bool AddCheckpoint(
      const std::shared_ptr<rpc::ActorCheckpointData> &actor_checkpoint_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncAddCheckpoint(
        actor_checkpoint_data,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  rpc::ActorCheckpointData GetCheckpoint(const ActorID &actor_id,
                                         const ActorCheckpointID &checkpoint_id) {
    std::promise<bool> promise;
    rpc::ActorCheckpointData actor_checkpoint_data;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncGetCheckpoint(
        checkpoint_id, actor_id,
        [&actor_checkpoint_data, &promise](
            Status status, const boost::optional<rpc::ActorCheckpointData> &result) {
          assert(result);
          actor_checkpoint_data.CopyFrom(*result);
          promise.set_value(true);
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return actor_checkpoint_data;
  }

  rpc::ActorCheckpointIdData GetCheckpointID(const ActorID &actor_id) {
    std::promise<bool> promise;
    rpc::ActorCheckpointIdData actor_checkpoint_id_data;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncGetCheckpointID(
        actor_id,
        [&actor_checkpoint_id_data, &promise](
            Status status, const boost::optional<rpc::ActorCheckpointIdData> &result) {
          assert(result);
          actor_checkpoint_id_data.CopyFrom(*result);
          promise.set_value(true);
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return actor_checkpoint_id_data;
  }

  bool SubscribeToNodeChange(
      const gcs::SubscribeCallback<ClientID, rpc::GcsNodeInfo> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeToNodeChange(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterSelf(const rpc::GcsNodeInfo &local_node_info) {
    Status status = gcs_client_->Nodes().RegisterSelf(local_node_info);
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

  bool UnregisterNode(const ClientID &node_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncUnregister(
        node_id, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool SubscribeToResources(
      const gcs::SubscribeCallback<ClientID, gcs::ResourceChangeNotification>
          &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeToResources(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  gcs::NodeInfoAccessor::ResourceMap GetResources(const ClientID &node_id) {
    gcs::NodeInfoAccessor::ResourceMap resource_map;
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetResources(
        node_id, [&resource_map, &promise](
                     Status status,
                     const boost::optional<gcs::NodeInfoAccessor::ResourceMap> &result) {
          if (result) {
            resource_map.insert(result->begin(), result->end());
          }
          promise.set_value(true);
        }));
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return resource_map;
  }

  bool UpdateResources(const ClientID &node_id,
                       const gcs::NodeInfoAccessor::ResourceMap &resource_map) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncUpdateResources(
        node_id, resource_map,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool DeleteResources(const ClientID &node_id,
                       const std::vector<std::string> &resource_names) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncDeleteResources(
        node_id, resource_names,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool SubscribeBatchHeartbeat(
      const gcs::ItemCallback<rpc::HeartbeatBatchTableData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeBatchHeartbeat(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool ReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> heartbeat) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncReportHeartbeat(
        heartbeat, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool SubscribeTask(
      const TaskID &task_id,
      const gcs::SubscribeCallback<TaskID, rpc::TaskTableData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncSubscribe(
        task_id, subscribe,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UnsubscribeTask(const TaskID &task_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncUnsubscribe(
        task_id, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
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

  bool DeleteTask(const std::vector<TaskID> &task_ids) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncDelete(
        task_ids, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
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

  bool UnsubscribeTaskLease(const TaskID &task_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Tasks().AsyncUnsubscribeTaskLease(
        task_id, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
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
      const gcs::SubscribeCallback<ObjectID, gcs::ObjectChangeNotification> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Objects().AsyncSubscribeToLocations(
        object_id, subscribe,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UnsubscribeToLocations(const ObjectID &object_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Objects().AsyncUnsubscribeToLocations(
        object_id, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool AddLocation(const ObjectID &object_id, const ClientID &node_id) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Objects().AsyncAddLocation(
        object_id, node_id,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RemoveLocation(const ObjectID &object_id, const ClientID &node_id) {
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
        object_id, [&locations, &promise](
                       Status status, const std::vector<rpc::ObjectTableData> &result) {
          locations = result;
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
      const gcs::SubscribeCallback<WorkerID, rpc::WorkerFailureData> &subscribe) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Workers().AsyncSubscribeToWorkerFailures(
        subscribe, [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool ReportWorkerFailure(
      const std::shared_ptr<rpc::WorkerFailureData> &worker_failure_data) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Workers().AsyncReportWorkerFailure(
        worker_failure_data,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms) {
    auto status = future.wait_for(timeout_ms);
    return status == std::future_status::ready && future.get();
  }

  void WaitPendingDone(std::atomic<int> &current_count, int expected_count) {
    auto condition = [&current_count, expected_count]() {
      return current_count == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  // GCS server.
  gcs::GcsServerConfig config;
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<std::thread> thread_gcs_server_;
  std::unique_ptr<boost::asio::io_service> io_service_;

  // GCS client.
  std::unique_ptr<gcs::GcsClient> gcs_client_;

  // Timeout waiting for GCS server reply, default is 2s.
  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(ServiceBasedGcsClientTest, TestJobInfo) {
  // Create job table data.
  JobID add_job_id = JobID::FromInt(1);
  auto job_table_data = Mocker::GenJobTableData(add_job_id);

  // Subscribe to finished jobs.
  std::atomic<int> finished_job_count(0);
  auto on_subscribe = [&finished_job_count](const JobID &job_id,
                                            const gcs::JobTableData &data) {
    finished_job_count++;
  };
  ASSERT_TRUE(SubscribeToFinishedJobs(on_subscribe));

  ASSERT_TRUE(AddJob(job_table_data));
  ASSERT_TRUE(MarkJobFinished(add_job_id));
  WaitPendingDone(finished_job_count, 1);
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
  ASSERT_TRUE(GetActor(actor_id).state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);

  // Cancel subscription to an actor.
  ASSERT_TRUE(UnsubscribeActor(actor_id));

  // Update dynamic states of actor in GCS.
  actor_table_data->set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  ASSERT_TRUE(UpdateActor(actor_id, actor_table_data));
  ASSERT_TRUE(GetActor(actor_id).state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  WaitPendingDone(actor_update_count, 1);
}

TEST_F(ServiceBasedGcsClientTest, TestActorCheckpoint) {
  // Create actor checkpoint data.
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data = Mocker::GenActorTableData(job_id);
  ActorID actor_id = ActorID::FromBinary(actor_table_data->actor_id());

  ActorCheckpointID checkpoint_id = ActorCheckpointID::FromRandom();
  auto checkpoint = std::make_shared<rpc::ActorCheckpointData>();
  checkpoint->set_actor_id(actor_table_data->actor_id());
  checkpoint->set_checkpoint_id(checkpoint_id.Binary());
  checkpoint->set_execution_dependency(checkpoint_id.Binary());

  // Add actor checkpoint data to GCS.
  ASSERT_TRUE(AddCheckpoint(checkpoint));

  // Get actor checkpoint data from GCS.
  auto get_checkpoint_result = GetCheckpoint(actor_id, checkpoint_id);
  ASSERT_TRUE(get_checkpoint_result.actor_id() == actor_id.Binary());

  // Get actor checkpoint id data from GCS.
  auto get_checkpoint_id_result = GetCheckpointID(actor_id);
  ASSERT_TRUE(get_checkpoint_id_result.checkpoint_ids_size() == 1);
  ASSERT_TRUE(get_checkpoint_id_result.checkpoint_ids(0) == checkpoint_id.Binary());
}

TEST_F(ServiceBasedGcsClientTest, TestActorSubscribeAll) {
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
  ASSERT_TRUE(RegisterActor(actor_table_data1));
  ASSERT_TRUE(RegisterActor(actor_table_data2));
  WaitPendingDone(actor_update_count, 2);
}

TEST_F(ServiceBasedGcsClientTest, TestNodeInfo) {
  // Create gcs node info.
  auto gcs_node1_info = Mocker::GenNodeInfo();
  ClientID node1_id = ClientID::FromBinary(gcs_node1_info->node_id());

  // Subscribe to node addition and removal events from GCS.
  std::atomic<int> register_count(0);
  std::atomic<int> unregister_count(0);
  auto on_subscribe = [&register_count, &unregister_count](const ClientID &node_id,
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
  sleep(1);
  EXPECT_EQ(gcs_client_->Nodes().GetSelfId(), node1_id);
  EXPECT_EQ(gcs_client_->Nodes().GetSelfInfo().node_id(), gcs_node1_info->node_id());
  EXPECT_EQ(gcs_client_->Nodes().GetSelfInfo().state(), gcs_node1_info->state());

  // Register a node to GCS.
  auto gcs_node2_info = Mocker::GenNodeInfo();
  ClientID node2_id = ClientID::FromBinary(gcs_node2_info->node_id());
  ASSERT_TRUE(RegisterNode(*gcs_node2_info));
  WaitPendingDone(register_count, 2);

  // Get information of all nodes from GCS.
  std::vector<rpc::GcsNodeInfo> node_list = GetNodeInfoList();
  EXPECT_EQ(node_list.size(), 2);
  ASSERT_TRUE(gcs_client_->Nodes().Get(node1_id));
  EXPECT_EQ(gcs_client_->Nodes().GetAll().size(), 2);

  // Cancel registration of local node to GCS.
  ASSERT_TRUE(UnregisterSelf());

  // Cancel registration of a node to GCS.
  ASSERT_TRUE(UnregisterNode(node2_id));
  WaitPendingDone(unregister_count, 2);

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
  auto on_subscribe = [&add_count, &remove_count](
                          const ClientID &id,
                          const gcs::ResourceChangeNotification &notification) {
    if (notification.IsAdded()) {
      ++add_count;
    } else if (notification.IsRemoved()) {
      ++remove_count;
    }
  };
  ASSERT_TRUE(SubscribeToResources(on_subscribe));

  // Register node.
  auto node_info = Mocker::GenNodeInfo();
  RAY_CHECK(RegisterNode(*node_info));

  // Update resources of node in GCS.
  ClientID node_id = ClientID::FromBinary(node_info->node_id());
  gcs::NodeInfoAccessor::ResourceMap resource_map;
  std::string key = "CPU";
  auto resource = std::make_shared<rpc::ResourceTableData>();
  resource->set_resource_capacity(1.0);
  resource_map[key] = resource;
  ASSERT_TRUE(UpdateResources(node_id, resource_map));
  WaitPendingDone(add_count, 1);
  ASSERT_TRUE(GetResources(node_id).count(key));

  // Delete resources of a node from GCS.
  ASSERT_TRUE(DeleteResources(node_id, {key}));
  WaitPendingDone(remove_count, 1);
  ASSERT_TRUE(GetResources(node_id).empty());
}

TEST_F(ServiceBasedGcsClientTest, TestNodeHeartbeat) {
  // Subscribe batched state of all nodes from GCS.
  std::atomic<int> heartbeat_batch_count(0);
  auto on_subscribe =
      [&heartbeat_batch_count](const gcs::HeartbeatBatchTableData &result) {
        ++heartbeat_batch_count;
      };
  ASSERT_TRUE(SubscribeBatchHeartbeat(on_subscribe));

  // Register node.
  auto node_info = Mocker::GenNodeInfo();
  RAY_CHECK(RegisterNode(*node_info));

  // Report heartbeat of a node to GCS.
  ClientID node_id = ClientID::FromBinary(node_info->node_id());
  auto heartbeat = std::make_shared<rpc::HeartbeatTableData>();
  heartbeat->set_client_id(node_id.Binary());
  ASSERT_TRUE(ReportHeartbeat(heartbeat));
  WaitPendingDone(heartbeat_batch_count, 1);
}

TEST_F(ServiceBasedGcsClientTest, TestTaskInfo) {
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  auto task_table_data = Mocker::GenTaskTableData(job_id.Binary(), task_id.Binary());

  // Subscribe to the event that the given task is added in GCS.
  std::atomic<int> task_count(0);
  auto task_subscribe = [&task_count](const TaskID &id,
                                      const rpc::TaskTableData &result) { ++task_count; };
  ASSERT_TRUE(SubscribeTask(task_id, task_subscribe));

  // Add a task to GCS.
  ASSERT_TRUE(AddTask(task_table_data));
  auto get_task_result = GetTask(task_id);
  ASSERT_TRUE(get_task_result.task().task_spec().task_id() == task_id.Binary());
  ASSERT_TRUE(get_task_result.task().task_spec().job_id() == job_id.Binary());

  // Cancel subscription to a task.
  ASSERT_TRUE(UnsubscribeTask(task_id));

  // Add a task to GCS again.
  ASSERT_TRUE(AddTask(task_table_data));

  // Assert unsubscribe succeeded.
  usleep(100 * 1000);
  EXPECT_EQ(task_count, 1);

  // Delete tasks from GCS.
  std::vector<TaskID> task_ids = {task_id};
  ASSERT_TRUE(DeleteTask(task_ids));

  // Subscribe to the event that the given task lease is added in GCS.
  std::atomic<int> task_lease_count(0);
  auto task_lease_subscribe = [&task_lease_count](
                                  const TaskID &id,
                                  const boost::optional<rpc::TaskLeaseData> &result) {
    ++task_lease_count;
  };
  ASSERT_TRUE(SubscribeTaskLease(task_id, task_lease_subscribe));

  // Add a task lease to GCS.
  ClientID node_id = ClientID::FromRandom();
  auto task_lease = Mocker::GenTaskLeaseData(task_id.Binary(), node_id.Binary());
  ASSERT_TRUE(AddTaskLease(task_lease));
  WaitPendingDone(task_lease_count, 2);

  // Cancel subscription to a task lease.
  ASSERT_TRUE(UnsubscribeTaskLease(task_id));

  // Add a task lease to GCS again.
  ASSERT_TRUE(AddTaskLease(task_lease));

  // Assert unsubscribe succeeded.
  usleep(100 * 1000);
  EXPECT_EQ(task_lease_count, 2);

  // Attempt task reconstruction to GCS.
  auto task_reconstruction_data = std::make_shared<rpc::TaskReconstructionData>();
  task_reconstruction_data->set_task_id(task_id.Binary());
  task_reconstruction_data->set_num_reconstructions(0);
  ASSERT_TRUE(AttemptTaskReconstruction(task_reconstruction_data));
}

TEST_F(ServiceBasedGcsClientTest, TestObjectInfo) {
  ObjectID object_id = ObjectID::FromRandom();
  ClientID node_id = ClientID::FromRandom();

  // Subscribe to any update of an object's location.
  std::atomic<int> object_add_count(0);
  std::atomic<int> object_remove_count(0);
  auto on_subscribe = [&object_add_count, &object_remove_count](
                          const ObjectID &object_id,
                          const gcs::ObjectChangeNotification &result) {
    if (!result.GetData().empty()) {
      if (result.IsAdded()) {
        ++object_add_count;
      } else if (result.IsRemoved()) {
        ++object_remove_count;
      }
    }
  };
  ASSERT_TRUE(SubscribeToLocations(object_id, on_subscribe));

  // Add location of object to GCS.
  ASSERT_TRUE(AddLocation(object_id, node_id));
  WaitPendingDone(object_add_count, 1);

  // Get object's locations from GCS.
  auto locations = GetLocations(object_id);
  ASSERT_EQ(locations.size(), 1);
  ASSERT_EQ(locations.back().manager(), node_id.Binary());

  // Remove location of object from GCS.
  ASSERT_TRUE(RemoveLocation(object_id, node_id));
  WaitPendingDone(object_remove_count, 1);
  ASSERT_TRUE(GetLocations(object_id).empty());

  // Cancel subscription to any update of an object's location.
  ASSERT_TRUE(UnsubscribeToLocations(object_id));

  // Add location of object to GCS again.
  ASSERT_TRUE(AddLocation(object_id, node_id));

  // Assert unsubscribe succeeded.
  usleep(100 * 1000);
  ASSERT_EQ(object_add_count, 1);
}

TEST_F(ServiceBasedGcsClientTest, TestStats) {
  // Add profile data to GCS.
  ClientID node_id = ClientID::FromRandom();
  auto profile_table_data = Mocker::GenProfileTableData(node_id);
  ASSERT_TRUE(AddProfileData(profile_table_data));
}

TEST_F(ServiceBasedGcsClientTest, TestWorkerInfo) {
  // Subscribe to all unexpected failure of workers from GCS.
  std::atomic<int> worker_failure_count(0);
  auto on_subscribe = [&worker_failure_count](const WorkerID &worker_id,
                                              const rpc::WorkerFailureData &result) {
    ++worker_failure_count;
  };
  ASSERT_TRUE(SubscribeToWorkerFailures(on_subscribe));

  // Report a worker failure to GCS.
  auto worker_failure_data = Mocker::GenWorkerFailureData();
  ASSERT_TRUE(ReportWorkerFailure(worker_failure_data));
  WaitPendingDone(worker_failure_count, 1);
}

TEST_F(ServiceBasedGcsClientTest, TestErrorInfo) {
  // Report a job error to GCS.
  JobID job_id = JobID::FromInt(1);
  auto error_table_data = Mocker::GenErrorTableData(job_id);
  ASSERT_TRUE(ReportJobError(error_table_data));
}

TEST_F(ServiceBasedGcsClientTest, TestDetectGcsAvailability) {
  // Create job table data.
  JobID add_job_id = JobID::FromInt(1);
  auto job_table_data = Mocker::GenJobTableData(add_job_id);

  RAY_LOG(INFO) << "Initializing GCS service, port = " << gcs_server_->GetPort();
  gcs_server_->Stop();
  thread_gcs_server_->join();

  gcs_server_.reset(new gcs::GcsServer(config));
  thread_gcs_server_.reset(new std::thread([this] { gcs_server_->Start(); }));

  // Wait until server starts listening.
  while (gcs_server_->GetPort() == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  RAY_LOG(INFO) << "GCS service restarted, port = " << gcs_server_->GetPort();

  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Jobs().AsyncAdd(
      job_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
  promise.get_future().get();
}

TEST_F(ServiceBasedGcsClientTest, TestGcsRedisFailureDetector) {
  // Stop redis.
  TearDownTestCase();

  // Sleep 3 times of gcs_redis_heartbeat_interval_milliseconds to make sure gcs_server
  // detects that the redis is failure and then stop itself.
  auto interval_ms = RayConfig::instance().gcs_redis_heartbeat_interval_milliseconds();
  std::this_thread::sleep_for(std::chrono::milliseconds(3 * interval_ms));

  // Check if GCS server has exited.
  RAY_CHECK(gcs_server_->IsStopped());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
