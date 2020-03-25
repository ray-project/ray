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
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {

static std::string redis_server_executable;
static std::string redis_client_executable;
static std::string libray_redis_module_path;

class ServiceBasedGcsGcsClientTest : public RedisServiceManagerForTest {
 public:
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

    // Create gcs client
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

  bool ReportHeartbeat(const std::shared_ptr<rpc::HeartbeatTableData> heartbeat) {
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncReportHeartbeat(
        heartbeat, [&promise](Status status) { promise.set_value(status.ok()); }));
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

 protected:
  bool WaitReady(const std::future<bool> &future,
                 const std::chrono::milliseconds &timeout_ms) {
    auto status = future.wait_for(timeout_ms);
    return status == std::future_status::ready;
  }

  void WaitPendingDone(int &current_count, int expected_count) {
    auto condition = [&current_count, expected_count]() {
      return current_count == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->set_is_dead(false);
    job_table_data->set_timestamp(std::time(nullptr));
    job_table_data->set_node_manager_address("127.0.0.1");
    job_table_data->set_driver_pid(5667L);
    return job_table_data;
  }

  std::shared_ptr<rpc::ActorTableData> GenActorTableData(const JobID &job_id) {
    auto actor_table_data = std::make_shared<rpc::ActorTableData>();
    ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    actor_table_data->set_actor_id(actor_id.Binary());
    actor_table_data->set_job_id(job_id.Binary());
    actor_table_data->set_state(
        rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
    actor_table_data->set_max_reconstructions(1);
    actor_table_data->set_remaining_reconstructions(1);
    return actor_table_data;
  }

  rpc::GcsNodeInfo GenGcsNodeInfo(const std::string &node_id) {
    rpc::GcsNodeInfo gcs_node_info;
    gcs_node_info.set_node_id(node_id);
    gcs_node_info.set_state(rpc::GcsNodeInfo_GcsNodeState_ALIVE);
    gcs_node_info.set_node_manager_address("127.0.0.1");
    return gcs_node_info;
  }

  std::shared_ptr<rpc::TaskTableData> GenTaskTableData(const std::string &job_id,
                                                       const std::string &task_id) {
    auto task_table_data = std::make_shared<rpc::TaskTableData>();
    rpc::Task task;
    rpc::TaskSpec task_spec;
    task_spec.set_job_id(job_id);
    task_spec.set_task_id(task_id);
    task.mutable_task_spec()->CopyFrom(task_spec);
    task_table_data->mutable_task()->CopyFrom(task);
    return task_table_data;
  }

  std::shared_ptr<rpc::TaskLeaseData> GenTaskLeaseData(const std::string &task_id,
                                                       const std::string &node_id) {
    auto task_lease_data = std::make_shared<rpc::TaskLeaseData>();
    task_lease_data->set_task_id(task_id);
    task_lease_data->set_node_manager_id(node_id);
    return task_lease_data;
  }

  // Gcs server
  gcs::GcsServerConfig config;
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<std::thread> thread_gcs_server_;
  std::unique_ptr<boost::asio::io_service> io_service_;

  // Gcs client
  std::unique_ptr<gcs::GcsClient> gcs_client_;

  // Timeout waiting for gcs server reply, default is 2s
  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(ServiceBasedGcsGcsClientTest, TestJobInfo) {
  // Create job_table_data
  JobID add_job_id = JobID::FromInt(1);
  auto job_table_data = GenJobTableData(add_job_id);

  std::promise<bool> promise;
  auto on_subscribe = [&promise, add_job_id](const JobID &job_id,
                                             const gcs::JobTableData &data) {
    ASSERT_TRUE(add_job_id == job_id);
    promise.set_value(true);
  };
  RAY_CHECK_OK(gcs_client_->Jobs().AsyncSubscribeToFinishedJobs(
      on_subscribe, [](Status status) { RAY_CHECK_OK(status); }));

  ASSERT_TRUE(AddJob(job_table_data));
  ASSERT_TRUE(MarkJobFinished(add_job_id));
  ASSERT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
}

TEST_F(ServiceBasedGcsGcsClientTest, TestActorInfo) {
  // Create actor_table_data
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data = GenActorTableData(job_id);
  ActorID actor_id = ActorID::FromBinary(actor_table_data->actor_id());

  // Subscribe
  std::promise<bool> promise_subscribe;
  std::atomic<int> subscribe_callback_count(0);
  auto on_subscribe = [&subscribe_callback_count](const ActorID &actor_id,
                                                  const gcs::ActorTableData &data) {
    ++subscribe_callback_count;
  };
  RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribe(actor_id, on_subscribe,
                                                    [&promise_subscribe](Status status) {
                                                      RAY_CHECK_OK(status);
                                                      promise_subscribe.set_value(true);
                                                    }));

  // Register actor
  ASSERT_TRUE(RegisterActor(actor_table_data));
  ASSERT_TRUE(GetActor(actor_id).state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);

  // Unsubscribe
  std::promise<bool> promise_unsubscribe;
  RAY_CHECK_OK(gcs_client_->Actors().AsyncUnsubscribe(
      actor_id, [&promise_unsubscribe](Status status) {
        RAY_CHECK_OK(status);
        promise_unsubscribe.set_value(true);
      }));
  ASSERT_TRUE(WaitReady(promise_unsubscribe.get_future(), timeout_ms_));

  // Update actor
  actor_table_data->set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  ASSERT_TRUE(UpdateActor(actor_id, actor_table_data));
  ASSERT_TRUE(GetActor(actor_id).state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  ASSERT_TRUE(WaitReady(promise_subscribe.get_future(), timeout_ms_));
  auto condition = [&subscribe_callback_count]() {
    return 1 == subscribe_callback_count;
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
}

TEST_F(ServiceBasedGcsGcsClientTest, TestActorCheckpoint) {
  // Create actor checkpoint
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data = GenActorTableData(job_id);
  ActorID actor_id = ActorID::FromBinary(actor_table_data->actor_id());

  ActorCheckpointID checkpoint_id = ActorCheckpointID::FromRandom();
  auto checkpoint = std::make_shared<rpc::ActorCheckpointData>();
  checkpoint->set_actor_id(actor_table_data->actor_id());
  checkpoint->set_checkpoint_id(checkpoint_id.Binary());
  checkpoint->set_execution_dependency(checkpoint_id.Binary());

  // Add checkpoint
  ASSERT_TRUE(AddCheckpoint(checkpoint));

  // Get Checkpoint
  auto get_checkpoint_result = GetCheckpoint(actor_id, checkpoint_id);
  ASSERT_TRUE(get_checkpoint_result.actor_id() == actor_id.Binary());

  // Get CheckpointID
  auto get_checkpoint_id_result = GetCheckpointID(actor_id);
  ASSERT_TRUE(get_checkpoint_id_result.checkpoint_ids_size() == 1);
  ASSERT_TRUE(get_checkpoint_id_result.checkpoint_ids(0) == checkpoint_id.Binary());
}

TEST_F(ServiceBasedGcsGcsClientTest, TestActorSubscribeAll) {
  // Create actor_table_data
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data1 = GenActorTableData(job_id);
  auto actor_table_data2 = GenActorTableData(job_id);

  // Subscribe all
  std::promise<bool> promise_subscribe_all;
  std::atomic<int> subscribe_all_callback_count(0);
  auto on_subscribe_all = [&subscribe_all_callback_count](
                              const ActorID &actor_id, const gcs::ActorTableData &data) {
    ++subscribe_all_callback_count;
  };
  RAY_CHECK_OK(gcs_client_->Actors().AsyncSubscribeAll(
      on_subscribe_all, [&promise_subscribe_all](Status status) {
        RAY_CHECK_OK(status);
        promise_subscribe_all.set_value(true);
      }));
  ASSERT_TRUE(WaitReady(promise_subscribe_all.get_future(), timeout_ms_));

  // Register actor
  ASSERT_TRUE(RegisterActor(actor_table_data1));
  ASSERT_TRUE(RegisterActor(actor_table_data2));
  auto condition = [&subscribe_all_callback_count]() {
    return 2 == subscribe_all_callback_count;
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
}

TEST_F(ServiceBasedGcsGcsClientTest, TestNodeInfo) {
  // Create gcs node info
  ClientID node1_id = ClientID::FromRandom();
  auto gcs_node1_info = GenGcsNodeInfo(node1_id.Binary());

  int register_count = 0;
  int unregister_count = 0;
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeToNodeChange(
      [&register_count, &unregister_count](const ClientID &node_id,
                                           const rpc::GcsNodeInfo &data) {
        if (data.state() == rpc::GcsNodeInfo::ALIVE) {
          ++register_count;
        } else if (data.state() == rpc::GcsNodeInfo::DEAD) {
          ++unregister_count;
        }
      },
      nullptr));

  // Register self
  ASSERT_TRUE(RegisterSelf(gcs_node1_info));
  sleep(1);
  EXPECT_EQ(gcs_client_->Nodes().GetSelfId(), node1_id);
  EXPECT_EQ(gcs_client_->Nodes().GetSelfInfo().node_id(), gcs_node1_info.node_id());
  EXPECT_EQ(gcs_client_->Nodes().GetSelfInfo().state(), gcs_node1_info.state());

  // Register node
  ClientID node2_id = ClientID::FromRandom();
  auto gcs_node2_info = GenGcsNodeInfo(node2_id.Binary());
  ASSERT_TRUE(RegisterNode(gcs_node2_info));
  WaitPendingDone(register_count, 2);

  // Get node list
  std::vector<rpc::GcsNodeInfo> node_list = GetNodeInfoList();
  EXPECT_EQ(node_list.size(), 2);
  EXPECT_EQ(register_count, 2);
  ASSERT_TRUE(gcs_client_->Nodes().Get(node1_id));
  EXPECT_EQ(gcs_client_->Nodes().GetAll().size(), 2);

  // Unregister self
  ASSERT_TRUE(UnregisterSelf());

  // Unregister node
  ASSERT_TRUE(UnregisterNode(node2_id));
  WaitPendingDone(unregister_count, 2);

  node_list = GetNodeInfoList();
  EXPECT_EQ(node_list.size(), 2);
  EXPECT_EQ(node_list[0].state(),
            rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  EXPECT_EQ(node_list[1].state(),
            rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
  EXPECT_EQ(unregister_count, 2);
  ASSERT_TRUE(gcs_client_->Nodes().IsRemoved(node2_id));
}

TEST_F(ServiceBasedGcsGcsClientTest, TestNodeResources) {
  int add_count = 0;
  int remove_count = 0;
  auto subscribe = [&add_count, &remove_count](
                       const ClientID &id,
                       const gcs::ResourceChangeNotification &notification) {
    if (notification.IsAdded()) {
      ++add_count;
    } else if (notification.IsRemoved()) {
      ++remove_count;
    }
  };
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeToResources(subscribe, nullptr));

  // Update resources
  ClientID node_id = ClientID::FromRandom();
  gcs::NodeInfoAccessor::ResourceMap resource_map;
  std::string key = "CPU";
  auto resource = std::make_shared<rpc::ResourceTableData>();
  resource->set_resource_capacity(1.0);
  resource_map[key] = resource;
  ASSERT_TRUE(UpdateResources(node_id, resource_map));
  WaitPendingDone(add_count, 1);
  auto get_resources_result = GetResources(node_id);
  ASSERT_TRUE(get_resources_result.count(key));

  // Delete resources
  ASSERT_TRUE(DeleteResources(node_id, {key}));
  WaitPendingDone(remove_count, 1);
  get_resources_result = GetResources(node_id);
  ASSERT_TRUE(get_resources_result.empty());
}

TEST_F(ServiceBasedGcsGcsClientTest, TestNodeHeartbeat) {
  int heartbeat_batch_count = 0;
  auto heartbeat_batch_subscribe =
      [&heartbeat_batch_count](const gcs::HeartbeatBatchTableData &result) {
        ++heartbeat_batch_count;
      };
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeBatchHeartbeat(
      heartbeat_batch_subscribe, nullptr));

  // Report heartbeat
  ClientID node_id = ClientID::FromRandom();
  auto heartbeat = std::make_shared<rpc::HeartbeatTableData>();
  heartbeat->set_client_id(node_id.Binary());
  ASSERT_TRUE(ReportHeartbeat(heartbeat));
  WaitPendingDone(heartbeat_batch_count, 1);
}

TEST_F(ServiceBasedGcsGcsClientTest, TestTaskInfo) {
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  auto task_table_data = GenTaskTableData(job_id.Binary(), task_id.Binary());

  int task_count = 0;
  auto task_subscribe = [&task_count](const TaskID &id,
                                      const rpc::TaskTableData &result) { ++task_count; };
  RAY_CHECK_OK(gcs_client_->Tasks().AsyncSubscribe(task_id, task_subscribe, nullptr));

  // Add task
  ASSERT_TRUE(AddTask(task_table_data));

  auto get_task_result = GetTask(task_id);
  ASSERT_TRUE(get_task_result.task().task_spec().task_id() == task_id.Binary());
  ASSERT_TRUE(get_task_result.task().task_spec().job_id() == job_id.Binary());
  RAY_CHECK_OK(gcs_client_->Tasks().AsyncUnsubscribe(task_id, nullptr));
  ASSERT_TRUE(AddTask(task_table_data));

  // Delete task
  std::vector<TaskID> task_ids = {task_id};
  ASSERT_TRUE(DeleteTask(task_ids));
  EXPECT_EQ(task_count, 1);

  // Add task lease
  int task_lease_count = 0;
  auto task_lease_subscribe = [&task_lease_count](
                                  const TaskID &id,
                                  const boost::optional<rpc::TaskLeaseData> &result) {
    ++task_lease_count;
  };
  RAY_CHECK_OK(gcs_client_->Tasks().AsyncSubscribeTaskLease(task_id, task_lease_subscribe,
                                                            nullptr));
  ClientID node_id = ClientID::FromRandom();
  auto task_lease = GenTaskLeaseData(task_id.Binary(), node_id.Binary());
  ASSERT_TRUE(AddTaskLease(task_lease));
  WaitPendingDone(task_lease_count, 2);

  RAY_CHECK_OK(gcs_client_->Tasks().AsyncUnsubscribeTaskLease(task_id, nullptr));
  ASSERT_TRUE(AddTaskLease(task_lease));
  EXPECT_EQ(task_lease_count, 2);

  // Attempt task reconstruction
  auto task_reconstruction_data = std::make_shared<rpc::TaskReconstructionData>();
  task_reconstruction_data->set_task_id(task_id.Binary());
  task_reconstruction_data->set_num_reconstructions(0);
  ASSERT_TRUE(AttemptTaskReconstruction(task_reconstruction_data));
}

TEST_F(ServiceBasedGcsGcsClientTest, TestDetectGcsAvailability) {
  // Create job_table_data
  JobID add_job_id = JobID::FromInt(1);
  auto job_table_data = GenJobTableData(add_job_id);

  RAY_LOG(INFO) << "Gcs service init port = " << gcs_server_->GetPort();
  gcs_server_->Stop();
  thread_gcs_server_->join();

  gcs_server_.reset(new gcs::GcsServer(config));
  thread_gcs_server_.reset(new std::thread([this] { gcs_server_->Start(); }));

  // Wait until server starts listening.
  while (gcs_server_->GetPort() == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  RAY_LOG(INFO) << "Gcs service restart success, port = " << gcs_server_->GetPort();

  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Jobs().AsyncAdd(
      job_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
  promise.get_future().get();
}

TEST_F(ServiceBasedGcsGcsClientTest, TestGcsRedisFailureDetector) {
  // Stop redis.
  TearDownTestCase();

  // Sleep 3 times of gcs_redis_heartbeat_interval_milliseconds to make sure gcs_server
  // detects that the redis is failure and then stop itself.
  auto interval_ms = RayConfig::instance().gcs_redis_heartbeat_interval_milliseconds();
  std::this_thread::sleep_for(std::chrono::milliseconds(3 * interval_ms));

  // Check if gcs server has exited.
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
