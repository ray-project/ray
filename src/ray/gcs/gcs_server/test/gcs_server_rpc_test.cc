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

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {

class GcsServerTest : public RedisServiceManagerForTest {
 public:
  void SetUp() override {
    gcs::GcsServerConfig config;
    config.grpc_server_port = 0;
    config.grpc_server_name = "MockedGcsServer";
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.is_test = true;
    config.redis_port = REDIS_SERVER_PORT;
    gcs_server_.reset(new gcs::GcsServer(config));

    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    thread_gcs_server_.reset(new std::thread([this] { gcs_server_->Start(); }));

    // Wait until server starts listening.
    while (gcs_server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Create gcs rpc client
    client_call_manager_.reset(new rpc::ClientCallManager(io_service_));
    client_.reset(
        new rpc::GcsRpcClient("0.0.0.0", gcs_server_->GetPort(), *client_call_manager_));
  }

  void TearDown() override {
    gcs_server_->Stop();
    io_service_.stop();
    thread_io_service_->join();
    thread_gcs_server_->join();
  }

  bool AddJob(const rpc::AddJobRequest &request) {
    std::promise<bool> promise;
    client_->AddJob(request,
                    [&promise](const Status &status, const rpc::AddJobReply &reply) {
                      RAY_CHECK_OK(status);
                      promise.set_value(true);
                    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool MarkJobFinished(const rpc::MarkJobFinishedRequest &request) {
    std::promise<bool> promise;
    client_->MarkJobFinished(request, [&promise](const Status &status,
                                                 const rpc::MarkJobFinishedReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterActorInfo(const rpc::RegisterActorInfoRequest &request) {
    std::promise<bool> promise;
    client_->RegisterActorInfo(
        request,
        [&promise](const Status &status, const rpc::RegisterActorInfoReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UpdateActorInfo(const rpc::UpdateActorInfoRequest &request) {
    std::promise<bool> promise;
    client_->UpdateActorInfo(request, [&promise](const Status &status,
                                                 const rpc::UpdateActorInfoReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  rpc::ActorTableData GetActorInfo(const std::string &actor_id) {
    rpc::GetActorInfoRequest request;
    request.set_actor_id(actor_id);
    rpc::ActorTableData actor_table_data;
    std::promise<bool> promise;
    client_->GetActorInfo(
        request, [&actor_table_data, &promise](const Status &status,
                                               const rpc::GetActorInfoReply &reply) {
          RAY_CHECK_OK(status);
          actor_table_data.CopyFrom(reply.actor_table_data());
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return actor_table_data;
  }

  bool AddActorCheckpoint(const rpc::AddActorCheckpointRequest &request) {
    std::promise<bool> promise;
    client_->AddActorCheckpoint(
        request,
        [&promise](const Status &status, const rpc::AddActorCheckpointReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  rpc::ActorCheckpointData GetActorCheckpoint(const std::string &actor_id,
                                              const std::string &checkpoint_id) {
    rpc::GetActorCheckpointRequest request;
    request.set_actor_id(actor_id);
    request.set_checkpoint_id(checkpoint_id);
    rpc::ActorCheckpointData checkpoint_data;
    std::promise<bool> promise;
    client_->GetActorCheckpoint(
        request, [&checkpoint_data, &promise](const Status &status,
                                              const rpc::GetActorCheckpointReply &reply) {
          RAY_CHECK_OK(status);
          checkpoint_data.CopyFrom(reply.checkpoint_data());
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return checkpoint_data;
  }

  rpc::ActorCheckpointIdData GetActorCheckpointID(const std::string &actor_id) {
    rpc::GetActorCheckpointIDRequest request;
    request.set_actor_id(actor_id);
    rpc::ActorCheckpointIdData checkpoint_id_data;
    std::promise<bool> promise;
    client_->GetActorCheckpointID(
        request, [&checkpoint_id_data, &promise](
                     const Status &status, const rpc::GetActorCheckpointIDReply &reply) {
          RAY_CHECK_OK(status);
          checkpoint_id_data.CopyFrom(reply.checkpoint_id_data());
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return checkpoint_id_data;
  }

  bool RegisterNode(const rpc::RegisterNodeRequest &request) {
    std::promise<bool> promise;
    client_->RegisterNode(
        request, [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });

    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UnregisterNode(const rpc::UnregisterNodeRequest &request) {
    std::promise<bool> promise;
    client_->UnregisterNode(
        request, [&promise](const Status &status, const rpc::UnregisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::vector<rpc::GcsNodeInfo> GetAllNodeInfo() {
    std::vector<rpc::GcsNodeInfo> node_info_list;
    rpc::GetAllNodeInfoRequest request;
    std::promise<bool> promise;
    client_->GetAllNodeInfo(
        request, [&node_info_list, &promise](const Status &status,
                                             const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.node_info_list_size(); ++index) {
            node_info_list.push_back(reply.node_info_list(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return node_info_list;
  }

  bool ReportHeartbeat(const rpc::ReportHeartbeatRequest &request) {
    std::promise<bool> promise;
    client_->ReportHeartbeat(request, [&promise](const Status &status,
                                                 const rpc::ReportHeartbeatReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UpdateResources(const rpc::UpdateResourcesRequest &request) {
    std::promise<bool> promise;
    client_->UpdateResources(request, [&promise](const Status &status,
                                                 const rpc::UpdateResourcesReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool DeleteResources(const rpc::DeleteResourcesRequest &request) {
    std::promise<bool> promise;
    client_->DeleteResources(request, [&promise](const Status &status,
                                                 const rpc::DeleteResourcesReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::map<std::string, gcs::ResourceTableData> GetResources(const std::string &node_id) {
    rpc::GetResourcesRequest request;
    request.set_node_id(node_id);
    std::map<std::string, gcs::ResourceTableData> resources;
    std::promise<bool> promise;
    client_->GetResources(request,
                          [&resources, &promise](const Status &status,
                                                 const rpc::GetResourcesReply &reply) {
                            RAY_CHECK_OK(status);
                            for (auto &resource : reply.resources()) {
                              resources[resource.first] = resource.second;
                            }
                            promise.set_value(true);
                          });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return resources;
  }

  bool AddObjectLocation(const rpc::AddObjectLocationRequest &request) {
    std::promise<bool> promise;
    client_->AddObjectLocation(
        request,
        [&promise](const Status &status, const rpc::AddObjectLocationReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });

    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RemoveObjectLocation(const rpc::RemoveObjectLocationRequest &request) {
    std::promise<bool> promise;
    client_->RemoveObjectLocation(
        request,
        [&promise](const Status &status, const rpc::RemoveObjectLocationReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });

    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::vector<rpc::ObjectTableData> GetObjectLocations(const std::string &object_id) {
    std::vector<rpc::ObjectTableData> object_locations;
    rpc::GetObjectLocationsRequest request;
    request.set_object_id(object_id);
    std::promise<bool> promise;
    client_->GetObjectLocations(
        request, [&object_locations, &promise](
                     const Status &status, const rpc::GetObjectLocationsReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.object_table_data_list_size(); ++index) {
            object_locations.push_back(reply.object_table_data_list(index));
          }
          promise.set_value(true);
        });

    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return object_locations;
  }

  bool AddTask(const rpc::AddTaskRequest &request) {
    std::promise<bool> promise;
    client_->AddTask(request,
                     [&promise](const Status &status, const rpc::AddTaskReply &reply) {
                       RAY_CHECK_OK(status);
                       promise.set_value(true);
                     });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  rpc::TaskTableData GetTask(const std::string &task_id) {
    rpc::TaskTableData task_data;
    rpc::GetTaskRequest request;
    request.set_task_id(task_id);
    std::promise<bool> promise;
    client_->GetTask(request, [&task_data, &promise](const Status &status,
                                                     const rpc::GetTaskReply &reply) {
      if (status.ok()) {
        task_data.CopyFrom(reply.task_data());
      }
      promise.set_value(true);
    });

    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return task_data;
  }

  bool DeleteTasks(const rpc::DeleteTasksRequest &request) {
    std::promise<bool> promise;
    client_->DeleteTasks(
        request, [&promise](const Status &status, const rpc::DeleteTasksReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool AddTaskLease(const rpc::AddTaskLeaseRequest &request) {
    std::promise<bool> promise;
    client_->AddTaskLease(
        request, [&promise](const Status &status, const rpc::AddTaskLeaseReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool AttemptTaskReconstruction(const rpc::AttemptTaskReconstructionRequest &request) {
    std::promise<bool> promise;
    client_->AttemptTaskReconstruction(
        request, [&promise](const Status &status,
                            const rpc::AttemptTaskReconstructionReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool AddProfileData(const rpc::AddProfileDataRequest &request) {
    std::promise<bool> promise;
    client_->AddProfileData(
        request, [&promise](const Status &status, const rpc::AddProfileDataReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool ReportJobError(const rpc::ReportJobErrorRequest &request) {
    std::promise<bool> promise;
    client_->ReportJobError(
        request, [&promise](const Status &status, const rpc::ReportJobErrorReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool ReportWorkerFailure(const rpc::ReportWorkerFailureRequest &request) {
    std::promise<bool> promise;
    client_->ReportWorkerFailure(
        request,
        [&promise](const Status &status, const rpc::ReportWorkerFailureReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool WaitReady(const std::future<bool> &future, uint64_t timeout_ms) {
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    return status == std::future_status::ready;
  }

 protected:
  // Gcs server
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<std::thread> thread_gcs_server_;
  boost::asio::io_service io_service_;

  // Gcs client
  std::unique_ptr<rpc::GcsRpcClient> client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;

  // Timeout waiting for gcs server reply, default is 5s
  const uint64_t timeout_ms_ = 5000;
};

TEST_F(GcsServerTest, TestActorInfo) {
  // Create actor_table_data
  JobID job_id = JobID::FromInt(1);
  auto actor_table_data = Mocker::GenActorTableData(job_id);

  // Register actor
  rpc::RegisterActorInfoRequest register_actor_info_request;
  register_actor_info_request.mutable_actor_table_data()->CopyFrom(*actor_table_data);
  ASSERT_TRUE(RegisterActorInfo(register_actor_info_request));
  rpc::ActorTableData result = GetActorInfo(actor_table_data->actor_id());
  ASSERT_TRUE(result.state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);

  // Update actor state
  rpc::UpdateActorInfoRequest update_actor_info_request;
  actor_table_data->set_state(
      rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);
  update_actor_info_request.set_actor_id(actor_table_data->actor_id());
  update_actor_info_request.mutable_actor_table_data()->CopyFrom(*actor_table_data);
  ASSERT_TRUE(UpdateActorInfo(update_actor_info_request));
  result = GetActorInfo(actor_table_data->actor_id());
  ASSERT_TRUE(result.state() ==
              rpc::ActorTableData_ActorState::ActorTableData_ActorState_DEAD);

  // Add actor checkpoint
  ActorCheckpointID checkpoint_id = ActorCheckpointID::FromRandom();
  rpc::ActorCheckpointData checkpoint;
  checkpoint.set_actor_id(actor_table_data->actor_id());
  checkpoint.set_checkpoint_id(checkpoint_id.Binary());
  checkpoint.set_execution_dependency(checkpoint_id.Binary());

  rpc::AddActorCheckpointRequest add_actor_checkpoint_request;
  add_actor_checkpoint_request.mutable_checkpoint_data()->CopyFrom(checkpoint);
  ASSERT_TRUE(AddActorCheckpoint(add_actor_checkpoint_request));
  rpc::ActorCheckpointData checkpoint_result =
      GetActorCheckpoint(actor_table_data->actor_id(), checkpoint_id.Binary());
  ASSERT_TRUE(checkpoint_result.actor_id() == actor_table_data->actor_id());
  ASSERT_TRUE(checkpoint_result.checkpoint_id() == checkpoint_id.Binary());
  rpc::ActorCheckpointIdData checkpoint_id_result =
      GetActorCheckpointID(actor_table_data->actor_id());
  ASSERT_TRUE(checkpoint_id_result.actor_id() == actor_table_data->actor_id());
  ASSERT_TRUE(checkpoint_id_result.checkpoint_ids_size() == 1);
}

TEST_F(GcsServerTest, TestJobInfo) {
  // Create job_table_data
  JobID job_id = JobID::FromInt(1);
  auto job_table_data = Mocker::GenJobTableData(job_id);

  // Add job
  rpc::AddJobRequest add_job_request;
  add_job_request.mutable_data()->CopyFrom(*job_table_data);
  ASSERT_TRUE(AddJob(add_job_request));

  // Mark job finished
  rpc::MarkJobFinishedRequest mark_job_finished_request;
  mark_job_finished_request.set_job_id(job_table_data->job_id());
  ASSERT_TRUE(MarkJobFinished(mark_job_finished_request));
}

TEST_F(GcsServerTest, TestNodeInfo) {
  // Create gcs node info
  auto gcs_node_info = Mocker::GenNodeInfo();

  // Register node info
  rpc::RegisterNodeRequest register_node_info_request;
  register_node_info_request.mutable_node_info()->CopyFrom(*gcs_node_info);
  ASSERT_TRUE(RegisterNode(register_node_info_request));
  std::vector<rpc::GcsNodeInfo> node_info_list = GetAllNodeInfo();
  ASSERT_TRUE(node_info_list.size() == 1);
  ASSERT_TRUE(node_info_list[0].state() ==
              rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_ALIVE);

  // Report heartbeat
  rpc::ReportHeartbeatRequest report_heartbeat_request;
  report_heartbeat_request.mutable_heartbeat()->set_client_id(gcs_node_info->node_id());
  ASSERT_TRUE(ReportHeartbeat(report_heartbeat_request));

  // Update node resources
  rpc::UpdateResourcesRequest update_resources_request;
  update_resources_request.set_node_id(gcs_node_info->node_id());
  rpc::ResourceTableData resource_table_data;
  resource_table_data.set_resource_capacity(1.0);
  std::string resource_name = "CPU";
  (*update_resources_request.mutable_resources())[resource_name] = resource_table_data;
  ASSERT_TRUE(UpdateResources(update_resources_request));
  auto resources = GetResources(gcs_node_info->node_id());
  ASSERT_TRUE(resources.size() == 1);

  // Delete node resources
  rpc::DeleteResourcesRequest delete_resources_request;
  delete_resources_request.set_node_id(gcs_node_info->node_id());
  delete_resources_request.add_resource_name_list(resource_name);
  ASSERT_TRUE(DeleteResources(delete_resources_request));
  resources = GetResources(gcs_node_info->node_id());
  ASSERT_TRUE(resources.empty());

  // Unregister node info
  rpc::UnregisterNodeRequest unregister_node_info_request;
  unregister_node_info_request.set_node_id(gcs_node_info->node_id());
  ASSERT_TRUE(UnregisterNode(unregister_node_info_request));
  node_info_list = GetAllNodeInfo();
  ASSERT_TRUE(node_info_list.size() == 1);
  ASSERT_TRUE(node_info_list[0].state() ==
              rpc::GcsNodeInfo_GcsNodeState::GcsNodeInfo_GcsNodeState_DEAD);
}

TEST_F(GcsServerTest, TestObjectInfo) {
  // Create object table data
  ObjectID object_id = ObjectID::FromRandom();
  ClientID node1_id = ClientID::FromRandom();
  ClientID node2_id = ClientID::FromRandom();

  // Add object location
  rpc::AddObjectLocationRequest add_object_location_request;
  add_object_location_request.set_object_id(object_id.Binary());
  add_object_location_request.set_node_id(node1_id.Binary());
  ASSERT_TRUE(AddObjectLocation(add_object_location_request));
  std::vector<rpc::ObjectTableData> object_locations =
      GetObjectLocations(object_id.Binary());
  ASSERT_TRUE(object_locations.size() == 1);
  ASSERT_TRUE(object_locations[0].manager() == node1_id.Binary());

  add_object_location_request.set_node_id(node2_id.Binary());
  ASSERT_TRUE(AddObjectLocation(add_object_location_request));
  object_locations = GetObjectLocations(object_id.Binary());
  ASSERT_TRUE(object_locations.size() == 2);

  // Remove object location
  rpc::RemoveObjectLocationRequest remove_object_location_request;
  remove_object_location_request.set_object_id(object_id.Binary());
  remove_object_location_request.set_node_id(node1_id.Binary());
  ASSERT_TRUE(RemoveObjectLocation(remove_object_location_request));
  object_locations = GetObjectLocations(object_id.Binary());
  ASSERT_TRUE(object_locations.size() == 1);
  ASSERT_TRUE(object_locations[0].manager() == node2_id.Binary());
}

TEST_F(GcsServerTest, TestTaskInfo) {
  // Create task_table_data
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  auto job_table_data = Mocker::GenTaskTableData(job_id.Binary(), task_id.Binary());

  // Add task
  rpc::AddTaskRequest add_task_request;
  add_task_request.mutable_task_data()->CopyFrom(*job_table_data);
  ASSERT_TRUE(AddTask(add_task_request));
  rpc::TaskTableData result = GetTask(task_id.Binary());
  ASSERT_TRUE(result.task().task_spec().job_id() == job_id.Binary());

  // Delete task
  rpc::DeleteTasksRequest delete_tasks_request;
  delete_tasks_request.add_task_id_list(task_id.Binary());
  ASSERT_TRUE(DeleteTasks(delete_tasks_request));
  result = GetTask(task_id.Binary());
  ASSERT_TRUE(!result.has_task());

  // Add task lease
  ClientID node_id = ClientID::FromRandom();
  auto task_lease_data = Mocker::GenTaskLeaseData(task_id.Binary(), node_id.Binary());
  rpc::AddTaskLeaseRequest add_task_lease_request;
  add_task_lease_request.mutable_task_lease_data()->CopyFrom(*task_lease_data);
  ASSERT_TRUE(AddTaskLease(add_task_lease_request));

  // Attempt task reconstruction
  rpc::AttemptTaskReconstructionRequest attempt_task_reconstruction_request;
  rpc::TaskReconstructionData task_reconstruction_data;
  task_reconstruction_data.set_task_id(task_id.Binary());
  task_reconstruction_data.set_node_manager_id(node_id.Binary());
  task_reconstruction_data.set_num_reconstructions(0);
  attempt_task_reconstruction_request.mutable_task_reconstruction()->CopyFrom(
      task_reconstruction_data);
  ASSERT_TRUE(AttemptTaskReconstruction(attempt_task_reconstruction_request));
}

TEST_F(GcsServerTest, TestStats) {
  rpc::ProfileTableData profile_table_data;
  profile_table_data.set_component_id(ClientID::FromRandom().Binary());
  rpc::AddProfileDataRequest add_profile_data_request;
  add_profile_data_request.mutable_profile_data()->CopyFrom(profile_table_data);
  ASSERT_TRUE(AddProfileData(add_profile_data_request));
}

TEST_F(GcsServerTest, TestErrorInfo) {
  // Report error
  rpc::ReportJobErrorRequest report_error_request;
  rpc::ErrorTableData error_table_data;
  JobID job_id = JobID::FromInt(1);
  error_table_data.set_job_id(job_id.Binary());
  report_error_request.mutable_error_data()->CopyFrom(error_table_data);
  ASSERT_TRUE(ReportJobError(report_error_request));
}

TEST_F(GcsServerTest, TestWorkerInfo) {
  rpc::WorkerFailureData worker_failure_data;
  worker_failure_data.mutable_worker_address()->set_ip_address("127.0.0.1");
  worker_failure_data.mutable_worker_address()->set_port(5566);
  rpc::ReportWorkerFailureRequest report_worker_failure_request;
  report_worker_failure_request.mutable_worker_failure()->CopyFrom(worker_failure_data);
  ASSERT_TRUE(ReportWorkerFailure(report_worker_failure_request));
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
