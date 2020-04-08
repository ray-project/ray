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

#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "gtest/gtest.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"
#include "ray/util/test_util.h"

namespace ray {

class GcsTableStorageTest : public gcs::StoreClientTestBase {
 public:
  GcsTableStorageTest() {}

  virtual ~GcsTableStorageTest() {}

  virtual void SetUp() override {
    io_service_pool_ = std::make_shared<IOServicePool>(io_service_num_);
    io_service_pool_->Run();

    gcs::StoreClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    store_client_ = std::make_shared<gcs::RedisStoreClient>(options);
    Status status = store_client_->Connect(io_service_pool_);
    RAY_CHECK_OK(status);
  }

  virtual void TearDown() override {
    store_client_->Disconnect();
    io_service_pool_->Stop();

    store_client_.reset();
    io_service_pool_.reset();
  }

  void InitStoreClient() override {}

 protected:
  std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
    auto data = std::make_shared<rpc::JobTableData>();
    data->set_job_id(job_id.Binary());
    data->set_is_dead(false);
    data->set_timestamp(std::time(nullptr));
    data->set_node_manager_address("127.0.0.1");
    data->set_driver_pid(5667L);
    return data;
  }

  std::shared_ptr<rpc::ActorTableData> GenActorTableData(JobID job_id, ActorID actor_id) {
    auto data = std::make_shared<rpc::ActorTableData>();
    data->set_actor_id(actor_id.Binary());
    data->set_job_id(job_id.Binary());
    data->set_state(rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
    data->set_max_reconstructions(1);
    data->set_remaining_reconstructions(1);
    return data;
  }

  std::shared_ptr<rpc::ActorCheckpointData> GenActorCheckpointData(
      ActorID actor_id, ActorCheckpointID checkpoint_id) {
    auto data = std::make_shared<rpc::ActorCheckpointData>();
    data->set_actor_id(actor_id.Binary());
    data->set_checkpoint_id(checkpoint_id.Binary());
    data->set_execution_dependency(checkpoint_id.Binary());
    return data;
  }

  std::shared_ptr<rpc::ActorCheckpointIdData> GenActorCheckpointIdData(
      ActorID actor_id, ActorCheckpointID checkpoint_id) {
    auto data = std::make_shared<rpc::ActorCheckpointIdData>();
    data->set_actor_id(actor_id.Binary());
    data->add_checkpoint_ids(checkpoint_id.Binary());
    return data;
  }

  std::shared_ptr<rpc::TaskTableData> GenTaskTableData(const JobID &job_id,
                                                       const TaskID &task_id) {
    auto data = std::make_shared<rpc::TaskTableData>();
    rpc::Task task;
    rpc::TaskSpec task_spec;
    task_spec.set_job_id(job_id.Binary());
    task_spec.set_task_id(task_id.Binary());
    task.mutable_task_spec()->CopyFrom(task_spec);
    data->mutable_task()->CopyFrom(task);
    return data;
  }

  std::shared_ptr<rpc::TaskLeaseData> GenTaskLeaseData(const TaskID &task_id,
                                                       const ClientID &node_id) {
    auto data = std::make_shared<rpc::TaskLeaseData>();
    data->set_task_id(task_id.Binary());
    data->set_node_manager_id(node_id.Binary());
    return data;
  }

  std::shared_ptr<rpc::TaskReconstructionData> GenTaskReconstructionData(
      const TaskID &task_id, const ClientID &node_id) {
    auto data = std::make_shared<rpc::TaskReconstructionData>();
    data->set_task_id(task_id.Binary());
    data->set_num_reconstructions(1);
    data->set_node_manager_id(node_id.Binary());
    return data;
  }

  std::shared_ptr<rpc::ObjectTableDataList> GenObjectTableDataList(
      const ClientID &node_id) {
    auto data = std::make_shared<rpc::ObjectTableDataList>();
    rpc::ObjectTableData object_table_data;
    object_table_data.set_manager(node_id.Binary());
    object_table_data.set_object_size(1);
    data->add_items()->CopyFrom(object_table_data);
    return data;
  }

  std::shared_ptr<rpc::GcsNodeInfo> GenGcsNodeInfo(const ClientID &node_id) {
    auto data = std::make_shared<rpc::GcsNodeInfo>();
    data->set_node_id(node_id.Binary());
    data->set_state(rpc::GcsNodeInfo_GcsNodeState_ALIVE);
    return data;
  }

  std::shared_ptr<rpc::ResourceMap> GenResourceMap(const ClientID &node_id) {
    rpc::ResourceTableData resource_table_data;
    resource_table_data.set_resource_capacity(1.0);
    auto data = std::make_shared<rpc::ResourceMap>();
    (*data->mutable_items())["attr1"] = resource_table_data;
    return data;
  }

  std::shared_ptr<rpc::HeartbeatTableData> GenHeartbeatTableData(
      const ClientID &node_id) {
    auto data = std::make_shared<rpc::HeartbeatTableData>();
    data->set_client_id(node_id.Binary());
    return data;
  }

  std::shared_ptr<rpc::HeartbeatBatchTableData> GenHeartbeatBatchTableData(
      const ClientID &node_id) {
    auto data = std::make_shared<rpc::HeartbeatBatchTableData>();
    rpc::HeartbeatTableData heartbeat_table_data;
    heartbeat_table_data.set_client_id(node_id.Binary());
    data->add_batch()->CopyFrom(heartbeat_table_data);
    return data;
  }

  std::shared_ptr<rpc::ErrorTableData> GenErrorInfoTable(const JobID &job_id) {
    auto data = std::make_shared<rpc::ErrorTableData>();
    data->set_job_id(job_id.Binary());
    return data;
  }

  std::shared_ptr<rpc::ProfileTableData> GenProfileTableData() {
    auto data = std::make_shared<rpc::ProfileTableData>();
    data->set_component_type("object_manager");
    return data;
  }

  std::shared_ptr<rpc::WorkerFailureData> GenWorkerFailureData() {
    auto data = std::make_shared<rpc::WorkerFailureData>();
    data->set_timestamp(std::time(nullptr));
    return data;
  }

  template <typename TABLE, typename KEY, typename VALUE>
  void Put(TABLE &table, const JobID &job_id, const KEY &key,
           const std::shared_ptr<VALUE> &value) {
    auto on_done = [this](Status status) { --pending_count_; };
    ++pending_count_;
    RAY_CHECK_OK(table.Put(job_id, key, value, on_done));
    WaitPendingDone();
  }

  template <typename TABLE, typename KEY, typename VALUE>
  int Get(TABLE &table, const JobID &job_id, const KEY &key, std::vector<VALUE> &values) {
    auto on_done = [this, &values](Status status, const boost::optional<VALUE> &result) {
      RAY_CHECK_OK(status);
      --pending_count_;
      values.clear();
      if (result) {
        values.push_back(*result);
      }
    };
    ++pending_count_;
    RAY_CHECK_OK(table.Get(job_id, key, on_done));
    WaitPendingDone();
    return values.size();
  }

  template <typename TABLE, typename VALUE>
  int GetAll(TABLE &table, const JobID &job_id, std::vector<VALUE> &values) {
    auto on_done = [this, &values](Status status, const std::vector<VALUE> &result) {
      RAY_CHECK_OK(status);
      values.assign(result.begin(), result.end());
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.GetAll(job_id, on_done));
    WaitPendingDone();
    return values.size();
  }

  template <typename TABLE, typename KEY>
  void Delete(TABLE &table, const JobID &job_id, const KEY &key) {
    auto on_done = [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.Delete(job_id, key, on_done));
    WaitPendingDone();
  }

  template <typename TABLE>
  void Delete(TABLE &table, const JobID &job_id) {
    auto on_done = [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.Delete(job_id, on_done));
    WaitPendingDone();
  }
};

TEST_F(GcsTableStorageTest, TestJobTableApi) {
  gcs::GcsJobTable table(store_client_);
  JobID job1_id = JobID::FromInt(1);
  JobID job2_id = JobID::FromInt(2);

  // Put.
  Put(table, job1_id, job1_id, GenJobTableData(job1_id));
  Put(table, job2_id, job2_id, GenJobTableData(job1_id));

  // Get.
  std::vector<rpc::JobTableData> values;
  ASSERT_EQ(Get(table, job2_id, job2_id, values), 1);
  ASSERT_EQ(Get(table, job2_id, job2_id, values), 1);
  ASSERT_EQ(GetAll(table, job1_id, values), 2);

  // Delete.
  Delete(table, job1_id, job1_id);
  ASSERT_EQ(Get(table, job1_id, job1_id, values), 0);
  Delete(table, job2_id);
  ASSERT_EQ(GetAll(table, job2_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestActorTableApi) {
  gcs::GcsActorTable table(store_client_);
  JobID job_id = JobID::FromInt(3);
  ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);

  // Put.
  Put(table, job_id, actor_id, GenActorTableData(job_id, actor_id));

  // Get.
  std::vector<rpc::ActorTableData> values;
  ASSERT_EQ(Get(table, job_id, actor_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, actor_id);
  ASSERT_EQ(Get(table, job_id, actor_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestActorCheckpointTableApi) {
  gcs::GcsActorCheckpointTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
  ActorCheckpointID checkpoint_id = ActorCheckpointID::FromRandom();

  // Put.
  Put(table, job_id, checkpoint_id, GenActorCheckpointData(actor_id, checkpoint_id));

  // Get.
  std::vector<rpc::ActorCheckpointData> values;
  ASSERT_EQ(Get(table, job_id, checkpoint_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, checkpoint_id);
  ASSERT_EQ(Get(table, job_id, checkpoint_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestActorCheckpointIdTableApi) {
  gcs::GcsActorCheckpointIdTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
  ActorCheckpointID checkpoint_id = ActorCheckpointID::FromRandom();

  // Put.
  Put(table, job_id, actor_id, GenActorCheckpointIdData(actor_id, checkpoint_id));

  // Get.
  std::vector<rpc::ActorCheckpointIdData> values;
  ASSERT_EQ(Get(table, job_id, actor_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, actor_id);
  ASSERT_EQ(Get(table, job_id, actor_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestTaskTableApi) {
  gcs::GcsTaskTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);

  // Put.
  Put(table, job_id, task_id, GenTaskTableData(job_id, task_id));

  // Get.
  std::vector<rpc::TaskTableData> values;
  ASSERT_EQ(Get(table, job_id, task_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, task_id);
  ASSERT_EQ(Get(table, job_id, task_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestTaskLeaseTableApi) {
  gcs::GcsTaskLeaseTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, job_id, task_id, GenTaskLeaseData(task_id, node_id));

  // Get.
  std::vector<rpc::TaskLeaseData> values;
  ASSERT_EQ(Get(table, job_id, task_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, task_id);
  ASSERT_EQ(Get(table, job_id, task_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestTaskReconstructionTableApi) {
  gcs::GcsTaskReconstructionTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, job_id, task_id, GenTaskReconstructionData(task_id, node_id));

  // Get.
  std::vector<rpc::TaskReconstructionData> values;
  ASSERT_EQ(Get(table, job_id, task_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, task_id);
  ASSERT_EQ(Get(table, job_id, task_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestObjectTableApi) {
  gcs::GcsObjectTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  ClientID node_id = ClientID::FromRandom();
  ObjectID object_id = ObjectID::FromRandom();

  // Put.
  Put(table, job_id, object_id, GenObjectTableDataList(node_id));

  // Get.
  std::vector<rpc::ObjectTableDataList> values;
  ASSERT_EQ(Get(table, job_id, object_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, object_id);
  ASSERT_EQ(Get(table, job_id, object_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestNodeTableApi) {
  gcs::GcsNodeTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, job_id, node_id, GenGcsNodeInfo(node_id));

  // Get.
  std::vector<rpc::GcsNodeInfo> values;
  ASSERT_EQ(Get(table, job_id, node_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, node_id);
  ASSERT_EQ(Get(table, job_id, node_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestNodeResourceTableApi) {
  gcs::GcsNodeResourceTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, job_id, node_id, GenResourceMap(node_id));

  // Get.
  std::vector<rpc::ResourceMap> values;
  ASSERT_EQ(Get(table, job_id, node_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, node_id);
  ASSERT_EQ(Get(table, job_id, node_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestHeartbeatTableApi) {
  gcs::GcsHeartbeatTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, job_id, node_id, GenHeartbeatTableData(node_id));

  // Get.
  std::vector<rpc::HeartbeatTableData> values;
  ASSERT_EQ(Get(table, job_id, node_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, node_id);
  ASSERT_EQ(Get(table, job_id, node_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestHeartbeatBatchTableApi) {
  gcs::GcsHeartbeatBatchTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, job_id, node_id, GenHeartbeatBatchTableData(node_id));

  // Get.
  std::vector<rpc::HeartbeatBatchTableData> values;
  ASSERT_EQ(Get(table, job_id, node_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, node_id);
  ASSERT_EQ(Get(table, job_id, node_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestErrorInfoTableApi) {
  gcs::GcsErrorInfoTable table(store_client_);
  JobID job_id = JobID::FromInt(1);

  // Put.
  Put(table, job_id, job_id, GenErrorInfoTable(job_id));

  // Get.
  std::vector<rpc::ErrorTableData> values;
  ASSERT_EQ(Get(table, job_id, job_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, job_id);
  ASSERT_EQ(Get(table, job_id, job_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestProfileTableApi) {
  gcs::GcsProfileTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  UniqueID unique_id = UniqueID::FromRandom();

  // Put.
  Put(table, job_id, unique_id, GenProfileTableData());

  // Get.
  std::vector<rpc::ProfileTableData> values;
  ASSERT_EQ(Get(table, job_id, unique_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, unique_id);
  ASSERT_EQ(Get(table, job_id, unique_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestWorkerFailureTableApi) {
  gcs::GcsWorkerFailureTable table(store_client_);
  JobID job_id = JobID::FromInt(1);
  WorkerID worker_id = WorkerID::FromRandom();

  // Put.
  Put(table, job_id, worker_id, GenWorkerFailureData());

  // Get.
  std::vector<rpc::WorkerFailureData> values;
  ASSERT_EQ(Get(table, job_id, worker_id, values), 1);
  ASSERT_EQ(GetAll(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id, worker_id);
  ASSERT_EQ(Get(table, job_id, worker_id, values), 0);
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
