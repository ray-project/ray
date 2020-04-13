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
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

class GcsTableStorageTest : public gcs::StoreClientTestBase {
 public:
  GcsTableStorageTest() {}

  virtual ~GcsTableStorageTest() {}

  void InitStoreClient() override {
    gcs::RedisClientOptions options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    redis_client_ = std::make_shared<gcs::RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);
  }

  void DisconnectStoreClient() override { redis_client_->Disconnect(); }

 protected:
  rpc::JobTableData GenJobTableData(JobID job_id) {
    rpc::JobTableData data;
    data.set_job_id(job_id.Binary());
    data.set_is_dead(false);
    data.set_timestamp(std::time(nullptr));
    data.set_node_manager_address("127.0.0.1");
    data.set_driver_pid(5667L);
    return data;
  }

  rpc::ActorTableData GenActorTableData(JobID job_id, ActorID actor_id) {
    rpc::ActorTableData data;
    data.set_actor_id(actor_id.Binary());
    data.set_job_id(job_id.Binary());
    data.set_state(rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
    data.set_max_reconstructions(1);
    data.set_remaining_reconstructions(1);
    return data;
  }

  rpc::ActorCheckpointData GenActorCheckpointData(ActorID actor_id,
                                                  ActorCheckpointID checkpoint_id) {
    rpc::ActorCheckpointData data;
    data.set_actor_id(actor_id.Binary());
    data.set_checkpoint_id(checkpoint_id.Binary());
    data.set_execution_dependency(checkpoint_id.Binary());
    return data;
  }

  rpc::ActorCheckpointIdData GenActorCheckpointIdData(ActorID actor_id,
                                                      ActorCheckpointID checkpoint_id) {
    rpc::ActorCheckpointIdData data;
    data.set_actor_id(actor_id.Binary());
    data.add_checkpoint_ids(checkpoint_id.Binary());
    return data;
  }

  rpc::TaskTableData GenTaskTableData(const JobID &job_id, const TaskID &task_id) {
    rpc::TaskTableData data;
    rpc::Task task;
    rpc::TaskSpec task_spec;
    task_spec.set_job_id(job_id.Binary());
    task_spec.set_task_id(task_id.Binary());
    task.mutable_task_spec()->CopyFrom(task_spec);
    data.mutable_task()->CopyFrom(task);
    return data;
  }

  rpc::TaskLeaseData GenTaskLeaseData(const TaskID &task_id, const ClientID &node_id) {
    rpc::TaskLeaseData data;
    data.set_task_id(task_id.Binary());
    data.set_node_manager_id(node_id.Binary());
    return data;
  }

  rpc::TaskReconstructionData GenTaskReconstructionData(const TaskID &task_id,
                                                        const ClientID &node_id) {
    rpc::TaskReconstructionData data;
    data.set_task_id(task_id.Binary());
    data.set_num_reconstructions(1);
    data.set_node_manager_id(node_id.Binary());
    return data;
  }

  rpc::ObjectTableDataList GenObjectTableDataList(const ClientID &node_id) {
    rpc::ObjectTableDataList data;
    rpc::ObjectTableData object_table_data;
    object_table_data.set_manager(node_id.Binary());
    object_table_data.set_object_size(1);
    data.add_items()->CopyFrom(object_table_data);
    return data;
  }

  rpc::GcsNodeInfo GenGcsNodeInfo(const ClientID &node_id) {
    rpc::GcsNodeInfo data;
    data.set_node_id(node_id.Binary());
    data.set_state(rpc::GcsNodeInfo_GcsNodeState_ALIVE);
    return data;
  }

  rpc::ResourceMap GenResourceMap(const ClientID &node_id) {
    rpc::ResourceTableData resource_table_data;
    resource_table_data.set_resource_capacity(1.0);
    rpc::ResourceMap data;
    (*data.mutable_items())["attr1"] = resource_table_data;
    return data;
  }

  rpc::HeartbeatTableData GenHeartbeatTableData(const ClientID &node_id) {
    rpc::HeartbeatTableData data;
    data.set_client_id(node_id.Binary());
    return data;
  }

  rpc::HeartbeatBatchTableData GenHeartbeatBatchTableData(const ClientID &node_id) {
    rpc::HeartbeatBatchTableData data;
    rpc::HeartbeatTableData heartbeat_table_data;
    heartbeat_table_data.set_client_id(node_id.Binary());
    data.add_batch()->CopyFrom(heartbeat_table_data);
    return data;
  }

  rpc::ErrorTableData GenErrorInfoTable(const JobID &job_id) {
    rpc::ErrorTableData data;
    data.set_job_id(job_id.Binary());
    return data;
  }

  rpc::ProfileTableData GenProfileTableData() {
    rpc::ProfileTableData data;
    data.set_component_type("object_manager");
    return data;
  }

  rpc::WorkerFailureData GenWorkerFailureData() {
    rpc::WorkerFailureData data;
    data.set_timestamp(std::time(nullptr));
    return data;
  }

  template <typename TABLE, typename KEY, typename VALUE>
  void Put(TABLE &table, const KEY &key, const VALUE &value) {
    auto on_done = [this](Status status) { --pending_count_; };
    ++pending_count_;
    RAY_CHECK_OK(table.Put(key, value, on_done));
    WaitPendingDone();
  }

  template <typename TABLE, typename KEY, typename VALUE>
  int Get(TABLE &table, const KEY &key, std::vector<VALUE> &values) {
    auto on_done = [this, &values](Status status, const boost::optional<VALUE> &result) {
      RAY_CHECK_OK(status);
      --pending_count_;
      values.clear();
      if (result) {
        values.push_back(*result);
      }
    };
    ++pending_count_;
    RAY_CHECK_OK(table.Get(key, on_done));
    WaitPendingDone();
    return values.size();
  }

  template <typename TABLE, typename KEY, typename VALUE>
  int GetAll(TABLE &table, std::vector<std::pair<KEY, VALUE>> &values) {
    auto on_done = [this, &values](Status status, bool has_more,
                                   const std::vector<std::pair<KEY, VALUE>> &result) {
      RAY_CHECK_OK(status);
      --pending_count_;
      values.clear();
      values.push_back(result);
    };
    ++pending_count_;
    RAY_CHECK_OK(table.GetAll(on_done));
    WaitPendingDone();
    return values.size();
  }

  template <typename TABLE, typename KEY>
  void Delete(TABLE &table, const KEY &key) {
    auto on_done = [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.Delete(key, on_done));
    WaitPendingDone();
  }

  template <typename TABLE, typename KEY, typename VALUE>
  int GetByJobId(TABLE &table, const JobID &job_id,
                 std::vector<std::pair<KEY, VALUE>> &values) {
    auto on_done = [this, &values](Status status, bool has_more,
                                   const std::vector<std::pair<KEY, VALUE>> &result) {
      RAY_CHECK_OK(status);
      values.assign(result.begin(), result.end());
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.GetByJobId(job_id, on_done));
    WaitPendingDone();
    return values.size();
  }

  template <typename TABLE>
  void DeleteByJobId(TABLE &table, const JobID &job_id) {
    auto on_done = [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    };
    ++pending_count_;
    RAY_CHECK_OK(table.DeleteByJobId(job_id, on_done));
    WaitPendingDone();
  }

  std::shared_ptr<gcs::RedisClient> redis_client_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
};

TEST_F(GcsTableStorageTest, TestJobTableApi) {
  auto table = gcs_table_storage_->JobTable();
  JobID job1_id = JobID::FromInt(1);
  JobID job2_id = JobID::FromInt(2);

  // Put.
  Put(table, job1_id, GenJobTableData(job1_id));
  Put(table, job2_id, GenJobTableData(job2_id));

  // Get.
  std::vector<rpc::JobTableData> values;
  ASSERT_EQ(Get(table, job2_id, values), 1);
  ASSERT_EQ(Get(table, job2_id, values), 1);

  // Delete.
  Delete(table, job1_id);
  ASSERT_EQ(Get(table, job1_id, values), 0);
  ASSERT_EQ(Get(table, job2_id, values), 1);
}

TEST_F(GcsTableStorageTest, TestActorTableApi) {
  auto table = gcs_table_storage_->ActorTable();
  JobID job_id = JobID::FromInt(3);
  ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);

  // Put.
  Put(table, actor_id, GenActorTableData(job_id, actor_id));

  // Get.
  std::vector<rpc::ActorTableData> values;
  ASSERT_EQ(Get(table, actor_id, values), 1);

  // Delete.
  Delete(table, actor_id);
  ASSERT_EQ(Get(table, actor_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestActorCheckpointTableApi) {
  auto table = gcs_table_storage_->ActorCheckpointTable();
  JobID job_id = JobID::FromInt(1);
  ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
  ActorCheckpointID checkpoint_id = ActorCheckpointID::FromRandom();

  // Put.
  Put(table, checkpoint_id, GenActorCheckpointData(actor_id, checkpoint_id));

  // Get.
  std::vector<rpc::ActorCheckpointData> values;
  ASSERT_EQ(Get(table, checkpoint_id, values), 1);

  // Delete.
  Delete(table, checkpoint_id);
  ASSERT_EQ(Get(table, checkpoint_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestActorCheckpointIdTableApi) {
  auto table = gcs_table_storage_->ActorCheckpointIdTable();
  JobID job_id = JobID::FromInt(1);
  ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
  ActorCheckpointID checkpoint_id = ActorCheckpointID::FromRandom();

  // Put.
  Put(table, actor_id, GenActorCheckpointIdData(actor_id, checkpoint_id));

  // Get.
  std::vector<rpc::ActorCheckpointIdData> values;
  ASSERT_EQ(Get(table, actor_id, values), 1);

  // Delete.
  Delete(table, actor_id);
  ASSERT_EQ(Get(table, actor_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestTaskTableApi) {
  auto table = gcs_table_storage_->TaskTable();
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);

  // Put.
  Put(table, task_id, GenTaskTableData(job_id, task_id));

  // Get.
  std::vector<rpc::TaskTableData> values;
  ASSERT_EQ(Get(table, task_id, values), 1);

  // Delete.
  Delete(table, task_id);
  ASSERT_EQ(Get(table, task_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestTaskLeaseTableApi) {
  auto table = gcs_table_storage_->TaskLeaseTable();
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, task_id, GenTaskLeaseData(task_id, node_id));

  // Get.
  std::vector<rpc::TaskLeaseData> values;
  ASSERT_EQ(Get(table, task_id, values), 1);

  // Delete.
  Delete(table, task_id);
  ASSERT_EQ(Get(table, task_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestTaskReconstructionTableApi) {
  auto table = gcs_table_storage_->TaskReconstructionTable();
  JobID job_id = JobID::FromInt(1);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, task_id, GenTaskReconstructionData(task_id, node_id));

  // Get.
  std::vector<rpc::TaskReconstructionData> values;
  ASSERT_EQ(Get(table, task_id, values), 1);

  // Delete.
  Delete(table, task_id);
  ASSERT_EQ(Get(table, task_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestObjectTableApi) {
  auto table = gcs_table_storage_->ObjectTable();
  JobID job_id = JobID::FromInt(1);
  ActorID actor_id = ActorID::NilFromJob(job_id);
  ClientID node_id = ClientID::FromRandom();
  ObjectID object_id = ObjectID::ForActorHandle(actor_id);

  // Put.
  Put(table, object_id, GenObjectTableDataList(node_id));

  // Get.
  std::vector<rpc::ObjectTableDataList> values;
  ASSERT_EQ(Get(table, object_id, values), 1);

  // Delete.
  Delete(table, object_id);
  ASSERT_EQ(Get(table, object_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestNodeTableApi) {
  auto table = gcs_table_storage_->NodeTable();
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, node_id, GenGcsNodeInfo(node_id));

  // Get.
  std::vector<rpc::GcsNodeInfo> values;
  ASSERT_EQ(Get(table, node_id, values), 1);

  // Delete.
  Delete(table, node_id);
  ASSERT_EQ(Get(table, node_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestNodeResourceTableApi) {
  auto table = gcs_table_storage_->NodeResourceTable();
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, node_id, GenResourceMap(node_id));

  // Get.
  std::vector<rpc::ResourceMap> values;
  ASSERT_EQ(Get(table, node_id, values), 1);

  // Delete.
  Delete(table, node_id);
  ASSERT_EQ(Get(table, node_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestHeartbeatTableApi) {
  auto table = gcs_table_storage_->HeartbeatTable();
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, node_id, GenHeartbeatTableData(node_id));

  // Get.
  std::vector<rpc::HeartbeatTableData> values;
  ASSERT_EQ(Get(table, node_id, values), 1);

  // Delete.
  Delete(table, node_id);
  ASSERT_EQ(Get(table, node_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestHeartbeatBatchTableApi) {
  auto table = gcs_table_storage_->HeartbeatBatchTable();
  ClientID node_id = ClientID::FromRandom();

  // Put.
  Put(table, node_id, GenHeartbeatBatchTableData(node_id));

  // Get.
  std::vector<rpc::HeartbeatBatchTableData> values;
  ASSERT_EQ(Get(table, node_id, values), 1);

  // Delete.
  Delete(table, node_id);
  ASSERT_EQ(Get(table, node_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestErrorInfoTableApi) {
  auto table = gcs_table_storage_->ErrorInfoTable();
  JobID job_id = JobID::FromInt(1);

  // Put.
  Put(table, job_id, GenErrorInfoTable(job_id));

  // Get.
  std::vector<rpc::ErrorTableData> values;
  ASSERT_EQ(Get(table, job_id, values), 1);

  // Delete.
  Delete(table, job_id);
  ASSERT_EQ(Get(table, job_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestProfileTableApi) {
  auto table = gcs_table_storage_->ProfileTable();
  UniqueID unique_id = UniqueID::FromRandom();

  // Put.
  Put(table, unique_id, GenProfileTableData());

  // Get.
  std::vector<rpc::ProfileTableData> values;
  ASSERT_EQ(Get(table, unique_id, values), 1);

  // Delete.
  Delete(table, unique_id);
  ASSERT_EQ(Get(table, unique_id, values), 0);
}

TEST_F(GcsTableStorageTest, TestWorkerFailureTableApi) {
  auto table = gcs_table_storage_->WorkerFailureTable();
  WorkerID worker_id = WorkerID::FromRandom();

  // Put.
  Put(table, worker_id, GenWorkerFailureData());

  // Get.
  std::vector<rpc::WorkerFailureData> values;
  ASSERT_EQ(Get(table, worker_id, values), 1);

  // Delete.
  Delete(table, worker_id);
  ASSERT_EQ(Get(table, worker_id, values), 0);
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
