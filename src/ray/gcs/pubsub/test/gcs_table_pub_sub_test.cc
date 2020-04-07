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

#include "ray/gcs/pubsub/gcs_table_pub_sub.h"
#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

class GcsTablePubSubTest : public RedisServiceManagerForTest {
 protected:
  virtual void SetUp() override {
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    gcs::RedisClientOptions redis_client_options("127.0.0.1", REDIS_SERVER_PORT, "",
                                                 true);
    client_ = std::make_shared<gcs::RedisClient>(redis_client_options);
    Status status = client_->Connect(io_service_);
    RAY_CHECK_OK(status);
  }

  virtual void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  template <typename TABLE_PUB_SUB, typename ID, typename Data>
  void Subscribe(TABLE_PUB_SUB &table_pub_sub, const ID &id, std::vector<Data> &result) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    auto subscribe = [&result](const ID &id, const Data &data) {
      result.push_back(data);
    };
    RAY_CHECK_OK(table_pub_sub.Subscribe(id, subscribe, done));
    WaitReady(promise.get_future(), timeout_ms_);
  }

  template <typename TABLE_PUB_SUB, typename ID, typename Data>
  void SubscribeAll(TABLE_PUB_SUB &table_pub_sub,
                    std::vector<std::pair<ID, Data>> &result) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    auto subscribe = [&result](const ID &id, const Data &data) {
      result.push_back(std::make_pair(id, data));
    };
    RAY_CHECK_OK(table_pub_sub.SubscribeAll(subscribe, done));
    WaitReady(promise.get_future(), timeout_ms_);
  }

  template <typename TABLE_PUB_SUB, typename ID>
  bool Unsubscribe(TABLE_PUB_SUB &table_pub_sub, const ID &id) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    RAY_CHECK_OK(table_pub_sub.Unsubscribe(id, done));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  template <typename TABLE_PUB_SUB, typename ID, typename Data>
  bool Publish(TABLE_PUB_SUB &table_pub_sub, const ID &id, const Data &data) {
    std::promise<bool> promise;
    auto done = [&promise](Status status) { promise.set_value(status.ok()); };
    RAY_CHECK_OK(table_pub_sub.Publish(id, data, done));
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms) {
    auto status = future.wait_for(timeout_ms);
    return status == std::future_status::ready && future.get();
  }

  template <typename Data>
  void WaitPendingDone(const std::vector<Data> &data, int expected_count) {
    auto condition = [&data, expected_count]() { return data.size() == expected_count; };
    EXPECT_TRUE(WaitForCondition(condition, timeout_ms_.count()));
  }

  std::shared_ptr<gcs::RedisClient> client_;
  JobID job_id_ = JobID::FromInt(1);
  TaskID task_id_ = TaskID::ForDriverTask(job_id_);
  ClientID node_id_ = ClientID::FromRandom();
  const std::chrono::milliseconds timeout_ms_{10000};

 private:
  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
};

TEST_F(GcsTablePubSubTest, TestJobTablePubSubApi) {
  gcs::GcsJobTablePubSub table_pub_sub(client_);
  rpc::JobTableData job_table_data;
  job_table_data.set_job_id(job_id_.Binary());

  std::vector<std::pair<JobID, rpc::JobTableData>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::JobTableData> result;
  Subscribe(table_pub_sub, job_id_, result);
  ASSERT_TRUE(Publish(table_pub_sub, job_id_, job_table_data));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, job_id_));
  ASSERT_TRUE(Publish(table_pub_sub, job_id_, job_table_data));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

TEST_F(GcsTablePubSubTest, TestActorTablePubSubApi) {
  gcs::GcsActorTablePubSub table_pub_sub(client_);
  ActorID actor_id = ActorID::Of(job_id_, RandomTaskId(), 0);
  rpc::ActorTableData actor_table_data;
  actor_table_data.set_job_id(job_id_.Binary());
  actor_table_data.set_actor_id(actor_id.Binary());

  std::vector<std::pair<ActorID, rpc::ActorTableData>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::ActorTableData> result;
  Subscribe(table_pub_sub, actor_id, result);
  ASSERT_TRUE(Publish(table_pub_sub, actor_id, actor_table_data));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, actor_id));
  ASSERT_TRUE(Publish(table_pub_sub, actor_id, actor_table_data));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

TEST_F(GcsTablePubSubTest, TestTaskTablePubSubApi) {
  gcs::GcsTaskTablePubSub table_pub_sub(client_);
  rpc::TaskTableData task_table_data;
  rpc::Task task;
  rpc::TaskSpec task_spec;
  task_spec.set_job_id(job_id_.Binary());
  task_spec.set_task_id(task_id_.Binary());
  task.mutable_task_spec()->CopyFrom(task_spec);
  task_table_data.mutable_task()->CopyFrom(task);

  std::vector<std::pair<TaskID, rpc::TaskTableData>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::TaskTableData> result;
  Subscribe(table_pub_sub, task_id_, result);
  ASSERT_TRUE(Publish(table_pub_sub, task_id_, task_table_data));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, task_id_));
  ASSERT_TRUE(Publish(table_pub_sub, task_id_, task_table_data));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

TEST_F(GcsTablePubSubTest, TestTaskLeaseTablePubSubApi) {
  gcs::GcsTaskLeaseTablePubSub table_pub_sub(client_);
  rpc::TaskLeaseData task_lease_data;
  task_lease_data.set_task_id(task_id_.Binary());

  std::vector<std::pair<TaskID, rpc::TaskLeaseData>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::TaskLeaseData> result;
  Subscribe(table_pub_sub, task_id_, result);
  ASSERT_TRUE(Publish(table_pub_sub, task_id_, task_lease_data));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, task_id_));
  ASSERT_TRUE(Publish(table_pub_sub, task_id_, task_lease_data));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

TEST_F(GcsTablePubSubTest, TestObjectTablePubSubApi) {
  gcs::GcsObjectTablePubSub table_pub_sub(client_);
  ObjectID object_id = ObjectID::FromRandom();
  rpc::ObjectTableData object_table_data;
  object_table_data.set_manager(node_id_.Binary());
  object_table_data.set_object_size(1);
  rpc::ObjectChange object_change;
  object_change.set_change_mode(rpc::GcsChangeMode::APPEND_OR_ADD);
  object_change.mutable_data()->CopyFrom(object_table_data);

  std::vector<std::pair<ObjectID, rpc::ObjectChange>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::ObjectChange> result;
  Subscribe(table_pub_sub, object_id, result);
  ASSERT_TRUE(Publish(table_pub_sub, object_id, object_change));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, object_id));
  ASSERT_TRUE(Publish(table_pub_sub, object_id, object_change));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

TEST_F(GcsTablePubSubTest, TestNodeTablePubSubApi) {
  gcs::GcsNodeTablePubSub table_pub_sub(client_);
  rpc::GcsNodeInfo gcs_node_info;
  gcs_node_info.set_node_id(node_id_.Binary());

  std::vector<std::pair<ClientID, rpc::GcsNodeInfo>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::GcsNodeInfo> result;
  Subscribe(table_pub_sub, node_id_, result);
  ASSERT_TRUE(Publish(table_pub_sub, node_id_, gcs_node_info));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, node_id_));
  ASSERT_TRUE(Publish(table_pub_sub, node_id_, gcs_node_info));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

TEST_F(GcsTablePubSubTest, TestNodeResourceTablePubSubApi) {
  gcs::GcsNodeResourceTablePubSub table_pub_sub(client_);
  rpc::ResourceTableData resource_table_data;
  resource_table_data.set_resource_capacity(1.0);
  rpc::ResourceMap resource_map;
  (*resource_map.mutable_items())["node1"] = resource_table_data;
  rpc::ResourceChange resource_change;
  resource_change.set_change_mode(rpc::GcsChangeMode::APPEND_OR_ADD);
  resource_change.mutable_data()->CopyFrom(resource_map);

  std::vector<std::pair<ClientID, rpc::ResourceChange>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::ResourceChange> result;
  Subscribe(table_pub_sub, node_id_, result);
  ASSERT_TRUE(Publish(table_pub_sub, node_id_, resource_change));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, node_id_));
  ASSERT_TRUE(Publish(table_pub_sub, node_id_, resource_change));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

TEST_F(GcsTablePubSubTest, TestHeartbeatTablePubSubApi) {
  gcs::GcsHeartbeatTablePubSub table_pub_sub(client_);
  rpc::HeartbeatTableData heartbeat_table_data;
  heartbeat_table_data.set_client_id(node_id_.Binary());

  std::vector<std::pair<ClientID, rpc::HeartbeatTableData>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::HeartbeatTableData> result;
  Subscribe(table_pub_sub, node_id_, result);
  ASSERT_TRUE(Publish(table_pub_sub, node_id_, heartbeat_table_data));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, node_id_));
  ASSERT_TRUE(Publish(table_pub_sub, node_id_, heartbeat_table_data));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

TEST_F(GcsTablePubSubTest, TestHeartbeatBatchTablePubSubApi) {
  gcs::GcsHeartbeatBatchTablePubSub table_pub_sub(client_);
  rpc::HeartbeatBatchTableData heartbeat_batch_table_data;
  heartbeat_batch_table_data.add_batch()->set_client_id(node_id_.Binary());

  std::vector<std::pair<ClientID, rpc::HeartbeatBatchTableData>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::HeartbeatBatchTableData> result;
  Subscribe(table_pub_sub, node_id_, result);
  ASSERT_TRUE(Publish(table_pub_sub, node_id_, heartbeat_batch_table_data));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, node_id_));
  ASSERT_TRUE(Publish(table_pub_sub, node_id_, heartbeat_batch_table_data));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

TEST_F(GcsTablePubSubTest, TestWorkerFailureTablePubSubApi) {
  gcs::GcsWorkerFailureTablePubSub table_pub_sub(client_);
  WorkerID worker_id = WorkerID::FromRandom();
  rpc::WorkerFailureData worker_failure_data;
  worker_failure_data.set_timestamp(std::time(nullptr));

  std::vector<std::pair<WorkerID, rpc::WorkerFailureData>> all_result;
  SubscribeAll(table_pub_sub, all_result);
  std::vector<rpc::WorkerFailureData> result;
  Subscribe(table_pub_sub, worker_id, result);
  ASSERT_TRUE(Publish(table_pub_sub, worker_id, worker_failure_data));
  WaitPendingDone(result, 1);
  WaitPendingDone(all_result, 1);
  ASSERT_TRUE(Unsubscribe(table_pub_sub, worker_id));
  ASSERT_TRUE(Publish(table_pub_sub, worker_id, worker_failure_data));
  usleep(100 * 1000);
  EXPECT_EQ(result.size(), 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
