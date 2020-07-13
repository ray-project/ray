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

#include "ray/gcs/gcs_client/global_state_accessor.h"

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {

class GlobalStateAccessorTest : public ::testing::Test {
 public:
  GlobalStateAccessorTest() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  virtual ~GlobalStateAccessorTest() { TestSetupUtil::ShutDownRedisServers(); }

 protected:
  void SetUp() override {
    config.grpc_server_port = 0;
    config.grpc_server_name = "MockedGcsServer";
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.is_test = true;
    config.redis_port = TEST_REDIS_SERVER_PORTS.front();

    io_service_.reset(new boost::asio::io_service());
    gcs_server_.reset(new gcs::GcsServer(config, *io_service_));
    gcs_server_->Start();

    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*io_service_));
      io_service_->run();
    }));

    // Wait until server starts listening.
    while (!gcs_server_->IsStarted()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Create GCS client.
    gcs::GcsClientOptions options(config.redis_address, config.redis_port,
                                  config.redis_password, config.is_test);
    gcs_client_.reset(new gcs::ServiceBasedGcsClient(options));
    RAY_CHECK_OK(gcs_client_->Connect(*io_service_));

    // Create global state.
    std::stringstream address;
    address << config.redis_address << ":" << config.redis_port;
    global_state_.reset(new gcs::GlobalStateAccessor(address.str(), "", true));
    RAY_CHECK(global_state_->Connect());
  }

  void TearDown() override {
    gcs_server_->Stop();
    io_service_->stop();
    gcs_server_.reset();
    thread_io_service_->join();

    gcs_client_->Disconnect();
    global_state_->Disconnect();
    global_state_.reset();
    TestSetupUtil::FlushAllRedisServers();
  }

  bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms) {
    auto status = future.wait_for(timeout_ms);
    return status == std::future_status::ready && future.get();
  }

  // GCS server.
  gcs::GcsServerConfig config;
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<boost::asio::io_service> io_service_;

  // GCS client.
  std::unique_ptr<gcs::GcsClient> gcs_client_;

  std::unique_ptr<gcs::GlobalStateAccessor> global_state_;

  // Timeout waiting for GCS server reply, default is 2s.
  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(GlobalStateAccessorTest, TestJobTable) {
  int job_count = 100;
  ASSERT_EQ(global_state_->GetAllJobInfo().size(), 0);
  for (int index = 0; index < job_count; ++index) {
    auto job_id = JobID::FromInt(index);
    auto job_table_data = Mocker::GenJobTableData(job_id);
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncAdd(
        job_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    WaitReady(promise.get_future(), timeout_ms_);
  }
  ASSERT_EQ(global_state_->GetAllJobInfo().size(), job_count);
}

TEST_F(GlobalStateAccessorTest, TestNodeTable) {
  int node_count = 100;
  ASSERT_EQ(global_state_->GetAllNodeInfo().size(), 0);
  // It's useful to check if index value will be marked as address suffix.
  for (int index = 0; index < node_count; ++index) {
    auto node_table_data =
        Mocker::GenNodeInfo(index, std::string("127.0.0.") + std::to_string(index));
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncRegister(
        *node_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    WaitReady(promise.get_future(), timeout_ms_);
  }
  auto node_table = global_state_->GetAllNodeInfo();
  ASSERT_EQ(node_table.size(), node_count);
  for (int index = 0; index < node_count; ++index) {
    rpc::GcsNodeInfo node_data;
    node_data.ParseFromString(node_table[index]);
    ASSERT_EQ(node_data.node_manager_address(),
              std::string("127.0.0.") + std::to_string(node_data.node_manager_port()));
  }
}

TEST_F(GlobalStateAccessorTest, TestNodeResourceTable) {
  int node_count = 100;
  ASSERT_EQ(global_state_->GetAllNodeInfo().size(), 0);
  for (int index = 0; index < node_count; ++index) {
    auto node_table_data =
        Mocker::GenNodeInfo(index, std::string("127.0.0.") + std::to_string(index));
    auto node_id = ClientID::FromBinary(node_table_data->node_id());
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncRegister(
        *node_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    WaitReady(promise.get_future(), timeout_ms_);
    ray::gcs::NodeInfoAccessor::ResourceMap resources;
    rpc::ResourceTableData resource_table_data;
    resource_table_data.set_resource_capacity(static_cast<double>(index + 1) + 0.1);
    resources[std::to_string(index)] =
        std::make_shared<rpc::ResourceTableData>(resource_table_data);
    RAY_IGNORE_EXPR(gcs_client_->Nodes().AsyncUpdateResources(
        node_id, resources, [](Status status) { RAY_CHECK(status.ok()); }));
  }
  auto node_table = global_state_->GetAllNodeInfo();
  ASSERT_EQ(node_table.size(), node_count);
  for (int index = 0; index < node_count; ++index) {
    rpc::GcsNodeInfo node_data;
    node_data.ParseFromString(node_table[index]);
    auto resource_map_str =
        global_state_->GetNodeResourceInfo(ClientID::FromBinary(node_data.node_id()));
    rpc::ResourceMap resource_map;
    resource_map.ParseFromString(resource_map_str);
    ASSERT_EQ(
        static_cast<uint32_t>(
            (*resource_map.mutable_items())[std::to_string(node_data.node_manager_port())]
                .resource_capacity()),
        node_data.node_manager_port() + 1);
  }
}

TEST_F(GlobalStateAccessorTest, TestInternalConfig) {
  rpc::StoredConfig initial_proto;
  initial_proto.ParseFromString(global_state_->GetInternalConfig());
  ASSERT_EQ(initial_proto.config().size(), 0);
  std::promise<bool> promise;
  std::unordered_map<std::string, std::string> begin_config;
  begin_config["key1"] = "value1";
  begin_config["key2"] = "value2";
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSetInternalConfig(begin_config));
  std::string returned;
  rpc::StoredConfig new_proto;
  auto end = std::chrono::system_clock::now() + timeout_ms_;
  while (std::chrono::system_clock::now() < end && new_proto.config().size() == 0) {
    returned = global_state_->GetInternalConfig();
    new_proto.ParseFromString(returned);
  }
  ASSERT_EQ(new_proto.config().size(), begin_config.size());
  for (auto pair : new_proto.config()) {
    ASSERT_EQ(pair.second, begin_config[pair.first]);
  }
}

TEST_F(GlobalStateAccessorTest, TestProfileTable) {
  int profile_count = 100;
  ASSERT_EQ(global_state_->GetAllProfileInfo().size(), 0);
  for (int index = 0; index < profile_count; ++index) {
    auto client_id = ClientID::FromRandom();
    auto profile_table_data = Mocker::GenProfileTableData(client_id);
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Stats().AsyncAddProfileData(
        profile_table_data,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    WaitReady(promise.get_future(), timeout_ms_);
  }
  ASSERT_EQ(global_state_->GetAllProfileInfo().size(), profile_count);
}

TEST_F(GlobalStateAccessorTest, TestObjectTable) {
  int object_count = 1;
  ASSERT_EQ(global_state_->GetAllObjectInfo().size(), 0);
  std::vector<ObjectID> object_ids;
  object_ids.reserve(object_count);
  for (int index = 0; index < object_count; ++index) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.emplace_back(object_id);
    ClientID node_id = ClientID::FromRandom();
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Objects().AsyncAddLocation(
        object_id, node_id,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    WaitReady(promise.get_future(), timeout_ms_);
  }
  ASSERT_EQ(global_state_->GetAllObjectInfo().size(), object_count);

  for (auto &object_id : object_ids) {
    ASSERT_TRUE(global_state_->GetObjectInfo(object_id));
  }
}

TEST_F(GlobalStateAccessorTest, TestActorTable) {
  int actor_count = 1;
  ASSERT_EQ(global_state_->GetAllActorInfo().size(), 0);
  auto job_id = JobID::FromInt(1);
  std::vector<ActorID> actor_ids;
  actor_ids.reserve(actor_count);
  for (int index = 0; index < actor_count; ++index) {
    auto actor_table_data = Mocker::GenActorTableData(job_id);
    actor_ids.emplace_back(ActorID::FromBinary(actor_table_data->actor_id()));
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Actors().AsyncRegister(
        actor_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    WaitReady(promise.get_future(), timeout_ms_);
  }
  ASSERT_EQ(global_state_->GetAllActorInfo().size(), actor_count);

  for (auto &actor_id : actor_ids) {
    ASSERT_TRUE(global_state_->GetActorInfo(actor_id));
  }
}

TEST_F(GlobalStateAccessorTest, TestWorkerTable) {
  ASSERT_EQ(global_state_->GetAllWorkerInfo().size(), 0);
  // Add worker info
  auto worker_table_data = Mocker::GenWorkerTableData();
  worker_table_data->mutable_worker_address()->set_worker_id(
      WorkerID::FromRandom().Binary());
  ASSERT_TRUE(global_state_->AddWorkerInfo(worker_table_data->SerializeAsString()));

  // Get worker info
  auto worker_id = WorkerID::FromBinary(worker_table_data->worker_address().worker_id());
  ASSERT_TRUE(global_state_->GetWorkerInfo(worker_id));

  // Add another worker info
  auto another_worker_data = Mocker::GenWorkerTableData();
  another_worker_data->mutable_worker_address()->set_worker_id(
      WorkerID::FromRandom().Binary());
  ASSERT_TRUE(global_state_->AddWorkerInfo(another_worker_data->SerializeAsString()));
  ASSERT_EQ(global_state_->GetAllWorkerInfo().size(), 2);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::TEST_REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
