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
    thread_io_service_->join();
    gcs_server_.reset();

    gcs_client_->Disconnect();
    global_state_->Disconnect();
    global_state_.reset();
    TestSetupUtil::FlushAllRedisServers();
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
    auto node_id = NodeID::FromBinary(node_table_data->node_id());
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncRegister(
        *node_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
    WaitReady(promise.get_future(), timeout_ms_);
    ray::gcs::NodeResourceInfoAccessor::ResourceMap resources;
    rpc::ResourceTableData resource_table_data;
    resource_table_data.set_resource_capacity(static_cast<double>(index + 1) + 0.1);
    resources[std::to_string(index)] =
        std::make_shared<rpc::ResourceTableData>(resource_table_data);
    RAY_IGNORE_EXPR(gcs_client_->NodeResources().AsyncUpdateResources(
        node_id, resources, [](Status status) { RAY_CHECK(status.ok()); }));
  }
  auto node_table = global_state_->GetAllNodeInfo();
  ASSERT_EQ(node_table.size(), node_count);
  for (int index = 0; index < node_count; ++index) {
    rpc::GcsNodeInfo node_data;
    node_data.ParseFromString(node_table[index]);
    auto resource_map_str =
        global_state_->GetNodeResourceInfo(NodeID::FromBinary(node_data.node_id()));
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

TEST_F(GlobalStateAccessorTest, TestGetAllResourceUsage) {
  std::unique_ptr<std::string> resources = global_state_->GetAllResourceUsage();
  rpc::ResourceUsageBatchData resource_usage_batch_data;
  resource_usage_batch_data.ParseFromString(*resources.get());
  ASSERT_EQ(resource_usage_batch_data.batch_size(), 0);

  auto node_table_data = Mocker::GenNodeInfo();
  std::promise<bool> promise;
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncRegister(
      *node_table_data, [&promise](Status status) { promise.set_value(status.ok()); }));
  WaitReady(promise.get_future(), timeout_ms_);
  auto node_table = global_state_->GetAllNodeInfo();
  ASSERT_EQ(node_table.size(), 1);

  // Report resource usage first time.
  std::promise<bool> promise1;
  auto resources1 = std::make_shared<rpc::ResourcesData>();
  resources1->set_node_id(node_table_data->node_id());
  RAY_CHECK_OK(gcs_client_->NodeResources().AsyncReportResourceUsage(
      resources1, [&promise1](Status status) { promise1.set_value(status.ok()); }));
  WaitReady(promise1.get_future(), timeout_ms_);

  resources = global_state_->GetAllResourceUsage();
  resource_usage_batch_data.ParseFromString(*resources.get());
  ASSERT_EQ(resource_usage_batch_data.batch_size(), 1);

  // Report changed resource usage.
  std::promise<bool> promise2;
  auto heartbeat2 = std::make_shared<rpc::ResourcesData>();
  heartbeat2->set_node_id(node_table_data->node_id());
  (*heartbeat2->mutable_resources_total())["CPU"] = 1;
  (*heartbeat2->mutable_resources_total())["GPU"] = 10;
  heartbeat2->set_resources_available_changed(true);
  (*heartbeat2->mutable_resources_available())["GPU"] = 5;
  RAY_CHECK_OK(gcs_client_->NodeResources().AsyncReportResourceUsage(
      heartbeat2, [&promise2](Status status) { promise2.set_value(status.ok()); }));
  WaitReady(promise2.get_future(), timeout_ms_);

  resources = global_state_->GetAllResourceUsage();
  resource_usage_batch_data.ParseFromString(*resources.get());
  ASSERT_EQ(resource_usage_batch_data.batch_size(), 1);
  auto resources_data = resource_usage_batch_data.mutable_batch()->at(0);
  ASSERT_EQ(resources_data.resources_total_size(), 2);
  ASSERT_EQ((*resources_data.mutable_resources_total())["CPU"], 1.0);
  ASSERT_EQ((*resources_data.mutable_resources_total())["GPU"], 10.0);
  ASSERT_EQ(resources_data.resources_available_size(), 1);
  ASSERT_EQ((*resources_data.mutable_resources_available())["GPU"], 5.0);

  // Report unchanged resource usage. (Only works with light resource usage report
  // enabled)
  std::promise<bool> promise3;
  auto heartbeat3 = std::make_shared<rpc::ResourcesData>();
  heartbeat3->set_node_id(node_table_data->node_id());
  (*heartbeat3->mutable_resources_available())["CPU"] = 1;
  (*heartbeat3->mutable_resources_available())["GPU"] = 6;
  RAY_CHECK_OK(gcs_client_->NodeResources().AsyncReportResourceUsage(
      heartbeat3, [&promise3](Status status) { promise3.set_value(status.ok()); }));
  WaitReady(promise3.get_future(), timeout_ms_);

  resources = global_state_->GetAllResourceUsage();
  resource_usage_batch_data.ParseFromString(*resources.get());
  ASSERT_EQ(resource_usage_batch_data.batch_size(), 1);
  resources_data = resource_usage_batch_data.mutable_batch()->at(0);
  ASSERT_EQ(resources_data.resources_total_size(), 2);
  ASSERT_EQ((*resources_data.mutable_resources_total())["CPU"], 1.0);
  ASSERT_EQ((*resources_data.mutable_resources_total())["GPU"], 10.0);
  ASSERT_EQ(resources_data.resources_available_size(), 1);
  ASSERT_EQ((*resources_data.mutable_resources_available())["GPU"], 5.0);
}

TEST_F(GlobalStateAccessorTest, TestProfileTable) {
  int profile_count = RayConfig::instance().maximum_profile_table_rows_count() + 1;
  ASSERT_EQ(global_state_->GetAllProfileInfo().size(), 0);
  for (int index = 0; index < profile_count; ++index) {
    auto node_id = NodeID::FromRandom();
    auto profile_table_data = Mocker::GenProfileTableData(node_id);
    std::promise<bool> promise;
    RAY_CHECK_OK(gcs_client_->Stats().AsyncAddProfileData(
        profile_table_data,
        [&promise](Status status) { promise.set_value(status.ok()); }));
    WaitReady(promise.get_future(), timeout_ms_);
  }
  ASSERT_EQ(global_state_->GetAllProfileInfo().size(),
            RayConfig::instance().maximum_profile_table_rows_count());
}

TEST_F(GlobalStateAccessorTest, TestObjectTable) {
  int object_count = 1;
  ASSERT_EQ(global_state_->GetAllObjectInfo().size(), 0);
  std::vector<ObjectID> object_ids;
  object_ids.reserve(object_count);
  for (int index = 0; index < object_count; ++index) {
    ObjectID object_id = ObjectID::FromRandom();
    object_ids.emplace_back(object_id);
    NodeID node_id = NodeID::FromRandom();
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

// TODO(sang): Add tests after adding asyncAdd
TEST_F(GlobalStateAccessorTest, TestPlacementGroupTable) {
  ASSERT_EQ(global_state_->GetAllPlacementGroupInfo().size(), 0);
}

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
