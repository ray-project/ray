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

#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "ray/common/status.h"
#include "ray/common/test_util.h"
#include "ray/raylet/raylet.h"
#include "ray/util/filesystem.h"

namespace ray {

namespace raylet {

std::string test_executable;

// TODO(hme): Get this working once the dust settles.
class TestObjectManagerBase : public ::testing::Test {
 public:
  TestObjectManagerBase() { RAY_LOG(INFO) << "TestObjectManagerBase: started."; }

  NodeManagerConfig GetNodeManagerConfig(std::string raylet_socket_name,
                                         std::string store_socket_name) {
    // Configuration for the node manager.
    ray::raylet::NodeManagerConfig node_manager_config;
    std::unordered_map<std::string, double> static_resource_conf;
    static_resource_conf = {{"CPU", 1}, {"GPU", 1}};
    node_manager_config.resource_config =
        ray::raylet::ResourceSet(std::move(static_resource_conf));
    node_manager_config.num_initial_workers = 0;
    // Use a default worker that can execute empty tasks with dependencies.
    std::vector<std::string> py_worker_command;
    py_worker_command.push_back("python");
    py_worker_command.push_back("../python/ray/workers/default_worker.py");
    py_worker_command.push_back(raylet_socket_name.c_str());
    py_worker_command.push_back(store_socket_name.c_str());
    node_manager_config.worker_commands[Language::PYTHON] = py_worker_command;
    return node_manager_config;
  };

  void SetUp() {
    // start store
    std::string store_sock_1 = TestSetupUtil::StartObjectStore("1");
    std::string store_sock_2 = TestSetupUtil::StartObjectStore("2");

    // start first server
    gcs::GcsClientOptions client_options("127.0.0.1", 6379, /*password*/ "", true);
    gcs_client_1 =
        std::shared_ptr<gcs::RedisGcsClient>(new gcs::RedisGcsClient(client_options));
    ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = store_sock_1;
    om_config_1.push_timeout_ms = 10000;
    server1.reset(new ray::raylet::Raylet(
        main_service, "raylet_1", "0.0.0.0", "127.0.0.1", 6379, "",
        GetNodeManagerConfig("raylet_1", store_sock_1), om_config_1, gcs_client_1));

    // start second server
    gcs_client_2 =
        std::shared_ptr<gcs::RedisGcsClient>(new gcs::RedisGcsClient(client_options));
    ObjectManagerConfig om_config_2;
    om_config_2.store_socket_name = store_sock_2;
    om_config_2.push_timeout_ms = 10000;
    server2.reset(new ray::raylet::Raylet(
        main_service, "raylet_2", "0.0.0.0", "127.0.0.1", 6379, "",
        GetNodeManagerConfig("raylet_2", store_sock_2), om_config_2, gcs_client_2));

    // connect to stores.
    RAY_CHECK_OK(client1.Connect(store_sock_1));
    RAY_CHECK_OK(client2.Connect(store_sock_2));
  }

  void TearDown() {
    Status client1_status = client1.Disconnect();
    Status client2_status = client2.Disconnect();
    ASSERT_TRUE(client1_status.ok() && client2_status.ok());

    this->server1.reset();
    this->server2.reset();

    ASSERT_EQ(TestSetupUtil::KillAllExecutable(plasma_store_server + GetExeSuffix()), 0);

    std::string cmd_str = test_executable.substr(0, test_executable.find_last_of("/"));
    ASSERT_EQ(unlink((cmd_str + "/raylet_1").c_str()), 0);
    ASSERT_EQ(unlink((cmd_str + "/raylet_2").c_str()), 0);
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size) {
    ObjectID object_id = ObjectID::FromRandom();
    RAY_LOG(DEBUG) << "ObjectID Created: " << object_id;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    RAY_CHECK_OK(client.Create(object_id, data_size, metadata, metadata_size, &data));
    RAY_CHECK_OK(client.Seal(object_id));
    return object_id;
  }

 protected:
  std::thread p;
  boost::asio::io_service main_service;
  std::shared_ptr<gcs::RedisGcsClient> gcs_client_1;
  std::shared_ptr<gcs::RedisGcsClient> gcs_client_2;
  std::unique_ptr<ray::raylet::Raylet> server1;
  std::unique_ptr<ray::raylet::Raylet> server2;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  std::vector<ObjectID> v1;
  std::vector<ObjectID> v2;
};

class TestObjectManagerIntegration : public TestObjectManagerBase {
 public:
  size_t num_expected_objects;

  int num_connected_clients = 0;

  ClientID node_id_1;
  ClientID node_id_2;

  void WaitConnections() {
    node_id_1 = gcs_client_1->Nodes().GetSelfId();
    node_id_2 = gcs_client_2->Nodes().GetSelfId();
    gcs_client_1->Nodes().AsyncSubscribeToNodeChange(
        [this](const ClientID &node_id, const rpc::GcsNodeInfo &data) {
          if (node_id == node_id_1 || node_id == node_id_2) {
            num_connected_clients += 1;
          }
          if (num_connected_clients == 2) {
            StartTests();
          }
        },
        nullptr);
  }

  void StartTests() {
    TestConnections();
    AddTransferTestHandlers();
    TestPush(100);
  }

  void AddTransferTestHandlers() {
    ray::Status status = ray::Status::OK();
    status = server1->object_manager_.SubscribeObjAdded(
        [this](const object_manager::protocol::ObjectInfoT &object_info) {
          v1.push_back(ObjectID::FromBinary(object_info.object_id));
          if (v1.size() == num_expected_objects && v1.size() == v2.size()) {
            TestPushComplete();
          }
        });
    RAY_CHECK_OK(status);
    status = server2->object_manager_.SubscribeObjAdded(
        [this](const object_manager::protocol::ObjectInfoT &object_info) {
          v2.push_back(ObjectID::FromBinary(object_info.object_id));
          if (v2.size() == num_expected_objects && v1.size() == v2.size()) {
            TestPushComplete();
          }
        });
    RAY_CHECK_OK(status);
  }

  void TestPush(int64_t data_size) {
    ray::Status status = ray::Status::OK();

    num_expected_objects = (size_t)1;
    ObjectID oid1 = WriteDataToClient(client1, data_size);
    server1->object_manager_.Push(oid1, node_id_2);
  }

  void TestPushComplete() {
    RAY_LOG(INFO) << "TestPushComplete: "
                  << " " << v1.size() << " " << v2.size();
    ASSERT_TRUE(v1.size() == v2.size());
    for (int i = -1; ++i < (int)v1.size();) {
      ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
    }
    v1.clear();
    v2.clear();
    main_service.stop();
  }

  void TestConnections() {
    RAY_LOG(INFO) << "\n"
                  << "Server client ids:"
                  << "\n";
    ClientID node_id_1 = gcs_client_1->Nodes().GetSelfId();
    ClientID node_id_2 = gcs_client_2->Nodes().GetSelfId();
    RAY_LOG(INFO) << "Server 1: " << node_id_1;
    RAY_LOG(INFO) << "Server 2: " << node_id_2;

    RAY_LOG(INFO) << "\n"
                  << "All connected clients:"
                  << "\n";
    auto data = gcs_client_2->Nodes().Get(node_id_1);
    RAY_LOG(INFO) << (ClientID::FromBinary(data->node_id()).IsNil());
    RAY_LOG(INFO) << "ClientID=" << ClientID::FromBinary(data->node_id());
    RAY_LOG(INFO) << "ClientIp=" << data->node_manager_address();
    RAY_LOG(INFO) << "ClientPort=" << data->node_manager_port();
    rpc::GcsNodeInfo data2;
    gcs_client_1->Nodes().Get(node_id_2);
    RAY_LOG(INFO) << "ClientID=" << ClientID::FromBinary(data2->node_id());
    RAY_LOG(INFO) << "ClientIp=" << data2->node_manager_address();
    RAY_LOG(INFO) << "ClientPort=" << data2->node_manager_port();
  }
};

TEST_F(TestObjectManagerIntegration, StartTestObjectManagerPush) {
  auto AsyncStartTests = main_service.wrap([this]() { WaitConnections(); });
  AsyncStartTests();
  main_service.run();
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::raylet::test_executable = std::string(argv[0]);
  ray::TEST_STORE_EXEC_PATH = std::string(argv[1]);
  return RUN_ALL_TESTS();
}
