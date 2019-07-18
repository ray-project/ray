#include <iostream>
#include <thread>

#include "gtest/gtest.h"

#include "ray/common/status.h"

#include "ray/raylet/raylet.h"

namespace ray {

namespace raylet {

std::string test_executable;
std::string store_executable;

// TODO(hme): Get this working once the dust settles.
class TestObjectManagerBase : public ::testing::Test {
 public:
  TestObjectManagerBase() { RAY_LOG(INFO) << "TestObjectManagerBase: started."; }

  std::string StartStore(const std::string &id) {
    std::string store_id = "/tmp/store";
    store_id = store_id + id;
    std::string plasma_command = store_executable + " -m 1000000000 -s " + store_id +
                                 " 1> /dev/null 2> /dev/null &";
    RAY_LOG(INFO) << plasma_command;
    int ec = system(plasma_command.c_str());
    RAY_CHECK(ec == 0);
    return store_id;
  }

  NodeManagerConfig GetNodeManagerConfig(std::string raylet_socket_name,
                                         std::string store_socket_name) {
    // Configuration for the node manager.
    ray::raylet::NodeManagerConfig node_manager_config;
    std::unordered_map<std::string, double> static_resource_conf;
    static_resource_conf = {{"CPU", 1}, {"GPU", 1}};
    node_manager_config.resource_config =
        ray::raylet::ResourceSet(std::move(static_resource_conf));
    node_manager_config.num_initial_workers = 0;
    node_manager_config.num_workers_per_process = 1;
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
    std::string store_sock_1 = StartStore("1");
    std::string store_sock_2 = StartStore("2");

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
    RAY_ARROW_CHECK_OK(client1.Connect(store_sock_1));
    RAY_ARROW_CHECK_OK(client2.Connect(store_sock_2));
  }

  void TearDown() {
    arrow::Status client1_status = client1.Disconnect();
    arrow::Status client2_status = client2.Disconnect();
    ASSERT_TRUE(client1_status.ok() && client2_status.ok());

    this->server1.reset();
    this->server2.reset();

    int s = system("killall plasma_store_server &");
    ASSERT_TRUE(!s);

    std::string cmd_str = test_executable.substr(0, test_executable.find_last_of("/"));
    s = system(("rm " + cmd_str + "/raylet_1").c_str());
    ASSERT_TRUE(!s);
    s = system(("rm " + cmd_str + "/raylet_2").c_str());
    ASSERT_TRUE(!s);
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size) {
    ObjectID object_id = ObjectID::FromRandom();
    RAY_LOG(DEBUG) << "ObjectID Created: " << object_id;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    RAY_ARROW_CHECK_OK(
        client.Create(object_id.ToPlasmaId(), data_size, metadata, metadata_size, &data));
    RAY_ARROW_CHECK_OK(client.Seal(object_id.ToPlasmaId()));
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
  uint num_expected_objects;

  int num_connected_clients = 0;

  ClientID client_id_1;
  ClientID client_id_2;

  void WaitConnections() {
    client_id_1 = gcs_client_1->client_table().GetLocalClientId();
    client_id_2 = gcs_client_2->client_table().GetLocalClientId();
    gcs_client_1->client_table().RegisterClientAddedCallback(
        [this](gcs::RedisGcsClient *client, const ClientID &id,
               const ClientTableDataT &data) {
          ClientID parsed_id = ClientID::FromBinary(data.client_id);
          if (parsed_id == client_id_1 || parsed_id == client_id_2) {
            num_connected_clients += 1;
          }
          if (num_connected_clients == 2) {
            StartTests();
          }
        });
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

    num_expected_objects = (uint)1;
    ObjectID oid1 = WriteDataToClient(client1, data_size);
    server1->object_manager_.Push(oid1, client_id_2);
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
    ClientID client_id_1 = gcs_client_1->client_table().GetLocalClientId();
    ClientID client_id_2 = gcs_client_2->client_table().GetLocalClientId();
    RAY_LOG(INFO) << "Server 1: " << client_id_1;
    RAY_LOG(INFO) << "Server 2: " << client_id_2;

    RAY_LOG(INFO) << "\n"
                  << "All connected clients:"
                  << "\n";
    ClientTableDataT data;
    gcs_client_2->client_table().GetClient(client_id_1, data);
    RAY_LOG(INFO) << (ClientID::FromBinary(data.client_id).IsNil());
    RAY_LOG(INFO) << "ClientID=" << ClientID::FromBinary(data.client_id);
    RAY_LOG(INFO) << "ClientIp=" << data.node_manager_address;
    RAY_LOG(INFO) << "ClientPort=" << data.node_manager_port;
    ClientTableDataT data2;
    gcs_client_1->client_table().GetClient(client_id_2, data2);
    RAY_LOG(INFO) << "ClientID=" << ClientID::FromBinary(data2.client_id);
    RAY_LOG(INFO) << "ClientIp=" << data2.node_manager_address;
    RAY_LOG(INFO) << "ClientPort=" << data2.node_manager_port;
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
  ray::raylet::store_executable = std::string(argv[1]);
  return RUN_ALL_TESTS();
}
