#include <iostream>
#include <thread>

#include "gtest/gtest.h"

#include "ray/raylet/raylet.h"

namespace ray {

namespace raylet {

std::string test_executable;  // NOLINT

class TestRaylet : public ::testing::Test {
 public:
  TestRaylet() { RAY_LOG(INFO) << "TestRaylet: started."; }

  std::string StartStore(const std::string &id) {
    std::string store_id = "/tmp/store";
    store_id = store_id + id;
    std::string test_dir = test_executable.substr(0, test_executable.find_last_of("/"));
    // TODO(hme): fix for GDB.
    // std::string plasma_dir = test_dir + "./../plasma";
    std::string plasma_dir = "/home/elibol/dev/xray/python/ray/core/src/plasma";
    std::string plasma_command = plasma_dir + "/plasma_store -m 1000000000 -s " +
        store_id + " 1> /dev/null 2> /dev/null &";
    RAY_LOG(INFO) << plasma_command;
    int ec = system(plasma_command.c_str());
    if (ec != 0) {
      throw std::runtime_error("failed to start plasma store.");
    };
    return store_id;
  }

  void SetUp() {
    // start store
    std::string store_sock_1 = StartStore("1");
    std::string store_sock_2 = StartStore("2");

    // configure
    NodeManagerConfig node_manager_config;
    std::unordered_map<std::string, double> static_resource_config;
    static_resource_config = {{"num_cpus", 1}, {"num_gpus", 1}};
    node_manager_config.resource_config = ResourceSet(std::move(static_resource_config));

    // start first server
    gcs_client_1 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ray::ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = store_sock_1;
    server1.reset(new Raylet(main_service, object_manager_service, std::string("hello1"),
                             node_manager_config, om_config_1, gcs_client_1));

    // start second server
    gcs_client_2 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ray::ObjectManagerConfig om_config_2;
    om_config_2.store_socket_name = store_sock_2;
    server2.reset(new Raylet(main_service, object_manager_service, std::string("hello2"),
                             node_manager_config, om_config_2, gcs_client_2));

    // connect to stores.
    ARROW_CHECK_OK(client1.Connect(store_sock_1, "", PLASMA_DEFAULT_RELEASE_DELAY));
    ARROW_CHECK_OK(client2.Connect(store_sock_2, "", PLASMA_DEFAULT_RELEASE_DELAY));
    this->StartLoop();
  }

  void TearDown() {
    this->StopLoop();
    arrow::Status client1_status = client1.Disconnect();
    arrow::Status client2_status = client2.Disconnect();
    ASSERT_TRUE(client1_status.ok() && client2_status.ok());

    this->server1.reset();
    this->server2.reset();

    int s = system("killall plasma_store &");
    ASSERT_TRUE(!s);

    std::string cmd_str = test_executable.substr(0, test_executable.find_last_of("/"));
    s = system(("rm " + cmd_str + "/hello1").c_str());
    ASSERT_TRUE(!s);
    s = system(("rm " + cmd_str + "/hello2").c_str());
    ASSERT_TRUE(!s);
  }

  void Loop() {
    main_service.run();
  };

  void StartLoop() {
    p = std::thread(&TestRaylet::Loop, this);
  };

  void StopLoop() {
    main_service.stop();
    p.join();
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size) {
    ObjectID object_id = ObjectID::from_random();
    RAY_LOG(DEBUG) << "ObjectID Created: " << object_id.hex().c_str();
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client.Create(object_id.to_plasma_id(), data_size, metadata,
                                 metadata_size, &data));
    ARROW_CHECK_OK(client.Seal(object_id.to_plasma_id()));
    return object_id;
  }

  void object_added_handler_1(const ObjectID &object_id) {
    RAY_LOG(INFO) << "Store 1 added: " << object_id.hex();
    v1.push_back(object_id);
  };

  void object_added_handler_2(const ObjectID &object_id) {
    RAY_LOG(INFO) << "Store 2 added: " << object_id.hex();
    v2.push_back(object_id);
  };

 protected:
  std::thread p;
  boost::asio::io_service main_service;
  boost::asio::io_service object_manager_service;

  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_1;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_2;
  std::unique_ptr<ray::raylet::Raylet> server1;
  std::unique_ptr<ray::raylet::Raylet> server2;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  std::vector<ObjectID> v1;
  std::vector<ObjectID> v2;
};

TEST_F(TestRaylet, TestRayletCommands) {
  ray::Status status = ray::Status::OK();
  // TODO(atumanov): assert status is OK everywhere it's returned.
  // TODO(hme): fix these tests (post gcs integration).
//
//  RAY_LOG(INFO) << "\n"
//                << "Server client ids:"
//                << "\n";
//  ClientID client_id_1 = server1->GetObjectManager().GetClientID();
//  ClientID client_id_2 = server2->GetObjectManager().GetClientID();
//  RAY_LOG(INFO) << "Server 1: " << client_id_1.hex();
//  RAY_LOG(INFO) << "Server 2: " << client_id_2.hex();
//
//  sleep(1);
//
//  RAY_LOG(INFO) << "\n"
//                << "All connected clients:"
//                << "\n";
//  const ClientTableDataT &data = gcs_client_1->client_table().GetClient(client_id_1);
//  RAY_LOG(INFO) << (ClientID::from_binary(data.client_id) == ClientID::nil());
//  RAY_LOG(INFO) << "ClientID=" << ClientID::from_binary(data.client_id);
//  RAY_LOG(INFO) << "ClientIp=" << data.node_manager_address;
//  RAY_LOG(INFO) << "ClientPort=" << data.local_scheduler_port;
//
//  const ClientTableDataT &data2 = gcs_client_1->client_table().GetClient(client_id_2);
//  RAY_LOG(INFO) << "ClientID=" << ClientID::from_binary(data2.client_id);
//  RAY_LOG(INFO) << "ClientIp=" << data2.node_manager_address;
//  RAY_LOG(INFO) << "ClientPort=" << data2.local_scheduler_port;
//
//  sleep(1);
//
//  status = server1->GetObjectManager().SubscribeObjAdded(
//      [this](const ObjectID &object_id) { object_added_handler_1(object_id); });
//  ASSERT_TRUE(status.ok());
//
//  status = server2->GetObjectManager().SubscribeObjAdded(
//      [this](const ObjectID &object_id) { object_added_handler_2(object_id); });
//  ASSERT_TRUE(status.ok());
//  sleep(1);
//  RAY_LOG(INFO) << "\n"
//                << "Test bidirectional pull"
//                << "\n";
//  for (int i = -1; ++i < 100;) {
//    ObjectID oid1 = WriteDataToClient(client1, 100);
//    ObjectID oid2 = WriteDataToClient(client2, 100);
//    status = server1->GetObjectManager().Pull(oid2);
//    status = server2->GetObjectManager().Pull(oid1);
//  }
//  sleep(1);
//  RAY_LOG(INFO) << v1.size() << " " << v2.size();
//  ASSERT_TRUE(v1.size() == v2.size());
//  for (int i = -1; ++i < (int) v1.size();) {
//    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
//  }
//  v1.clear();
//  v2.clear();
//
//  RAY_LOG(INFO) << "\n"
//                << "Test pull 1 from 2"
//                << "\n";
//  for (int i = -1; ++i < 3;) {
//    ObjectID oid2 = WriteDataToClient(client2, 100);
//    status = server1->GetObjectManager().Pull(oid2);
//  }
//  sleep(1);
//  RAY_LOG(INFO) << v1.size() << " " << v2.size();
//  ASSERT_TRUE(v1.size() == v2.size());
//  for (int i = -1; ++i < (int) v1.size();) {
//    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
//  }
//  v1.clear();
//  v2.clear();
//
//  RAY_LOG(INFO) << "\n"
//                << "Test pull 2 from 1"
//                << "\n";
//  for (int i = -1; ++i < 3;) {
//    ObjectID oid1 = WriteDataToClient(client1, 100);
//    status = server2->GetObjectManager().Pull(oid1);
//  }
//  sleep(1);
//  RAY_LOG(INFO) << v1.size() << " " << v2.size();
//  ASSERT_TRUE(v1.size() == v2.size());
//  for (int i = -1; ++i < (int) v1.size();) {
//    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
//  }
//  v1.clear();
//  v2.clear();
//
//  RAY_LOG(INFO) << "\n"
//                << "Test push 1 to 2"
//                << "\n";
//  for (int i = -1; ++i < 3;) {
//    ObjectID oid1 = WriteDataToClient(client1, 100);
//    status = server1->GetObjectManager().Push(oid1, client_id_2);
//  }
//  sleep(1);
//  RAY_LOG(INFO) << v1.size() << " " << v2.size();
//  ASSERT_TRUE(v1.size() == v2.size());
//  for (int i = -1; ++i < (int) v1.size();) {
//    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
//  }
//  v1.clear();
//  v2.clear();
//
//  RAY_LOG(INFO) << "\n"
//                << "Test push 2 to 1"
//                << "\n";
//  for (int i = -1; ++i < 3;) {
//    ObjectID oid2 = WriteDataToClient(client2, 100);
//    status = server2->GetObjectManager().Push(oid2, client_id_1);
//  }
//  sleep(1);
//  RAY_LOG(INFO) << v1.size() << " " << v2.size();
//  ASSERT_TRUE(v1.size() == v2.size());
//  for (int i = -1; ++i < (int) v1.size();) {
//    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
//  }
//  v1.clear();
//  v2.clear();
//
//  RAY_LOG(INFO) << "\n"
//                << "Test bidirectional push"
//                << "\n";
//  for (int i = -1; ++i < 3;) {
//    ObjectID oid1 = WriteDataToClient(client1, 100);
//    ObjectID oid2 = WriteDataToClient(client2, 100);
//    status = server1->GetObjectManager().Push(oid1, client_id_2);
//    status = server2->GetObjectManager().Push(oid2, client_id_1);
//  }
//  sleep(1);
//  RAY_LOG(INFO) << v1.size() << " " << v2.size();
//  ASSERT_TRUE(v1.size() == v2.size());
//  for (int i = -1; ++i < (int) v1.size();) {
//    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
//  }
//  v1.clear();
//  v2.clear();

}

} // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::raylet::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
