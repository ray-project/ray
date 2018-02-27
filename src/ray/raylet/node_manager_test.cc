#include <iostream>
#include <thread>
#include <algorithm>

#include "gtest/gtest.h"

#include "node_manager.h"

namespace ray {

std::string test_executable;  // NOLINT

class TestNodeManager : public ::testing::Test {
public:

  TestNodeManager() {
    std::cout << "TestNodeManager: started." << std::endl;
  }

  std::string StartStore(const std::string &id){
    std::string store_id = "/tmp/store";
    store_id = store_id + id;
    std::string test_dir = test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_dir = test_dir + "./../plasma";
    std::string plasma_command = plasma_dir + "/plasma_store -m 1000000000 -s " + store_id + " 1> /dev/null 2> /dev/null &";
    cout << plasma_command << endl;
    int ec = system(plasma_command.c_str());
    if(ec != 0){
      throw std::runtime_error("failed to start plasma store.");
    };
    return store_id;
  }

  void SetUp() {

    // start store
    std::string store_sock_1 = StartStore("1");
    std::string store_sock_2 = StartStore("2");

    // configure
    std::unordered_map<string, double> static_resource_config;
    static_resource_config = {{"num_cpus", 1}, {"num_gpus", 1}};
    ray::ResourceSet resource_config(std::move(static_resource_config));

    // start mock gcs
    mock_gcs_client = shared_ptr<GcsClient>(new GcsClient());

    // start first server
    ray::OMConfig om_config_1;
    om_config_1.store_socket_name = store_sock_1;
    shared_ptr<ray::ObjectDirectory> od = shared_ptr<ray::ObjectDirectory>(new ray::ObjectDirectory());
    od->InitGcs(mock_gcs_client);
    server1.reset(new NodeServer(io_service,
                                 std::string("hello1"),
                                 resource_config,
                                 om_config_1,
                                 mock_gcs_client,
                                 od));

    // start second server
    ray::OMConfig om_config_2;
    om_config_2.store_socket_name = store_sock_2;
    shared_ptr<ray::ObjectDirectory> od2 = shared_ptr<ray::ObjectDirectory>(new ray::ObjectDirectory());
    od2->InitGcs(mock_gcs_client);
    server2.reset(new NodeServer(io_service,
                                 std::string("hello2"),
                                 resource_config,
                                 om_config_2,
                                 mock_gcs_client,
                                 od2));

    // connect to stores.
    ARROW_CHECK_OK(client1.Connect(store_sock_1, "", PLASMA_DEFAULT_RELEASE_DELAY));
    ARROW_CHECK_OK(client2.Connect(store_sock_2, "", PLASMA_DEFAULT_RELEASE_DELAY));
    this->StartLoop();
  }

  void TearDown() {
    this->StopLoop();
    client1.Disconnect();
    client2.Disconnect();

    this->server1->Terminate();
    this->server2->Terminate();

    int s = system("killall plasma_store &");
    ASSERT_TRUE(!s);

    std::string cmd_str = test_executable.substr(0, test_executable.find_last_of("/"));
    // cout << cmd_str << endl;
    s = system(("rm " + cmd_str + "/hello1").c_str());
    ASSERT_TRUE(!s);
    s = system(("rm " + cmd_str + "/hello2").c_str());
    ASSERT_TRUE(!s);
  }

  void Loop(){
    io_service.run();
  };

  void StartLoop(){
    p = std::thread(&TestNodeManager::Loop, this);
  };

  void StopLoop(){
    io_service.stop();
    p.join();
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size){
    ObjectID object_id = ObjectID::from_random();
    // cout << "ObjectID Created: " << object_id.hex().c_str() << endl;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client.Create(object_id.to_plasma_id(), data_size, metadata, metadata_size, &data));
    ARROW_CHECK_OK(client.Seal(object_id.to_plasma_id()));
    return object_id;
  }

  void object_added_handler_1(const ObjectID &object_id){
    cout << "Store 1 added: " << object_id.hex() << endl;
    v1.push_back(object_id);
  };

  void object_added_handler_2(const ObjectID &object_id){
    cout << "Store 2 added: " << object_id.hex() << endl;
    v2.push_back(object_id);
  };

 protected:

  std::thread p;
  boost::asio::io_service io_service;
  shared_ptr<ray::GcsClient> mock_gcs_client;
  unique_ptr<ray::NodeServer> server1;
  unique_ptr<ray::NodeServer> server2;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  vector<ObjectID> v1;
  vector<ObjectID> v2;

};

TEST_F(TestNodeManager, TestNodeManagerCommands) {
  cout << endl << "All connected clients:" << endl << endl;
  vector<ClientID> client_ids = mock_gcs_client->client_table().GetClientIds();
  for (auto client_id : client_ids) {
    ClientInformation info = mock_gcs_client->client_table().GetClientInformation(client_id);
    cout << "ClientID=" << client_id.hex() << endl;
    cout << "ClientIp=" << info.GetIp() << endl;
    cout << "ClientPort=" << info.GetPort() << endl;
    ASSERT_TRUE(client_id == info.GetClientId());
  }

  sleep(1);

  cout << endl << "Server client ids:" << endl << endl;

  server1->GetObjectManager().SubscribeObjAdded(
      [this](const ObjectID &object_id){
        object_added_handler_1(object_id);
      }
  );

  server2->GetObjectManager().SubscribeObjAdded(
      [this](const ObjectID &object_id){
        object_added_handler_2(object_id);
      }
  );

  ClientID client_id_1 = server1->GetObjectManager().GetClientID();
  ClientID client_id_2 = server2->GetObjectManager().GetClientID();
  cout << "Server 1: " << client_id_1.hex() << endl;
  cout << "Server 2: " << client_id_2.hex() << endl;

  sleep(1);

  cout << endl << "Test send 1 to 2" << endl << endl;
  for(int i=-1;++i<3;){
    ObjectID oid1 = WriteDataToClient(client1, 100);
    server1->GetObjectManager().Push(oid1, client_id_2);
  }
  sleep(1);
  cout << v1.size() << " " << v2.size() << endl;
  for(int i=-1;++i < (int) v1.size();){
    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
  }
  v1.clear();
  v2.clear();

  cout << endl << "Test send 2 to 1" << endl << endl;
  for(int i=-1;++i<3;){
    ObjectID oid2 = WriteDataToClient(client2, 100);
    server2->GetObjectManager().Push(oid2, client_id_1);
  }
  sleep(1);
  cout << v1.size() << " " << v2.size() << endl;
  for(int i=-1;++i < (int) v1.size();){
    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
  }
  v1.clear();
  v2.clear();

  cout << endl << "Test bidirectional" << endl << endl;
  for(int i=-1;++i<3;){
    ObjectID oid1 = WriteDataToClient(client1, 100);
    ObjectID oid2 = WriteDataToClient(client2, 100);
    ASSERT_TRUE(oid1.size() > 0);
    ASSERT_TRUE(oid2.size() > 0);
    server1->GetObjectManager().Push(oid1, client_id_2);
    server2->GetObjectManager().Push(oid2, client_id_1);
  }
  sleep(1);
  cout << v1.size() << " " << v2.size() << endl;
  for(int i=-1;++i < (int) v1.size();){
    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
  }
  v1.clear();
  v2.clear();

  ASSERT_TRUE(true);
}

} // namespace ray

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}