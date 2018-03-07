#include <iostream>
#include <thread>

#include "gtest/gtest.h"

#include "ray/raylet/raylet.h"

using namespace std;
namespace ray {

std::string test_executable;  // NOLINT

class TestRaylet : public ::testing::Test {
public:

  TestRaylet() {
    std::cout << "TestRaylet: started." << std::endl;
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
    ray::ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = store_sock_1;
    server1.reset(new Raylet(io_service,
                                 std::string("hello1"),
                                 resource_config,
                                 om_config_1,
                                 mock_gcs_client));

    // start second server
    ray::ObjectManagerConfig om_config_2;
    om_config_2.store_socket_name = store_sock_2;
    server2.reset(new Raylet(io_service,
                                 std::string("hello2"),
                                 resource_config,
                                 om_config_2,
                                 mock_gcs_client));

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
    p = std::thread(&TestRaylet::Loop, this);
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
  unique_ptr<ray::Raylet> server1;
  unique_ptr<ray::Raylet> server2;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  vector<ObjectID> v1;
  vector<ObjectID> v2;

};

TEST_F(TestRaylet, TestRayletCommands) {
  ray::Status status = ray::Status::OK();
  // TODO(atumanov): assert status is OK everywhere it's returned.
  cout << endl << "All connected clients:" << endl << endl;
  status = mock_gcs_client->client_table().GetClientIds([this](const vector<ClientID> &client_ids){
    mock_gcs_client->client_table().GetClientInformationSet(
        client_ids,
        [this](const std::vector<ClientInformation> &info_vec){
          for (const auto &info : info_vec) {
            cout << "ClientID=" << info.GetClientId().hex() << endl;
            cout << "ClientIp=" << info.GetIp() << endl;
            cout << "ClientPort=" << info.GetPort() << endl;
          }
        },
        [](Status status){}
    );
  });

  sleep(1);

  cout << endl << "Server client ids:" << endl << endl;

  status = server1->GetObjectManager().SubscribeObjAdded(
      [this](const ObjectID &object_id){
        object_added_handler_1(object_id);
      }
  );
  ASSERT_TRUE(status.ok());

  status = server2->GetObjectManager().SubscribeObjAdded(
      [this](const ObjectID &object_id){
        object_added_handler_2(object_id);
      }
  );
  ASSERT_TRUE(status.ok());

  ClientID client_id_1 = server1->GetObjectManager().GetClientID();
  ClientID client_id_2 = server2->GetObjectManager().GetClientID();
  cout << "Server 1: " << client_id_1.hex() << endl;
  cout << "Server 2: " << client_id_2.hex() << endl;

  sleep(1);

  cout << endl << "Test bidirectional pull" << endl << endl;
  for(int i=-1;++i<100;){
    ObjectID oid1 = WriteDataToClient(client1, 100);
    ObjectID oid2 = WriteDataToClient(client2, 100);
    status = server1->GetObjectManager().Pull(oid2);
    status = server2->GetObjectManager().Pull(oid1);
  }
  sleep(1);
  cout << v1.size() << " " << v2.size() << endl;
  ASSERT_TRUE(v1.size() == v2.size());
  for(int i=-1;++i < (int) v1.size();){
    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
  }
  v1.clear();
  v2.clear();

  cout << endl << "Test pull 1 from 2" << endl << endl;
  for(int i=-1;++i<3;){
    ObjectID oid2 = WriteDataToClient(client2, 100);
    status = server1->GetObjectManager().Pull(oid2);
  }
  sleep(1);
  cout << v1.size() << " " << v2.size() << endl;
  ASSERT_TRUE(v1.size() == v2.size());
  for(int i=-1;++i < (int) v1.size();){
    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
  }
  v1.clear();
  v2.clear();

  cout << endl << "Test pull 2 from 1" << endl << endl;
  for(int i=-1;++i<3;){
    ObjectID oid1 = WriteDataToClient(client1, 100);
    status = server2->GetObjectManager().Pull(oid1);
  }
  sleep(1);
  cout << v1.size() << " " << v2.size() << endl;
  ASSERT_TRUE(v1.size() == v2.size());
  for(int i=-1;++i < (int) v1.size();){
    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
  }
  v1.clear();
  v2.clear();

  cout << endl << "Test push 1 to 2" << endl << endl;
  for(int i=-1;++i<3;){
    ObjectID oid1 = WriteDataToClient(client1, 100);
    status = server1->GetObjectManager().Push(oid1, client_id_2);
  }
  sleep(1);
  cout << v1.size() << " " << v2.size() << endl;
  ASSERT_TRUE(v1.size() == v2.size());
  for(int i=-1;++i < (int) v1.size();){
    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
  }
  v1.clear();
  v2.clear();

  cout << endl << "Test push 2 to 1" << endl << endl;
  for(int i=-1;++i<3;){
    ObjectID oid2 = WriteDataToClient(client2, 100);
    status = server2->GetObjectManager().Push(oid2, client_id_1);
  }
  sleep(1);
  cout << v1.size() << " " << v2.size() << endl;
  ASSERT_TRUE(v1.size() == v2.size());
  for(int i=-1;++i < (int) v1.size();){
    ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
  }
  v1.clear();
  v2.clear();

  cout << endl << "Test bidirectional push" << endl << endl;
  for(int i=-1;++i<3;){
    ObjectID oid1 = WriteDataToClient(client1, 100);
    ObjectID oid2 = WriteDataToClient(client2, 100);
    status = server1->GetObjectManager().Push(oid1, client_id_2);
    status = server2->GetObjectManager().Push(oid2, client_id_1);
  }
  sleep(1);
  cout << v1.size() << " " << v2.size() << endl;
  ASSERT_TRUE(v1.size() == v2.size());
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
