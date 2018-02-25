#include "thread"
#include <iostream>
#include "memory"

#include "gtest/gtest.h"

#include "plasma/common.h"
#include "plasma/plasma.h"
#include "plasma/events.h"
#include "plasma/protocol.h"
#include "plasma/client.h"

#include "ray/status.h"

#include "object_manager.h"

using namespace std;

namespace ray {

std::string test_executable;  // NOLINT

class TestObjectManager : public ::testing::Test {

 public:

  TestObjectManager() {
    cout << "TestObjectManager: started." << endl;
  }

  void SetUp() {
    // start store
    std::string om_dir =
        test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_dir = om_dir + "./../plasma";
    std::string plasma_command = plasma_dir + "/plasma_store -m 1000000000 -s /tmp/store 1> /dev/null 2> /dev/null &";
    int s = system(plasma_command.c_str());
    ASSERT_TRUE(!s);

    // start mock gcs
    mock_gcs_client = shared_ptr<GcsClient>(new GcsClient());
    // mock_gcs_client->Register();

    // start nodeserver

    // start om 1
    OMConfig config;
    config.store_socket_name = "/tmp/store";
    shared_ptr<ObjectDirectory> od = shared_ptr<ObjectDirectory>(new ObjectDirectory());
    od->InitGcs(mock_gcs_client);
    om = unique_ptr<ObjectManager>(new ObjectManager(io_service, config, od));

    //start om 2
    OMConfig config2;
    config2.store_socket_name = "/tmp/store";
    shared_ptr<ObjectDirectory> od2 = shared_ptr<ObjectDirectory>(new ObjectDirectory());
    od2->InitGcs(mock_gcs_client);
    om2 = unique_ptr<ObjectManager>(new ObjectManager(io_service, config2, od2));

    // start client connection
    ARROW_CHECK_OK(client_.Connect("/tmp/store", "", PLASMA_DEFAULT_RELEASE_DELAY));

    //start loop
    this->StartLoop();
  }

  void TearDown() {
    this->StopLoop();
    client_.Disconnect();
    om->Terminate();
    om2->Terminate();
    int s = system("killall plasma_store &");
    ASSERT_TRUE(!s);
  }

  void Loop(){
    io_service.run();
  };

  void StartLoop(){
    p = std::thread(&TestObjectManager::Loop, this);
  };

  void StopLoop(){
    io_service.stop();
    p.join();
  }

 protected:
  std::thread p;
  plasma::PlasmaClient client_;
  plasma::PlasmaClient client2_;
  boost::asio::io_service io_service;

  shared_ptr<GcsClient> mock_gcs_client;
  unique_ptr<ObjectManager> om;
  unique_ptr<ObjectManager> om2;

};

TEST_F(TestObjectManager, TestPush) {
  // test object push between two object managers.
  ASSERT_TRUE(true);
  sleep(1);
}

//TEST_F(TestObjectManager, TestPull) {
//  ObjectID object_id = ObjectID().from_random();
//  DBClientID dbc_id = DBClientID().from_random();
//  cout << "ObjectID: " << object_id.hex().c_str() << endl;
//  cout << "DBClientID: " << dbc_id.hex().c_str() << endl;
//  om->Pull(object_id, dbc_id);
//  om->Pull(object_id);
//  ASSERT_TRUE(true);
//  sleep(1);
//}

void ObjectAdded(const ObjectID &object_id){
  cout << "ObjectID Added: " << object_id.hex().c_str() << endl;
}

//TEST_F(TestObjectManager, TestNotifications) {
//  om->SubscribeObjAdded(ObjectAdded);
//  // put object
//  for(int i=-1;++i<10;){
//    ObjectID object_id = ObjectID::from_random();
//    cout << "ObjectID Created: " << object_id.hex().c_str() << endl;
//    int64_t data_size = 100;
//    uint8_t metadata[] = {5};
//    int64_t metadata_size = sizeof(metadata);
//    std::shared_ptr<Buffer> data;
//    ARROW_CHECK_OK(client_.Create(object_id.to_plasma_id(), data_size, metadata, metadata_size, &data));
//    ARROW_CHECK_OK(client_.Seal(object_id.to_plasma_id()));
//  }
//  // TODO(hme): Can we do this without sleeping?
//  sleep(1);
//}

} // namespace ray

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
