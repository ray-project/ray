#include <iostream>
#include <memory>
#include <thread>

#include "gtest/gtest.h"
#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/events.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"

#include "ray/status.h"

#include "object_manager.h"

using namespace std;

namespace ray {

std::string test_executable;  // NOLINT

class TestObjectManager : public ::testing::Test {
 public:
  TestObjectManager() { cout << "TestObjectManager: started." << endl; }

  void SetUp() {
    // start store
    std::string om_dir = test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_dir = om_dir + "./../plasma";
    std::string plasma_command =
        plasma_dir +
        "/plasma_store -m 1000000000 -s /tmp/store 1> /dev/null 2> /dev/null &";
    int s = system(plasma_command.c_str());
    ASSERT_TRUE(!s);

    // start mock gcs
    mock_gcs_client_ = shared_ptr<GcsClient>(new GcsClient());
    // mock_gcs_client_->Register();

    // start nodeserver

    // start om 1
    ObjectManagerConfig config;
    config.store_socket_name = "/tmp/store";
    object_manager_1_ = unique_ptr<ObjectManager>(
        new ObjectManager(io_service_, config, mock_gcs_client_));

    // start om 2
    //    ObjectManagerConfig config2;
    //    config2.store_socket_name = "/tmp/store";
    //    shared_ptr<ObjectDirectory> od2 = shared_ptr<ObjectDirectory>(new
    //    ObjectDirectory());
    //    od2->InitGcs(mock_gcs_client_);
    //    object_manager_2_ = unique_ptr<ObjectManager>(new ObjectManager(io_service,
    //    config2, od2));

    // start client connection
    ARROW_CHECK_OK(client_.Connect("/tmp/store", "", PLASMA_DEFAULT_RELEASE_DELAY));

    // start loop
    this->StartLoop();
  }

  void TearDown() {
    this->StopLoop();
    arrow::Status arrow_status = client_.Disconnect();
    ASSERT_TRUE(arrow_status.ok());
    ray::Status ray_status = object_manager_1_->Terminate();
    ASSERT_TRUE(ray_status.ok());
    // object_manager_2_->Terminate();
    int s = system("killall plasma_store &");
    ASSERT_TRUE(!s);
  }

  void Loop() { io_service_.run(); };

  void StartLoop() { process_thread_ = std::thread(&TestObjectManager::Loop, this); };

  void StopLoop() {
    io_service_.stop();
    process_thread_.join();
  }

 protected:
  std::thread process_thread_;
  plasma::PlasmaClient client_;
  plasma::PlasmaClient client2_;
  boost::asio::io_service io_service_;

  shared_ptr<GcsClient> mock_gcs_client_;
  unique_ptr<ObjectManager> object_manager_1_;
  unique_ptr<ObjectManager> object_manager_2_;
};

// TEST_F(TestObjectManager, TestPush) {
//  // test object push between two object managers.
//  ASSERT_TRUE(true);
//  sleep(1);
//}

// TEST_F(TestObjectManager, TestPull) {
//  ObjectID object_id = ObjectID().from_random();
//  DBClientID dbc_id = DBClientID().from_random();
//  cout << "ObjectID: " << object_id.hex().c_str() << endl;
//  cout << "DBClientID: " << dbc_id.hex().c_str() << endl;
//  om->Pull(object_id, dbc_id);
//  om->Pull(object_id);
//  ASSERT_TRUE(true);
//  sleep(1);
//}

void ObjectAdded(const ObjectID &object_id) {
  cout << "ObjectID Added: " << object_id.hex().c_str() << endl;
}

TEST_F(TestObjectManager, TestNotifications) {
  ray::Status status = object_manager_1_->SubscribeObjAdded(ObjectAdded);
  ASSERT_TRUE(status.ok());
  // put object
  for (int i = 0; i < 10; ++i) {
    ObjectID object_id = ObjectID::from_random();
    cout << "ObjectID Created: " << object_id.hex().c_str() << endl;
    int64_t data_size = 100;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client_.Create(object_id.to_plasma_id(), data_size, metadata,
                                  metadata_size, &data));
    ARROW_CHECK_OK(client_.Seal(object_id.to_plasma_id()));
  }
  // TODO(hme): Can we do this without sleeping?
  sleep(1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
