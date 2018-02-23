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
    ARROW_CHECK_OK(client_.Connect("/tmp/store", "", PLASMA_DEFAULT_RELEASE_DELAY));
    ARROW_CHECK_OK(client2_.Connect("/tmp/store", "", PLASMA_DEFAULT_RELEASE_DELAY));

    // start store
    std::string om_dir =
        test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_dir = om_dir + "../../plasma";
    std::string plasma_command = plasma_dir + "/plasma_store -m 1000000000 -s /tmp/store 1> /dev/null 2> /dev/null &";
    cout << plasma_command.c_str() << "\n";
    int s = system(plasma_command.c_str());
    ASSERT_TRUE(!s);

    // start om
    boost::asio::io_service io_service;
    OMConfig config;
    config.store_socket_name = "/tmp/store";
    om = unique_ptr<ObjectManager>(new ObjectManager(io_service, &config));
  }

  virtual void Finish() {
    ARROW_CHECK_OK(client_.Disconnect());
    ARROW_CHECK_OK(client2_.Disconnect());
    int s = system("killall plasma_store &");
    ASSERT_TRUE(!s);
  }

 protected:
  plasma::PlasmaClient client_;
  plasma::PlasmaClient client2_;

  unique_ptr<ObjectManager> om;

};

void ObjectReceived(Status status,
                    const ObjectID &object_id,
                    const DBClientID &dbclient_id){

}

TEST_F(TestObjectManager, TestObjectManagerCommands) {
  ObjectID object_id = ObjectID().from_random();
  DBClientID dbc_id = DBClientID().from_random();
  cout << "ObjectID: " << object_id.hex().c_str() << endl;
  cout << "DBClientID: " << dbc_id.hex().c_str() << endl;

  UniqueID cb_id1 = om->Pull(object_id, dbc_id);
  cout << "UniqueID1: " << cb_id1.hex().c_str() << endl;

  UniqueID cb_id2 = om->Pull(object_id);
  cout << "UniqueID2: " << cb_id2.hex().c_str() << endl;

  // TODO(hme): Use RAY_CHECK_OK
  ASSERT_TRUE(om->Cancel(cb_id1).ok());
  ASSERT_TRUE(true);
}

void ObjectAdded(const ObjectID &object_id){
  cout << "ObjectID: " << object_id.hex().c_str() << endl;
}

TEST_F(TestObjectManager, TestNotifications) {
  ARROW_CHECK_OK(client_.Connect("/tmp/store", "", PLASMA_DEFAULT_RELEASE_DELAY));

  om->SubscribeObjAdded(ObjectAdded);
  sleep(1);

  // put object
  ObjectID object_id = ObjectID::from_random();
  cout << "ObjectID: " << object_id.hex().c_str() << endl;
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id.to_plasma_id(), data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id.to_plasma_id()));
  sleep(1);

}

} // namespace ray

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
