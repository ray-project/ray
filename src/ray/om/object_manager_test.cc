#include <iostream>
#include "memory"

#include "gtest/gtest.h"

#include "ray/status.h"

#include "object_manager.h"

using namespace std;

namespace ray {

class TestObjectManager : public ::testing::Test {

 public:

  TestObjectManager() {
    om = unique_ptr<ObjectManager>(new ObjectManager());
    cout << "TestObjectManager: started." << endl;
  }

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

  UniqueID cb_id1 = om->Pull(object_id, dbc_id, &ObjectReceived);
  cout << "UniqueID1: " << cb_id1.hex().c_str() << endl;

  UniqueID cb_id2 = om->Pull(object_id, &ObjectReceived);
  cout << "UniqueID2: " << cb_id2.hex().c_str() << endl;

  // TODO(hme): Use RAY_CHECK_OK
  ASSERT_TRUE(om->Cancel(cb_id1).ok());
}

}