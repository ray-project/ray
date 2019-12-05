#include <vector>

#include "gtest/gtest.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"

namespace ray {

class ReferenceCountTest : public ::testing::Test {
 protected:
  std::unique_ptr<ReferenceCounter> rc;
  virtual void SetUp() { rc = std::unique_ptr<ReferenceCounter>(new ReferenceCounter); }

  virtual void TearDown() {}
};

// Tests basic incrementing/decrementing of direct reference counts. An entry should only
// be removed once its reference count reaches zero.
TEST_F(ReferenceCountTest, TestBasic) {
  std::vector<ObjectID> out;
  ObjectID id = ObjectID::FromRandom();
  rc->AddLocalReference(id);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  rc->AddLocalReference(id);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  rc->AddLocalReference(id);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  rc->RemoveLocalReference(id, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveLocalReference(id, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveLocalReference(id, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 1);
}

// Tests the basic logic for dependencies - when an ObjectID with dependencies
// goes out of scope (i.e., reference count reaches zero), all of its dependencies
// should have their reference count decremented and be removed if it reaches zero.
TEST_F(ReferenceCountTest, TestDependencies) {
  std::vector<ObjectID> out;
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  ObjectID id3 = ObjectID::FromRandom();

  std::shared_ptr<std::vector<ObjectID>> deps = std::make_shared<std::vector<ObjectID>>();
  deps->push_back(id2);
  deps->push_back(id3);
  rc->AddOwnedObject(id1, TaskID::Nil(), rpc::Address(), deps);

  rc->AddLocalReference(id1);
  rc->AddLocalReference(id1);
  rc->AddLocalReference(id3);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 3);

  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 3);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 2);

  rc->RemoveLocalReference(id3, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 3);
}

// Tests the case where two entries share the same set of dependencies. When one
// entry goes out of scope, it should decrease the reference count for the dependencies
// but they should still be nonzero until the second entry goes out of scope and all
// direct dependencies to the dependencies are removed.
TEST_F(ReferenceCountTest, TestSharedDependencies) {
  std::vector<ObjectID> out;
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  ObjectID id3 = ObjectID::FromRandom();
  ObjectID id4 = ObjectID::FromRandom();

  std::shared_ptr<std::vector<ObjectID>> deps = std::make_shared<std::vector<ObjectID>>();
  deps->push_back(id3);
  deps->push_back(id4);
  rc->AddOwnedObject(id1, TaskID::Nil(), rpc::Address(), deps);
  rc->AddOwnedObject(id2, TaskID::Nil(), rpc::Address(), deps);

  rc->AddLocalReference(id1);
  rc->AddLocalReference(id2);
  rc->AddLocalReference(id4);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 4);

  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 3);
  ASSERT_EQ(out.size(), 1);
  rc->RemoveLocalReference(id2, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 3);

  rc->RemoveLocalReference(id4, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 4);
}

// Tests the case when an entry has a dependency that itself has a
// dependency. In this case, when the first entry goes out of scope
// it should decrease the reference count for its dependency, causing
// that entry to go out of scope and decrease its dependencies' reference counts.
TEST_F(ReferenceCountTest, TestRecursiveDependencies) {
  std::vector<ObjectID> out;
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  ObjectID id3 = ObjectID::FromRandom();
  ObjectID id4 = ObjectID::FromRandom();

  std::shared_ptr<std::vector<ObjectID>> deps2 =
      std::make_shared<std::vector<ObjectID>>();
  deps2->push_back(id3);
  deps2->push_back(id4);
  rc->AddOwnedObject(id2, TaskID::Nil(), rpc::Address(), deps2);

  std::shared_ptr<std::vector<ObjectID>> deps1 =
      std::make_shared<std::vector<ObjectID>>();
  deps1->push_back(id2);
  rc->AddOwnedObject(id1, TaskID::Nil(), rpc::Address(), deps1);

  rc->AddLocalReference(id1);
  rc->AddLocalReference(id2);
  rc->AddLocalReference(id4);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 4);

  rc->RemoveLocalReference(id2, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 4);
  ASSERT_EQ(out.size(), 0);
  rc->RemoveLocalReference(id1, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 1);
  ASSERT_EQ(out.size(), 3);

  rc->RemoveLocalReference(id4, &out);
  ASSERT_EQ(rc->NumObjectIDsInScope(), 0);
  ASSERT_EQ(out.size(), 4);
}

// Tests that we can get the owner address correctly for objects that we own,
// objects that we borrowed via a serialized object ID, and objects whose
// origin we do not know.
TEST_F(ReferenceCountTest, TestOwnerAddress) {
  auto object_id = ObjectID::FromRandom();
  TaskID task_id = TaskID::ForFakeTask();
  rpc::Address address;
  address.set_ip_address("1234");
  auto deps = std::make_shared<std::vector<ObjectID>>();
  rc->AddOwnedObject(object_id, task_id, address, deps);

  TaskID added_id;
  rpc::Address added_address;
  ASSERT_TRUE(rc->GetOwner(object_id, &added_id, &added_address));
  ASSERT_EQ(task_id, added_id);
  ASSERT_EQ(address.ip_address(), added_address.ip_address());

  auto object_id2 = ObjectID::FromRandom();
  task_id = TaskID::ForFakeTask();
  address.set_ip_address("5678");
  rc->AddOwnedObject(object_id2, task_id, address, deps);
  ASSERT_TRUE(rc->GetOwner(object_id2, &added_id, &added_address));
  ASSERT_EQ(task_id, added_id);
  ASSERT_EQ(address.ip_address(), added_address.ip_address());

  auto object_id3 = ObjectID::FromRandom();
  ASSERT_FALSE(rc->GetOwner(object_id3, &added_id, &added_address));
  rc->AddLocalReference(object_id3);
  ASSERT_FALSE(rc->GetOwner(object_id3, &added_id, &added_address));
}

// Tests that the ref counts are properly integrated into the local
// object memory store.
TEST(MemoryStoreIntegrationTest, TestSimple) {
  ObjectID id1 = ObjectID::FromRandom().WithDirectTransportType();
  ObjectID id2 = ObjectID::FromRandom().WithDirectTransportType();
  uint8_t data[] = {1, 2, 3, 4, 5, 6, 7, 8};
  RayObject buffer(std::make_shared<LocalMemoryBuffer>(data, sizeof(data)), nullptr);

  auto rc = std::shared_ptr<ReferenceCounter>(new ReferenceCounter());
  CoreWorkerMemoryStore store(nullptr, rc);

  // Tests putting an object with no references is ignored.
  RAY_CHECK_OK(store.Put(buffer, id2));
  ASSERT_EQ(store.Size(), 0);

  // Tests ref counting overrides remove after get option.
  rc->AddLocalReference(id1);
  RAY_CHECK_OK(store.Put(buffer, id1));
  ASSERT_EQ(store.Size(), 1);
  std::vector<std::shared_ptr<RayObject>> results;
  WorkerContext ctx(WorkerType::WORKER, JobID::Nil());
  RAY_CHECK_OK(store.Get({id1}, /*num_objects*/ 1, /*timeout_ms*/ -1, ctx,
                         /*remove_after_get*/ true, &results));
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(store.Size(), 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
