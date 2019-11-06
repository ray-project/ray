#include <vector>

#include "gtest/gtest.h"
#include "ray/core_worker/reference_count.h"

namespace ray {

class ReferenceCountTest : public ::testing::Test {
 protected:
  std::unique_ptr<ReferenceCounter> rc;
  virtual void SetUp() { rc = std::unique_ptr<ReferenceCounter>(new ReferenceCounter); }

  virtual void TearDown() {}
};

TEST_F(ReferenceCountTest, TestBasic) {
  ObjectID id = ObjectID::FromRandom();
  rc->AddReference(id);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);
  rc->AddReference(id);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);
  rc->AddReference(id);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);
  rc->RemoveReference(id);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);
  rc->RemoveReference(id);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);
  rc->RemoveReference(id);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 0);
}

TEST_F(ReferenceCountTest, TestDependencies) {
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  ObjectID id3 = ObjectID::FromRandom();

  std::shared_ptr<std::vector<ObjectID>> deps = std::make_shared<std::vector<ObjectID>>();
  deps->push_back(id2);
  deps->push_back(id3);
  rc->SetDependencies(id1, deps);

  rc->AddReference(id1);
  rc->AddReference(id1);
  rc->AddReference(id3);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 3);

  rc->RemoveReference(id1);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 3);
  rc->RemoveReference(id1);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);

  rc->RemoveReference(id3);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 0);
}

TEST_F(ReferenceCountTest, TestSharedDependencies) {
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  ObjectID id3 = ObjectID::FromRandom();
  ObjectID id4 = ObjectID::FromRandom();

  std::shared_ptr<std::vector<ObjectID>> deps = std::make_shared<std::vector<ObjectID>>();
  deps->push_back(id3);
  deps->push_back(id4);
  rc->SetDependencies(id1, deps);
  rc->SetDependencies(id2, deps);

  rc->AddReference(id1);
  rc->AddReference(id2);
  rc->AddReference(id4);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 4);

  rc->RemoveReference(id1);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 3);
  rc->RemoveReference(id2);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);

  rc->RemoveReference(id4);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 0);
}

TEST_F(ReferenceCountTest, TestRecursiveDependencies) {
  ObjectID id1 = ObjectID::FromRandom();
  ObjectID id2 = ObjectID::FromRandom();
  ObjectID id3 = ObjectID::FromRandom();
  ObjectID id4 = ObjectID::FromRandom();

  std::shared_ptr<std::vector<ObjectID>> deps1 =
      std::make_shared<std::vector<ObjectID>>();
  deps1->push_back(id2);
  rc->SetDependencies(id1, deps1);

  std::shared_ptr<std::vector<ObjectID>> deps2 =
      std::make_shared<std::vector<ObjectID>>();
  deps2->push_back(id3);
  deps2->push_back(id4);
  rc->SetDependencies(id2, deps2);

  rc->AddReference(id1);
  rc->AddReference(id2);
  rc->AddReference(id4);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 4);

  rc->RemoveReference(id2);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 4);
  rc->RemoveReference(id1);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);

  rc->RemoveReference(id4);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
