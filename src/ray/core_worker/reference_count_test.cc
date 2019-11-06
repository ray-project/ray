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

  std::shared_ptr<std::vector<ObjectID>> deps;
  deps->push_back(id2);
  deps->push_back(id3);

  rc->SetDependencies(id1, deps);
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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
