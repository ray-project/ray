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
  rc->AddReference(id, 3);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);
  rc->RemoveReference(id);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);
  rc->RemoveReference(id);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 1);
  rc->RemoveReference(id);
  ASSERT_EQ(rc->GetAllInScopeObjectIDs().size(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
