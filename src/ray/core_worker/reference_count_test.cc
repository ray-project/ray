#include <vector>

#include "gtest/gtest.h"
#include "ray/core_worker/reference_count.h"

namespace ray {

class ReferenceCountTest : public ::testing::Test {
 protected:
  std::unique_ptr<ReferenceCounter> reference_counter;
  virtual void SetUp() {
    reference_counter = std::unique_ptr<ReferenceCounter>(new ReferenceCounter);
  }

  virtual void TearDown() {}
};

TEST_F(ReferenceCountTest, TestBasic) {
  ObjectID id = ObjectID::FromRandom();
  reference_counter.AddReference(id, 3);
  ASSERT_EQ(reference_counter.GetAllInScopeObjectIDs().size(), 1);
  reference_counter.RemoveReference(id);
  ASSERT_EQ(reference_counter.GetAllInScopeObjectIDs().size(), 1);
  reference_counter.RemoveReference(id);
  ASSERT_EQ(reference_counter.GetAllInScopeObjectIDs().size(), 1);
  reference_counter.RemoveReference(id);
  ASSERT_EQ(reference_counter.GetAllInScopeObjectIDs().size(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
